/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.table.query;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.mergetree.lookup.LookupSerializerFactory;
import org.apache.paimon.mergetree.lookup.PersistValueProcessor;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import static org.apache.paimon.lookup.LookupStoreFactory.bfGenerator;
import static org.apache.paimon.mergetree.LookupFile.localFilePrefix;

/**
 * Implementation for {@link TableQuery} for caching data and file in local.
 *
 * <p>Thread-safety: {@link #lookup} may be called from many threads concurrently (e.g. by {@code
 * KvQueryServer} worker threads), while {@link #refreshFiles} is typically called from a single
 * task thread. Concurrency is handled at bucket granularity: lookups against different (partition,
 * bucket) pairs run in parallel; per-bucket access (lookup vs refresh, and lookup vs lookup) is
 * serialized on the {@link LookupLevels} instance, because {@link LookupLevels} mutates internal
 * state (file cache bookkeeping, level updates) that is not thread-safe.
 */
public class LocalTableQuery implements TableQuery {

    private final ConcurrentMap<BinaryRow, ConcurrentMap<Integer, LookupLevels<KeyValue>>>
            tableView;

    private final CoreOptions options;

    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;

    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;

    // Shared across all buckets: CacheManager owns the memory budget. The
    // LookupStoreFactory itself (which wraps a stateful SliceComparator) is built fresh per
    // bucket inside newLookupLevels, mirroring MergeTreeCompactManagerFactory#createLookupLevels.
    private final CacheManager cacheManager;

    private final int startLevel;

    private IOManager ioManager;

    @Nullable private Cache<String, LookupFile> lookupFileCache;

    private final RowType rowType;
    private final RowType partitionType;

    @Nullable private Filter<InternalRow> cacheRowFilter;

    public LocalTableQuery(FileStoreTable table) {
        this.options = table.coreOptions();
        this.tableView = new ConcurrentHashMap<>();
        FileStore<?> tableStore = table.store();
        if (!(tableStore instanceof KeyValueFileStore)) {
            throw new UnsupportedOperationException(
                    "Table Query only supports table with primary key.");
        }
        KeyValueFileStore store = (KeyValueFileStore) tableStore;

        this.readerFactoryBuilder = store.newReaderFactoryBuilder();
        this.rowType = table.schema().logicalRowType();
        this.partitionType = table.schema().logicalPartitionType();
        this.keyComparatorSupplier = new KeyComparatorSupplier(readerFactoryBuilder.keyType());
        this.cacheManager =
                new CacheManager(
                        options.lookupCacheMaxMemory(), options.lookupCacheHighPrioPoolRatio());
        startLevel = options.needLookup() ? 1 : 0;
    }

    public void refreshFiles(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> beforeFiles,
            List<DataFileMeta> dataFiles) {
        ConcurrentMap<Integer, LookupLevels<KeyValue>> buckets =
                tableView.computeIfAbsent(partition, k -> new ConcurrentHashMap<>());
        LookupLevels<KeyValue> lookupLevels = buckets.get(bucket);
        if (lookupLevels == null) {
            // Initial phase: ignore beforeFiles as they represent deletions from previous state.
            // computeIfAbsent guarantees we build LookupLevels at most once per (partition,
            // bucket), even if two refreshers race here.
            buckets.computeIfAbsent(bucket, b -> newLookupLevels(partition, b, dataFiles));
        } else {
            // Lock the per-bucket LookupLevels: Levels.update mutates level0/levels in place and
            // would corrupt concurrent lookups on the same bucket.
            synchronized (lookupLevels) {
                lookupLevels.getLevels().update(beforeFiles, dataFiles);
            }
        }
    }

    private LookupLevels<KeyValue> newLookupLevels(
            BinaryRow partition, int bucket, List<DataFileMeta> dataFiles) {
        Levels levels = new Levels(keyComparatorSupplier.get(), dataFiles, options.numLevels());
        // TODO pass DeletionVector factory
        KeyValueFileReaderFactory factory =
                readerFactoryBuilder.build(partition, bucket, DeletionVector.emptyFactory());
        Options options = this.options.toConfiguration();
        Cache<String, LookupFile> fileCache = getOrCreateLookupFileCache();
        RowType keyType = readerFactoryBuilder.keyType();
        // Per-bucket LookupStoreFactory: the underlying SliceComparator keeps reusable
        // RowReader state and is NOT thread-safe, so sharing one factory (and hence one
        // comparator) across buckets would let concurrent lookups on different buckets
        // corrupt each other's reads.
        LookupStoreFactory lookupStoreFactory =
                LookupStoreFactory.create(
                        this.options,
                        cacheManager,
                        new RowCompactedSerializer(keyType).createSliceComparator());

        RowType readValueType = readerFactoryBuilder.readValueType();
        return new LookupLevels<>(
                schemaId -> readValueType,
                0L,
                levels,
                keyComparatorSupplier.get(),
                keyType,
                PersistValueProcessor.factory(readValueType),
                LookupSerializerFactory.INSTANCE.get(),
                file -> {
                    RecordReader<KeyValue> reader = factory.createRecordReader(file);
                    if (cacheRowFilter != null) {
                        reader = reader.filter(keyValue -> cacheRowFilter.test(keyValue.value()));
                    }
                    return reader;
                },
                file ->
                        Preconditions.checkNotNull(ioManager, "IOManager is required.")
                                .createChannel(
                                        localFilePrefix(partitionType, partition, bucket, file))
                                .getPathFile(),
                lookupStoreFactory,
                bfGenerator(options),
                fileCache);
    }

    private synchronized Cache<String, LookupFile> getOrCreateLookupFileCache() {
        if (lookupFileCache == null) {
            Options conf = this.options.toConfiguration();
            lookupFileCache =
                    LookupFile.createCache(
                            conf.get(CoreOptions.LOOKUP_CACHE_FILE_RETENTION),
                            conf.get(CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE));
        }
        return lookupFileCache;
    }

    @Nullable
    @Override
    public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key) throws IOException {
        ConcurrentMap<Integer, LookupLevels<KeyValue>> buckets = tableView.get(partition);
        if (buckets == null || buckets.isEmpty()) {
            return null;
        }
        LookupLevels<KeyValue> lookupLevels = buckets.get(bucket);
        if (lookupLevels == null) {
            return null;
        }

        // Lock the per-bucket LookupLevels only. Lookups against different buckets (and different
        // partitions) run in parallel; lookups against the same bucket serialize because
        // LookupLevels mutates its own file-cache bookkeeping during a miss.
        KeyValue kv;
        synchronized (lookupLevels) {
            kv = lookupLevels.lookup(key, startLevel);
        }
        if (kv == null || kv.valueKind().isRetract()) {
            return null;
        } else {
            return kv.value();
        }
    }

    @Override
    public LocalTableQuery withValueProjection(int[] projection) {
        this.readerFactoryBuilder.withReadValueType(rowType.project(projection));
        return this;
    }

    public LocalTableQuery withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        return this;
    }

    public LocalTableQuery withCacheRowFilter(Filter<InternalRow> cacheRowFilter) {
        this.cacheRowFilter = cacheRowFilter;
        return this;
    }

    @Override
    public InternalRowSerializer createValueSerializer() {
        return InternalSerializers.create(readerFactoryBuilder.readValueType());
    }

    @Override
    public void close() throws IOException {
        for (Map.Entry<BinaryRow, ConcurrentMap<Integer, LookupLevels<KeyValue>>> buckets :
                tableView.entrySet()) {
            for (Map.Entry<Integer, LookupLevels<KeyValue>> bucket :
                    buckets.getValue().entrySet()) {
                // Drain in-flight lookups on this bucket before tearing it down.
                LookupLevels<KeyValue> lookupLevels = bucket.getValue();
                synchronized (lookupLevels) {
                    lookupLevels.close();
                }
            }
        }
        if (lookupFileCache != null) {
            lookupFileCache.invalidateAll();
        }
        tableView.clear();
    }
}
