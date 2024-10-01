#pragma once
#include "defs.h"
#include <ydb/core/tablet/tablet_counters.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

// don't change order!
#define FLAT_EXECUTOR_SIMPLE_COUNTERS_MAP(XX) \
    XX(DB_TX_IN_FLY, "ExecutorTxInFly") \
    XX(LOG_REDO_COUNT, "LogRedoItems") \
    XX(LOG_REDO_MEMORY, "LogRedoMemory") \
    XX(LOG_REDO_SOLIDS, "LogRedoLargeGlobIds") \
    XX(LOG_ALTER_BYTES, "LogAlterBytes") \
    XX(LOG_SNAP_BYTES, "LogSnapBytes") \
    XX(LOG_RIVER_LEVEL, "LogRiverLevel") \
    XX(DB_WARM_OPS, "DbMemTableOps") \
    XX(DB_WARM_BYTES, "DbMemTableBytes") \
    XX(DB_ROWS_ERASE, "DbRowsErase") \
    XX(DB_ROWS_TOTAL, "DbRowsTotal") \
    XX(DB_META_BYTES, "DbMetaBytes") \
    XX(DB_INDEX_BYTES, "DbIndexBytes") \
    XX(DB_OTHER_BYTES, "DbOtherBytes") \
    XX(DB_BYKEY_BYTES, "DbByKeyBytes") \
    XX(DB_PLAIN_BYTES, "DbPlainBytes") \
    XX(DB_CODED_BYTES, "DbCodedBytes") \
    XX(DB_ELOBS_BYTES, "DbELargeObjsBytes") \
    XX(DB_ELOBS_ITEMS, "DbELargeObjsItems") \
    XX(DB_OUTER_BYTES, "DbOuterBytes") \
    XX(DB_OUTER_ITEMS, "DbOuterItems") \
    XX(DB_DATA_BYTES, "DbDataBytes") \
    XX(DB_PARTS_COUNT, "DbPartsCount") \
    XX(DB_UNIQUE_DATA_BYTES, "DbUniqueDataBytes") \
    XX(DB_UNIQUE_PARTS_COUNT, "DbUniquePartsCount") \
    XX(DB_UNIQUE_ROWS_ERASE, "DbUniqueRowsErase") \
    XX(DB_UNIQUE_ROWS_TOTAL, "DbUniqueRowsTotal") \
    XX(DB_UNIQUE_PLAIN_BYTES, "DbUniquePlainBytes") \
    XX(DB_UNIQUE_CODED_BYTES, "DbUniqueCodedBytes") \
    XX(DB_UNIQUE_ELOBS_BYTES, "DbUniqueELargeObjsBytes") \
    XX(DB_UNIQUE_ELOBS_ITEMS, "DbUniqueELargeObjsItems") \
    XX(DB_UNIQUE_OUTER_BYTES, "DbUniqueOuterBytes") \
    XX(DB_UNIQUE_OUTER_ITEMS, "DbUniqueOuterItems") \
    XX(DB_UNIQUE_KEEP_BYTES, "DbUniqueKeepBytes") \
    XX(GC_BLOBS_UNCOMMITTED, "GcBlobsUncommitted") \
    XX(GC_BLOBS_CREATED, "GcBlobsCreated") \
    XX(GC_BLOBS_DELETED, "GcBlobsDeleted") \
    XX(GC_BARRIERS_ACTIVE, "GcBarriersActive") \
    XX(CACHE_FRESH_SIZE, "CacheFreshSize") \
    XX(CACHE_STAGING_SIZE, "CacheStagingSize") \
    XX(CACHE_WARM_SIZE, "CacheWarmSize") \
    XX(CACHE_PINNED_SET, "CachePinned") \
    XX(CACHE_PINNED_LOAD, "CachePinnedLoad") \
    XX(CACHE_TOTAL_COLLECTIONS, "CacheTotalCollections") \
    XX(CACHE_TOTAL_SHARED_BODY, "CacheTotalSharedBody") \
    XX(CACHE_TOTAL_PINNED_BODY, "CacheTotalPinnedBody") \
    XX(CACHE_TOTAL_EXCLUSIVE, "CacheTotalExclusive") \
    XX(CACHE_TOTAL_SHARED_PENDING, "CacheTotalSharedPending") \
    XX(CACHE_TOTAL_STICKY, "CacheTotalSticky") \
    XX(USED_TABLET_MEMORY, "UsedTabletMemory") \
    XX(USED_TABLET_TX_MEMORY, "UsedTabletTxMemory") \
    XX(USED_DYNAMIC_TX_MEMORY, "UsedDynamicTxMemory") \
    XX(CONSUMED_STORAGE, "ConsumedStorage") \
    XX(CONSUMED_MEMORY, "ConsumedMemory") \
    XX(COMPACTION_READ_IN_FLY, "CompactionReadInFly") \
    XX(DB_FLAT_INDEX_BYTES, "DbFlatIndexBytes") \
    XX(DB_B_TREE_INDEX_BYTES, "DbBTreeIndexBytes") \

// don't change order!
#define FLAT_EXECUTOR_CUMULATIVE_COUNTERS_MAP(XX) \
    XX(LOG_COMMITS, "LogCommits") \
    XX(LOG_WRITTEN, "LogWritten") \
    XX(LOG_EMBEDDED, "LogEmbedded" ) \
    XX(LOG_REDO_WRITTEN, "LogRedoWritten") \
    XX(TABLET_BYTES_WRITTEN, "TabletBytesWritten") \
    XX(TABLET_BLOBS_WRITTEN, "TabletBlobsWritten") \
    XX(TABLET_BYTES_READ, "TabletBytesRead") \
    XX(TABLET_BLOBS_READ, "TabletBlobsRead") \
    XX(COMP_BYTES_WRITTEN, "CompactionBytesWritten") \
    XX(COMP_BLOBS_WRITTEN, "CompactionBlobsWritten") \
    XX(COMP_BYTES_READ, "CompactionBytesRead") \
    XX(COMP_BLOBS_READ, "CompactionBlobsRead") \
    XX(DB_ANNEX_ITEMS_GROW, "DbAnnexItemsGrow") \
    XX(DB_ELOBS_ITEMS_GROW, "DbELargeObjsItemsGrow") \
    XX(DB_ELOBS_ITEMS_GONE, "DbELargeObjsItemsGone") \
    XX(DB_REDO_WRITTEN_BYTES, "DbRedoWrittenBytes") \
    XX(DB_ANNEX_WRITTEN_BYTES, "DbAnnexWrittenBytes") \
    XX(TX_COUNT_ALL, "Tx(all)") \
    XX(TX_QUEUED, "TxQueued") \
    XX(TX_RETRIED, "TxRetried") \
    XX(TX_FINISHED, "TxFinished") \
    XX(TX_POSTPONED, "TxPostponed") \
    XX(TX_MEM_REQUESTS, "TxMemoryRequests") \
    XX(TX_MEM_CAPTURES, "TxMemoryCaptures") \
    XX(TX_MEM_ATTACHES, "TxMemoryAttaches") \
    XX(TX_DATA_RELEASES, "TxDataReleases") \
    XX(TX_RO_COMPLETED, "Tx(ro complete)") \
    XX(TX_RW_COMPLETED, "Tx(rw complete)") \
    XX(TX_TERMINATED, "Tx(terminated)") \
    XX(TX_CHARGE_WEEDED, "TxKeyChargeWeeded") \
    XX(TX_CHARGE_SIEVED, "TxKeyChargeSieved") \
    XX(TX_SELECT_WEEDED, "TxKeySelectWeeded") \
    XX(TX_SELECT_SIEVED, "TxKeySelectSieved") \
    XX(TX_SELECT_NO_KEY, "TxKeySelectNoKey") \
    XX(TX_BYTES_READ, "TxReadBytes") \
    XX(TX_CACHE_HITS, "TxPageCacheHits") \
    XX(TX_CACHE_MISSES, "TxPageCacheMisses") \
    XX(GC_DISCOVERED, "GcBlobsDiscovered") \
    XX(GC_DISCARDED, "GcBlobsDiscarded") \
    XX(GC_FORGOTTEN, "GcBlobsForgotten") \
    XX(GC_KEEPSET, "GcKeepFlagsSet") \
    XX(GC_NOTKEEPSET, "GcNotKeepFlagsSet") \
    XX(CONSUMED_CPU, "ConsumedCPU") \
    XX(COMPACTION_READ_POSTPONED, "CompactionReadPostponed") \
    XX(COMPACTION_READ_CACHE_HITS, "CompactionReadCacheHits") \
    XX(COMPACTION_READ_CACHE_MISSES, "CompactionReadCacheMisses") \
    XX(COMPACTION_READ_LOAD_BYTES, "CompactionReadLoadBytes") \
    XX(COMPACTION_READ_LOAD_PAGES, "CompactionReadLoadPages") \
    XX(TX_BYTES_CACHED, "TxCachedBytes") \
    XX(TX_BYTES_WASTED, "TxWastedBytes") \

// don't change order!
#define FLAT_EXECUTOR_PERCENTILE_COUNTERS_MAP(XX) \
    XX(TX_PERCENTILE_LATENCY_RO, "TxRoLatency") \
    XX(TX_PERCENTILE_LATENCY_RW, "TxRwLatency") \
    XX(TX_PERCENTILE_LATENCY_COMMIT, "TxCommitLatency") \
    XX(TX_PERCENTILE_EXECUTE_CPUTIME, "TxExecuteCPUTime") \
    XX(TX_PERCENTILE_BOOKKEEPING_CPUTIME, "TxBookkeepingCPUTime") \
    XX(TX_PERCENTILE_COMMITED_CPUTIME, "TxCommitedCPUTime") \
    XX(TX_PERCENTILE_LOGSNAP_CPUTIME, "LogSnapCPUTime") \
    XX(TX_PERCENTILE_PARTSWITCH_CPUTIME, "PartSwitchCPUTime") \
    XX(TX_PERCENTILE_TOUCHED_BLOCKS, "TouchedBlocks") \
    XX(TX_PERCENTILE_DB_DATA_BYTES, "HIST(DbDataBytes)") \
    XX(TX_PERCENTILE_TABLET_BYTES_WRITTEN, "HIST(TabletBytesWritten)") \
    XX(TX_PERCENTILE_TABLET_BYTES_READ, "HIST(TabletBytesRead)") \
    XX(TX_PERCENTILE_CONSUMED_CPU, "HIST(ConsumedCPU)") \
    XX(TX_PERCENTILE_FOLLOWERSYNC_LATENCY, "FollowerSyncLatency") \
    XX(TX_PERCENTILE_COMMIT_REDO_BYTES, "TxCommitRedoBytes")

class TExecutorCounters : public TTabletCountersBase {
public:
    enum ESimpleCounter {
        FLAT_EXECUTOR_SIMPLE_COUNTERS_MAP(ENUM_VALUE_GEN_NO_VALUE)
        SIMPLE_COUNTER_SIZE
    };
    static const char* SimpleCounterNames[SIMPLE_COUNTER_SIZE];

    enum ECumulativeCounter {
        FLAT_EXECUTOR_CUMULATIVE_COUNTERS_MAP(ENUM_VALUE_GEN_NO_VALUE)
        CUMULATIVE_COUNTER_SIZE
    };
    static const char* CumulativeCounterNames[CUMULATIVE_COUNTER_SIZE];

    enum EPercentileCounter {
        FLAT_EXECUTOR_PERCENTILE_COUNTERS_MAP(ENUM_VALUE_GEN_NO_VALUE)
        PERCENTILE_COUNTER_SIZE
    };
    static const char* PercentileCounterNames[PERCENTILE_COUNTER_SIZE];

    TExecutorCounters();
};

}}
