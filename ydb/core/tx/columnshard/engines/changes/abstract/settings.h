#pragma once
#include <ydb/core/tx/columnshard/splitter/settings.h>
#include <util/datetime/base.h>
#include <util/system/types.h>
#include <utility>
namespace NKikimr::NOlap {

struct TCompactionLimits {
    static constexpr const ui64 MIN_GOOD_BLOB_SIZE = 256 * 1024; // some BlobStorage constant
    static constexpr const ui64 MAX_BLOB_SIZE = 8 * 1024 * 1024; // some BlobStorage constant
    static constexpr const ui64 EVICT_HOT_PORTION_BYTES = 1 * 1024 * 1024;
    static constexpr const ui64 DEFAULT_EVICTION_BYTES = 64 * 1024 * 1024;
    static constexpr const ui64 MAX_BLOBS_TO_DELETE = 10000;

    static constexpr const ui64 OVERLOAD_INSERT_TABLE_SIZE_BY_PATH_ID = (ui64)1 << 30;
    static constexpr const ui64 WARNING_INSERT_TABLE_SIZE_BY_PATH_ID = 0.5 * OVERLOAD_INSERT_TABLE_SIZE_BY_PATH_ID;
    static constexpr const ui64 WARNING_INSERT_TABLE_COUNT_BY_PATH_ID = 100;

    static constexpr const i64 OVERLOAD_GRANULE_SIZE = 20 * MAX_BLOB_SIZE;
    static constexpr const i64 WARNING_OVERLOAD_GRANULE_SIZE = 0.25 * OVERLOAD_GRANULE_SIZE;

    static constexpr const i64 WARNING_INSERTED_PORTIONS_SIZE = 0.5 * WARNING_OVERLOAD_GRANULE_SIZE;
    static constexpr const ui32 WARNING_INSERTED_PORTIONS_COUNT = 100;
    static constexpr const TDuration CompactionTimeout = TDuration::Minutes(3);

    ui32 GoodBlobSize{MIN_GOOD_BLOB_SIZE};
    ui32 GranuleBlobSplitSize{MAX_BLOB_SIZE};

    ui32 InGranuleCompactSeconds = 2 * 60; // Trigger in-granule compaction to guarantee no PK intersections

    i64 GranuleOverloadSize = OVERLOAD_GRANULE_SIZE;
    i64 GranuleSizeForOverloadPrevent = WARNING_OVERLOAD_GRANULE_SIZE;
    i64 GranuleIndexedPortionsSizeLimit = WARNING_INSERTED_PORTIONS_SIZE;
    ui32 GranuleIndexedPortionsCountLimit = WARNING_INSERTED_PORTIONS_COUNT;
};

}
