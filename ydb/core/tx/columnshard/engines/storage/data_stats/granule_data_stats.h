#pragma once

#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/counters/portions.h>

namespace NKikimr::NOlap {
class TPortionInfo;
}

namespace NKikimr::NColumnShard {

// The portions of a granule or of the whole shard; a group without Evicted is in the default storage
class TGranuleDataStats {
private:
    NOlap::TSimplePortionsGroupInfo Written;
    NOlap::TSimplePortionsGroupInfo Compacted;
    NOlap::TSimplePortionsGroupInfo EvictedWritten;
    NOlap::TSimplePortionsGroupInfo EvictedCompacted;
    // waiting for cleanup
    NOlap::TSimplePortionsGroupInfo Removed;
    NOlap::TSimplePortionsGroupInfo RemovedEvicted;

    // signed: the small-blob threshold may change between a portion's add and its remove, reads clamp at zero
    i64 SmallBlobsBytes = 0;
    i64 SmallBlobsCount = 0;

    NOlap::TSimplePortionsGroupInfo& GetGroup(const NOlap::TPortionInfo& portion);

public:
    void AddPortion(const NOlap::TPortionInfo& portion, const ui64 smallBlobThresholdBytes);
    void RemovePortion(const NOlap::TPortionInfo& portion, const ui64 smallBlobThresholdBytes);
    bool IsEmpty() const;

    const NOlap::TSimplePortionsGroupInfo& GetWritten() const {
        return Written;
    }

    const NOlap::TSimplePortionsGroupInfo& GetCompacted() const {
        return Compacted;
    }

    // everything in the default storage: the table's data as schemeshard sees it
    NOlap::TSimplePortionsGroupInfo GetDefaultStorage() const {
        return Written + Compacted + Removed;
    }

    NOlap::TSimplePortionsGroupInfo GetEvicted() const {
        return EvictedWritten + EvictedCompacted;
    }

    NOlap::TSimplePortionsGroupInfo GetRemoved() const {
        return Removed + RemovedEvicted;
    }

    // in any storage
    NOlap::TSimplePortionsGroupInfo GetActiveWritten() const {
        return Written + EvictedWritten;
    }

    // in any storage
    NOlap::TSimplePortionsGroupInfo GetActiveCompacted() const {
        return Compacted + EvictedCompacted;
    }

    NOlap::TSmallBlobsStat GetSmallBlobs() const {
        return { .VolumeBytes = (ui64)std::max<i64>(0, SmallBlobsBytes), .Count = (ui64)std::max<i64>(0, SmallBlobsCount) };
    }
};

}   // namespace NKikimr::NColumnShard
