#pragma once
#include "abstract.h"
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/storage/granule.h>

namespace NKikimr::NOlap::NDataLocks {

class TSnapshotLock: public ILock {
private:
    using TBase = ILock;
    const TSnapshot SnapshotBarrier;
    const THashSet<TTabletId> PathIds;
protected:
    virtual bool DoIsLocked(const TPortionInfo& portion) const override {
        return PathIds.contains((TTabletId)portion.GetPathId()) && portion.RecordSnapshotMin() <= SnapshotBarrier;
    }
    virtual bool DoIsLocked(const TGranuleMeta& granule) const override {
        return PathIds.contains((TTabletId)granule.GetPathId());
    }
public:
    TSnapshotLock(const TSnapshot& snapshotBarrier, const THashSet<TTabletId>& pathIds, const bool readOnly = false)
        : TBase(readOnly)
        , SnapshotBarrier(snapshotBarrier)
        , PathIds(pathIds)
    {

    }
};

}