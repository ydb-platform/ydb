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
    virtual std::optional<TString> DoIsLocked(const TPortionInfo& portion) const override {
        if (PathIds.contains((TTabletId)portion.GetPathId()) && portion.RecordSnapshotMin() <= SnapshotBarrier) {
            return GetLockName();
        }
        return {};
    }
    virtual std::optional<TString> DoIsLocked(const TGranuleMeta& granule) const override {
        if (PathIds.contains((TTabletId)granule.GetPathId())) {
            return GetLockName();
        }
        return {};
    }
public:
    TSnapshotLock(const TString& lockName, const TSnapshot& snapshotBarrier, const THashSet<TTabletId>& pathIds, const bool readOnly = false)
        : TBase(lockName, readOnly)
        , SnapshotBarrier(snapshotBarrier)
        , PathIds(pathIds)
    {

    }
};

}