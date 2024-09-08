#pragma once
#include "abstract.h"
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/storage/granule.h>

namespace NKikimr::NOlap::NDataLocks {

class TSnapshotLock: public ILock {
private:
    using TBase = ILock;
    const TSnapshot SnapshotBarrier;
    const THashSet<ui64> PathIds;
protected:
    virtual std::optional<TString> IsLocked(const TPortionInfo& portion, const TLockScope& scope) const override {
        Y_UNUSED(scope);
        if (PathIds.contains(portion.GetPathId()) && portion.RecordSnapshotMin() <= SnapshotBarrier) {
            return GetLockName();
        }
        return {};
    }
    virtual bool IsEmpty() const override {
        return PathIds.empty();
    }
    virtual std::optional<TString> IsLocked(const TGranuleMeta& granule, const TLockScope& scope) const override {
        Y_UNUSED(scope);
        if (PathIds.contains(granule.GetPathId())) {
            return GetLockName();
        }
        return {};
    }
    virtual std::optional<TString> IsLockedTableSchema(const ui64 pathId, const TLockScope& scope) const override {
        Y_UNUSED(pathId);
        Y_UNUSED(scope);
        return {};
    }
public:
    TSnapshotLock(const TString& lockName, const TSnapshot& snapshotBarrier, const THashSet<ui64>& pathIds)
        : TBase(lockName)
        , SnapshotBarrier(snapshotBarrier)
        , PathIds(pathIds)
    {
        AFL_VERIFY(SnapshotBarrier.Valid());
    }
};

}