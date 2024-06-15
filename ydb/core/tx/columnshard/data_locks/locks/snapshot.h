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
    virtual std::optional<TString> DoIsLocked(const TPortionInfo& portion, const THashSet<TString>& /*excludedLocks*/) const override {
        if (PathIds.contains(portion.GetPathId()) && portion.RecordSnapshotMin() <= SnapshotBarrier) {
            return GetLockName();
        }
        return {};
    }
    virtual bool DoIsEmpty() const override {
        return PathIds.empty();
    }
    virtual std::optional<TString> DoIsLocked(const TGranuleMeta& granule, const THashSet<TString>& /*excludedLocks*/) const override {
        if (PathIds.contains(granule.GetPathId())) {
            return GetLockName();
        }
        return {};
    }
public:
    TSnapshotLock(const TString& lockName, const TSnapshot& snapshotBarrier, const THashSet<ui64>& pathIds, const bool readOnly = false)
        : TBase(lockName, readOnly)
        , SnapshotBarrier(snapshotBarrier)
        , PathIds(pathIds)
    {
        AFL_VERIFY(SnapshotBarrier.Valid());
    }
};

}