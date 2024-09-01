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
    virtual std::optional<TString> DoIsLocked(const TPortionInfo& portion, const TLockFilter&) const override {
        if (PathIds.contains(portion.GetPathId()) && portion.RecordSnapshotMin() <= SnapshotBarrier) {
            return GetLockName();
        }
        return {};
    }
    virtual bool DoIsEmpty() const override {
        return PathIds.empty();
    }
    virtual std::optional<TString> DoIsLocked(const TGranuleMeta& granule, const TLockFilter& filter) const override {
        return DoIsLocked(granule.GetPathId(), filter);
    }
    virtual std::optional<TString> DoIsLocked(const ui64 pathId, const TLockFilter&) const override {
        if (PathIds.contains(pathId)) {
            return GetLockName();
        }
        return {};
    }
public:
    TSnapshotLock(const TString& lockName, const TSnapshot& snapshotBarrier, const THashSet<ui64>& pathIds, const ELockCategory category)
        : TBase(lockName, category)
        , SnapshotBarrier(snapshotBarrier)
        , PathIds(pathIds)
    {
        AFL_VERIFY(SnapshotBarrier.Valid());
    }
};

}