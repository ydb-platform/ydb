#pragma once
#include "abstract.h"
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/storage/granule.h>

namespace NKikimr::NOlap::NDataLocks {

class TListPortionsLock: public ILock {
private:
    using TBase = ILock;
    THashSet<TPortionAddress> Portions;
    THashSet<TTabletId> Granules;
protected:
    virtual std::optional<TString> DoIsLocked(const TPortionInfo& portion) const override {
        if (Portions.contains(portion.GetAddress())) {
            return GetLockName();
        }
        return {};
    }
    virtual std::optional<TString> DoIsLocked(const TGranuleMeta& granule) const override {
        if (Granules.contains((TTabletId)granule.GetPathId())) {
            return GetLockName();
        }
        return {};
    }
    bool DoIsEmpty() const override {
        return Portions.empty();
    }
public:
    TListPortionsLock(const TString& lockName, const std::vector<std::shared_ptr<TPortionInfo>>& portions, const bool readOnly = false)
        : TBase(lockName, readOnly)
    {
        for (auto&& p : portions) {
            Portions.emplace(p->GetAddress());
            Granules.emplace((TTabletId)p->GetPathId());
        }
    }

    TListPortionsLock(const TString& lockName, const std::vector<TPortionInfo>& portions, const bool readOnly = false)
        : TBase(lockName, readOnly) {
        for (auto&& p : portions) {
            Portions.emplace(p.GetAddress());
            Granules.emplace((TTabletId)p.GetPathId());
        }
    }

    template <class T, class TGetter>
    TListPortionsLock(const TString& lockName, const std::vector<T>& portions, const TGetter& g, const bool readOnly = false)
        : TBase(lockName, readOnly) {
        for (auto&& p : portions) {
            const auto address = g(p);
            Portions.emplace(address);
            Granules.emplace((TTabletId)address.GetPathId());
        }
    }

    template <class T>
    TListPortionsLock(const TString& lockName, const THashMap<TPortionAddress, T>& portions, const bool readOnly = false)
        : TBase(lockName, readOnly) {
        for (auto&& p : portions) {
            const auto address = p.first;
            Portions.emplace(address);
            Granules.emplace((TTabletId)address.GetPathId());
        }
    }
};

}