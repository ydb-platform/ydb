#pragma once
#include "abstract.h"
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/storage/granule.h>

namespace NKikimr::NOlap::NDataLocks {

class TListPortionsLock: public ILock {
private:
    using TBase = ILock;
    THashSet<TPortionAddress> Portions;
    THashSet<ui64> Granules;
protected:
    virtual std::optional<TString> IsLocked(const TPortionInfo& portion, const TLockScope& scope) const override {
        Y_UNUSED(scope);
        if (Portions.contains(portion.GetAddress())) {
            return GetLockName();
        }
        return {};
    }
    virtual std::optional<TString> IsLocked(const TGranuleMeta& granule, const TLockScope& scope) const override {
        Y_UNUSED(scope);
        if (Granules.contains(granule.GetPathId())) {
            return GetLockName();
        }
        return {};
    }
    virtual std::optional<TString> IsLockedTableSchema(const ui64 pathId, const TLockScope& scope) const override {
        Y_UNUSED(pathId);
        Y_UNUSED(scope);
        return {};
    }
    bool IsEmpty() const override {
        return Portions.empty();
    }
public:
    TListPortionsLock(const TString& lockName, const std::vector<std::shared_ptr<TPortionInfo>>& portions)
        : TBase(lockName)
    {
        for (auto&& p : portions) {
            Portions.emplace(p->GetAddress());
            Granules.emplace(p->GetPathId());
        }
    }

    TListPortionsLock(const TString& lockName, const std::vector<TPortionInfo>& portions)
        : TBase(lockName) {
        for (auto&& p : portions) {
            Portions.emplace(p.GetAddress());
            Granules.emplace(p.GetPathId());
        }
    }

    template <class T, class TGetter>
    TListPortionsLock(const TString& lockName, const std::vector<T>& portions, const TGetter& g)
        : TBase(lockName) {
        for (auto&& p : portions) {
            const auto address = g(p);
            Portions.emplace(address);
            Granules.emplace(address.GetPathId());
        }
    }

    template <class T>
    TListPortionsLock(const TString& lockName, const THashMap<TPortionAddress, T>& portions)
        : TBase(lockName) {
        for (auto&& p : portions) {
            const auto address = p.first;
            Portions.emplace(address);
            Granules.emplace(address.GetPathId());
        }
    }
};

class TListTablesLock: public ILock {
private:
    using TBase = ILock;
    THashSet<ui64> Tables;
protected:
    virtual std::optional<TString> IsLocked(const TPortionInfo& portion, const TLockScope&) const override {
        if (Tables.contains(portion.GetPathId())) {
            return GetLockName();
        }
        return {};
    }
    virtual std::optional<TString> IsLocked(const TGranuleMeta& granule, const TLockScope&) const override {
        if (Tables.contains(granule.GetPathId())) {
            return GetLockName();
        }
        return {};
    }
    virtual std::optional<TString> IsLockedTableSchema(const ui64 pathId, const TLockScope& scope) const override {
        Y_UNUSED(pathId);
        Y_UNUSED(scope);
        return {};
    }
    bool IsEmpty() const override {
        return Tables.empty();
    }
public:
    TListTablesLock(const TString& lockName, const THashSet<ui64>& tables)
        : TBase(lockName)
        , Tables(tables)
    {
    }
};

}