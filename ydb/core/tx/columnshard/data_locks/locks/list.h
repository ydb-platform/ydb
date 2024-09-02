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
    virtual std::optional<TString> DoIsLocked(const TPortionInfo& portion, const TLockFilter&) const override {
        if (Portions.contains(portion.GetAddress())) {
            return GetLockName();
        }
        return {};
    }
    virtual std::optional<TString> DoIsLocked(const TGranuleMeta& granule, const TLockFilter&) const override {
        if (Granules.contains(granule.GetPathId())) {
            return GetLockName();
        }
        return {};
    }
    virtual std::optional<TString> DoIsLocked(const ui64 /*pathId*/, const TLockFilter&) const override {
        return {};
    }
    bool DoIsEmpty() const override {
        return Portions.empty();
    }
public:
    TListPortionsLock(const TString& lockName, const std::vector<std::shared_ptr<TPortionInfo>>& portions)
        : TBase(lockName, ELockCategory::Generic)
    {
        for (auto&& p : portions) {
            Portions.emplace(p->GetAddress());
            Granules.emplace(p->GetPathId());
        }
    }

    TListPortionsLock(const TString& lockName, const std::vector<TPortionInfo>& portions)
        : TBase(lockName, ELockCategory::Generic) {
        for (auto&& p : portions) {
            Portions.emplace(p.GetAddress());
            Granules.emplace(p.GetPathId());
        }
    }

    template <class T, class TGetter>
    TListPortionsLock(const TString& lockName, const std::vector<T>& portions, const TGetter& g)
        : TBase(lockName, ELockCategory::Generic) {
        for (auto&& p : portions) {
            const auto address = g(p);
            Portions.emplace(address);
            Granules.emplace(address.GetPathId());
        }
    }

    template <class T>
    TListPortionsLock(const TString& lockName, const THashMap<TPortionAddress, T>& portions)
        : TBase(lockName, ELockCategory::Generic) {
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
    virtual std::optional<TString> DoIsLocked(const TPortionInfo& portion, const TLockFilter& filter) const override {
        return DoIsLocked(portion.GetPathId(), filter);
    }
    virtual std::optional<TString> DoIsLocked(const TGranuleMeta& granule, const TLockFilter& filter) const override {
        return DoIsLocked(granule.GetPathId(), filter);
    }
    virtual std::optional<TString> DoIsLocked(const ui64 pathId, const TLockFilter&) const override {
        if (Tables.contains(pathId)) {
            return GetLockName();
        }
        return {};
    }
    bool DoIsEmpty() const override {
        return Tables.empty();
    }
public:
    TListTablesLock(const TString& lockName, const THashSet<ui64>& tables, const ELockCategory category)
        : TBase(lockName, category)
        , Tables(tables)
    {
    }
};

}