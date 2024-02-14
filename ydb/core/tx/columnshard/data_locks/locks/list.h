#pragma once
#include "abstract.h"
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/storage/granule.h>

namespace NKikimr::NOlap::NDataLocks {

class TListPortionsLock: public ILock {
private:
    THashSet<TPortionAddress> Portions;
    THashSet<TTabletId> Granules;
protected:
    virtual bool DoIsLocked(const TPortionInfo& portion) const override {
        return Portions.contains(portion.GetAddress());
    }
    virtual bool DoIsLocked(const TGranuleMeta& granule) const override {
        return Granules.contains((TTabletId)granule.GetPathId());
    }
    bool DoIsEmpty() const override {
        return Portions.empty();
    }
public:
    TListPortionsLock(const std::vector<std::shared_ptr<TPortionInfo>>& portions) {
        for (auto&& p : portions) {
            Portions.emplace(p->GetAddress());
            Granules.emplace((TTabletId)p->GetPathId());
        }
    }

    TListPortionsLock(const std::vector<TPortionInfo>& portions) {
        for (auto&& p : portions) {
            Portions.emplace(p.GetAddress());
            Granules.emplace((TTabletId)p.GetPathId());
        }
    }

    template <class T, class TGetter>
    TListPortionsLock(const std::vector<T>& portions, const TGetter& g) {
        for (auto&& p : portions) {
            const auto address = g(p);
            Portions.emplace(address);
            Granules.emplace((TTabletId)address.GetPathId());
        }
    }

    template <class T>
    TListPortionsLock(const THashMap<TPortionAddress, T>& portions) {
        for (auto&& p : portions) {
            const auto address = p.first;
            Portions.emplace(address);
            Granules.emplace((TTabletId)address.GetPathId());
        }
    }
};

}