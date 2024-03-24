#pragma once
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

#include <ydb/library/accessor/accessor.h>

#include <util/generic/hash.h>

namespace NKikimr::NOlap {
class TPortionInfo;
}

namespace NKikimr::NOlap::NActualizer {

class TTieringProcessContext;

class TAddExternalContext {
private:
    YDB_READONLY_DEF(TInstant, Now);
    YDB_ACCESSOR(bool, PortionExclusiveGuarantee, true);
    const THashMap<ui64, std::shared_ptr<TPortionInfo>>& Portions;
public:
    TAddExternalContext(const TInstant now, const THashMap<ui64, std::shared_ptr<TPortionInfo>>& portions)
        : Now(now)
        , Portions(portions)
    {

    }

    const THashMap<ui64, std::shared_ptr<TPortionInfo>>& GetPortions() const {
        return Portions;
    }
};

class TExternalTasksContext {
private:
    const THashMap<ui64, std::shared_ptr<TPortionInfo>>& Portions;
    const THashSet<ui64>& PortionsToCompact;
public:
    const THashSet<ui64>& GetPortionsToCompact() const {
        return PortionsToCompact;
    }

    const THashMap<ui64, std::shared_ptr<TPortionInfo>>& GetPortions() const {
        return Portions;
    }

    const std::shared_ptr<TPortionInfo>& GetPortionVerified(const ui64 portionId) const {
        auto it = Portions.find(portionId);
        AFL_VERIFY(it != Portions.end());
        return it->second;
    }

    TExternalTasksContext(const THashMap<ui64, std::shared_ptr<TPortionInfo>>& portions, const THashSet<ui64>& portionsToCompact)
        : Portions(portions)
        , PortionsToCompact(portionsToCompact)
    {

    }
};

class TInternalTasksContext {
public:
};

}