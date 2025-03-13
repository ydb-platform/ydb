#pragma once

#include "portions.h"
#include <ydb/core/tx/columnshard/common/portion.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NColumnShard {

class TPortionIndexStats {
public:
    class TPortionClass {
    private:
        YDB_READONLY_DEF(NOlap::NPortion::EProduced, Produced);

    public:
        TPortionClass(const NOlap::TPortionInfo& portion);

        operator size_t() const {
            return (ui64)Produced;
        }
    };

    class IStatsSelector {
    public:
        ~IStatsSelector() = default;

    public:
        virtual bool Select(const TPortionClass& portionClass) const = 0;
    };

private:
    using TStatsByClass = THashMap<TPortionClass, NOlap::TSimplePortionsGroupInfo>;
    TStatsByClass TotalStats;
    THashMap<TInternalPathId, TStatsByClass> StatsByPathId;

    static NOlap::TSimplePortionsGroupInfo SelectStats(const TStatsByClass& container, const IStatsSelector& selector) {
        NOlap::TSimplePortionsGroupInfo result;
        for (const auto& [portionClass, stats] : container) {
            if (selector.Select(portionClass)) {
                result += stats;
            }
        }
        return result;
    }

public:
    void AddPortion(const NOlap::TPortionInfo& portion);
    void RemovePortion(const NOlap::TPortionInfo& portion);

    NOlap::TSimplePortionsGroupInfo GetTotalStats(const IStatsSelector& selector) const {
        return SelectStats(TotalStats, selector);
    }

    NOlap::TSimplePortionsGroupInfo GetTableStats(const NColumnShard::TInternalPathId pathId, const IStatsSelector& selector) const {
        if (auto* findTable = StatsByPathId.FindPtr(pathId)) {
            return SelectStats(*findTable, selector);
        }
        return {};
    }

    ui64 GetTablesCount() const {
        return StatsByPathId.size();
    }

public:
    class TActivePortions : public IStatsSelector {
    public:
        bool Select(const TPortionClass& portionClass) const override {
            return portionClass.GetProduced() == NOlap::NPortion::EProduced::INSERTED ||
                   portionClass.GetProduced() == NOlap::NPortion::EProduced::COMPACTED ||
                   portionClass.GetProduced() == NOlap::NPortion::EProduced::SPLIT_COMPACTED;
        }
    };

    template <NOlap::NPortion::EProduced Type>
    class TPortionsByType : public IStatsSelector {
    public:
        bool Select(const TPortionClass& portionClass) const override {
            return portionClass.GetProduced() == Type;
        }
    };
};

}