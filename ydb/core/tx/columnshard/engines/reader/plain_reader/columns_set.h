#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap::NPlainReader {

class TColumnsSet {
private:
    YDB_READONLY_DEF(std::set<ui32>, ColumnIds);
    YDB_READONLY_DEF(std::set<TString>, ColumnNames);
    std::vector<TString> ColumnNamesVector;
    YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, Schema);

    void Rebuild() {
        ColumnNamesVector.clear();
        ColumnNames.clear();
        for (auto&& i : Schema->field_names()) {
            ColumnNamesVector.emplace_back(i);
            ColumnNames.emplace(i);
        }
    }

public:
    TColumnsSet() = default;

    const std::vector<TString>& GetColumnNamesVector() const {
        return ColumnNamesVector;
    }

    ui32 GetSize() const {
        return ColumnIds.size();
    }

    bool ColumnsOnly(const std::vector<std::string>& fieldNames) const;

    TColumnsSet(const std::set<ui32>& columnIds, const TIndexInfo& indexInfo)
        : ColumnIds(columnIds)
    {
        Schema = indexInfo.GetColumnsSchema(ColumnIds);
        Rebuild();
    }

    TColumnsSet(const std::vector<ui32>& columnIds, const TIndexInfo& indexInfo)
        : ColumnIds(columnIds.begin(), columnIds.end())
    {
        Schema = indexInfo.GetColumnsSchema(ColumnIds);
        Rebuild();
    }

    bool Contains(const std::shared_ptr<TColumnsSet>& columnsSet) const {
        if (!columnsSet) {
            return true;
        }
        return Contains(*columnsSet);
    }

    bool IsEqual(const std::shared_ptr<TColumnsSet>& columnsSet) const {
        if (!columnsSet) {
            return false;
        }
        return IsEqual(*columnsSet);
    }

    bool Contains(const TColumnsSet& columnsSet) const {
        for (auto&& i : columnsSet.ColumnIds) {
            if (!ColumnIds.contains(i)) {
                return false;
            }
        }
        return true;
    }

    bool IsEqual(const TColumnsSet& columnsSet) const {
        if (columnsSet.GetColumnIds().size() != ColumnIds.size()) {
            return false;
        }
        auto itA = ColumnIds.begin();
        auto itB = columnsSet.ColumnIds.begin();
        while (itA != ColumnIds.end()) {
            if (*itA != *itB) {
                return false;
            }
            ++itA;
            ++itB;
        }
        return true;
    }

    TString DebugString() const;

    TColumnsSet operator+(const TColumnsSet& external) const;

    TColumnsSet operator-(const TColumnsSet& external) const;
};

class TFetchingPlan {
private:
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, FilterStage);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, FetchingStage);
    bool CanUseEarlyFilterImmediatelyFlag = false;
public:
    TFetchingPlan(const std::shared_ptr<TColumnsSet>& filterStage, const std::shared_ptr<TColumnsSet>& fetchingStage, const bool canUseEarlyFilterImmediately)
        : FilterStage(filterStage)
        , FetchingStage(fetchingStage)
        , CanUseEarlyFilterImmediatelyFlag(canUseEarlyFilterImmediately) {

    }

    TString DebugString() const {
        return TStringBuilder() << "{filter=" << (FilterStage ? FilterStage->DebugString() : "NO") << ";fetching=" <<
            (FetchingStage ? FetchingStage->DebugString() : "NO") << ";use_filter=" << CanUseEarlyFilterImmediatelyFlag << "}";
    }

    bool CanUseEarlyFilterImmediately() const {
        return CanUseEarlyFilterImmediatelyFlag;
    }
};

}
