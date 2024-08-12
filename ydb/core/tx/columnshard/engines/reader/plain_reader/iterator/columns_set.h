#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <util/string/join.h>

namespace NKikimr::NOlap::NReader::NPlain {

enum class EStageFeaturesIndexes {
    Filter = 0,
    Fetching = 1,
    Merge = 2
};

class TIndexesSet {
private:
    YDB_READONLY_DEF(std::vector<ui32>, IndexIds);
    YDB_READONLY_DEF(std::set<ui32>, IndexIdsSet);
public:
    TIndexesSet(const std::set<ui32>& indexIds)
        : IndexIds(indexIds.begin(), indexIds.end())
        , IndexIdsSet(indexIds) {
        AFL_VERIFY(IndexIds.size() == IndexIdsSet.size())("indexes", JoinSeq(",", IndexIds));
    }

    TIndexesSet(const ui32& indexId)
        : IndexIds({indexId})
        , IndexIdsSet({indexId}) {
    }

    ui32 GetIndexesCount() const {
        return IndexIds.size();
    }

    TString DebugString() const {
        return TStringBuilder() << JoinSeq(",", IndexIds);
    }
};

class TColumnsSet {
private:
    YDB_READONLY_DEF(std::set<ui32>, ColumnIds);
    YDB_READONLY_DEF(std::set<TString>, ColumnNames);
    std::vector<TString> ColumnNamesVector;
    YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, Schema);
    ISnapshotSchema::TPtr FullReadSchema;
    YDB_READONLY_DEF(ISnapshotSchema::TPtr, FilteredSchema);

    void Rebuild();

public:
    TColumnsSet() = default;
    bool IsEmpty() const {
        return ColumnIds.empty();
    }

    bool operator!() const {
        return IsEmpty();
    }

    const std::vector<TString>& GetColumnNamesVector() const {
        return ColumnNamesVector;
    }

    ui32 GetColumnsCount() const {
        return ColumnIds.size();
    }

    bool ColumnsOnly(const std::vector<std::string>& fieldNames) const;

    std::shared_ptr<TColumnsSet> BuildSamePtr(const std::set<ui32>& columnIds) const {
        return std::make_shared<TColumnsSet>(columnIds, FullReadSchema);
    }

    TColumnsSet(const std::set<ui32>& columnIds, const ISnapshotSchema::TPtr& fullReadSchema)
        : ColumnIds(columnIds)
        , FullReadSchema(fullReadSchema)
    {
        AFL_VERIFY(!!FullReadSchema);
        Schema = FullReadSchema->GetIndexInfo().GetColumnsSchema(ColumnIds);
        Rebuild();
    }

    TColumnsSet(const std::vector<ui32>& columnIds, const ISnapshotSchema::TPtr& fullReadSchema)
        : ColumnIds(columnIds.begin(), columnIds.end())
        , FullReadSchema(fullReadSchema)
    {
        AFL_VERIFY(!!FullReadSchema);
        Schema = FullReadSchema->GetIndexInfo().GetColumnsSchema(ColumnIds);
        Rebuild();
    }

    const ISnapshotSchema& GetFilteredSchemaVerified() const {
        AFL_VERIFY(FilteredSchema);
        return *FilteredSchema;
    }

    const std::shared_ptr<ISnapshotSchema>& GetFilteredSchemaPtrVerified() const {
        AFL_VERIFY(FilteredSchema);
        return FilteredSchema;
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

    bool Cross(const TColumnsSet& columnsSet) const {
        for (auto&& i : columnsSet.ColumnIds) {
            if (ColumnIds.contains(i)) {
                return true;
            }
        }
        return false;
    }

    std::set<ui32> Intersect(const TColumnsSet& columnsSet) const {
        std::set<ui32> result;
        for (auto&& i : columnsSet.ColumnIds) {
            if (ColumnIds.contains(i)) {
                result.emplace(i);
            }
        }
        return result;
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

}
