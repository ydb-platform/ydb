#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap::NPlainReader {

class TColumnsSet {
private:
    YDB_READONLY_DEF(std::set<ui32>, ColumnIds);
    YDB_READONLY_DEF(std::set<TString>, ColumnNames);
    mutable std::optional<std::vector<TString>> ColumnNamesVector;
    YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, Schema);
public:
    TColumnsSet() = default;

    const std::vector<TString>& GetColumnNamesVector() const {
        if (!ColumnNamesVector) {
            ColumnNamesVector = std::vector<TString>(ColumnNames.begin(), ColumnNames.end());
        }
        return *ColumnNamesVector;
    }

    ui32 GetSize() const {
        return ColumnIds.size();
    }

    bool ColumnsOnly(const std::vector<std::string>& fieldNames) const {
        if (fieldNames.size() != GetSize()) {
            return false;
        }
        std::set<std::string> fieldNamesSet;
        for (auto&& i : fieldNames) {
            if (!fieldNamesSet.emplace(i).second) {
                return false;
            }
            if (!ColumnNames.contains(TString(i.data(), i.size()))) {
                return false;
            }
        }
        return true;
    }

    TColumnsSet(const std::set<ui32>& columnIds, const TIndexInfo& indexInfo) {
        ColumnIds = columnIds;
        Schema = indexInfo.GetColumnsSchema(ColumnIds);
        for (auto&& i : ColumnIds) {
            ColumnNames.emplace(indexInfo.GetColumnName(i));
        }
    }

    TColumnsSet(const std::vector<ui32>& columnIds, const TIndexInfo& indexInfo) {
        for (auto&& i : columnIds) {
            Y_VERIFY(ColumnIds.emplace(i).second);
            ColumnNames.emplace(indexInfo.GetColumnName(i));
        }
        Schema = indexInfo.GetColumnsSchema(ColumnIds);
    }

    bool Contains(const std::shared_ptr<TColumnsSet>& columnsSet) const {
        if (!columnsSet) {
            return true;
        }
        return Contains(*columnsSet);
    }

    bool Contains(const TColumnsSet& columnsSet) const {
        for (auto&& i : columnsSet.ColumnIds) {
            if (!ColumnIds.contains(i)) {
                return false;
            }
        }
        return true;
    }

    TString DebugString() const;

    TColumnsSet operator+(const TColumnsSet& external) const {
        TColumnsSet result = *this;
        result.ColumnIds.insert(external.ColumnIds.begin(), external.ColumnIds.end());
        result.ColumnNames.insert(external.ColumnNames.begin(), external.ColumnNames.end());
        auto fields = result.Schema->fields();
        for (auto&& i : external.Schema->fields()) {
            if (!result.Schema->GetFieldByName(i->name())) {
                fields.emplace_back(i);
            }
        }
        result.Schema = std::make_shared<arrow::Schema>(fields);
        return result;
    }

    TColumnsSet operator-(const TColumnsSet& external) const {
        TColumnsSet result = *this;
        for (auto&& i : external.ColumnIds) {
            result.ColumnIds.erase(i);
        }
        for (auto&& i : external.ColumnNames) {
            result.ColumnNames.erase(i);
        }
        arrow::FieldVector fields;
        for (auto&& i : Schema->fields()) {
            if (!external.Schema->GetFieldByName(i->name())) {
                fields.emplace_back(i);
            }
        }
        result.Schema = std::make_shared<arrow::Schema>(fields);
        return result;
    }
};

}
