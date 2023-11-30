#include "columns_set.h"
#include <util/string/join.h>
#include <ydb/core/tx/columnshard/engines/scheme/filtered_scheme.h>

namespace NKikimr::NOlap::NPlainReader {

TString TColumnsSet::DebugString() const {
    return TStringBuilder() << "("
        << "column_ids=" << JoinSeq(",", ColumnIds) << ";"
        << "column_names=" << JoinSeq(",", ColumnNames) << ";"
        << ");";
}

NKikimr::NOlap::NPlainReader::TColumnsSet TColumnsSet::operator-(const TColumnsSet& external) const {
    TColumnsSet result = *this;
    for (auto&& i : external.ColumnIds) {
        result.ColumnIds.erase(i);
    }
    arrow::FieldVector fields;
    for (auto&& i : Schema->fields()) {
        if (!external.Schema->GetFieldByName(i->name())) {
            fields.emplace_back(i);
        }
    }
    result.Schema = std::make_shared<arrow::Schema>(fields);
    result.Rebuild();
    return result;
}

NKikimr::NOlap::NPlainReader::TColumnsSet TColumnsSet::operator+(const TColumnsSet& external) const {
    TColumnsSet result = *this;
    result.ColumnIds.insert(external.ColumnIds.begin(), external.ColumnIds.end());
    auto fields = result.Schema->fields();
    for (auto&& i : external.Schema->fields()) {
        if (!result.Schema->GetFieldByName(i->name())) {
            fields.emplace_back(i);
        }
    }
    result.Schema = std::make_shared<arrow::Schema>(fields);
    result.Rebuild();
    return result;
}

bool TColumnsSet::ColumnsOnly(const std::vector<std::string>& fieldNames) const {
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

void TColumnsSet::Rebuild() {
    ColumnNamesVector.clear();
    ColumnNames.clear();
    for (auto&& i : Schema->field_names()) {
        ColumnNamesVector.emplace_back(i);
        ColumnNames.emplace(i);
    }
    if (ColumnIds.size()) {
        FilteredSchema = std::make_shared<TFilteredSnapshotSchema>(FullReadSchema, ColumnIds);
    } else {
        FilteredSchema = FullReadSchema;
    }
}

}
