#include "columns_set.h"
#include <util/string/join.h>

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

NKikimr::NOlap::NPlainReader::TColumnsSet TColumnsSet::operator+(const TColumnsSet& external) const {
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

}
