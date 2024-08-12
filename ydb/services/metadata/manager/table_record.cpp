#include "table_record.h"
#include "ydb_value_operator.h"

#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/log.h>
#include <util/string/builder.h>
#include <util/string/join.h>

namespace NKikimr::NMetadata::NInternal {

bool TTableRecord::CompareColumns(const TTableRecord& item, const std::vector<TString>& columnIds) const {
    for (auto&& i : columnIds) {
        auto itSelf = Values.find(i);
        auto itItem = item.Values.find(i);
        if (itSelf == Values.end()) {
            if (itItem == item.Values.end()) {
                continue;
            } else {
                return false;
            }
        } else if (itItem == item.Values.end()) {
            return false;
        } else {
            if (!TYDBValue::Compare(itSelf->second, itItem->second)) {
                return false;
            }
        }
    }
    return true;
}

bool TTableRecord::HasColumns(const std::vector<TString>& columnIds) const {
    for (auto&& i : columnIds) {
        if (!Values.contains(i)) {
            return false;
        }
    }
    return true;
}

ui32 TTableRecord::CountIntersectColumns(const std::vector<TString>& columnIds) const {
    ui32 result = 0;
    for (auto&& i : columnIds) {
        if (Values.contains(i)) {
            ++result;
        }
    }
    return result;
}

bool TTableRecord::TakeValuesFrom(const TTableRecord& item, const NModifications::NColumnMerger::TMergerFactory& mergerFactory) {
    for (auto&& i : item.Values) {
        if (!mergerFactory(i.first)(Values[i.first], i.second)) {
            return false;
        }
    }
    return true;
}

const Ydb::Value* TTableRecord::GetValuePtr(const TString& columnId) const {
    auto it = Values.find(columnId);
    if (it == Values.end()) {
        return nullptr;
    }
    return &it->second;
}

TTableRecord& TTableRecord::SetColumn(const TString& columnId, const Ydb::Value& v) {
    Values[columnId] = v;
    return *this;
}

Ydb::ResultSet TTableRecord::BuildRecordSet() const {
    Ydb::ResultSet result;
    Ydb::Value row;
    for (auto&& i : Values) {
        Ydb::Column column;
        column.set_name(i.first);
//        column.set_type(i.second.type());
        *result.add_columns() = column;
        *row.add_items() = i.second;
    }
    *result.add_rows() = row;
    return result;
}

bool TTableRecord::SameColumns(const TTableRecord& item) const {
    if (Values.size() != item.Values.size()) {
        return false;
    }
    auto itSelf = Values.begin();
    auto itItem = item.Values.begin();
    for (; itSelf != Values.end() && itItem != Values.end(); ++itSelf, ++itItem) {
        if (itSelf->first != itItem->first) {
            return false;
        }
    }
    return true;
}

std::vector<Ydb::Column> TTableRecord::SelectOwnedColumns(const std::vector<Ydb::Column>& columns) const {
    std::vector<Ydb::Column> result;
    for (auto&& i : columns) {
        if (Values.contains(i.name())) {
            result.emplace_back(i);
        }
    }
    return result;
}

ui32 TTableRecords::AddRecordImpl(const TTableRecord& record) {
    Y_ABORT_UNLESS(Columns.size());
    ui32 foundColumns = 0;
    Records.resize(Records.size() + 1);
    for (ui32 i = 0; i < Columns.size(); ++i) {
        const Ydb::Value* v = record.GetValuePtr(Columns[i].name());
        if (v) {
            if (!TYDBValue::IsSameType(*v, Columns[i].type())) {
                ALS_ERROR(NKikimrServices::METADATA_MANAGER);
                Y_DEBUG_ABORT_UNLESS(false);
                continue;
            }
            ++foundColumns;
            *Records.back().add_items() = *v;
        } else {
            *Records.back().add_items() = TYDBValue::NullValue();
        }
    }
    return foundColumns;
}

Ydb::TypedValue TTableRecords::BuildVariableTupleRecords() const {
    Ydb::TypedValue result;
    if (Columns.size() > 1) {
        for (auto&& r : Records) {
            *result.mutable_value()->add_items() = r;
        }
        auto* tuple = result.mutable_type()->mutable_list_type()->mutable_item()->mutable_tuple_type();
        for (auto&& i : Columns) {
            *tuple->add_elements() = i.type();
        }
    } else if (Columns.size() == 1) {
        for (auto&& r : Records) {
            *result.mutable_value()->add_items() = r.items()[0];
        }
        *result.mutable_type()->mutable_list_type()->mutable_item() = Columns[0].type();
    }
    return result;
}

Ydb::TypedValue TTableRecords::BuildVariableStructRecords() const {
    Ydb::TypedValue result;
    for (auto&& r : Records) {
        *result.mutable_value()->add_items() = r;
    }
    auto* structDescr = result.mutable_type()->mutable_list_type()->mutable_item()->mutable_struct_type();
    for (auto&& i : Columns) {
        auto& m = *structDescr->add_members();
        *m.mutable_type() = i.type();
        m.set_name(i.name());
    }
    return result;
}

TString TTableRecords::BuildColumnsSchemaTuple() const {
    TStringBuilder sb;
    if (Columns.size() > 1) {
        sb << "Tuple<";
        std::vector<TString> types;
        for (auto&& i : Columns) {
            types.emplace_back(TYDBValue::TypeToString(i.type()));
        }
        sb << JoinSeq(", ", types) << ">";
    } else if (Columns.size() == 1) {
        sb << TYDBValue::TypeToString(Columns[0].type());
    }
    return sb;
}

TString TTableRecords::BuildColumnsSchemaStruct() const {
    TStringBuilder sb;
    sb << "Struct<";
    std::vector<TString> types;
    for (auto&& i : Columns) {
        types.emplace_back(i.name() + ":" + TYDBValue::TypeToString(i.type()));
    }
    sb << JoinSeq(", ", types) << ">";
    return sb;
}

std::vector<TString> TTableRecords::GetColumnIds() const {
    std::vector<TString> result;
    for (auto&& i : Columns) {
        result.emplace_back(i.name());
    }
    return result;
}

Ydb::Table::ExecuteDataQueryRequest TTableRecords::BuildUpsertQuery(const TString& tablePath) const {
    Ydb::Table::ExecuteDataQueryRequest result;
    TStringBuilder sb;
    sb << "DECLARE $objects AS List<" << BuildColumnsSchemaStruct() << ">;" << Endl;
    sb << "UPSERT INTO `" + tablePath + "`" << Endl;
    sb << "SELECT " << JoinSeq(",", GetColumnIds()) << " FROM AS_TABLE($objects)" << Endl;
    ALS_DEBUG(NKikimrServices::METADATA_PROVIDER) << sb;
    result.mutable_query()->set_yql_text(sb);
    (*result.mutable_parameters())["$objects"] = BuildVariableStructRecords();
    return result;
}

Ydb::Table::ExecuteDataQueryRequest TTableRecords::BuildInsertQuery(const TString& tablePath) const {
    Ydb::Table::ExecuteDataQueryRequest result;
    TStringBuilder sb;
    sb << "DECLARE $objects AS List<" << BuildColumnsSchemaStruct() << ">;" << Endl;
    sb << "INSERT INTO `" + tablePath + "`" << Endl;
    sb << "SELECT " << JoinSeq(",", GetColumnIds()) << " FROM AS_TABLE($objects)" << Endl;
    ALS_DEBUG(NKikimrServices::METADATA_PROVIDER) << sb;
    result.mutable_query()->set_yql_text(sb);
    (*result.mutable_parameters())["$objects"] = BuildVariableStructRecords();
    return result;
}

Ydb::Table::ExecuteDataQueryRequest TTableRecords::BuildSelectQuery(const TString& tablePath) const {
    Ydb::Table::ExecuteDataQueryRequest result;
    TStringBuilder sb;
    sb << "DECLARE $ids AS List<" << BuildColumnsSchemaTuple() << ">;" << Endl;
    sb << "SELECT * FROM `" + tablePath + "`" << Endl;
    if (GetColumnIds().size() > 1) {
        sb << "WHERE (" << JoinSeq(", ", GetColumnIds()) << ") IN $ids" << Endl;
    } else if (GetColumnIds().size() == 1) {
        sb << "WHERE " << GetColumnIds()[0] << " IN $ids" << Endl;
    }
    ALS_DEBUG(NKikimrServices::METADATA_PROVIDER) << sb;
    result.mutable_query()->set_yql_text(sb);
    (*result.mutable_parameters())["$ids"] = BuildVariableTupleRecords();
    return result;
}

Ydb::Table::ExecuteDataQueryRequest TTableRecords::BuildDeleteQuery(const TString& tablePath) const {
    Ydb::Table::ExecuteDataQueryRequest result;
    TStringBuilder sb;
    sb << "DECLARE $ids AS List<" << BuildColumnsSchemaTuple() << ">;" << Endl;
    sb << "DELETE FROM `" + tablePath + "`" << Endl;
    sb << "WHERE (" << JoinSeq(", ", GetColumnIds()) << ") IN $ids" << Endl;
    ALS_DEBUG(NKikimrServices::METADATA_PROVIDER) << sb;
    result.mutable_query()->set_yql_text(sb);
    (*result.mutable_parameters())["$ids"] = BuildVariableTupleRecords();
    return result;
}

Ydb::Table::ExecuteDataQueryRequest TTableRecords::BuildUpdateQuery(const TString& tablePath) const {
    Ydb::Table::ExecuteDataQueryRequest result;
    TStringBuilder sb;
    sb << "DECLARE $objects AS List<" << BuildColumnsSchemaStruct() << ">;" << Endl;
    sb << "UPDATE `" + tablePath + "` ON" << Endl;
    sb << "SELECT " << JoinSeq(",", GetColumnIds()) << " FROM AS_TABLE($objects)" << Endl;
    ALS_DEBUG(NKikimrServices::METADATA_PROVIDER) << sb;
    result.mutable_query()->set_yql_text(sb);
    (*result.mutable_parameters())["$objects"] = BuildVariableStructRecords();
    return result;
}

void TTableRecords::AddColumn(const Ydb::Column& c, const Ydb::Value& v) {
    for (auto&& i : Columns) {
        Y_ABORT_UNLESS(i.name() != c.name());
    }
    Columns.emplace_back(c);
    for (auto&& i : Records) {
        *i.add_items() = v;
    }
}

TTableRecords TTableRecords::SelectColumns(const std::vector<TString>& columnIds) const {
    std::set<TString> columnIdsSet(columnIds.begin(), columnIds.end());
    TTableRecords result;
    std::vector<ui32> idxs;
    ui32 idx = 0;
    for (auto&& i : Columns) {
        if (columnIdsSet.contains(i.name())) {
            result.Columns.emplace_back(i);
            idxs.emplace_back(idx);
        }
        ++idx;
    }
    result.Records.reserve(Records.size());
    for (auto&& i : Records) {
        result.Records.emplace_back(Ydb::Value());
        for (auto&& idx : idxs) {
            *result.Records.back().add_items() = i.items()[idx];
        }
    }
    return result;
}

}
