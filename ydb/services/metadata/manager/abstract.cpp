#include "abstract.h"

namespace NKikimr::NMetadata::NModifications {

TTableSchema::TTableSchema(const THashMap<ui32, TSysTables::TTableColumnInfo>& description) {
    std::map<TString, Ydb::Column> columns;
    std::map<ui32, Ydb::Column> pkColumns;
    for (auto&& [_, i] : description) {
        Ydb::Column column;
        column.set_name(i.Name);
        column.mutable_type()->set_type_id(::Ydb::Type::PrimitiveTypeId(i.PType.GetTypeId()));
        if (i.KeyOrder >= 0) {
            Y_VERIFY(pkColumns.emplace(i.KeyOrder, std::move(column)).second);
        } else {
            Y_VERIFY(columns.emplace(i.Name, std::move(column)).second);
        }
    }
    for (auto&& i : pkColumns) {
        AddColumn(true, i.second);
    }
    for (auto&& i : columns) {
        AddColumn(false, i.second);
    }
}

NKikimr::NMetadata::NModifications::TTableSchema& TTableSchema::AddColumn(const bool primary, const Ydb::Column& info) noexcept {
    Columns.emplace_back(primary, info);
    YDBColumns.emplace_back(info);
    if (primary) {
        PKColumns.emplace_back(info);
        PKColumnIds.emplace_back(info.name());
    }
    return *this;
}

}
