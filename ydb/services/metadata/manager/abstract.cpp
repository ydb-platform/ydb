#include "abstract.h"

namespace NKikimr::NMetadata::NModifications {

TTableSchema::TTableSchema(const Ydb::Table::DescribeTableResult& description) {
    std::map<TString, Ydb::Column> columns;
    for (auto&& i : description.columns()) {
        Ydb::Column column;
        column.set_name(i.name());
        if (i.type().has_optional_type()) {
            *column.mutable_type() = i.type().optional_type().item();
        } else {
            *column.mutable_type() = i.type();
        }
        columns.emplace(i.name(), std::move(column));
    }
    for (auto&& i : description.primary_key()) {
        auto it = columns.find(i);
        Y_VERIFY(it != columns.end());
        AddColumn(true, it->second);
        columns.erase(it);
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
