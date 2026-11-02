#pragma once

#include "kqp_stage_graph.h"

namespace NKikimr::NKqp {

template <typename TRange>
NJson::TJsonValue MakeColumnsJson(const TRange& columns, const TInfoUnitRegistry& registry) {
    NJson::TJsonValue result(NJson::EJsonValueType::JSON_ARRAY);
    for (const auto id : columns) {
        result.AppendValue(registry.GetDisplayName(id));
    }
    return result;
}

inline NJson::TJsonValue MakeSortColumnsJson(const TSortIUs& columns, const TInfoUnitRegistry& registry) {
    NJson::TJsonValue result(NJson::EJsonValueType::JSON_ARRAY);
    for (const auto& [id, order] : columns.Items()) {
        NJson::TJsonValue column(NJson::EJsonValueType::JSON_MAP);
        column["Column"] = registry.GetDisplayName(id);
        column["Ascending"] = order.Ascending;
        column["NullsFirst"] = order.NullsFirst;
        result.AppendValue(std::move(column));
    }
    return result;
}

} // namespace NKikimr::NKqp
