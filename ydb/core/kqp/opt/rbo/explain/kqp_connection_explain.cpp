#include "../kqp_stage_graph.h"

namespace NKikimr::NKqp {
namespace {

TString FormatSortElements(const TSortIUs& sortElements, const TInfoUnitRegistry& registry) {
    TStringBuilder result;
    TStringBuf separator;
    for (const auto& [id, order] : sortElements.Items()) {
        result << separator << registry.GetDisplayName(id) << (order.Ascending ? " asc " : " desc ")
            << (order.NullsFirst ? "nulls first" : "nulls last");
        separator = ", ";
    }
    return result;
}

NJson::TJsonValue MakeKeyColumnsJson(const TOrderedIUs<>& keys, const TInfoUnitRegistry& registry) {
    NJson::TJsonValue keyColumns(NJson::EJsonValueType::JSON_ARRAY);
    for (const auto id : keys.Items()) {
        keyColumns.AppendValue(TString(TStringBuilder() << registry.GetDisplayName(id)));
    }
    return keyColumns;
}

NJson::TJsonValue MakeSortColumnsJson(const TSortIUs& sortElements, const TInfoUnitRegistry& registry) {
    NJson::TJsonValue sortColumns(NJson::EJsonValueType::JSON_ARRAY);
    for (const auto& [id, order] : sortElements.Items()) {
        TStringBuilder sortColumn;
        sortColumn << registry.GetDisplayName(id) << " (" << (order.Ascending ? "Asc" : "Desc") << ")";
        sortColumns.AppendValue(TString(sortColumn));
    }
    return sortColumns;
}

} // namespace

NJson::TJsonValue TConnection::ToJson(const TInfoUnitRegistry&) const {
    NJson::TJsonValue json(NJson::EJsonValueType::JSON_MAP);
    json["PlanNodeType"] = "Connection";
    json["Node Type"] = Type;
    return json;
}

NJson::TJsonValue TUnionAllConnection::ToJson(const TInfoUnitRegistry& registry) const {
    auto json = TConnection::ToJson(registry);
    if (Parallel) {
        json["Parallel"] = "True";
    }
    return json;
}

NJson::TJsonValue TShuffleConnection::ToJson(const TInfoUnitRegistry& registry) const {
    auto json = TConnection::ToJson(registry);
    Y_ENSURE(HashFuncType, "Hash function type must be assigned before building explain JSON.");

    const auto hashFunc = ToString(*HashFuncType);
    json["HashFunc"] = hashFunc;

    const auto keyColumns = MakeKeyColumnsJson(Keys, registry);
    json["KeyColumns"] = keyColumns;
    return json;
}

NJson::TJsonValue TMergeConnection::ToJson(const TInfoUnitRegistry& registry) const {
    auto json = TConnection::ToJson(registry);
    const auto sortBy = FormatSortElements(Order, registry);
    json["SortBy"] = sortBy;
    json["SortColumns"] = MakeSortColumnsJson(Order, registry);
    return json;
}

} // namespace NKikimr::NKqp
