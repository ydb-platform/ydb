#include "../kqp_stage_graph.h"
#include "kqp_explain_json.h"

namespace NKikimr::NKqp {

NJson::TJsonValue TConnection::ToJson(const TInfoUnitRegistry&) const {
    NJson::TJsonValue json(NJson::EJsonValueType::JSON_MAP);
    json["PlanNodeType"] = "Connection";
    json["Node Type"] = Type;
    return json;
}

NJson::TJsonValue TUnionAllConnection::ToJson(const TInfoUnitRegistry& registry) const {
    auto json = TConnection::ToJson(registry);
    if (Parallel) {
        json["Parallel"] = true;
    }
    return json;
}

NJson::TJsonValue TShuffleConnection::ToJson(const TInfoUnitRegistry& registry) const {
    auto json = TConnection::ToJson(registry);
    Y_ENSURE(HashFuncType, "Hash function type must be assigned before building explain JSON.");

    const auto hashFunc = ToString(*HashFuncType);
    json["HashFunc"] = hashFunc;

    const auto keyColumns = MakeColumnsJson(Keys.Items(), registry);
    json["KeyColumns"] = keyColumns;
    return json;
}

NJson::TJsonValue TMergeConnection::ToJson(const TInfoUnitRegistry& registry) const {
    auto json = TConnection::ToJson(registry);
    json["SortBy"] = MakeSortColumnsJson(Order, registry);
    json["SortColumns"] = MakeSortColumnsJson(Order, registry);
    return json;
}

} // namespace NKikimr::NKqp
