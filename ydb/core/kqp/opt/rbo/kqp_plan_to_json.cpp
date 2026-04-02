#include "kqp_operator.h"
#include <library/cpp/json/writer/json.h>
#include <library/cpp/json/json_reader.h>


namespace NKikimr {
namespace NKqp {

NJson::TJsonValue TOpRoot::GetExecutionJson(ui64 & nodeCounter, ui32 explainFlags) {
    Y_UNUSED(explainFlags);

    // First construct the ResultSet
    NJson::TJsonValue result;
    result["PlanNodeId"] = nodeCounter++;
    result["PlanNodeType"] = "ResultSet";
    result["Node Type"] = "ResultSet";

    // Add the final stage
    NJson::TJsonValue stage;
    NJson::TJsonValue plans;

    stage["PlanNodeId"] = nodeCounter++;
    stage["Node Type"] = "Stage";
    plans.AppendValue(std::move(stage));
    result["Plans"] = plans;

    return result;
}

NJson::TJsonValue TOpRoot::GetExplainJson(ui64 & nodeCounter, ui32 explainFlags) {
    Y_UNUSED(explainFlags);

    NJson::TJsonValue result;
    result["PlanNodeId"] = nodeCounter++;
    result["PlanNodeType"] = "ResultSet";
    result["Node Type"] = "ResultSet";

    return result;
}

}
}