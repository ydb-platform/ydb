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
    NJson::TJsonValue finalStage;
    NJson::TJsonValue plans;

    finalStage["PlanNodeId"] = nodeCounter++;
    finalStage["Node Type"] = "Stage";

    // We first build a map of stage_id -> operator list
    // Then iterate through stage ids and output a plan node for each stage

    THashMap<int, TVector<TIntrusivePtr<IOperator>>> stageOpMap;
    std::set<int> stages;

    for (auto it : *this) {
        auto & currOp = it.Current;
        int stageId = *currOp->Props.StageId;
        if (!stageOpMap.contains(stageId)) {
            stageOpMap.insert({stageId, {}});
        }

        auto & stageOps = stageOpMap.at(stageId);

        if (currOp->Kind != EOperator::Map && currOp->Kind != EOperator::EmptySource) {
            stageOps.insert(stageOps.begin(), currOp);
        }

        stages.insert(stageId);
    }

    THashMap<std::pair<int, int>, NJson::TJsonValue> processedStages;

    // Iterate though stages in acsending order, this guarantees that we will build a tree in the right order
    for (int stageId : stages) {
        auto ops = stageOpMap.at(stageId);
        TString stageName;

        // First, build the list of operators in the stage
        auto operatorList = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
        for (auto & op : ops) {
            if (stageName == "") {
                stageName = op->GetExplainName();
            } else {
                stageName = stageName + "-" + op->GetExplainName();
            }

            operatorList.AppendValue(op->ToJson(explainFlags));
        }

        // Build a list of subplans - these are connection objects of input stages
        auto planList = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
        const auto & stageInputs = PlanProps.StageGraph.StageInputs.at(stageId);
        for (auto inputId : stageInputs) {
            planList.AppendValue( processedStages.at(std::make_pair(inputId, stageId)));
        }

        const auto & stageOutputs = PlanProps.StageGraph.StageOutputs.at(stageId);

        // If this is the final stage, add the child plans and operators to it
        if(stageOutputs.empty()) {
            finalStage["Operators"] = operatorList;
            finalStage["Plans"] = planList;
        }

        // Otherwise, construct a new plan object for each outgoing connection of the stage
        // and include the stage in each connection
        else {
            for (int outputStageId : stageOutputs) {
                const auto & conns = PlanProps.StageGraph.GetConnections(stageId, outputStageId);
                Y_ASSERT(conns.size()==1);
                const auto & conn = conns[0];

                auto connJson = NJson::TJsonValue(NJson::EJsonValueType::JSON_MAP);
                connJson["PlanNodeType"] = "Connection";
                connJson["Node Type"] = conn->GetExplainName();
                connJson["PlanNodeId"] = nodeCounter++;

                auto stage = NJson::TJsonValue(NJson::EJsonValueType::JSON_MAP);
                stage["Node Type"] = stageName;
                stage["Operators"] = operatorList;
                stage["Plans"] = planList;
                stage["PlanNodeId"] = nodeCounter++;

                auto connPlans = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
                connPlans.AppendValue(stage);
                connJson["Plans"] = connPlans;

                processedStages.insert({std::make_pair(stageId, outputStageId), connJson});
            }
        }
    }

    plans.AppendValue(finalStage);
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