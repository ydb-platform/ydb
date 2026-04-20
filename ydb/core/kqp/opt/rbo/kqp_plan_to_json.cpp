#include "kqp_operator.h"
#include <library/cpp/json/writer/json.h>
#include <library/cpp/json/json_reader.h>

namespace {

using namespace NKikimr::NKqp;

void AddOptimizerEstimates(NJson::TJsonValue& json, const TIntrusivePtr<IOperator>& op) {
    json["E-Rows"] = TStringBuilder() << op->Props.Statistics->ERows;
    json["E-Size"] = TStringBuilder() << op->Props.Statistics->EBytes;
    json["E-Cost"] = TStringBuilder() << *op->Props.Cost;
}

NJson::TJsonValue MakeJson(const TIntrusivePtr<IOperator>& op, ui32 explainFlags) {
    auto res = op->ToJson(explainFlags);

    AddOptimizerEstimates(res, op);
    return res;
}


NJson::TJsonValue GetExplainJsonRec(const TIntrusivePtr<IOperator>& op, ui64& nodeCounter, ui32 explainFlags) {
    NJson::TJsonValue result;
    result["PlanNodeId"] = nodeCounter++;
    result["Node Type"] = op->GetExplainName();
    NJson::TJsonValue operatorList = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
    operatorList.AppendValue(MakeJson(op, explainFlags));
    result["Operators"] = operatorList;

    auto getChildJson = [&](const auto& child) {
        auto childJson = GetExplainJsonRec(child, nodeCounter, explainFlags);

        // Insert shuffle connections if needed
        // (currently always needed for GraceJoin)
        NJson::TJsonValue connectionJson = childJson;
        if (op->Kind == EOperator::Join) {
            auto join = CastOperator<TOpJoin>(op);
            if (join->Props.JoinAlgo == NKikimr::NKqp::EJoinAlgoType::GraceJoin) {
                connectionJson = NJson::TJsonValue(NJson::EJsonValueType::JSON_MAP);
                connectionJson["PlanNodeType"] = "Connection";
                connectionJson["Node Type"] = "HashShuffle";
                connectionJson["PlanNodeId"] = nodeCounter++;

                NJson::TJsonValue plans(NJson::EJsonValueType::JSON_ARRAY);
                plans.AppendValue(childJson);
                connectionJson["Plans"] = plans;
            }
        }

        return connectionJson;
    };

    if (op->Children.size()){
        NJson::TJsonValue plans = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
        for (const auto& child : op->Children) {
            plans.AppendValue(getChildJson(child));
        }
        result["Plans"] = plans;
    }

    return result;
}

}

namespace NKikimr {
namespace NKqp {

NJson::TJsonValue TOpRoot::GetExecutionJson(ui64& nodeCounter, ui32 explainFlags) {
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

        //if (currOp->Kind != EOperator::Map && currOp->Kind != EOperator::EmptySource) {
        if (currOp->Kind != EOperator::EmptySource) {

            YQL_CLOG(TRACE, CoreDq) << "Adding operator to explain json: " << currOp->GetExplainName() << ", stageId: " << stageId;

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

            operatorList.AppendValue(MakeJson(op, explainFlags));
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
            finalStage["Node Type"] = stageName;
            if (ops.size()) {
                finalStage["Operators"] = operatorList;
            }
            if (stageInputs.size()) {
                finalStage["Plans"] = planList;
            }
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
                if (ops.size()) {
                    stage["Operators"] = operatorList;
                }
                if (stageInputs.size()) {
                    stage["Plans"] = planList;
                }
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

// For explain JSON we recurse over the operators of the plan
NJson::TJsonValue TOpRoot::GetExplainJson(ui64& nodeCounter, ui32 explainFlags) {
    Y_UNUSED(explainFlags);

    NJson::TJsonValue result;
    result["PlanNodeId"] = nodeCounter++;
    result["PlanNodeType"] = "ResultSet";
    result["Node Type"] = "ResultSet";

    NJson::TJsonValue plans = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
    plans.AppendValue(GetExplainJsonRec(GetInput(), nodeCounter, explainFlags));
    result["Plans"] = plans;

    return result;
}

}
}