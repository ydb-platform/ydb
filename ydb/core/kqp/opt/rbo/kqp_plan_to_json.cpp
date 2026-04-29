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

struct TExplainJsonContext {
    const TStageGraph& StageGraph;
    ui64& NodeCounter;
    ui32 ExplainFlags;

    ui64 NextNodeId() {
        return NodeCounter++;
    }
};

NJson::TJsonValue GetExplainJsonRec(const TIntrusivePtr<IOperator>& op, TExplainJsonContext& ctx) {
    NJson::TJsonValue result;
    result["PlanNodeId"] = ctx.NextNodeId();
    result["Node Type"] = op->GetExplainName();
    NJson::TJsonValue operatorList = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
    operatorList.AppendValue(MakeJson(op, ctx.ExplainFlags));
    result["Operators"] = operatorList;

    auto getChildJson = [&](const auto& child, ui32 childIndex) {
        auto childJson = GetExplainJsonRec(child, ctx);

        // Insert shuffle connections if the stage graph has them between this child and the join.
        NJson::TJsonValue connectionJson = childJson;
        if (op->Kind == EOperator::Join) {
            auto connection = ctx.StageGraph.GetInputConnection(
                static_cast<ui32>(*op->Props.StageId),
                childIndex
            );

            if (IsConnection<TShuffleConnection>(connection)) {
                connectionJson = NJson::TJsonValue(NJson::EJsonValueType::JSON_MAP);
                connectionJson["PlanNodeType"] = "Connection";
                connectionJson["Node Type"] = connection->GetExplainName();
                connectionJson["PlanNodeId"] = ctx.NextNodeId();

                NJson::TJsonValue plans(NJson::EJsonValueType::JSON_ARRAY);
                plans.AppendValue(childJson);
                connectionJson["Plans"] = plans;
            }
        }

        return connectionJson;
    };

    if (op->Children.size()){
        NJson::TJsonValue plans = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
        for (ui32 i = 0; i < op->Children.size(); ++ i) {
            plans.AppendValue(getChildJson(op->Children[i], i));
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
    NJson::TJsonValue result;
    result["PlanNodeId"] = nodeCounter++;
    result["PlanNodeType"] = "ResultSet";
    result["Node Type"] = "ResultSet";

    NJson::TJsonValue plans = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
    TExplainJsonContext ctx{PlanProps.StageGraph, nodeCounter, explainFlags};
    plans.AppendValue(GetExplainJsonRec(GetInput(), ctx));
    result["Plans"] = plans;

    return result;
}

}
}