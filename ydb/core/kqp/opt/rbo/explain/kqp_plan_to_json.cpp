#include "../kqp_operator.h"

#include <yql/essentials/utils/log/log.h>


#include <cmath>

namespace NKikimr::NKqp {

namespace {

void AddOptimizerEstimates(NJson::TJsonValue& json, IOperator* op) {
    auto addEstimate = [&](TStringBuf name, double value) {
        // Non-finite estimates cannot be represented by a JSON number.
        if (std::isfinite(value)) {
            json[name] = value;
        }
    };
    addEstimate("E-Rows", op->Props.Statistics->ERows);
    addEstimate("E-Size", op->Props.Statistics->EBytes);
    addEstimate("E-Cost", *op->Props.Cost);
}

NJson::TJsonValue MakeJson(IOperator* op, ui32 explainFlags, const TInfoUnitRegistry& registry) {
    auto res = op->ToJson(explainFlags, registry);

    AddOptimizerEstimates(res, op);
    return res;
}

NJson::TJsonValue MakeJson(IOperator* op, ui32 operatorId, ui32 explainFlags, const TInfoUnitRegistry& registry) {
    auto res = MakeJson(op, explainFlags, registry);
    res["OperatorId"] = operatorId;
    return res;
}

struct TExplainJsonContext {
    const TPlanProps& PlanProps;
    const TInfoUnitRegistry& Registry;
    ui64& NodeCounter;
    const THashMap<IOperator*, ui32>& OperatorIds;
    ui32 ExplainFlags;

    ui64 NextNodeId() {
        return NodeCounter++;
    }
};

bool IsReplicateOperator(const IOperator& op) {
    return op.Kind == EOperator::Replicate;
}

// Replicate ports only rebind IDs; explain shows the shared producer under each consumer.
IOperator* SkipReplicatePorts(IOperator* op) {
    while (op->Kind == EOperator::Replicate) {
        op = CastOperator<TOpReplicate>(*op).GetReplicate().GetInput().Get();
    }
    return op;
}

NJson::TJsonValue GetExplainJsonRec(IOperator* op, TExplainJsonContext& ctx) {
    op = SkipReplicatePorts(op);
    NJson::TJsonValue result;
    result["PlanNodeId"] = ctx.NextNodeId();
    result["Node Type"] = op->GetExplainName();
    NJson::TJsonValue operatorList = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
    auto operatorJson = MakeJson(op, ctx.ExplainFlags, ctx.Registry);
    // Synthetic operators that are absent from the execution plan cannot be correlated with runtime stats.
    if (auto operatorId = ctx.OperatorIds.find(op); operatorId != ctx.OperatorIds.end()) {
        operatorJson["OperatorId"] = operatorId->second;
    }
    operatorList.AppendValue(std::move(operatorJson));
    result["Operators"] = operatorList;

    auto getChildJson = [&](const auto& child, ui32 childIndex) {
        auto childJson = GetExplainJsonRec(child, ctx);

        // Explicitly show all connections except TMapConnection
        const auto* edge = ctx.PlanProps.GetStageInputEdge(*op, childIndex);
        const auto* connection = edge ? &edge->Connection() : nullptr;
        if (connection && !IsConnection<TMapConnection>(*connection)) {
            auto connectionJson = connection->ToJson(ctx.Registry);
            connectionJson["PlanNodeId"] = ctx.NextNodeId();

            NJson::TJsonValue plans(NJson::EJsonValueType::JSON_ARRAY);
            plans.AppendValue(childJson);
            connectionJson["Plans"] = plans;

            return connectionJson;
        }

        return childJson;
    };

    if (op->GetChildren().size()){
        NJson::TJsonValue plans = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
        for (ui32 i = 0; i < op->GetChildren().size(); ++ i) {
            plans.AppendValue(getChildJson(op->GetChildren()[i], i));
        }
        result["Plans"] = plans;
    }

    return result;
}

} // anonymous namespace

NJson::TJsonValue TOpRoot::GetExecutionJson(ui64& nodeCounter, ui32& operatorIdx, THashMap<IOperator*, ui32>& operatorIds, ui32 explainFlags) {
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

    THashMap<int, TVector<IOperator*>> stageOpMap;
    std::set<int> stages;

    for (const auto& it : *this) {
        auto & currOp = it.Current;
        int stageId = *currOp->Props.StageId;
        if (!stageOpMap.contains(stageId)) {
            stageOpMap.insert({stageId, {}});
        }

        auto & stageOps = stageOpMap.at(stageId);

        if (currOp->Kind != EOperator::EmptySource && !IsReplicateOperator(*currOp)) {
            // This map defines which operators can be correlated across execution and simplified plans.
            operatorIds.insert({currOp, operatorIdx++});

            YQL_CLOG(TRACE, CoreDq) << "Adding operator to explain json: " << currOp->GetExplainName() << ", stageId: " << stageId;

            stageOps.insert(stageOps.begin(), currOp);
        }

        stages.insert(stageId);
    }

    THashMap<TStageEdgeId, NJson::TJsonValue> processedStages;

    for (const auto* graphStage : PlanProps.StageGraph.TopologicalOrder()) {
        const auto stageId = graphStage->Id();
        if (!stages.contains(stageId)) {
            continue;
        }
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

            operatorList.AppendValue(MakeJson(op, operatorIds.at(op), explainFlags, PlanProps.InfoUnitRegistry));
        }

        // Build a list of subplans - these are connection objects of input stages
        auto planList = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
        const auto& stageInputs = PlanProps.StageGraph.IncomingEdges(*graphStage);
        for (const auto* edge : stageInputs) {
            planList.AppendValue(processedStages.at(edge->Id()));
        }
        const auto& stageOutputs = PlanProps.StageGraph.OutgoingEdges(*graphStage);

        // If this is the final stage, add the child plans and operators to it
        if(stageOutputs.empty()) {
            finalStage["Node Type"] = stageName;
            finalStage["StageGuid"] = graphStage->Guid();
            if (ops.size()) {
                finalStage["Operators"] = operatorList;
            }
            if (stageInputs.size()) {
                finalStage["Plans"] = planList;
            }
        }

        // A stage with multiple outputs is rendered once. Other connections refer to it as a CTE.
        else {
            TString cteName;
            for (const auto* edge : stageOutputs) {
                auto connJson = edge->Connection().ToJson(PlanProps.InfoUnitRegistry);
                connJson["PlanNodeId"] = nodeCounter++;

                if (!cteName.empty()) {
                    connJson["CTE Name"] = cteName;
                } else {
                    auto stage = NJson::TJsonValue(NJson::EJsonValueType::JSON_MAP);
                    stage["Node Type"] = stageName;
                    stage["StageGuid"] = graphStage->Guid();

                    if (ops.size()) {
                        stage["Operators"] = operatorList;
                    }
                    if (stageInputs.size()) {
                        stage["Plans"] = planList;
                    }
                    if (stageOutputs.size() > 1) {
                        cteName = TStringBuilder() << stageName << "_" << stageId;
                        stage["Parent Relationship"] = "InitPlan";
                        stage["Subplan Name"] = "CTE " + cteName;
                    }
                    stage["PlanNodeId"] = nodeCounter++;

                    connJson["Plans"].AppendValue(std::move(stage));
                }

                processedStages.emplace(edge->Id(), std::move(connJson));
            }
        }
    }

    plans.AppendValue(finalStage);
    result["Plans"] = plans;

    return result;
}

// For explain JSON we recurse over the operators of the plan
NJson::TJsonValue TOpRoot::GetExplainJson(ui64& nodeCounter, const THashMap<IOperator*, ui32>& operatorIds, ui32 explainFlags) {
    NJson::TJsonValue result;
    result["PlanNodeId"] = nodeCounter++;
    result["PlanNodeType"] = "ResultSet";
    result["Node Type"] = "ResultSet";

    NJson::TJsonValue plans = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
    TExplainJsonContext ctx{PlanProps, PlanProps.InfoUnitRegistry, nodeCounter, operatorIds, explainFlags};
    plans.AppendValue(GetExplainJsonRec(GetInput().Get(), ctx));
    result["Plans"] = plans;

    return result;
}

} // namespace NKikimr::NKqp
