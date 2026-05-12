#include "kqp_operator.h"
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <library/cpp/json/writer/json.h>
#include <library/cpp/json/json_reader.h>

namespace {

using namespace NKikimr::NKqp;

void AddOptimizerEstimates(NJson::TJsonValue& json, const TIntrusivePtr<IOperator>& op) {
    json["E-Rows"] = TStringBuilder() << op->Props.Statistics->ERows;
    json["E-Size"] = TStringBuilder() << op->Props.Statistics->EBytes;
    json["E-Cost"] = TStringBuilder() << *op->Props.Cost;
}

NJson::TJsonValue MakeJson(const TIntrusivePtr<IOperator>& op, ui32 operatorId, ui32 explainFlags) {
    auto res = op->ToJson(explainFlags);

    AddOptimizerEstimates(res, op);
    res["OperatorId"] = operatorId;
    return res;
}

struct TExplainJsonContext {
    const TStageGraph& StageGraph;
    ui64& NodeCounter;
    const THashMap<IOperator*, ui32>& OperatorIds;
    ui32 ExplainFlags;

    ui64 NextNodeId() {
        return NodeCounter++;
    }
};

TIntrusivePtr<TConnection> GetChildStageConnection(const TIntrusivePtr<IOperator>& op, ui32 childIndex, const TStageGraph& graph) {
    if (!op->Props.StageId || childIndex >= op->Children.size()) {
        return {};
    }

    const auto& child = op->Children[childIndex];
    if (!child->Props.StageId || *op->Props.StageId == *child->Props.StageId) {
        return {};
    }

    const auto parentStageId = static_cast<ui32>(*op->Props.StageId);
    const auto childStageId = static_cast<ui32>(*child->Props.StageId);

    // Operator children are wired to stage inputs in the same order as TStageGraph::Connect calls.
    ui32 childStageOccurrence = 0;
    for (ui32 i = 0; i < childIndex; ++i) {
        const auto& sibling = op->Children[i];
        if (sibling->Props.StageId && *sibling->Props.StageId != *op->Props.StageId
            && static_cast<ui32>(*sibling->Props.StageId) == childStageId)
        {
            ++childStageOccurrence;
        }
    }

    return graph.TryGetConnection(childStageId, parentStageId, childStageOccurrence);
}

NJson::TJsonValue GetExplainJsonRec(const TIntrusivePtr<IOperator>& op, TExplainJsonContext& ctx) {
    NJson::TJsonValue result;
    result["PlanNodeId"] = ctx.NextNodeId();
    result["Node Type"] = op->GetExplainName();
    NJson::TJsonValue operatorList = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
    operatorList.AppendValue(MakeJson(op, ctx.OperatorIds.at(op.Get()), ctx.ExplainFlags));
    result["Operators"] = operatorList;

    auto getChildJson = [&](const auto& child, ui32 childIndex) {
        auto childJson = GetExplainJsonRec(child, ctx);

        // Explicitly show all connections except TMapConnection
        const auto connection = GetChildStageConnection(op, childIndex, ctx.StageGraph);
        if (connection && !IsConnection<TMapConnection>(connection)) {
            auto connectionJson = connection->ToJson();
            connectionJson["PlanNodeId"] = ctx.NextNodeId();

            NJson::TJsonValue plans(NJson::EJsonValueType::JSON_ARRAY);
            plans.AppendValue(childJson);
            connectionJson["Plans"] = plans;

            return connectionJson;
        }

        return childJson;
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

void FindPlanNodes(const NJson::TJsonValue& node, const TString& key, std::vector<NJson::TJsonValue>& results) {
    if (node.IsArray()) {
        for (const auto& item: node.GetArray()) {
            FindPlanNodes(item, key, results);
        }
        return;
    }

    if (!node.IsMap()) {
        return;
    }

    if (auto* valueNode = node.GetValueByPath(key)) {
        results.push_back(*valueNode);
    }

    for (const auto& [_, value]: node.GetMap()) {
        FindPlanNodes(value, key, results);
    }
}

bool FindStageAndOpByOpId(NJson::TJsonValue& planNode, int opId, NJson::TJsonValue*& stage, NJson::TJsonValue*& op, int& operatorIdx) {

    if (planNode.IsArray()) {
        for (auto& item: planNode.GetArraySafe()) {
            if (FindStageAndOpByOpId(item, opId, stage, op, operatorIdx)) {
                return true;
            }
        }
        return false;
    } else if (planNode.IsMap()) {
        if (planNode.GetMapSafe().contains("Operators")) {
            auto& operatorArray = planNode.GetMapSafe().at("Operators").GetArraySafe();
            for (size_t i=0; i<operatorArray.size(); i++) {
                auto& item = operatorArray.at(i);
                if (item.GetMapSafe().at("OperatorId").GetInteger() == opId) {
                    operatorIdx = i;
                    stage = &planNode;
                    op = &item;
                    return true;
                }
            }
        }
        if (planNode.GetMapSafe().contains("Plans")) {
            for (auto& item: planNode.GetMapSafe().at("Plans").GetArraySafe()) {
                if (FindStageAndOpByOpId(item, opId, stage, op, operatorIdx)) {
                    return true;
                }
            }
            return false;
        }
    }

    return false;
}

bool FindConnection(NJson::TJsonValue& planNode, const TString& stageGuid, bool& fromBroadcast, int& parentTaskCount) {
    if (planNode.IsArray()) {
        for (auto& item: planNode.GetArraySafe()) {
            if (FindConnection(item, stageGuid, fromBroadcast, parentTaskCount)) {
                return true;
            }
        }
        return false;
    } else if (planNode.IsMap()) {
        const auto& planMap = planNode.GetMapSafe();
        if (planMap.contains("PlanNodeType") && planMap.at("PlanNodeType") == "Connection") {
            const auto& subplan = planMap.at("Plans").GetArraySafe().at(0);
            if (subplan.GetMapSafe().at("StageGuid") == stageGuid) {
                fromBroadcast = planMap.at("Node Type") == "Broadcast";
                if (planMap.contains("Stats") && planMap.at("Stats").GetMapSafe().contains("Tasks")) {
                    parentTaskCount = planMap.at("Stats").GetMapSafe().at("Tasks").GetIntegerSafe();
                }
                return true;
            }
        }
        if (planNode.GetMapSafe().contains("Plans")) {
            for (auto& item: planNode.GetMapSafe().at("Plans").GetArraySafe()) {
                if (FindConnection(item, stageGuid, fromBroadcast, parentTaskCount)) {
                    return true;
                }
            }
            return false;
        }
    }

    return false;
}

double ComputeCpuTimes(NJson::TJsonValue& plan) {
    double currCpuTime = 0;

    if (plan.GetMapSafe().contains("Plans")) {
        for (auto& p : plan.GetMapSafe().at("Plans").GetArraySafe()) {
            currCpuTime += ComputeCpuTimes(p);
        }
    }

    if (plan.GetMapSafe().contains("Operators")) {
        auto& ops = plan.GetMapSafe().at("Operators").GetArraySafe();
        auto& op = ops[0].GetMapSafe();
        if (op.contains("A-SelfCpu")) {
            currCpuTime += op["A-SelfCpu"].GetDoubleSafe();
            op["A-Cpu"] = currCpuTime;
        }
    }

    return currCpuTime;
}

void AddStatsToSimplifiedPlan(NJson::TJsonValue& txPlan) {
    auto& simplifiedPlan = txPlan.GetMapSafe().at("SimplifiedPlan");
    auto& execPlan = txPlan.GetMapSafe().at("Plans")[0];

    // Extract all operator ids from SimplifiedPlan and look up stages and operators
    // in the execution plan
    std::vector<NJson::TJsonValue> opIds;
    FindPlanNodes(simplifiedPlan, "OperatorId", opIds);

    for (auto & idNode : opIds) {
        int execOperatorIdx;
        int explainOperatorIdx;
        NJson::TJsonValue* execPlanStage;
        NJson::TJsonValue* execPlanOp;
        NJson::TJsonValue* explainPlanStage;
        NJson::TJsonValue* explainPlanOp;

        Y_ENSURE(FindStageAndOpByOpId(execPlan, idNode.GetIntegerSafe(), execPlanStage, execPlanOp, execOperatorIdx));
        Y_ENSURE(FindStageAndOpByOpId(simplifiedPlan, idNode.GetIntegerSafe(), explainPlanStage, explainPlanOp, explainOperatorIdx));

        if(!execPlanStage->GetMapSafe().contains("Stats")) {
            continue;
        }

        const auto& stats = execPlanStage->GetMapSafe().at("Stats").GetMapSafe();
        const auto& opName = execPlanOp->GetMapSafe().at("Name").GetStringSafe();

        TString opType;
        if (opName.Contains("Join")) {
            opType = "Join";
        } else if (opName == "Filter") {
            opType = "Filter";
        } else if (opName == "Aggregate") {
            opType = "Aggregation";
        }

        bool operatorRows = false;
        bool operatorSize = false;

        if (opName == "TableFullScan" && stats.contains("Table")) {
            for (auto& opStat : stats.at("Table").GetArraySafe()) {
                if (opStat.IsMap()) {
                    auto& opMap = opStat.GetMapSafe();
                    if (opMap.contains("ReadRows")) {
                        explainPlanOp->InsertValue("A-Rows", opMap.at("ReadRows").GetMapSafe().at("Sum").GetDouble());
                        operatorRows = true;
                    }
                    if (opMap.contains("ReadBytes")) {
                        explainPlanOp->InsertValue("A-Size", opMap.at("ReadBytes").GetMapSafe().at("Sum").GetDouble());
                        operatorSize = true;
                    }
                }
            }
        } else if(stats.contains("Operator")) {
            for (auto& opStat : stats.at("Operator").GetArraySafe()) {
                if (opStat.IsMap()) {
                    auto& opMap = opStat.GetMapSafe();
                    if(opMap.contains("Type") && opMap.at("Type").GetStringSafe() == opType && 
                            opMap.contains("Id") && opMap.at("Id").GetStringSafe() == std::to_string(execOperatorIdx)) {

                        if (opMap.contains("Rows")) {
                            explainPlanOp->InsertValue("A-Rows", opMap.at("Rows").GetMapSafe().at("Sum").GetDouble());
                            operatorRows = true;
                        }
                        if (opMap.contains("Bytes")) {
                            explainPlanOp->InsertValue("A-Size", opMap.at("Bytes").GetMapSafe().at("Sum").GetDouble());
                            operatorSize = true;
                        }
                    }
                    break;

                }
            }
        }

        if (execOperatorIdx == 0 && !explainPlanOp->GetMapSafe().contains("A-Rows")) {
            // Find enclosing connection to process broadcast correctly
            bool fromBroadcast = false;
            int parentTaskCount = 0;
            TString stageGuid = execPlanStage->GetMapSafe().at("StageGuid").GetStringSafe();
            FindConnection(execPlan, stageGuid, fromBroadcast, parentTaskCount);
            // top level rows/size have to match stage output
            if (!operatorRows && stats.contains("OutputRows")) {
                auto outputRows = stats.at("OutputRows");
                int aRows = outputRows.IsMap() ? outputRows.GetMapSafe().at("Sum").GetIntegerSafe() : outputRows.GetIntegerSafe();
    
                if (fromBroadcast && parentTaskCount && (aRows % parentTaskCount == 0)) {
                    aRows /= parentTaskCount;
                }
                explainPlanOp->InsertValue("A-Rows", aRows);
            }
            if (!operatorSize && stats.contains("OutputBytes")) {
                auto outputBytes = stats.at("OutputBytes");
                double aSize = outputBytes.IsMap() ? outputBytes.GetMapSafe().at("Sum").GetDouble() : outputBytes.GetDouble();
                if (fromBroadcast && parentTaskCount) {
                    aSize /= parentTaskCount;
                }
                explainPlanOp->InsertValue("A-Size", aSize);
            }

            // cpu usage available for stage only, so assign it to top level operator
            if (stats.contains("CpuTimeUs")) {
                double opCpuTime;

                auto& cpuTime = stats.at("CpuTimeUs");
                if (cpuTime.IsMap()) {
                    opCpuTime = cpuTime.GetMapSafe().at("Max").GetDoubleSafe();
                } else {
                    opCpuTime = cpuTime.GetDoubleSafe();
                }

                explainPlanOp->InsertValue("A-SelfCpu", opCpuTime / 1000.0);
            }
        }
    }

    ComputeCpuTimes(simplifiedPlan);
}

}

namespace NKikimr {
namespace NKqp {

NJson::TJsonValue TOpRoot::GetExecutionJson(ui64& nodeCounter, THashMap<IOperator*, ui32>& operatorIds, ui32 explainFlags) {
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
    ui32 operatorId = 0;

    for (auto it : *this) {
        auto & currOp = it.Current;
        operatorIds.insert({currOp.Get(), operatorId++});
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

            operatorList.AppendValue(MakeJson(op, operatorIds.at(op.Get()), explainFlags));
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
            finalStage["StageGuid"] = PlanProps.StageGraph.StageGUIDs.at(stageId);
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
                const auto& conns = PlanProps.StageGraph.GetConnections(stageId, outputStageId);
                Y_ASSERT(conns.size() == 1);
                const auto& conn = conns[0];

                auto connJson = conn->ToJson();
                connJson["PlanNodeId"] = nodeCounter++;

                auto stage = NJson::TJsonValue(NJson::EJsonValueType::JSON_MAP);
                stage["Node Type"] = stageName;
                stage["StageGuid"] = PlanProps.StageGraph.StageGUIDs.at(stageId);

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
NJson::TJsonValue TOpRoot::GetExplainJson(ui64& nodeCounter, const THashMap<IOperator*, ui32>& operatorIds, ui32 explainFlags) {
    NJson::TJsonValue result;
    result["PlanNodeId"] = nodeCounter++;
    result["PlanNodeType"] = "ResultSet";
    result["Node Type"] = "ResultSet";

    NJson::TJsonValue plans = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
    TExplainJsonContext ctx{PlanProps.StageGraph, nodeCounter, operatorIds, explainFlags};
    plans.AppendValue(GetExplainJsonRec(GetInput(), ctx));
    result["Plans"] = plans;

    return result;
}

TString SerializeRBOExplainPlan(NJson::TJsonValue txPlan) {
    NJson::TJsonValue queryPlan;
    NJson::TJsonValue meta;

    meta["version"] = "0.2";
    meta["type"] = "query";

    queryPlan["meta"] = meta;

    queryPlan["SimplifiedPlan"] = txPlan.GetMapSafe().at("SimplifiedPlan");
    txPlan.EraseValue("SimplifiedPlan");

    NJson::TJsonValue queryNode;
    queryNode["Node Type"] = "Query";
    queryNode["PlanNodeType"] = "Query";
    queryNode["Plans"] = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
    queryNode["Plans"] = txPlan.GetMapSafe().at("Plans");

    queryPlan["Plan"] = queryNode;

    NJsonWriter::TBuf writer;
    writer.WriteJsonValue(&queryPlan, true, PREC_NDIGITS, 17);
    return writer.Str();
}

TString SerializeRBOAnalyzePlan(const TVector<const TString>& txPlans, const NKqpProto::TKqpStatsQuery& queryStats, const TString& poolId = "") {
    Y_UNUSED(queryStats);
    Y_UNUSED(poolId);
    auto txPlan = txPlans.at(txPlans.size()-1);
    NJson::TJsonValue txPlanJson;
    NJson::ReadJsonTree(txPlan, &txPlanJson, true);

    AddStatsToSimplifiedPlan(txPlanJson);
    return SerializeRBOExplainPlan(txPlanJson);
}

}
}