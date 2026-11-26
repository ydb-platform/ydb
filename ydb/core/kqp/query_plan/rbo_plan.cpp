#include "query_plan.h"

#include <yql/essentials/utils/log/log.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json.h>

#include <util/generic/hash.h>

namespace NKikimr::NKqp {
namespace {

struct TOperatorLocation {
    NJson::TJsonValue* Stage = nullptr;
    NJson::TJsonValue* Operator = nullptr;
    int OperatorIndex = 0;
    const NJson::TJsonValue* RuntimeStats = nullptr;
};

struct TConnectionInfo {
    bool FromBroadcast = false;
    int ParentTaskCount = 0;
};

struct TPlanLookupIndex {
    THashMap<i64, TOperatorLocation> Operators;
    THashMap<TString, TConnectionInfo> Connections;
    TVector<std::pair<i64, NJson::TJsonValue*>> OperatorOrder;
};

enum class EPlanIndexKind {
    Execution,
    Simplified,
};

THashMap<TString, const NJson::TJsonValue*> IndexRuntimeStats(const NJson::TJsonValue& stage) {
    THashMap<TString, const NJson::TJsonValue*> result;
    if (!stage.Has("Stats") || !stage["Stats"].Has("Operator")) {
        return result;
    }

    for (const auto& stat : stage["Stats"]["Operator"].GetArraySafe()) {
        if (!stat.IsMap() || !stat.Has("Type") || !stat["Type"].IsString()) {
            continue;
        }
        const auto [it, inserted] = result.emplace(stat["Type"].GetStringSafe(), &stat);
        if (!inserted) {
            it->second = nullptr;
        }
    }

    THashMap<TString, size_t> candidates;
    for (const auto& op : stage["Operators"].GetArraySafe()) {
        if (op.Has("RuntimeStatsType")) {
            ++candidates[op["RuntimeStatsType"].GetStringSafe()];
        }
    }
    // Runtime IDs currently identify counters within a type, not execution
    // array positions. Several operators can share the same counter ("0"), so
    // only a unique type match in this stage can be attributed to an operator.
    for (auto& [type, stat] : result) {
        if (candidates[type] != 1) {
            stat = nullptr;
        }
    }
    return result;
}

void BuildPlanLookupIndex(
    NJson::TJsonValue& planNode,
    TPlanLookupIndex& index,
    EPlanIndexKind kind)
{
    if (planNode.IsArray()) {
        for (auto& item: planNode.GetArraySafe()) {
            BuildPlanLookupIndex(item, index, kind);
        }
        return;
    }

    if (!planNode.IsMap()) {
        return;
    }

    auto& planMap = planNode.GetMapSafe();
    if (auto operators = planMap.find("Operators"); operators != planMap.end()) {
        auto& operatorArray = operators->second.GetArraySafe();
        const auto runtimeStats = kind == EPlanIndexKind::Execution
            ? IndexRuntimeStats(planNode) : THashMap<TString, const NJson::TJsonValue*>{};
        for (size_t i = 0; i < operatorArray.size(); ++i) {
            auto& item = operatorArray.at(i);
            auto& itemMap = item.GetMapSafe();
            if (auto itemId = itemMap.find("OperatorId"); itemId != itemMap.end()) {
                const i64 id = itemId->second.GetIntegerSafe();
                if (kind == EPlanIndexKind::Simplified) {
                    index.OperatorOrder.emplace_back(id, &item);
                }
                const NJson::TJsonValue* stat = nullptr;
                if (auto type = itemMap.find("RuntimeStatsType"); type != itemMap.end()) {
                    if (auto it = runtimeStats.find(type->second.GetStringSafe()); it != runtimeStats.end()) {
                        stat = it->second;
                    }
                }
                index.Operators.try_emplace(
                    id,
                    TOperatorLocation{&planNode, &item, static_cast<int>(i), stat}
                );
            }
        }
    }

    if (kind == EPlanIndexKind::Execution) {
        if (planMap.contains("PlanNodeType") && planMap.at("PlanNodeType") == "Connection"
            && !planMap.contains("CTE Name")) {
            auto& subplan = planMap.at("Plans").GetArraySafe().at(0);
            TConnectionInfo connectionInfo;
            connectionInfo.FromBroadcast = planMap.at("Node Type") == "Broadcast";
            if (planMap.contains("Stats") && planMap.at("Stats").GetMapSafe().contains("Tasks")) {
                connectionInfo.ParentTaskCount = planMap.at("Stats").GetMapSafe().at("Tasks").GetIntegerSafe();
            }
            index.Connections.try_emplace(
                subplan.GetMapSafe().at("StageGuid").GetStringSafe(),
                connectionInfo
            );
        }
    }

    if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
        for (auto& item: plans->second.GetArraySafe()) {
            BuildPlanLookupIndex(item, index, kind);
        }
    }
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

bool IsTableReadOperator(const TString& opName) {
    return opName == "TableFullScan" || opName == "TableRangeScan";
}

TString GetLastPathComponent(const TString& path) {
    auto slash = path.rfind('/');
    return (slash == TString::npos) ? path : path.substr(slash + 1);
}

void AddStatsToSimplifiedPlan(NJson::TJsonValue& txPlan) {
    auto& simplifiedPlan = txPlan.GetMapSafe().at("SimplifiedPlan");
    auto& execPlan = txPlan.GetMapSafe().at("Plans")[0];

    TPlanLookupIndex execPlanIndex;
    TPlanLookupIndex simplifiedPlanIndex;
    BuildPlanLookupIndex(simplifiedPlan, simplifiedPlanIndex, EPlanIndexKind::Simplified);
    execPlanIndex.Operators.reserve(simplifiedPlanIndex.OperatorOrder.size());
    execPlanIndex.Connections.reserve(simplifiedPlanIndex.OperatorOrder.size());
    BuildPlanLookupIndex(execPlan, execPlanIndex, EPlanIndexKind::Execution);

    for (const auto& [opId, explainPlanOp] : simplifiedPlanIndex.OperatorOrder) {
        const auto execLocation = execPlanIndex.Operators.find(opId);
        const auto explainLocation = simplifiedPlanIndex.Operators.find(opId);
        Y_ENSURE(execLocation != execPlanIndex.Operators.end());
        Y_ENSURE(explainLocation != simplifiedPlanIndex.Operators.end());

        auto* execPlanStage = execLocation->second.Stage;
        auto* execPlanOp = execLocation->second.Operator;
        const int execOperatorIdx = execLocation->second.OperatorIndex;

        if(!execPlanStage->GetMapSafe().contains("Stats")) {
            continue;
        }

        const auto& stats = execPlanStage->GetMapSafe().at("Stats").GetMapSafe();
        const auto& opName = execPlanOp->GetMapSafe().at("Name").GetStringSafe();

        bool operatorRows = false;
        bool operatorSize = false;

        if (IsTableReadOperator(opName) && stats.contains("Table")) {
            TString tableName;
            if (explainPlanOp->GetMapSafe().contains("Path")) {
                tableName = explainPlanOp->GetMapSafe().at("Path").GetStringSafe();
            } else if (explainPlanOp->GetMapSafe().contains("Table")) {
                tableName = explainPlanOp->GetMapSafe().at("Table").GetStringSafe();
            }

            for (auto& opStat : stats.at("Table").GetArraySafe()) {
                if (opStat.IsMap()) {
                    auto& opMap = opStat.GetMapSafe();
                    if (tableName && opMap.contains("Path")) {
                        const auto statPath = opMap.at("Path").GetStringSafe();
                        if (statPath != tableName && GetLastPathComponent(statPath) != tableName) {
                            continue;
                        }
                    }

                    if (opMap.contains("ReadRows")) {
                        explainPlanOp->InsertValue("A-Rows", opMap.at("ReadRows").GetMapSafe().at("Sum").GetDouble());
                        operatorRows = true;
                    }
                    if (opMap.contains("ReadBytes")) {
                        explainPlanOp->InsertValue("A-Size", opMap.at("ReadBytes").GetMapSafe().at("Sum").GetDouble());
                        operatorSize = true;
                    }
                    break;
                }
            }
        } else if (const auto* opStat = execLocation->second.RuntimeStats) {
            const auto& opMap = opStat->GetMapSafe();
            if (opMap.contains("Rows")) {
                explainPlanOp->InsertValue("A-Rows", opMap.at("Rows").GetMapSafe().at("Sum").GetDouble());
                operatorRows = true;
            }
            if (opMap.contains("Bytes")) {
                explainPlanOp->InsertValue("A-Size", opMap.at("Bytes").GetMapSafe().at("Sum").GetDouble());
                operatorSize = true;
            }
        }

        const bool useStageStats = execOperatorIdx == 0 ||
            (execPlanOp->Has("UseStageStats") && (*execPlanOp)["UseStageStats"].GetBooleanSafe());
        if (useStageStats) {
            // Find enclosing connection to process broadcast correctly
            const TString stageGuid = execPlanStage->GetMapSafe().at("StageGuid").GetStringSafe();
            const auto connection = execPlanIndex.Connections.find(stageGuid);
            const bool fromBroadcast = connection != execPlanIndex.Connections.end() && connection->second.FromBroadcast;
            const int parentTaskCount = connection == execPlanIndex.Connections.end() ? 0 : connection->second.ParentTaskCount;
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
        }

        if (execOperatorIdx == 0 && explainPlanOp == explainLocation->second.Operator) {
            // Shared subplans can occur in several simplified branches. Every
            // occurrence gets rows/bytes, but count stage CPU only once.
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

void RemoveOperatorMetadata(NJson::TJsonValue& planNode) {
    auto& planMap = planNode.GetMapSafe();
    if (auto operators = planMap.find("Operators"); operators != planMap.end()) {
        for (auto& op : operators->second.GetArraySafe()) {
            op.EraseValue("OperatorId");
            op.EraseValue("RuntimeStatsType");
            op.EraseValue("UseStageStats");
        }
    }

    if (auto plans = planMap.find("Plans"); plans != planMap.end()) {
        for (auto& child : plans->second.GetArraySafe()) {
            RemoveOperatorMetadata(child);
        }
    }
}

} // anonymous namespace

TString SerializeRBOExplainPlan(NJson::TJsonValue txPlan) {
    NJson::TJsonValue queryPlan;
    NJson::TJsonValue meta;

    meta["version"] = "0.2";
    meta["type"] = "query";

    queryPlan["meta"] = meta;

    // Correlation metadata is internal to ANALYZE, not part of the published plan.
    RemoveOperatorMetadata(txPlan["SimplifiedPlan"]);
    for (auto& plan : txPlan["Plans"].GetArraySafe()) {
        RemoveOperatorMetadata(plan);
    }

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

TString SerializeRBOAnalyzePlan(const TVector<const TString>& txPlans, const NKqpProto::TKqpStatsQuery& queryStats, const TString& poolId) {
    Y_UNUSED(queryStats);
    Y_UNUSED(poolId);

    YQL_CLOG(TRACE, CoreDq) << "Serialize analyze plan";


    // Commit-only transactions may have no plan. Each other transaction has
    // the same query plan, with statistics for its own stages filled in.
    NJson::TJsonValue txPlanJson;
    THashMap<TString, NJson::TJsonValue> stageStats;
    auto collectStats = [&](const auto& self, const NJson::TJsonValue& node) -> void {
        if (node.IsMap()) {
            if (node.Has("StageGuid") && node.Has("Stats")) {
                stageStats[node["StageGuid"].GetStringSafe()] = node["Stats"];
            }
            for (const auto& [key, value] : node.GetMapSafe()) {
                self(self, value);
            }
        } else if (node.IsArray()) {
            for (const auto& value : node.GetArraySafe()) {
                self(self, value);
            }
        }
    };
    for (const auto& plan : txPlans) {
        if (plan.empty()) {
            continue;
        }
        NJson::ReadJsonTree(plan, &txPlanJson, true);
        collectStats(collectStats, txPlanJson);
    }
    if (!txPlanJson.IsDefined()) {
        return "";
    }
    auto applyStats = [&](const auto& self, NJson::TJsonValue& node) -> void {
        if (node.IsMap()) {
            if (node.Has("StageGuid")) {
                if (const auto it = stageStats.find(node["StageGuid"].GetStringSafe()); it != stageStats.end()) {
                    node["Stats"] = it->second;
                }
            }
            for (auto& [key, value] : node.GetMapSafe()) {
                self(self, value);
            }
        } else if (node.IsArray()) {
            for (auto& value : node.GetArraySafe()) {
                self(self, value);
            }
        }
    };
    applyStats(applyStats, txPlanJson);

    AddStatsToSimplifiedPlan(txPlanJson);
    return SerializeRBOExplainPlan(txPlanJson);
}

} // namespace NKikimr::NKqp
