#include "query_plan.h"

#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>

#include <yql/essentials/utils/log/log.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/string/strip.h>
#include <util/string/vector.h>

#include <functional>
#include <regex>

namespace NKikimr::NKqp {

using namespace NYql;

namespace {

using ModifyFunction = std::function<void (NJson::TJsonValue& node)>;

void ModifyPlan(NJson::TJsonValue& plan, const ModifyFunction& modify) {
    modify(plan);

    auto& map = plan.GetMapSafe();
    if (map.contains("Plans")) {
        for (auto& subplan : map.at("Plans").GetArraySafe()) {
            ModifyPlan(subplan, modify);
        }
    }
}

template<typename T>
void SetNonZero(NJson::TJsonValue& node, const TStringBuf& name, T value) {
    if (value) {
        node[name] = value;
    }
}

void BuildPlanIndex(
    const NJson::TJsonValue& plan,
    THashMap<int, const NJson::TJsonValue*>& planIndex,
    THashMap<TString, const NJson::TJsonValue*>& precomputes
) {
    if (plan.GetMapSafe().contains("PlanNodeId")){
        auto id = plan.GetMapSafe().at("PlanNodeId").GetIntegerSafe();
        planIndex[id] = &plan;
    }

    if (plan.GetMapSafe().contains("Subplan Name")) {
        const auto& precomputeName = plan.GetMapSafe().at("Subplan Name").GetStringSafe();

        auto pos = precomputeName.find("precompute");
        if (pos != TString::npos) {
            precomputes[precomputeName.substr(pos)] = &plan;
        } else if (precomputeName.size()>=4 && precomputeName.find("CTE ") != TString::npos) {
            precomputes[precomputeName.substr(4)] = &plan;
        }
    }

    if (plan.GetMapSafe().contains("Plans")) {
        for (const auto& p : plan.GetMapSafe().at("Plans").GetArraySafe()) {
            BuildPlanIndex(p, planIndex, precomputes);
        }
    }
}

TVector<NJson::TJsonValue> RemoveRedundantNodes(NJson::TJsonValue& plan, const THashSet<TString>& redundantNodes) {
    auto& planMap = plan.GetMapSafe();

    TVector<NJson::TJsonValue> children;
    if (planMap.contains("Plans") && planMap.at("Plans").IsArray()) {
        for (auto& child : planMap.at("Plans").GetArraySafe()) {
            auto newChildren = RemoveRedundantNodes(child, redundantNodes);
            children.insert(children.end(), newChildren.begin(), newChildren.end());
        }
    }

    planMap.erase("Plans");
    if (!children.empty()) {
        auto& plans = planMap["Plans"];
        for (auto& child :  children) {
            plans.AppendValue(child);
        }
    }

    if (!planMap.contains("Node Type")) {
        return {};
    }
    const auto typeName = planMap.at("Node Type").GetStringSafe();
    if (redundantNodes.contains(typeName) || typeName.find("Precompute") != TString::npos) {
        return children;
    }

    return {plan};
}

void CopySimplifiedConnectionFields(const NJson::TJsonValue& from, NJson::TJsonValue& to) {
    static const TVector<TString> fields = {
        "Blocks",
        "HashFunc",
        "KeyColumns",
        "Parallel",
        "SortBy",
        "SortColumns",
    };

    const auto& fromMap = from.GetMapSafe();
    for (const auto& field : fields) {
        if (auto it = fromMap.find(field); it != fromMap.end()) {
            to[field] = it->second;
        }
    }
}

class TQueryPlanReconstructor {
public:
    TQueryPlanReconstructor(
        const THashMap<int, const NJson::TJsonValue*>& planIndex,
        const THashMap<TString, const NJson::TJsonValue*>& precomputes
    )
        : PlanIndex(planIndex)
        , Precomputes(precomputes)
        , NodeIDCounter(0)
        , Budget(10'000)
    {}

    NJson::TJsonValue Reconstruct(
        const NJson::TJsonValue& plan
    ) {
        return ReconstructImpl(plan, 0, 0, false, nullptr, Nothing());
    }

private:
    static void ApplyCpuTime(NJson::TJsonValue& op, const TMaybe<double>& cpuTimeMs) {
        if (cpuTimeMs) {
            op["A-SelfCpu"] = *cpuTimeMs;
        }
    }

    NJson::TJsonValue ReconstructImpl(
        const NJson::TJsonValue& plan,
        int operatorIndex,
        int parentTaskCount,
        bool fromBroadcast,
        const NJson::TJsonValue* inheritedTableStats,
        TMaybe<double> inheritedCpuTimeMs
    ) {
        int currentNodeId = NodeIDCounter++;

        int taskCount = parentTaskCount;
        const NJson::TJsonValue* ownTableStats = nullptr;
        TMaybe<double> ownCpuTimeMs;
        if (plan.GetMapSafe().contains("Stats")) {
            const auto& stats = plan.GetMapSafe().at("Stats").GetMapSafe();
            if (stats.contains("Tasks")) {
                taskCount = stats.at("Tasks").GetIntegerSafe();
            }
            if (stats.contains("Table")) {
                ownTableStats = &stats.at("Table");
            }
            if (operatorIndex == 0 && stats.contains("CpuTimeUs")) {
                const auto& cpuTime = stats.at("CpuTimeUs");
                const double cpuTimeUs = cpuTime.IsMap()
                    ? cpuTime.GetMapSafe().at("Max").GetDoubleSafe()
                    : cpuTime.GetDoubleSafe();
                ownCpuTimeMs = cpuTimeUs / 1000.0;
            }
        }
        const auto effectiveCpuTimeMs = ownCpuTimeMs ? ownCpuTimeMs : inheritedCpuTimeMs;

        NJson::TJsonValue result;
        result["PlanNodeId"] = currentNodeId;

        if (--Budget <= 0) {
            YQL_CLOG(DEBUG, ProviderKqp) << "Can't build the plan - recursion depth has been exceeded!";
            return result;
        }

        if (plan.GetMapSafe().contains("PlanNodeType")) {
            result["PlanNodeType"] = plan.GetMapSafe().at("PlanNodeType").GetStringSafe();
        }

        if (plan.GetMapSafe().at("Node Type") == "TableLookupJoin" && plan.GetMapSafe().contains("Table")) {
            result["Node Type"] = "LookupJoin";
            NJson::TJsonValue newOps;
            NJson::TJsonValue op;

            op["Name"] = "Lookup";
            op["LookupKeyColumns"] = plan.GetMapSafe().at("LookupKeyColumns");
            ApplyCpuTime(op, effectiveCpuTimeMs);

            newOps.AppendValue(std::move(op));
            result["Operators"] = std::move(newOps);

            NJson::TJsonValue newPlans;

            NJson::TJsonValue lookupPlan;
            lookupPlan["Node Type"] = "TableLookup";
            lookupPlan["PlanNodeType"] = "TableLookup";

            NJson::TJsonValue lookupOps;
            NJson::TJsonValue lookupOp;

            lookupOp["Name"] = "TableLookup";
            lookupOp["Columns"] = plan.GetMapSafe().at("Columns");
            lookupOp["LookupKeyColumns"] = plan.GetMapSafe().at("LookupKeyColumns");
            lookupOp["Table"] = plan.GetMapSafe().at("Table");

            if (plan.GetMapSafe().contains("E-Cost")) {
                lookupOp["E-Cost"] = plan.GetMapSafe().at("E-Cost");
            }
            if (plan.GetMapSafe().contains("E-Rows")) {
                lookupOp["E-Rows"] = plan.GetMapSafe().at("E-Rows");
            }
            if (plan.GetMapSafe().contains("E-Size")) {
                lookupOp["E-Size"] = plan.GetMapSafe().at("E-Size");
            }

            lookupOps.AppendValue(std::move(lookupOp));
            lookupPlan["Operators"] = std::move(lookupOps);

	        if (plan.GetMapSafe().contains("Plans")) {
                newPlans.AppendValue(ReconstructImpl(plan.GetMapSafe().at("Plans").GetArraySafe()[0], 0, taskCount, false, inheritedTableStats, Nothing()));
            }

            newPlans.AppendValue(std::move(lookupPlan));

            result["Plans"] = std::move(newPlans);

            return result;
        }

        if (!plan.GetMapSafe().contains("Operators")) {
            NJson::TJsonValue planInputs;

            result["Node Type"] = plan.GetMapSafe().at("Node Type").GetStringSafe();
            if (result.GetMapSafe().contains("PlanNodeType")
                && result.GetMapSafe().at("PlanNodeType") == "Connection")
            {
                CopySimplifiedConnectionFields(plan, result);
            }

            if (plan.GetMapSafe().contains("CTE Name")) {
                auto precompute = plan.GetMapSafe().at("CTE Name").GetStringSafe();
                if (Precomputes.contains(precompute)) {
                    planInputs.AppendValue(ReconstructImpl(*Precomputes.at(precompute), 0, taskCount, false, nullptr, Nothing()));
                }
            }

            if (!plan.GetMapSafe().contains("Plans")) {
                result["Plans"] = std::move(planInputs);
                return result;
            }

            if (plan.GetMapSafe().at("Node Type").GetStringSafe() == "TableLookup") {
                NJson::TJsonValue newOps;
                NJson::TJsonValue op;

                op["Name"] = "TableLookup";
                op["Columns"] = plan.GetMapSafe().at("Columns");
                op["LookupKeyColumns"] = plan.GetMapSafe().at("LookupKeyColumns");
                op["Table"] = plan.GetMapSafe().at("Table");

                if (plan.GetMapSafe().contains("E-Cost")) {
                    op["E-Cost"] = plan.GetMapSafe().at("E-Cost");
                }
                if (plan.GetMapSafe().contains("E-Rows")) {
                    op["E-Rows"] = plan.GetMapSafe().at("E-Rows");
                }
                if (plan.GetMapSafe().contains("E-Size")) {
                    op["E-Size"] = plan.GetMapSafe().at("E-Size");
                }
                ApplyCpuTime(op, effectiveCpuTimeMs);

                newOps.AppendValue(std::move(op));

                result["Operators"] = std::move(newOps);
                return result;
            }

            const auto effectiveTableStats = ownTableStats ? ownTableStats : inheritedTableStats;
            const auto isRegularPlan = [](const NJson::TJsonValue& child) {
                const auto& childMap = child.GetMapSafe();
                const bool isCte = !childMap.contains("Operators") && childMap.contains("CTE Name");
                return !isCte && childMap.at("Node Type").GetStringSafe().find("Precompute") == TString::npos;
            };
            TMaybe<double> childCpuTimeMs;
            if (effectiveCpuTimeMs) {
                size_t regularPlansCount = 0;
                for (const auto& p : plan.GetMapSafe().at("Plans").GetArraySafe()) {
                    if (isRegularPlan(p)) {
                        ++regularPlansCount;
                    }
                }
                if (regularPlansCount == 1) {
                    childCpuTimeMs = effectiveCpuTimeMs;
                }
            }
            for (const auto& p : plan.GetMapSafe().at("Plans").GetArraySafe()) {
                if (!p.GetMapSafe().contains("Operators") && p.GetMapSafe().contains("CTE Name")) {
                    auto precompute = p.GetMapSafe().at("CTE Name").GetStringSafe();
                    if (Precomputes.contains(precompute)) {
                        planInputs.AppendValue(ReconstructImpl(*Precomputes.at(precompute), 0, taskCount, false, nullptr, Nothing()));
                    }
                } else if (isRegularPlan(p)) {
                    planInputs.AppendValue(ReconstructImpl(p, 0, taskCount, fromBroadcast, effectiveTableStats, childCpuTimeMs));
                }
            }
            result["Plans"] = planInputs;
            return result;
        }

        if (plan.GetMapSafe().contains("CTE Name") && plan.GetMapSafe().at("Node Type").GetStringSafe() == "ConstantExpr") {
            auto precompute = plan.GetMapSafe().at("CTE Name").GetStringSafe();
            if (!Precomputes.contains(precompute)) {
                result["Node Type"] = plan.GetMapSafe().at("Node Type");
                return result;
            }

            return ReconstructImpl(*Precomputes.at(precompute), 0, taskCount, false, nullptr, Nothing());
        }

        auto ops = plan.GetMapSafe().at("Operators").GetArraySafe();
        auto op = ops[operatorIndex];

        TVector<NJson::TJsonValue> planInputs;

        auto opName = op.GetMapSafe().at("Name").GetStringSafe();

        THashSet<ui32> processedExternalOperators;
        THashSet<ui32> processedInternalOperators;
        for (auto opInput : op.GetMapSafe().at("Inputs").GetArraySafe()) {

            if (opInput.GetMapSafe().contains("ExternalPlanNodeId")) {
                auto inputPlanKey = opInput.GetMapSafe().at("ExternalPlanNodeId").GetIntegerSafe();

                if (processedExternalOperators.contains(inputPlanKey)) {
                    continue;
                }
                processedExternalOperators.insert(inputPlanKey);

                const auto& inputPlan = *PlanIndex.at(inputPlanKey);
                planInputs.push_back( ReconstructImpl(inputPlan, 0, taskCount, inputPlan.GetMapSafe().at("Node Type").GetStringSafe() == "Broadcast", ownTableStats, Nothing()) );
            } else if (opInput.GetMapSafe().contains("InternalOperatorId")) {
                auto inputPlanId = opInput.GetMapSafe().at("InternalOperatorId").GetIntegerSafe();

                if (processedInternalOperators.contains(inputPlanId)) {
                    continue;
                }
                processedInternalOperators.insert(inputPlanId);

                planInputs.push_back( ReconstructImpl(plan, inputPlanId, taskCount, false, inheritedTableStats, Nothing()) );
            }
        }

        if (op.GetMapSafe().contains("Inputs")) {
            op.GetMapSafe().erase("Inputs");
        }

        if (
            op.GetMapSafe().contains("Input")
            || op.GetMapSafe().contains("ToFlow")
            || op.GetMapSafe().contains("Member")
            || op.GetMapSafe().contains("AssumeSorted")
            || op.GetMapSafe().contains("Iterator")
            || op.GetMapSafe().contains("CombineByKey")
        ) {

            TString maybePrecompute = "";
            if (op.GetMapSafe().contains("Input")) {
                maybePrecompute = op.GetMapSafe().at("Input").GetStringSafe();
            } else if (op.GetMapSafe().contains("ToFlow")) {
                maybePrecompute = op.GetMapSafe().at("ToFlow").GetStringSafe();
            } else if (op.GetMapSafe().contains("Member")) {
                maybePrecompute = op.GetMapSafe().at("Member").GetStringSafe();
            } else if (op.GetMapSafe().contains("AssumeSorted")) {
                maybePrecompute = op.GetMapSafe().at("AssumeSorted").GetStringSafe();
            } else if (op.GetMapSafe().contains("Iterator")) {
                maybePrecompute = op.GetMapSafe().at("Iterator").GetStringSafe();
            } else if (op.GetMapSafe().contains("CombineByKey")) {
                maybePrecompute = op.GetMapSafe().at("CombineByKey").GetStringSafe();
            }

            if (Precomputes.contains(maybePrecompute) && planInputs.empty()) {
                planInputs.push_back(ReconstructImpl(*Precomputes.at(maybePrecompute), 0, taskCount, false, nullptr, Nothing()));
            }
        }

        result["Node Type"] = opName;

        auto operatorSize = false;
        auto operatorRows = false;
        const auto applyTableStats = [&](const NJson::TJsonValue& tableStats) {
            TString tablePath;
            if (op.GetMapSafe().contains("Path")) {
                tablePath = op.GetMapSafe().at("Path").GetStringSafe();
            } else if (op.GetMapSafe().contains("Table")) {
                tablePath = op.GetMapSafe().at("Table").GetStringSafe();
            }
            if (tablePath) {
                for (auto& opStat : tableStats.GetArraySafe()) {
                    if (opStat.IsMap()) {
                        auto& opMap = opStat.GetMapSafe();
                        if (opMap.contains("Path") && opMap.at("Path").GetStringSafe() == tablePath) {
                            if (opMap.contains("ReadRows")) {
                                op["A-Rows"] = opMap.at("ReadRows").GetMapSafe().at("Sum").GetDouble();
                                operatorRows = true;
                            }
                            if (opMap.contains("ReadBytes")) {
                                op["A-Size"] = opMap.at("ReadBytes").GetMapSafe().at("Sum").GetDouble();
                                operatorSize = true;
                            }
                            break;
                        }
                    }
                }
            }
        };

        if (plan.GetMapSafe().contains("Stats")) {
            const auto& stats = plan.GetMapSafe().at("Stats").GetMapSafe();

            TString opType;
            TString opId = "0";

            if (opName.Contains("Join (")) {
                opType = "Join";
            } else if (opName == "Filter") {
                opType = "Filter";
            } else if (opName == "Aggregate") {
                opType = "Aggregation";
            }

            if (opType) {
                if (op.GetMapSafe().contains("Id")) {
                    opId = op["Id"].GetStringSafe();
                }

                for (ui32 i = 0; i < ops.size(); i++) {
                    if (i != static_cast<ui32>(operatorIndex)) {
                        auto op1 = ops[i];
                        auto op1Name = op1.GetMapSafe().at("Name").GetStringSafe();
                        TString op1Type;
                        TString op1Id = "0";

                        if (op1Name.Contains("Join (")) {
                            op1Type = "Join";
                        } else if (op1Name == "Filter") {
                            if (!op1.GetMapSafe().contains("Pushdown") || op1.GetMapSafe().at("Pushdown").GetStringSafe() != "True") {
                                op1Type = "Filter";
                            }
                        } else if (op1Name == "Aggregate") {
                            op1Type = "Aggregation";
                        }

                        if (op1Type == opType) {
                            if (op1.GetMapSafe().contains("Id")) {
                                op1Id = op1["Id"].GetStringSafe();
                            }

                            if (opId == op1Id) {
                                // colission detected, do not apply stats
                                opType = "";
                                break;
                            }
                        }
                    }
                }
            }

            if (opName == "TableFullScan" && ownTableStats) {
                applyTableStats(*ownTableStats);
            }

            if (opType && stats.contains("Operator")) {
                for (auto& opStat : stats.at("Operator").GetArraySafe()) {
                    if (opStat.IsMap()) {
                        auto& opMap = opStat.GetMapSafe();
                        if (opMap.contains("Type") && opMap.at("Type").GetStringSafe() == opType
                            && opMap.contains("Id") && opMap.at("Id").GetStringSafe() == opId) {

                            if (opMap.contains("Rows")) {
                                op["A-Rows"] = opMap.at("Rows").GetMapSafe().at("Sum").GetDouble();
                                operatorRows = true;
                            }
                            if (opMap.contains("Bytes")) {
                                op["A-Size"] = opMap.at("Bytes").GetMapSafe().at("Sum").GetDouble();
                                operatorSize = true;
                            }
                            break;
                        }
                    }
                }
            }

            if (operatorIndex == 0 && !op.GetMapSafe().contains("A-Rows")) {
                // top level rows/size have to match stage output
                if (!operatorRows && stats.contains("OutputRows")) {
                    auto outputRows = stats.at("OutputRows");
                    i64 aRows = outputRows.IsMap() ? outputRows.GetMapSafe().at("Sum").GetIntegerSafe() : outputRows.GetIntegerSafe();
                    if (fromBroadcast && parentTaskCount && (aRows % parentTaskCount == 0)) {
                        aRows /= parentTaskCount;
                    }
                    op["A-Rows"] = aRows;
                }
                if (!operatorSize && stats.contains("OutputBytes")) {
                    auto outputBytes = stats.at("OutputBytes");
                    double aSize = outputBytes.IsMap() ? outputBytes.GetMapSafe().at("Sum").GetDouble() : outputBytes.GetDouble();
                    if (fromBroadcast && parentTaskCount) {
                        aSize /= parentTaskCount;
                    }
                    op["A-Size"] = aSize;
                }
            }
        }

        if (operatorIndex == 0) {
            ApplyCpuTime(op, effectiveCpuTimeMs);
        }

        if (opName == "TableFullScan" && !ownTableStats && inheritedTableStats) {
            applyTableStats(*inheritedTableStats);
        }

        // erase some redundant info from the table scan
        if (op.IsMap()) {
            auto& map = op.GetMapSafe();

            if (map.contains("Table") && map.contains("Path")) {
                TFsPath path = map.at("Path").GetString();
                op["Table"] = path.GetName();
                map.erase("Path");
            }

            if (map.contains("Scan")) {
                map.erase("Scan");
            }

            if (map.contains("ReadRangesPointPrefixLen")) {
                map.erase("ReadRangesPointPrefixLen");
            }
        }

        NJson::TJsonValue newOps;
        newOps.AppendValue(std::move(op));
        result["Operators"] = std::move(newOps);

        if (!planInputs.empty()){
            NJson::TJsonValue plans;
            for(auto&& i : planInputs) {
                plans.AppendValue(std::move(i));
            }
            result["Plans"] = std::move(plans);
        }

        return result;
    }

private:
    const THashMap<int, const NJson::TJsonValue*>& PlanIndex;
    const THashMap<TString, const NJson::TJsonValue*>& Precomputes;
    ui32 NodeIDCounter;
    i32 Budget; // Prevent bugs with inf recursion
};

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

NJson::TJsonValue SimplifyQueryPlan(NJson::TJsonValue& plan) {
     static const THashSet<TString> redundantNodes = {
        "UnionAll",
        "Broadcast",
        "Map",
        "Merge",
        "Collect",
        "Stage",
        "Iterator",
        "PartitionByKey",
        "ToFlow",
        "Member",
        "AssumeSorted",
        "CombineByKey"
    };

    NJson::TJsonValue reconstructedPlan;
    {
        THashMap<int, const NJson::TJsonValue*> planIndex;
        THashMap<TString, const NJson::TJsonValue*> precomputes;

        BuildPlanIndex(plan, planIndex, precomputes);

        TQueryPlanReconstructor reconstructor(planIndex, precomputes);
        reconstructedPlan = reconstructor.Reconstruct(plan);
    }
    plan = std::move(reconstructedPlan);

    RemoveRedundantNodes(plan, redundantNodes);
    ComputeCpuTimes(plan);

    return plan;
}

TString AddSimplifiedPlan(const TString& planText, bool analyzeMode, const NJson::TJsonValue& optimizerStats = {}) {
    Y_UNUSED(analyzeMode);
    NJson::TJsonValue planJson;
    NJson::ReadJsonTree(planText, &planJson, true);
    if (!planJson.GetMapSafe().contains("Plan")){
        return planText;
    }

    NJson::TJsonValue planCopy;
    NJson::ReadJsonTree(planText, &planCopy, true);

    auto simplifiedPlan = SimplifyQueryPlan(planCopy.GetMapSafe().at("Plan"));
    if (optimizerStats.IsDefined()) {
        simplifiedPlan["OptimizerStats"] = optimizerStats;
    }
    planJson["SimplifiedPlan"] = simplifiedPlan;

    return planJson.GetStringRobust();
}

} // anonymous namespace

TString SerializeTxPlans(
    const TVector<const TString>& txPlans,
    const TString commonPlanInfo,
    const TString& queryStats,
    const NJson::TJsonValue& optimizerStats)
{
    YQL_CVLOG(NYql::NLog::ELevel::TRACE, NYql::NLog::EComponent::Core) << "JSON_PLAN: SerializeTxPlans";
    NJsonWriter::TBuf writer;
    writer.SetIndentSpaces(2);

    writer.BeginObject();
    writer.WriteKey("meta");
    writer.BeginObject();
    writer.WriteKey("version").WriteString("0.2");
    writer.WriteKey("type").WriteString("query");
    writer.EndObject();

    if (!commonPlanInfo.empty()) {
        NJson::TJsonValue commonPlanJson;
        NJson::ReadJsonTree(commonPlanInfo, &commonPlanJson, true);

        writer.WriteKey("tables");
        writer.WriteJsonValue(&commonPlanJson, false, PREC_NDIGITS, 17);
    }

    writer.WriteKey("Plan");
    writer.BeginObject();
    writer.WriteKey("Node Type").WriteString("Query");
    writer.WriteKey("PlanNodeType").WriteString("Query");

    if (queryStats) {
        NJson::TJsonValue queryStatsJson;
        NJson::ReadJsonTree(queryStats, &queryStatsJson, true);

        writer.WriteKey("Stats");
        writer.WriteJsonValue(&queryStatsJson, false, PREC_NDIGITS, 17);
    }

    writer.WriteKey("Plans");
    writer.BeginList();

    auto removeStageGuid = [](NJson::TJsonValue& node) {
        auto& map = node.GetMapSafe();
        if (map.contains("StageGuid")) {
            map.erase("StageGuid");
        }
    };

    for (auto txPlanIt = txPlans.rbegin(); txPlanIt != txPlans.rend(); ++txPlanIt) {
        if (txPlanIt->empty()) {
            continue;
        }

        NJson::TJsonValue txPlanJson;
        NJson::ReadJsonTree(*txPlanIt, &txPlanJson, true);

        for (auto& subplan : txPlanJson.GetMapSafe().at("Plans").GetArraySafe()) {
            ModifyPlan(subplan, removeStageGuid);
            writer.WriteJsonValue(&subplan, true, PREC_NDIGITS, 17);
        }
    }

    writer.EndList();
    writer.EndObject();
    writer.EndObject();

    auto resultPlan =  writer.Str();
    return AddSimplifiedPlan(resultPlan, false, optimizerStats);
}

void FillAggrStat(NJson::TJsonValue& node, const NYql::NDqProto::TDqStatsAggr& aggr, const TString& name, bool keepZero = false) {
    auto min = aggr.GetMin();
    auto max = aggr.GetMax();
    auto sum = aggr.GetSum();
    if (keepZero || min || max || sum) {
        auto& aggrStat = node.InsertValue(name, NJson::JSON_MAP);
        aggrStat["Min"] = min;
        aggrStat["Max"] = max;
        aggrStat["Sum"] = sum;
        aggrStat["Count"] = aggr.GetCnt();
        if (aggr.GetHistory().size()) {
            auto& aggrHistory = aggrStat.InsertValue("History", NJson::JSON_ARRAY);
            for (auto& h : aggr.GetHistory()) {
                aggrHistory.AppendValue(h.GetTimeMs());
                aggrHistory.AppendValue(h.GetValue());
            }
        }
    }
}

void FillAsyncAggrStat(NJson::TJsonValue& node, const NYql::NDqProto::TDqAsyncStatsAggr& asyncAggr) {
    if (asyncAggr.HasBytes()) {
        FillAggrStat(node, asyncAggr.GetBytes(), "Bytes");
    }
    if (asyncAggr.HasDecompressedBytes()) {
        FillAggrStat(node, asyncAggr.GetDecompressedBytes(), "DecompressedBytes");
    }
    if (asyncAggr.HasRows()) {
        FillAggrStat(node, asyncAggr.GetRows(), "Rows");
    }
    if (asyncAggr.HasChunks()) {
        FillAggrStat(node, asyncAggr.GetChunks(), "Chunks");
    }
    if (asyncAggr.HasSplits()) {
        FillAggrStat(node, asyncAggr.GetSplits(), "Splits");
    }

    if (asyncAggr.HasFirstMessageMs()) {
        FillAggrStat(node, asyncAggr.GetFirstMessageMs(), "FirstMessageMs");
    }
    if (asyncAggr.HasPauseMessageMs()) {
        FillAggrStat(node, asyncAggr.GetPauseMessageMs(), "PauseMessageMs");
    }
    if (asyncAggr.HasResumeMessageMs()) {
        FillAggrStat(node, asyncAggr.GetResumeMessageMs(), "ResumeMessageMs");
    }
    if (asyncAggr.HasLastMessageMs()) {
        FillAggrStat(node, asyncAggr.GetLastMessageMs(), "LastMessageMs");
    }

    if (asyncAggr.HasWaitTimeUs()) {
        FillAggrStat(node, asyncAggr.GetWaitTimeUs(), "WaitTimeUs");
    }
    if (asyncAggr.HasWaitPeriods()) {
        FillAggrStat(node, asyncAggr.GetWaitPeriods(), "WaitPeriods");
    }
    if (asyncAggr.HasActiveTimeUs()) {
        FillAggrStat(node, asyncAggr.GetActiveTimeUs(), "ActiveTimeUs");
    }
    if (asyncAggr.HasFirstMessageMs() && asyncAggr.HasLastMessageMs()) {
        auto firstMessageMs = asyncAggr.GetFirstMessageMs().GetMin();
        auto lastMessageMs = asyncAggr.GetLastMessageMs().GetMax();
        if (firstMessageMs && lastMessageMs > firstMessageMs) {
            auto& aggrStat = node.InsertValue("ActiveMessageMs", NJson::JSON_MAP);
            aggrStat["Min"] = firstMessageMs;
            aggrStat["Max"] = lastMessageMs;
            aggrStat["Count"] = asyncAggr.GetFirstMessageMs().GetCnt();
        }
    }
    if (asyncAggr.HasPauseMessageMs() && asyncAggr.HasResumeMessageMs()) {
        auto firstMessageMs = asyncAggr.GetPauseMessageMs().GetMin();
        auto lastMessageMs = asyncAggr.GetResumeMessageMs().GetMax();
        if (firstMessageMs && lastMessageMs > firstMessageMs) {
            auto& aggrStat = node.InsertValue("WaitMessageMs", NJson::JSON_MAP);
            aggrStat["Min"] = firstMessageMs;
            aggrStat["Max"] = lastMessageMs;
            aggrStat["Count"] = asyncAggr.GetPauseMessageMs().GetCnt();
        }
    }
}

TString AddExecStatsToTxPlan(const TString& txPlanJson, const NYql::NDqProto::TDqExecutionStats& stats, bool newRboEnabled) {
    if (txPlanJson.empty()) {
        return {};
    }

    YQL_CVLOG(NYql::NLog::ELevel::TRACE, NYql::NLog::EComponent::Core) << "JSON_PLAN: AddExecStatsToTxPlan";

    THashMap<TProtoStringType, const NYql::NDqProto::TDqStageStats*> stages;
    THashMap<ui32, TString> stageIdToGuid;
    for (const auto& stage : stats.GetStages()) {
        stages[stage.GetStageGuid()] = &stage;
        stageIdToGuid[stage.GetStageId()] = stage.GetStageGuid();
    }

    NJson::TJsonValue root;
    NJson::ReadJsonTree(txPlanJson, &root, true);

    auto fillInputStats = [](NJson::TJsonValue& node, const NYql::NDqProto::TDqInputChannelStats& inputStats) {
        node["ChannelId"] = inputStats.GetChannelId();
        node["SrcStageId"] = inputStats.GetSrcStageId();

        SetNonZero(node, "Bytes", inputStats.GetPush().GetBytes());
        SetNonZero(node, "Rows", inputStats.GetPush().GetRows());

        SetNonZero(node, "WaitTimeUs", inputStats.GetPush().GetWaitTimeUs());
    };

    auto fillOutputStats = [](NJson::TJsonValue& node, const NYql::NDqProto::TDqOutputChannelStats& outputStats) {
        node["ChannelId"] = outputStats.GetChannelId();
        node["DstStageId"] = outputStats.GetDstStageId();

        SetNonZero(node, "Bytes", outputStats.GetPop().GetBytes());
        SetNonZero(node, "Rows", outputStats.GetPop().GetRows());

        SetNonZero(node, "WaitTimeUs", outputStats.GetPop().GetWaitTimeUs());
        SetNonZero(node, "SpilledBytes", outputStats.GetSpilledBytes());
    };

    auto fillTaskStats = [&](NJson::TJsonValue& node, const NYql::NDqProto::TDqTaskStats& taskStats) {
        node["TaskId"] = taskStats.GetTaskId();

        SetNonZero(node, "Host", taskStats.GetHostName());
        SetNonZero(node, "NodeId", taskStats.GetNodeId());
        SetNonZero(node, "InputRows", taskStats.GetInputRows());
        SetNonZero(node, "InputBytes", taskStats.GetInputBytes());
        SetNonZero(node, "OutputRows", taskStats.GetOutputRows());
        SetNonZero(node, "OutputBytes", taskStats.GetOutputBytes());
        SetNonZero(node, "ResultRows", taskStats.GetResultRows());
        SetNonZero(node, "ResultBytes", taskStats.GetResultBytes());
        SetNonZero(node, "IngressRows", taskStats.GetIngressRows());
        SetNonZero(node, "IngressBytes", taskStats.GetIngressBytes());
        SetNonZero(node, "IngressDecompressedBytes", taskStats.GetIngressDecompressedBytes());
        SetNonZero(node, "EgressRows", taskStats.GetEgressRows());
        SetNonZero(node, "EgressBytes", taskStats.GetEgressBytes());

        SetNonZero(node, "StartTimeMs", taskStats.GetStartTimeMs());   // need to be reviewed
        SetNonZero(node, "FinishTimeMs", taskStats.GetFinishTimeMs()); // need to be reviewed

        SetNonZero(node, "ComputeTimeUs", taskStats.GetComputeCpuTimeUs());

        SetNonZero(node, "WaitInputTimeUs", taskStats.GetWaitInputTimeUs());
        SetNonZero(node, "WaitOutputTimeUs", taskStats.GetWaitOutputTimeUs());

        NKqpProto::TKqpTaskExtraStats taskExtraStats;
        if (taskStats.GetExtra().UnpackTo(&taskExtraStats)) {
            SetNonZero(node, "ScanRetries", taskExtraStats.GetScanTaskExtraStats().GetRetriesCount());
        }

        for (auto& inputStats : taskStats.GetInputChannels()) {
            auto& inputNode = node["InputChannels"].AppendValue(NJson::TJsonValue());
            fillInputStats(inputNode, inputStats);
        }

        for (auto& outputStats : taskStats.GetOutputChannels()) {
            auto& outputNode = node["OutputChannels"].AppendValue(NJson::TJsonValue());
            fillOutputStats(outputNode, outputStats);
        }
    };

    auto fillCaStats = [&](NJson::TJsonValue& node, const NYql::NDqProto::TDqComputeActorStats& caStats) {
        SetNonZero(node, "CpuTimeUs", caStats.GetCpuTimeUs());
        SetNonZero(node, "DurationUs", caStats.GetDurationUs());

        SetNonZero(node, "PeakMemoryUsageBytes", caStats.GetMkqlMaxMemoryUsage());

        for (auto& taskStats : caStats.GetTasks()) {
            auto& taskNode = node["Tasks"].AppendValue(NJson::TJsonValue());
            fillTaskStats(taskNode, taskStats);
        }
    };

    THashMap<TString, ui32> guidToPlaneId;

    auto collectPlanNodeId = [&](NJson::TJsonValue& node) {
        if (auto stageGuid = node.GetMapSafe().FindPtr("StageGuid")) {
            if (auto planNodeId = node.GetMapSafe().FindPtr("PlanNodeId")) {
                guidToPlaneId[stageGuid->GetStringSafe()] = planNodeId->GetIntegerSafe();
            }
        }
    };

    auto addStatsToPlanNode = [&](NJson::TJsonValue& node) {
        if (auto stageGuid = node.GetMapSafe().FindPtr("StageGuid")) {
            if (auto stat = stages.FindPtr(stageGuid->GetStringSafe())) {
                auto& stats = node["Stats"];
                if ((*stat)->HasUseLlvm()) {
                    stats["UseLlvm"] = (*stat)->GetUseLlvm();
                } else {
                    stats["UseLlvm"] = "undefined";
                }

                stats["PhysicalStageId"] = (*stat)->GetStageId();
                stats["Tasks"] = (*stat)->GetTotalTasksCount();
                stats["FinishedTasks"] = (*stat)->GetFinishedTasksCount();
                if (!(*stat)->GetNodes().empty()) {
                    auto& nodesStats = stats.InsertValue("Nodes", NJson::JSON_ARRAY);
                    for (const auto& node : (*stat)->GetNodes()) {
                        auto& nodeInfo = nodesStats.AppendValue(NJson::JSON_MAP);
                        nodeInfo["NodeId"] = node.GetNodeId();
                        nodeInfo["Tasks"] = node.GetTasks();
                        nodeInfo["Finished"] = node.GetFinished();
                    }
                }
                if (auto updateTimeUs = (*stat)->GetUpdateTimeMs(); updateTimeUs) {
                    stats["UpdateTimeMs"] = updateTimeUs;
                }

                auto& introspections = stats.InsertValue("Introspections", NJson::JSON_ARRAY);
                for (const auto& intro : (*stat)->GetIntrospections()) {
                    introspections.AppendValue(intro);
                }

                stats["StageDurationUs"] = (*stat)->GetStageDurationUs();

                if ((*stat)->GetBaseTimeMs()) {
                    stats["BaseTimeMs"] = (*stat)->GetBaseTimeMs();
                }

                if ((*stat)->HasDurationUs()) {
                    FillAggrStat(stats, (*stat)->GetDurationUs(), "DurationUs");
                }
                if ((*stat)->HasWaitInputTimeUs()) {
                    FillAggrStat(stats, (*stat)->GetWaitInputTimeUs(), "WaitInputTimeUs");
                }
                if ((*stat)->HasWaitOutputTimeUs()) {
                    FillAggrStat(stats, (*stat)->GetWaitOutputTimeUs(), "WaitOutputTimeUs");
                }
                if ((*stat)->HasInputRows()) {
                    FillAggrStat(stats, (*stat)->GetInputRows(), "InputRows");
                }
                if ((*stat)->HasInputBytes()) {
                    FillAggrStat(stats, (*stat)->GetInputBytes(), "InputBytes");
                }
                if ((*stat)->HasOutputRows()) {
                    // A reported zero is distinct from a missing row counter
                    // when RBO correlates operators with execution statistics.
                    FillAggrStat(stats, (*stat)->GetOutputRows(), "OutputRows", newRboEnabled);
                }
                if ((*stat)->HasOutputBytes()) {
                    FillAggrStat(stats, (*stat)->GetOutputBytes(), "OutputBytes");
                }
                if ((*stat)->HasResultRows()) {
                    FillAggrStat(stats, (*stat)->GetResultRows(), "ResultRows");
                }
                if ((*stat)->HasResultBytes()) {
                    FillAggrStat(stats, (*stat)->GetResultBytes(), "ResultBytes");
                }
                if ((*stat)->HasIngressRows()) {
                    FillAggrStat(stats, (*stat)->GetIngressRows(), "IngressRows");
                }
                if ((*stat)->HasIngressBytes()) {
                    FillAggrStat(stats, (*stat)->GetIngressBytes(), "IngressBytes");
                }
                if ((*stat)->HasIngressDecompressedBytes()) {
                    FillAggrStat(stats, (*stat)->GetIngressDecompressedBytes(), "IngressDecompressedBytes");
                }
                if ((*stat)->HasEgressRows()) {
                    FillAggrStat(stats, (*stat)->GetEgressRows(), "EgressRows");
                }
                if ((*stat)->HasEgressBytes()) {
                    FillAggrStat(stats, (*stat)->GetEgressBytes(), "EgressBytes");
                }
                if ((*stat)->HasCpuTimeUs()) {
                    FillAggrStat(stats, (*stat)->GetCpuTimeUs(), "CpuTimeUs");
                }
                if ((*stat)->HasSourceCpuTimeUs()) {
                    FillAggrStat(stats, (*stat)->GetSourceCpuTimeUs(), "SourceCpuTimeUs");
                }
                if ((*stat)->HasMemoryUsageMB()) {
                    FillAggrStat(stats, (*stat)->GetMemoryUsageMB(), "MemoryUsageMB");
                }
                if ((*stat)->HasMaxMemoryUsage()) {
                    FillAggrStat(stats, (*stat)->GetMaxMemoryUsage(), "MaxMemoryUsage");
                }
                if ((*stat)->HasSpillingComputeBytes()) {
                    FillAggrStat(stats, (*stat)->GetSpillingComputeBytes(), "SpillingComputeBytes");
                }
                if ((*stat)->HasSpillingChannelBytes()) {
                    FillAggrStat(stats, (*stat)->GetSpillingChannelBytes(), "SpillingChannelBytes");
                }
                if ((*stat)->HasSpillingComputeTimeUs()) {
                    FillAggrStat(stats, (*stat)->GetSpillingComputeTimeUs(), "SpillingComputeTimeUs");
                }
                if ((*stat)->HasSpillingChannelTimeUs()) {
                    FillAggrStat(stats, (*stat)->GetSpillingChannelTimeUs(), "SpillingChannelTimeUs");
                }

                if (!(*stat)->GetIngress().empty()) {
                    auto& ingressStats = stats.InsertValue("Ingress", NJson::JSON_ARRAY);
                    for (auto ingress : (*stat)->GetIngress()) {
                        auto& ingressInfo = ingressStats.AppendValue(NJson::JSON_MAP);
                        ingressInfo["Name"] = ingress.first;
                        if (ingress.second.HasExternal()) {
                            auto& node = ingressInfo.InsertValue("External", NJson::JSON_MAP);
                            auto& externalInfo = ingress.second.GetExternal();
                            if (externalInfo.HasExternalRows()) {
                                FillAggrStat(node, externalInfo.GetExternalRows(), "ExternalRows");
                            }
                            if (externalInfo.HasExternalBytes()) {
                                FillAggrStat(node, externalInfo.GetExternalBytes(), "ExternalBytes");
                            }
                            if (externalInfo.HasStorageRows()) {
                                FillAggrStat(node, externalInfo.GetStorageRows(), "StorageRows");
                            }
                            if (externalInfo.HasStorageBytes()) {
                                FillAggrStat(node, externalInfo.GetStorageBytes(), "StorageBytes");
                            }
                            if (externalInfo.HasCpuTimeUs()) {
                                FillAggrStat(node, externalInfo.GetCpuTimeUs(), "CpuTimeUs");
                            }
                            if (externalInfo.HasWaitInputTimeUs()) {
                                FillAggrStat(node, externalInfo.GetWaitInputTimeUs(), "WaitInputTimeUs");
                            }
                            if (externalInfo.HasWaitOutputTimeUs()) {
                                FillAggrStat(node, externalInfo.GetWaitOutputTimeUs(), "WaitOutputTimeUs");
                            }
                            if (externalInfo.HasFirstMessageMs()) {
                                FillAggrStat(node, externalInfo.GetFirstMessageMs(), "FirstMessageMs");
                            }
                            if (externalInfo.HasLastMessageMs()) {
                                FillAggrStat(node, externalInfo.GetLastMessageMs(), "LastMessageMs");
                            }
                            SetNonZero(node, "PartitionCount", externalInfo.GetPartitionCount());
                            SetNonZero(node, "FinishedPartitionCount", externalInfo.GetFinishedPartitionCount());
                        }
                        if (ingress.second.HasIngress()) {
                            FillAsyncAggrStat(ingressInfo.InsertValue("Ingress", NJson::JSON_MAP), ingress.second.GetIngress());
                        }
                        if (ingress.second.HasPush()) {
                            FillAsyncAggrStat(ingressInfo.InsertValue("Push", NJson::JSON_MAP), ingress.second.GetPush());
                        }
                        if (ingress.second.HasPop()) {
                            FillAsyncAggrStat(ingressInfo.InsertValue("Pop", NJson::JSON_MAP), ingress.second.GetPop());
                        }
                    }
                }
                if (!(*stat)->GetInput().empty()) {
                    auto& inputStats = stats.InsertValue("Input", NJson::JSON_ARRAY);
                    for (auto input : (*stat)->GetInput()) {
                        auto stageGuid = stageIdToGuid.at(input.first);
                        auto planNodeIdIt = guidToPlaneId.find(stageGuid);
                        if (planNodeIdIt == guidToPlaneId.end()) {
                            AFL_ENSURE(newRboEnabled)("stage_guid", stageGuid);
                            continue;
                        }
                        auto& inputInfo = inputStats.AppendValue(NJson::JSON_MAP);
                        auto planNodeId = planNodeIdIt->second;
                        inputInfo["Name"] = ToString(planNodeId);
                        if (input.second.HasPush()) {
                            FillAsyncAggrStat(inputInfo.InsertValue("Push", NJson::JSON_MAP), input.second.GetPush());
                        }
                        if (input.second.HasPop()) {
                            FillAsyncAggrStat(inputInfo.InsertValue("Pop", NJson::JSON_MAP), input.second.GetPop());
                        }
                        if (auto localBytes = input.second.GetLocalBytes()) {
                            inputInfo["LocalBytes"] = localBytes;
                        }
                    }
                }
                if (!(*stat)->GetOutput().empty()) {
                    auto& outputStats = stats.InsertValue("Output", NJson::JSON_ARRAY);
                    for (auto output : (*stat)->GetOutput()) {
                        TString outputName;
                        if (output.first == 0) {
                            outputName = "RESULT";
                        } else {
                            auto stageGuid = stageIdToGuid.at(output.first);
                            auto planNodeIdIt = guidToPlaneId.find(stageGuid);
                            if (planNodeIdIt == guidToPlaneId.end()) {
                                AFL_ENSURE(newRboEnabled)("stage_guid", stageGuid);
                                continue;
                            }
                            auto planNodeId = planNodeIdIt->second;
                            outputName = ToString(planNodeId);
                        }
                        auto& outputInfo = outputStats.AppendValue(NJson::JSON_MAP);
                        outputInfo["Name"] = outputName;
                        if (output.second.HasPush()) {
                            FillAsyncAggrStat(outputInfo.InsertValue("Push", NJson::JSON_MAP), output.second.GetPush());
                        }
                        if (output.second.HasPop()) {
                            FillAsyncAggrStat(outputInfo.InsertValue("Pop", NJson::JSON_MAP), output.second.GetPop());
                        }
                        if (auto localBytes = output.second.GetLocalBytes()) {
                            outputInfo["LocalBytes"] = localBytes;
                        }
                    }
                }
                if (!(*stat)->GetEgress().empty()) {
                    auto& egressStats = stats.InsertValue("Egress", NJson::JSON_ARRAY);
                    for (auto egress : (*stat)->GetEgress()) {
                        auto& egressInfo = egressStats.AppendValue(NJson::JSON_MAP);
                        egressInfo["Name"] = egress.first;
                        if (egress.second.HasPush()) {
                            FillAsyncAggrStat(egressInfo.InsertValue("Push", NJson::JSON_MAP), egress.second.GetPush());
                        }
                        if (egress.second.HasPop()) {
                            FillAsyncAggrStat(egressInfo.InsertValue("Pop", NJson::JSON_MAP), egress.second.GetPop());
                        }
                        if (egress.second.HasEgress()) {
                            FillAsyncAggrStat(egressInfo.InsertValue("Egress", NJson::JSON_MAP), egress.second.GetEgress());
                        }
                    }
                }
                if (!(*stat)->GetOperatorJoin().empty() || !(*stat)->GetOperatorFilter().empty() || !(*stat)->GetOperatorAggregation().empty()) {
                    auto& operatorStats = stats.InsertValue("Operator", NJson::JSON_ARRAY);
                    for (auto& [id, op] : (*stat)->GetOperatorJoin()) {
                        auto& operatorInfo = operatorStats.AppendValue(NJson::JSON_MAP);
                        operatorInfo["Type"] = "Join";
                        operatorInfo["Id"] = id;
                        if (op.HasBytes()) {
                            FillAggrStat(operatorInfo, op.GetBytes(), "Bytes");
                        }
                        if (op.HasRows()) {
                            FillAggrStat(operatorInfo, op.GetRows(), "Rows", newRboEnabled);
                        }
                    }
                    for (auto& [id, op] : (*stat)->GetOperatorFilter()) {
                        auto& operatorInfo = operatorStats.AppendValue(NJson::JSON_MAP);
                        operatorInfo["Type"] = "Filter";
                        operatorInfo["Id"] = id;
                        if (op.HasBytes()) {
                            FillAggrStat(operatorInfo, op.GetBytes(), "Bytes");
                        }
                        if (op.HasRows()) {
                            FillAggrStat(operatorInfo, op.GetRows(), "Rows", newRboEnabled);
                        }
                    }
                    for (auto& [id, op] : (*stat)->GetOperatorAggregation()) {
                        auto& operatorInfo = operatorStats.AppendValue(NJson::JSON_MAP);
                        operatorInfo["Type"] = "Aggregation";
                        operatorInfo["Id"] = id;
                        if (op.HasBytes()) {
                            FillAggrStat(operatorInfo, op.GetBytes(), "Bytes");
                        }
                        if (op.HasRows()) {
                            FillAggrStat(operatorInfo, op.GetRows(), "Rows", newRboEnabled);
                        }
                    }
                }
                if (!(*stat)->GetTables().empty()) {
                    auto& tableStats = stats.InsertValue("Table", NJson::JSON_ARRAY);
                    for (auto& t : (*stat)->GetTables()) {
                        auto& tableInfo = tableStats.AppendValue(NJson::JSON_MAP);
                        tableInfo["Path"] = t.GetTablePath();
                        if (t.HasReadBytes()) {
                            FillAggrStat(tableInfo, t.GetReadBytes(), "ReadBytes");
                        }
                        if (t.HasReadRows()) {
                            FillAggrStat(tableInfo, t.GetReadRows(), "ReadRows");
                        }
                    }
                }
                if (const auto& mkqlStat = (*stat)->GetMkql(); !mkqlStat.empty()) {
                    auto& mkqlStats = stats.InsertValue("Mkql", NJson::JSON_MAP);
                    for (const auto& [name, m] : mkqlStat) {
                        FillAggrStat(mkqlStats, m, name);
                    }
                }

                NKqpProto::TKqpStageExtraStats kqpStageStats;
                if ((*stat)->GetExtra().UnpackTo(&kqpStageStats)) {
                    auto& nodesStats = stats.InsertValue("NodesScanShards", NJson::JSON_ARRAY);
                    for (auto&& i : kqpStageStats.GetNodeStats()) {
                        auto& nodeInfo = nodesStats.AppendValue(NJson::JSON_MAP);
                        nodeInfo.InsertValue("shards_count", i.GetShardsCount());
                        nodeInfo.InsertValue("node_id", i.GetNodeId());
                    }
                }

                for (auto& caStats : (*stat)->GetComputeActors()) {
                    auto& caNode = stats["ComputeNodes"].AppendValue(NJson::TJsonValue());
                    fillCaStats(caNode, caStats);
                }
            }
        }
    };

    ModifyPlan(root, collectPlanNodeId);
    ModifyPlan(root, addStatsToPlanNode);

    // Executer TxId of this execution phase, so that the plan can be matched with LWTrace
    // records. It is not reported for literal-only phases, which never reach shards.
    NKqpProto::TKqpExecutionExtraStats executionExtraStats;
    if (stats.HasExtra() && stats.GetExtra().UnpackTo(&executionExtraStats) && executionExtraStats.GetTxId()) {
        const ui64 txId = executionExtraStats.GetTxId();
        root["TxId"] = txId;
        // SerializeTxPlans() keeps only the subplans of the tx plan root, so the value has to be
        // duplicated into them to survive the final serialization.
        if (root.GetMapSafe().contains("Plans") && root.GetMapSafe().at("Plans").IsArray()) {
            for (auto& plan : root.GetMapSafe().at("Plans").GetArraySafe()) {
                plan["TxId"] = txId;
            }
        }
    }

    if (stats.GetNodes().size()) {
        if (root.GetMapSafe().contains("Plans") && root.GetMapSafe().at("Plans").IsArray()) {
            for (auto& plan : root.GetMapSafe().at("Plans").GetArraySafe()) {
                auto& nodesStats = plan.InsertValue("Nodes", NJson::JSON_ARRAY);
                for (auto&& node : stats.GetNodes()) {
                    auto& nodeInfo = nodesStats.AppendValue(NJson::JSON_MAP);
                    nodeInfo["NodeId"] = node.GetNodeId();
                    nodeInfo["Tasks"] = node.GetTotalTasksCount();
                    nodeInfo["FinishedTasks"] = node.GetFinishedTasksCount();
                    if (node.GetBaseTimeMs()) {
                        nodeInfo["BaseTimeMs"] = node.GetBaseTimeMs();
                    }
                    FillAggrStat(nodeInfo, node.GetCpuTimeUs(), "CpuTimeUs");
                    FillAggrStat(nodeInfo, node.GetMemoryUsageMB(), "MemoryUsageMB");
                    FillAggrStat(nodeInfo, node.GetMaxMemoryUsage(), "MaxMemoryUsage");
                    FillAggrStat(nodeInfo, node.GetInputBytes(), "InputBytes");
                    FillAggrStat(nodeInfo, node.GetOutputBytes(), "OutputBytes");
                    FillAggrStat(nodeInfo, node.GetResultBytes(), "ResultBytes");
                    FillAggrStat(nodeInfo, node.GetIngressBytes(), "IngressBytes");
                    FillAggrStat(nodeInfo, node.GetEgressBytes(), "EgressBytes");
                    FillAggrStat(nodeInfo, node.GetSpillingComputeBytes(), "SpillingComputeBytes");
                    FillAggrStat(nodeInfo, node.GetSpillingChannelBytes(), "SpillingChannelBytes");
                    FillAggrStat(nodeInfo, node.GetSpillingComputeTimeUs(), "SpillingComputeTimeUs");
                    FillAggrStat(nodeInfo, node.GetSpillingChannelTimeUs(), "SpillingChannelTimeUs");
                    if (node.GetGlobalMemoryUsageMB().size()) {
                        auto& history = nodeInfo.InsertValue("GlobalMemoryUsageMB", NJson::JSON_MAP);

                        auto& time = history.InsertValue("TimeMs", NJson::JSON_ARRAY);
                        for (auto& u : node.GetGlobalMemoryUsageMB()) {
                            time.AppendValue(u.GetTimeMs());
                        }

                        auto& physical = history.InsertValue("MemPhysicalUsage", NJson::JSON_ARRAY);
                        for (auto& u : node.GetGlobalMemoryUsageMB()) {
                            physical.AppendValue(u.GetMemPhysicalUsage());
                        }

                        auto& sysAlloc = history.InsertValue("MemSysAllocated", NJson::JSON_ARRAY);
                        for (auto& u : node.GetGlobalMemoryUsageMB()) {
                            sysAlloc.AppendValue(u.GetMemSysAllocated());
                        }

                        auto& sysFragm = history.InsertValue("MemSysFragmented", NJson::JSON_ARRAY);
                        for (auto& u : node.GetGlobalMemoryUsageMB()) {
                            sysFragm.AppendValue(u.GetMemSysFragmented());
                        }

                        auto& arrow = history.InsertValue("MemArrowDefault", NJson::JSON_ARRAY);
                        for (auto& u : node.GetGlobalMemoryUsageMB()) {
                            arrow.AppendValue(u.GetMemArrowDefault());
                        }

                        auto& mkqlAlloc = history.InsertValue("MemMkqlAllocated", NJson::JSON_ARRAY);
                        for (auto& u : node.GetGlobalMemoryUsageMB()) {
                            mkqlAlloc.AppendValue(u.GetMemMkqlAllocated());
                        }

                        auto& mkqlFree = history.InsertValue("MemMkqlFreeList", NJson::JSON_ARRAY);
                        for (auto& u : node.GetGlobalMemoryUsageMB()) {
                            mkqlFree.AppendValue(u.GetMemMkqlFreeList());
                        }

                        auto& outputBytes = history.InsertValue("OutputInflightBytes", NJson::JSON_ARRAY);
                        for (auto& u : node.GetGlobalMemoryUsageMB()) {
                            outputBytes.AppendValue(u.GetOutputInflightBytes());
                        }

                        auto& localBytes = history.InsertValue("LocalInflightBytes", NJson::JSON_ARRAY);
                        for (auto& u : node.GetGlobalMemoryUsageMB()) {
                            localBytes.AppendValue(u.GetLocalInflightBytes());
                        }

                        auto& inputBytes = history.InsertValue("InputInflightBytes", NJson::JSON_ARRAY);
                        for (auto& u : node.GetGlobalMemoryUsageMB()) {
                            inputBytes.AppendValue(u.GetInputInflightBytes());
                        }

                        auto& queryAllocated = history.InsertValue("MemQueryAllocated", NJson::JSON_ARRAY);
                        for (auto& u : node.GetGlobalMemoryUsageMB()) {
                            queryAllocated.AppendValue(u.GetMemQueryAllocated());
                        }
                    }
                }
                break;
            }
        }
    }

    NJsonWriter::TBuf txWriter;
    txWriter.WriteJsonValue(&root, true, PREC_NDIGITS, 17);
    auto resultPlan = txWriter.Str();
    if (!newRboEnabled) {
        return AddSimplifiedPlan(resultPlan, true);
    } else {
        return resultPlan;
    }
}

TString SerializeAnalyzePlan(const NKqpProto::TKqpStatsQuery& queryStats, bool newRboEnabled, const TString& poolId) {
    TVector<const TString> txPlans;
    for (const auto& execStats: queryStats.GetExecutions()) {
        for (const auto& txPlan: execStats.GetTxPlansWithStats()) {
            txPlans.push_back(txPlan);
        }
    }

    if (newRboEnabled) {
        return SerializeRBOAnalyzePlan(txPlans, queryStats, poolId);
    }

    NJsonWriter::TBuf writer;
    writer.BeginObject();

    if (queryStats.HasCompilation()) {
        const auto& compilation = queryStats.GetCompilation();

        writer.WriteKey("Compilation");
        writer.BeginObject();
        writer.WriteKey("FromCache").WriteBool(compilation.GetFromCache());
        writer.WriteKey("DurationUs").WriteLongLong(compilation.GetDurationUs());
        writer.WriteKey("CpuTimeUs").WriteLongLong(compilation.GetCpuTimeUs());
        writer.EndObject();
    }

    writer.WriteKey("ProcessCpuTimeUs").WriteLongLong(queryStats.GetWorkerCpuTimeUs());
    writer.WriteKey("TotalDurationUs").WriteLongLong(queryStats.GetDurationUs());
    if (poolId) {
        writer.WriteKey("QueuedTimeUs").WriteLongLong(queryStats.GetQueuedTimeUs());
        writer.WriteKey("ResourcePoolId").WriteString(poolId);
    }
    writer.EndObject();

    return SerializeTxPlans(txPlans, "", writer.Str());
}

TString SerializeScriptPlan(const TVector<const TString>& queryPlans) {
    NJsonWriter::TBuf writer(NJsonWriter::HEM_UNSAFE);
    writer.SetIndentSpaces(2);

    writer.BeginObject();
    writer.WriteKey("meta");
    writer.BeginObject();
    writer.WriteKey("version").WriteString("0.2");
    writer.WriteKey("type").WriteString("script");
    writer.EndObject();

    writer.WriteKey("queries");
    writer.BeginList();

    for (auto planIt = queryPlans.rbegin(); planIt != queryPlans.rend(); ++planIt) {
        if (planIt->empty()) {
            continue;
        }

        NJson::TJsonValue root;
        NJson::ReadJsonTree(*planIt, &root, true);
        auto planMap = root.GetMapSafe();

        writer.BeginObject();
        if (auto tableAccesses = planMap.FindPtr("tables")) {
            writer.WriteKey("tables");
            writer.WriteJsonValue(tableAccesses, false, PREC_NDIGITS, 17);
        }
        if (auto dqPlan = planMap.FindPtr("Plan")) {
            writer.WriteKey("Plan");
            writer.WriteJsonValue(dqPlan, false, PREC_NDIGITS, 17);
            writer.WriteKey("SimplifiedPlan");
            auto simplifiedPlan = SimplifyQueryPlan(*dqPlan);
            writer.WriteJsonValue(&simplifiedPlan, false, PREC_NDIGITS, 17);
        }
        writer.EndObject();
    }

    writer.EndList();
    writer.EndObject();

    return writer.Str();
}

} // namespace NKikimr::NKqp
