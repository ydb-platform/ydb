#include "plan2svg.h"

#include "format.h"
#include "parse.h"
#include "svg.h"

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>
#include <util/stream/output.h>
#include <util/string/cast.h>

namespace NPlan2Svg {

void TPlan::Load(const NJson::TJsonValue& node) {
    if (auto* subplanNameNode = node.GetValueByPath("Subplan Name")) {
        auto subplanName = subplanNameNode->GetStringSafe();
        if (subplanName.StartsWith("CTE ")) {
            Viz.CteSubPlans[subplanName] = this;
        }
    }

    if (auto* subNode = node.GetValueByPath("Plans")) {
        for (auto& plan : subNode->GetArray()) {
            TString nodeType;
            if (auto* nodeTypeNode = plan.GetValueByPath("Node Type")) {
                nodeType = nodeTypeNode->GetStringSafe();
            }
            if (auto* planNodeTypeNode = plan.GetValueByPath("PlanNodeType")) {
                auto planNodeType = planNodeTypeNode->GetStringSafe();
                ythrow yexception() << "Unexpected plan node type [" << planNodeType << "]";
            } else {
                Stages.push_back(std::make_shared<TStage>(Viz.NextGroupId(), this, nodeType));
                LoadStage(Stages.back(), plan, 0);
            }
        }
    } else if (auto* cteNameNode = node.GetValueByPath("CTE Name")) {
        CtePlanRef = "CTE " + cteNameNode->GetStringSafe();
    }

    if (!TotalCpuTimes.empty()) {
        TotalCpuTime.Load(TotalCpuTimes, TotalCpuValues, TotalCpuTimes.front(), TotalCpuTimes.back());
    }

    if (auto* subNode = node.GetValueByPath("Nodes")) {
        for (auto& node : subNode->GetArray()) {
            LoadNode(node);
        }
        std::sort(Nodes.begin(), Nodes.end(),
            [](std::shared_ptr<TClusterNode>& a, std::shared_ptr<TClusterNode>& b) {
                return a->NodeId < b->NodeId;
            }
        );
    }
}

void TPlan::LoadNode(const NJson::TJsonValue& node) {
    if (auto* nodeIdNode = node.GetValueByPath("NodeId")) {
        auto clusterNode = std::make_shared<TClusterNode>(nodeIdNode->GetIntegerSafe());
        if (auto* tasksNode = node.GetValueByPath("Tasks")) {
            clusterNode->Tasks = tasksNode->GetIntegerSafe();
        }
        if (auto* finishedTasksNode = node.GetValueByPath("FinishedTasks")) {
            clusterNode->FinishedTasks = finishedTasksNode->GetIntegerSafe();
        }
        /*
        if (auto* outputBytesNode = node.GetValueByPath("OutputBytes")) {
            clusterNode->OutputBytes = std::make_shared<TSingleMetric>(NodeOutputBytes, *outputBytesNode);
            clusterNode->OutputBytes->FirstMessage.Min = clusterNode->OutputBytes->History.MinTime;
            clusterNode->OutputBytes->FirstMessage.Max = clusterNode->OutputBytes->FirstMessage.Min;
            clusterNode->OutputBytes->LastMessage.Max = clusterNode->OutputBytes->History.MaxTime;
            clusterNode->OutputBytes->LastMessage.Min = clusterNode->OutputBytes->LastMessage.Max;
        }
        */
        if (auto* maxMemoryUsageNode = node.GetValueByPath("MaxMemoryUsage")) {
            clusterNode->MaxMemoryUsage = std::make_shared<TSingleMetric>(NodeMaxMemoryUsage, *maxMemoryUsageNode);
            clusterNode->MaxMemoryUsage->MinMaxDistribution = false;
        }
        if (auto* memoryUsageNode = node.GetValueByPath("MemoryUsageMB")) {
            clusterNode->MemoryUsage = std::make_shared<TSingleMetric>(NodeMemoryUsage, *memoryUsageNode);
            clusterNode->MemoryUsage->MinMaxDistribution = false;
        }
        if (auto* cpuTimeNode = node.GetValueByPath("CpuTimeUs")) {
            clusterNode->CpuTime = std::make_shared<TSingleMetric>(NodeCpuTime, *cpuTimeNode);
            clusterNode->CpuTime->MinMaxDistribution = false;
        }
        if (auto* globNode = node.GetValueByPath("GlobalMemoryUsageMB")) {
            if (auto* timeNode = globNode->GetValueByPath("TimeMs")) {
                if (timeNode->GetType() == NJson::JSON_ARRAY) {
                    std::vector<ui64> times;
                    for (const auto& subNode : timeNode->GetArray()) {
                        times.push_back(subNode.GetIntegerSafe());
                    }
                    // arrow + mkqlAllocated
                    if (auto* physical = globNode->GetValueByPath("MemPhysicalUsage")) {
                        clusterNode->MemPhysicalUsage.Load(times, *physical, 0, 0);
                    }
                    if (auto* sysAlloc = globNode->GetValueByPath("MemSysAllocated")) {
                        clusterNode->MemSysAllocated.Load(times, *sysAlloc, 0, 0);
                    }
                    if (auto* sysFragm = globNode->GetValueByPath("MemSysFragmented")) {
                        clusterNode->MemSysFragmented.Load(times, *sysFragm, 0, 0);
                    }
                    if (auto* arrow = globNode->GetValueByPath("MemArrowDefault")) {
                        clusterNode->MemArrowDefault.Load(times, *arrow, 0, 0);
                    }
                    if (auto* mkqlAlloc = globNode->GetValueByPath("MemMkqlAllocated")) {
                        clusterNode->MemMkqlAllocated.Load(times, *mkqlAlloc, 0, 0);
                    }
                    if (auto* mkqlFree = globNode->GetValueByPath("MemMkqlFreeList")) {
                        clusterNode->MemMkqlFreeList.Load(times, *mkqlFree, 0, 0);
                    }
                    if (auto* outputBytes = globNode->GetValueByPath("OutputInflightBytes")) {
                        clusterNode->OutputInflightBytes.Load(times, *outputBytes, 0, 0);
                    }
                    if (auto* localBytes = globNode->GetValueByPath("LocalInflightBytes")) {
                        clusterNode->LocalInflightBytes.Load(times, *localBytes, 0, 0);
                    }
                    if (auto* inputBytes = globNode->GetValueByPath("InputInflightBytes")) {
                        clusterNode->InputInflightBytes.Load(times, *inputBytes, 0, 0);
                    }
                    clusterNode->MemPhysicalUsage.DisplayMaxValue = clusterNode->MemPhysicalUsage.MaxValue;
                    clusterNode->MemSysAllocated.DisplayMaxValue = clusterNode->MemSysAllocated.MaxValue;
                    clusterNode->MemSysFragmented.DisplayMaxValue = clusterNode->MemSysFragmented.MaxValue;

                    clusterNode->MemArrowDefault.DisplayMaxValue = clusterNode->MemArrowDefault.MaxValue;
                    for (ui32 i = 0; i < std::min(clusterNode->MemArrowDefault.Values.size(), clusterNode->MemMkqlAllocated.Values.size()); i++) {
                        clusterNode->MemArrowDefault.Values[i].second += clusterNode->MemMkqlAllocated.Values[i].second;
                        clusterNode->MemArrowDefault.MaxValue = std::max(clusterNode->MemArrowDefault.MaxValue, clusterNode->MemArrowDefault.Values[i].second);
                    }
                    clusterNode->MemMkqlAllocated.DisplayMaxValue = clusterNode->MemMkqlAllocated.MaxValue;
                    clusterNode->MemMkqlFreeList.DisplayMaxValue = clusterNode->MemMkqlFreeList.MaxValue;

                    clusterNode->LocalInflightBytes.DisplayMaxValue = clusterNode->LocalInflightBytes.MaxValue;
                    for (ui32 i = 0; i < std::min(clusterNode->LocalInflightBytes.Values.size(), clusterNode->InputInflightBytes.Values.size()); i++) {
                        clusterNode->LocalInflightBytes.Values[i].second += clusterNode->InputInflightBytes.Values[i].second;
                        clusterNode->LocalInflightBytes.MaxValue = std::max(clusterNode->LocalInflightBytes.MaxValue, clusterNode->LocalInflightBytes.Values[i].second);
                    }
                    clusterNode->OutputInflightBytes.DisplayMaxValue = clusterNode->OutputInflightBytes.MaxValue;
                    for (ui32 i = 0; i < std::min(clusterNode->OutputInflightBytes.Values.size(), clusterNode->LocalInflightBytes.Values.size()); i++) {
                        clusterNode->OutputInflightBytes.Values[i].second += clusterNode->LocalInflightBytes.Values[i].second;
                        clusterNode->OutputInflightBytes.MaxValue = std::max(clusterNode->OutputInflightBytes.MaxValue, clusterNode->OutputInflightBytes.Values[i].second);
                    }
                    clusterNode->InputInflightBytes.DisplayMaxValue = clusterNode->InputInflightBytes.MaxValue;
                }
            }

        }
        /*
        if (auto* inputBytesNode = node.GetValueByPath("InputBytes")) {
            clusterNode->InputBytes = std::make_shared<TSingleMetric>(NodeInputBytes, *inputBytesNode);
        }
        if (auto* ingressBytesNode = node.GetValueByPath("IngressBytes")) {
            clusterNode->IngressBytes = std::make_shared<TSingleMetric>(NodeIngressBytes, *ingressBytesNode);
        }
        */
        Nodes.push_back(clusterNode);
    }
 }

void TPlan::ResolveCteRefs() {
    if (CtePlanRef) {
        auto it = Viz.CteStages.find(CtePlanRef);
        if (it == Viz.CteStages.end()) {
            ythrow yexception() << "Can not find CTE Ref " << CtePlanRef;
        }
        CtePlan = it->second->Plan;
    }

    for (auto& cteRef : CteRefs) {
        auto it = Viz.CteStages.find(cteRef.first);
        if (it == Viz.CteStages.end()) {
            ythrow yexception() << "Can not find CTE Ref " << cteRef.first;
        }

        cteRef.second->FromStage = it->second;
        if (cteRef.second->StatsNode) {
            if (auto* inputNode = cteRef.second->StatsNode->GetValueByPath("Input")) {
                for (const auto& subNode : inputNode->GetArray()) {
                    if (auto* nameNode = subNode.GetValueByPath("Name")) {
                        if (ToString(it->second->PlanNodeId) == nameNode->GetStringSafe()) {
                            if (auto* statNode = GetInputStatNode(subNode)) {
                                if (auto* bytesNode = statNode->GetValueByPath("Bytes")) {
                                    cteRef.second->InputBytes = std::make_shared<TSingleMetric>(InputBytes,
                                        *bytesNode, 0, 0,
                                        statNode->GetValueByPath("FirstMessageMs"),
                                        statNode->GetValueByPath("LastMessageMs"),
                                        statNode->GetValueByPath("WaitTimeUs.History")
                                    );
                                    Min0(cteRef.second->Stage.MinTime, cteRef.second->InputBytes->MinTime);
                                    Max0(cteRef.second->Stage.MaxTime, cteRef.second->InputBytes->MaxTime);
                                    Max0(MaxTime, cteRef.second->InputBytes->MaxTime);
                                } else {
                                    cteRef.second->InputBytes = std::make_shared<TSingleMetric>(InputBytes);
                                }
                                if (auto* rowsNode = statNode->GetValueByPath("Rows")) {
                                    cteRef.second->InputRows = std::make_shared<TSingleMetric>(InputRows, *rowsNode);
                                } else {
                                    cteRef.second->InputRows = std::make_shared<TSingleMetric>(InputRows);
                                }
                                if (auto* chunksNode = statNode->GetValueByPath("Chunks")) {
                                    if (auto* sumNode = chunksNode->GetValueByPath("Sum")) {
                                        cteRef.second->InputChunks = sumNode->GetIntegerSafe();
                                        if (cteRef.second->InputChunks) {
                                            cteRef.second->InputChunkSize = std::make_shared<TScalarMetric>(InputChunkSize,
                                                cteRef.second->InputBytes->Details.Sum / cteRef.second->InputChunks);
                                        }
                                    }
                                }
                            }
                            if (auto* localBytesNode = subNode.GetValueByPath("LocalBytes")) {
                                cteRef.second->InputLocalBytes = localBytesNode->GetIntegerSafe();
                            }
                        }
                    }
                }
            }
        }
        if (cteRef.second->FromStage->StatsNode) {
            if (auto* outputNode = cteRef.second->FromStage->StatsNode->GetValueByPath("Output")) {
                for (const auto& subNode : outputNode->GetArray()) {
                    if (auto* nameNode = subNode.GetValueByPath("Name")) {
                        if (ToString(cteRef.second->Stage.PlanNodeId) == nameNode->GetStringSafe()) {
                            if (auto* statNode = GetOutputStatNode(subNode)) {
                                if (auto* bytesNode = statNode->GetValueByPath("Bytes")) {
                                    cteRef.second->CteOutputBytes = std::make_shared<TSingleMetric>(OutputBytes,
                                        *bytesNode, 0, 0,
                                        statNode->GetValueByPath("FirstMessageMs"),
                                        statNode->GetValueByPath("LastMessageMs"),
                                        statNode->GetValueByPath("WaitTimeUs.History")
                                    );
                                    Min0(cteRef.second->FromStage->MinTime, cteRef.second->CteOutputBytes->MinTime);
                                    Max0(cteRef.second->FromStage->MaxTime, cteRef.second->CteOutputBytes->MaxTime);
                                    Max0(MaxTime, cteRef.second->CteOutputBytes->MaxTime);
                                } else {
                                    cteRef.second->CteOutputBytes = std::make_shared<TSingleMetric>(OutputBytes);
                                }
                                if (auto* rowsNode = statNode->GetValueByPath("Rows")) {
                                    cteRef.second->CteOutputRows = std::make_shared<TSingleMetric>(OutputRows, *rowsNode);
                                    cteRef.second->CteOperatorOutputRows = std::make_shared<TSingleMetric>(OperatorOutputRows, *rowsNode);
                                } else {
                                    cteRef.second->CteOutputRows = std::make_shared<TSingleMetric>(OutputRows);
                                }
                                if (auto* chunksNode = statNode->GetValueByPath("Chunks")) {
                                    if (auto* sumNode = chunksNode->GetValueByPath("Sum")) {
                                        cteRef.second->CteOutputChunks = sumNode->GetIntegerSafe();
                                        if (cteRef.second->CteOutputChunks) {
                                            cteRef.second->CteOutputChunkSize = std::make_shared<TScalarMetric>(OutputChunkSize,
                                                cteRef.second->CteOutputBytes->Details.Sum / cteRef.second->CteOutputChunks);
                                        }
                                    }
                                }
                            }
                            if (auto* localBytesNode = subNode.GetValueByPath("LocalBytes")) {
                                cteRef.second->CteOutputLocalBytes = localBytesNode->GetIntegerSafe();
                            }
                        }
                    }
                }
            }
        }
    }
}

void TPlan::ResolveOperatorInputs() {
    for (auto& s : Stages) {
        for (auto& op : s->Operators) {
            for (auto& input : op.Inputs) {
                if (input.PlanNodeId && !NodeToSource.contains(input.PlanNodeId)) {
                    input.StageId = NodeToConnection.at(input.PlanNodeId)->FromStage->PhysicalStageId;
                }
            }
        }
    }
}

void TPlan::MergeTotalCpu(std::shared_ptr<TSingleMetric> cpuTime) {

            std::vector<ui64> updatedCpuTimes;
            std::vector<ui64> updatedCpuValues;

            auto itt = TotalCpuTimes.begin();
            auto itv = TotalCpuValues.begin();
            auto ith = cpuTime->History.Values.begin();

            ui64 v0 = 0;
            ui64 v1 = 0;
            ui64 t = 0;

            while (itt != TotalCpuTimes.end() || ith != cpuTime->History.Values.end()) {

                if (itt == TotalCpuTimes.end()) {
                    t = ith->first;
                    v1 = ith->second;
                    ith++;
                } else if (ith == cpuTime->History.Values.end()) {
                    t = *itt++;
                    v0 = *itv++;
                } else if (*itt == ith->first) {
                    t = *itt++;
                    v0 = *itv++;
                    v1 = ith->second;
                    ith++;
                } else if (*itt > ith->first) {
                    t = ith->first;
                    v1 = ith->second;
                    ith++;
                } else {
                    t = *itt++;
                    v0 = *itv++;
                }

                updatedCpuTimes.push_back(t);
                updatedCpuValues.push_back(v0 + v1);
            }

            TotalCpuTimes.swap(updatedCpuTimes);
            TotalCpuValues.swap(updatedCpuValues);
}

void TPlan::LoadStage(std::shared_ptr<TStage> stage, const NJson::TJsonValue& node, TConnection* outputConnection) {

    if (auto* planNodeIdNode = node.GetValueByPath("PlanNodeId")) {
        stage->PlanNodeId = planNodeIdNode->GetIntegerSafe();
    }

    if (outputConnection) {
        stage->OutputPlanNodeId = outputConnection->PlanNodeId;
    }

    if (auto* subplanNameNode = node.GetValueByPath("Subplan Name")) {
        auto subplanName = subplanNameNode->GetStringSafe();
        if (subplanName.StartsWith("CTE ")) {
            Viz.CteStages[subplanName] = stage;
        }
    }

    if (!stage->StatsNode) {
        stage->StatsNode = node.GetValueByPath("Stats");
    }
    auto operators = node.GetValueByPath("Operators");

    std::vector<TOperatorInfo> externalOperators;
    TStage* externalStage = nullptr;

    if (operators) {
        TString prevFilter;
        auto operatorsArray = operators->GetArray();
        for (const auto& subNode : operatorsArray) {
            if (auto* nameNode = subNode.GetValueByPath("Name")) {
                TString name = nameNode->GetStringSafe();
                TString info;
                bool blocks = false;
                TString operatorType = "";
                TString operatorId = "0";
                auto externalOperator = false;

                if (/* name == "Iterator" || */ name == "Member" || name == "ToFlow") {
                    continue;
                }

                if (auto* blocksNode = subNode.GetValueByPath("Blocks")) {
                    blocks = blocksNode->GetStringSafe() == "True";
                }

                if (name == "Filter" && prevFilter) {
                    if (auto* predicateNode = subNode.GetValueByPath("Predicate")) {
                        auto filter = predicateNode->GetStringSafe();
                        if (filter == prevFilter) {
                            continue;
                        }
                    }
                }
                prevFilter = "";

                TStringBuilder builder;

                if (name == "Limit") {
                    if (auto* limitNode = subNode.GetValueByPath("Limit")) {
                        info = limitNode->GetStringSafe();
                    }
                } else if (name == "Sort") {
                    if (auto* sortByNode = subNode.GetValueByPath("SortBy")) {
                        auto sortBy = sortByNode->GetStringSafe();
                        while (true) {
                            auto p = sortBy.find("row.");
                            if (p == sortBy.npos) {
                                break;
                            }
                            sortBy.erase(p, 4);
                        }
                        if (sortBy) {
                            info = sortBy;
                        }
                    }
                } else if (name == "Filter") {
                    operatorType = "Filter";
                    if (auto* predicateNode = subNode.GetValueByPath("Predicate")) {
                        auto filter = predicateNode->GetStringSafe();
                        prevFilter = filter;
                        while (true) {
                            auto p = filter.find("item.");
                            if (p == filter.npos) {
                                break;
                            }
                            filter.erase(p, 5);
                        }
                        while(true) {
                            auto p = filter.find('<');
                            if (p == filter.npos) {
                                break;
                            }
                            filter.erase(p, 1);
                            filter.insert(p, "&lt;");
                        }
                        info = filter;
                    }
                    if (auto* pushdownNode = subNode.GetValueByPath("Pushdown")) {
                        externalOperator = pushdownNode->GetStringSafe() == "True";
                    }
                } else if (name == "Aggregate") {
                    operatorType = "Aggregation";
                    TStringBuilder builder;
                    if (auto* aggregationNode = subNode.GetValueByPath("Aggregation")) {
                        auto aggr = aggregationNode->GetStringSafe();
                        if (aggr) {
                            if (aggr.StartsWith("{")) {
                                aggr.erase(aggr.begin());
                            }
                            if (aggr.EndsWith("}")) {
                                aggr.erase(aggr.end() - 1);
                            }
                            while (true) {
                                auto p = aggr.find("_yql_agg_");
                                if (p == aggr.npos) {
                                    break;
                                }
                                auto l = 9;
                                auto p1 = aggr.begin() + p + l;
                                while (p1 != aggr.end() && *p1 >= '0' && *p1 <= '9') {
                                    p1++;
                                    l++;
                                }
                                auto yqlAgg = aggr.substr(p, l);
                                if (p1 != aggr.end() && *p1 == ':') {
                                    p1++;
                                    l++;
                                    if (p1 != aggr.end() && *p1 == ' ') {
                                        p1++;
                                        l++;
                                    }
                                }
                                aggr.erase(p, l);

                                auto extraChars = 7;
                                p = aggr.find(",state." + yqlAgg);
                                if (p == aggr.npos) {
                                    p = aggr.find("state." + yqlAgg + ",");
                                }
                                if (p == aggr.npos) {
                                    p = aggr.find("state." + yqlAgg);
                                    extraChars = 6;
                                }
                                if (p != aggr.npos) {
                                    aggr.erase(p, yqlAgg.size() + extraChars);
                                }
                            }
                            while (true) {
                                auto p = aggr.find("item.");
                                if (p == aggr.npos) {
                                    break;
                                }
                                aggr.erase(p, 5);
                            }
                            builder << aggr;
                        }
                    }
                    if (auto* groupByNode = subNode.GetValueByPath("GroupBy")) {
                        auto groupBy = groupByNode->GetStringSafe();
                        while (true) {
                            auto p = groupBy.find("item.");
                            if (p == groupBy.npos) {
                                break;
                            }
                            groupBy.erase(p, 5);
                        }
                        if (groupBy) {
                            if (TString(builder)) {
                                builder << ", ";
                            }
                            builder << "Group By: " << groupBy;
                        }
                    }
                    info = builder;
                    if (auto* pushdownNode = subNode.GetValueByPath("Pushdown")) {
                        externalOperator = pushdownNode->GetStringSafe() == "True";
                    }
                } else if (name == "TableFullScan" || name == "TablePointLookup" || name == "TableRangeScan") {
                    TStringBuilder builder;
                    if (auto* tableNode = subNode.GetValueByPath("Table")) {
                        builder << ParseTableOrIndexName(tableNode->GetStringSafe());
                    }
                    builder << ParseColumns(subNode.GetValueByPath("ReadColumns"));

                    if (name == "TablePointLookup" || name == "TableRangeScan") {
                        builder << ": ";
                        auto* readRangesNode = subNode.GetValueByPath("ReadRanges");
                        if (!readRangesNode) {
                            readRangesNode = subNode.GetValueByPath("ReadRange");
                        }
                        if (readRangesNode) {
                            bool firstRange = true;
                            for (const auto& subNode : readRangesNode->GetArray()) {
                                if (firstRange) {
                                    firstRange = false;
                                } else {
                                    builder << ", ";
                                }
                                builder << subNode.GetStringSafe();
                            }
                        }
                    }

                    info = builder;
                    externalOperator = true;
                } else if (name == "TopSort" || name == "Top") {
                    TStringBuilder builder;
                    if (auto* limitNode = subNode.GetValueByPath("Limit")) {
                        auto limit = limitNode->GetStringSafe();
                        if (limit) {
                            builder << "Limit: " << limit;
                        }
                    }
                    if (auto* topSortByNode = subNode.GetValueByPath("TopSortBy")) {
                        auto topSortBy = topSortByNode->GetStringSafe();
                        if (topSortBy) {
                            if (TString(builder)) {
                                builder << ", ";
                            }
                            builder << "TopSortBy: " << topSortBy;
                        }
                    }
                    info = builder;
                } else if (name.Contains("Join")) {
                    operatorType = "Join";
                    if (auto* conditionNode = subNode.GetValueByPath("Condition")) {
                        info = conditionNode->GetStringSafe();
                    }
                }

                std::vector<TOperatorInput> inputs;

                auto* inputsArrayNode = subNode.GetValueByPath("Inputs");
                if (inputsArrayNode && !inputsArrayNode->GetArraySafe().empty()) {
                    for (const auto& inputNode : inputsArrayNode->GetArray()) {
                        if (auto* internalOperatorIdNode = inputNode.GetValueByPath("InternalOperatorId")) {
                            auto internalOperatorId = internalOperatorIdNode->GetUIntegerSafe();
                            if (internalOperatorId && std::find_if(inputs.begin(), inputs.end(),
                                    [=](const TOperatorInput& input) { return input.OperatorId == internalOperatorId; }) == inputs.end()) {
                                if (internalOperatorId < operatorsArray.size()) {
                                    TString precomputeRef;
                                    auto* node = &operatorsArray[internalOperatorId];
                                    if (auto* nameNode = node->GetValueByPath("Name")) {
                                        auto name = nameNode->GetStringSafe();
                                        if (name == "Iterator") {
                                            if (auto* inputsArrayNode = node->GetValueByPath("Inputs")) {
                                                auto inputsArray = inputsArrayNode->GetArray();
                                                if (!inputsArray.empty()) {
                                                    if (auto* nextIdNode = inputsArray[0].GetValueByPath("InternalOperatorId")) {
                                                        auto nextId = nextIdNode->GetUIntegerSafe();
                                                        if (nextId < operatorsArray.size()) {
                                                            auto* nextNode = &operatorsArray[nextId];
                                                            if (auto* nameNode = nextNode->GetValueByPath("Name")) {
                                                                node = nextNode;
                                                                name = nameNode->GetStringSafe();
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        if (name == "Member" || name == "ToFlow") {
                                            if (auto* refNode = node->GetValueByPath(name)) {
                                                precomputeRef = "CTE " + refNode->GetStringSafe();
                                            }
                                        }
                                    }
                                    inputs.emplace_back();
                                    inputs.back().OperatorId = internalOperatorId;
                                    inputs.back().PrecomputeRef = precomputeRef;
                                }
                            }
                        }
                        if (auto* externalPlanNodeIdNode = inputNode.GetValueByPath("ExternalPlanNodeId")) {
                            auto externalPlanNodeId = externalPlanNodeIdNode->GetIntegerSafe();
                            if (externalPlanNodeId && std::find_if(inputs.begin(), inputs.end(),
                                    [=](const TOperatorInput& input) { return input.PlanNodeId == externalPlanNodeId; }) == inputs.end()) {
                                inputs.emplace_back();
                                inputs.back().PlanNodeId = externalPlanNodeId;
                            }
                        }
                    }
                } else if (auto* precomputeRefNode = subNode.GetValueByPath("Input")) {
                    inputs.emplace_back();
                    inputs.back().PrecomputeRef = "CTE " + precomputeRefNode->GetStringSafe();
                }

                if (externalOperator && !stage->External) {
                    externalOperators.emplace_back(name, info);
                    externalOperators.back().Estimations = GetEstimation(subNode);
                    externalOperators.back().Inputs.swap(inputs);
                    externalOperators.back().Blocks = blocks;
                } else {
                    stage->Operators.emplace_back(name, info);
                    stage->Operators.back().Estimations = GetEstimation(subNode);
                    stage->Operators.back().Inputs.swap(inputs);
                    stage->Operators.back().Blocks = blocks;
                }

                if (stage->StatsNode) {
                    if (!externalOperator) {
                        const NJson::TJsonValue* operatorStatNode = nullptr;
                        if (operatorType) {
                            if (auto* operatorNode = stage->StatsNode->GetValueByPath("Operator")) {
                                TStringBuilder builder;
                                for (const auto& subNode : operatorNode->GetArray()) {
                                    TString id = "";
                                    if (auto* idNode = subNode.GetValueByPath("Id")) {
                                        id = idNode->GetStringSafe();
                                    }
                                    TString type = "";
                                    if (auto* typeNode = subNode.GetValueByPath("Type")) {
                                        type = typeNode->GetStringSafe();
                                    }
                                    if (operatorType == type && operatorId == id) {
                                        if (operatorStatNode) {
                                            // collision
                                            operatorStatNode = nullptr;
                                            break;
                                        } else {
                                            operatorStatNode = &subNode;
                                        }
                                    }
                                }
                            }
                        }
                        if (operatorStatNode) {
                            if (auto* rowsStatsNode = operatorStatNode->GetValueByPath("Rows")) {
                                stage->Operators.back().OutputRows = std::make_shared<TSingleMetric>(OperatorOutputRows, *rowsStatsNode);
                            }
                        }
                    }

                    if (name == "TableFullScan" || name == "TablePointLookup" || name == "TableRangeScan") {
                        Y_ENSURE(externalOperator);
                        if (stage->IngressName) {
                            ythrow yexception() << "Plan stage already has Ingress [" << stage->IngressName << "]";
                        }
                        stage->IngressName = name;
                        TString tablePath;
                        if (auto* pathNode = subNode.GetValueByPath("Path")) {
                            tablePath = pathNode->GetStringSafe();
                        } else if (auto* tableNode = subNode.GetValueByPath("Table")) {
                            tablePath = tableNode->GetStringSafe();
                        }
                        if (tablePath) {
                            if (auto* ingressTopNode = stage->StatsNode->GetValueByPath("Ingress")) {
                                // only 1 ingress node is possible (???)
                                auto& ingress0 = (*ingressTopNode)[0];
                                if (auto* nameNode = ingress0.GetValueByPath("Name")) {
                                    if (nameNode->GetStringSafe() == "CS") {
                                        externalOperators.back().Blocks = true;
                                    }
                                }
                                auto* ingressNode = ingress0.GetValueByPath("Ingress");
                                if (ingressNode) {
                                    if (!ingressNode->GetValueByPath("Bytes.Sum")) {
                                        ingressNode = nullptr;
                                    }
                                }
                                if (!ingressNode) {
                                    ingressNode = GetInputStatNode(ingress0);
                                }
                                if (ingressNode) {
                                    if (auto* bytesNode = ingressNode->GetValueByPath("Bytes")) {
                                        stage->IngressBytes = std::make_shared<TSingleMetric>(IngressBytes,
                                            *bytesNode, 0, 0,
                                            ingressNode->GetValueByPath("FirstMessageMs"),
                                            ingressNode->GetValueByPath("LastMessageMs"),
                                            ingressNode->GetValueByPath("WaitTimeUs.History")
                                        );
                                        Min0(stage->MinTime, stage->IngressBytes->MinTime);
                                        Max0(stage->MaxTime, stage->IngressBytes->MaxTime);
                                    } else {
                                        stage->IngressBytes = std::make_shared<TSingleMetric>(IngressBytes);
                                    }
                                    if (auto* rowsNode = ingressNode->GetValueByPath("Rows")) {
                                        stage->IngressRows = std::make_shared<TSingleMetric>(IngressRows, *rowsNode);
                                    } else {
                                        stage->IngressRows = std::make_shared<TSingleMetric>(IngressRows);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if (!externalOperators.empty() && !stage->External) {
        auto connection = std::make_shared<TConnection>(Viz.NextGroupId(), *stage, "External", 0);
        connection->Blocks = true;
        stage->Connections.push_back(connection);
        Stages.push_back(std::make_shared<TStage>(Viz.NextGroupId(), this, "External"));
        StageToExternalConnection[Stages.back().get()] = connection.get();
        stage->IngressConnection = connection.get();
        connection->FromStage = Stages.back();
        Stages.back()->Operators = externalOperators;
        externalStage = Stages.back().get();
        Stages.back()->External = true;
    }

    if (stage->Operators.empty()) {
        stage->Operators.emplace_back(stage->NodeType, "");
        // add inputs + outputs from connections
    }

    if (outputConnection) {
        stage->Operators.front().Blocks |= outputConnection->Blocks;
    }

    const NJson::TJsonValue* inputNode = nullptr;

    if (stage->StatsNode) {
        if (externalStage) {
            if (auto* ingressTopNode = stage->StatsNode->GetValueByPath("Ingress")) {
                // only 1 ingress node is possible (???)
                auto& ingress0 = (*ingressTopNode)[0];
                if (auto* externalNode = ingress0.GetValueByPath("External")) {
                    if (auto* externalBytesNode = externalNode->GetValueByPath("ExternalBytes")) {
                        externalStage->EgressBytes = std::make_shared<TSingleMetric>(ExternalBytes, *externalBytesNode, 0, 0,
                            externalNode->GetValueByPath("FirstMessageMs"),
                            externalNode->GetValueByPath("LastMessageMs")
                        );
                    }
                    if (auto* externalRowsNode = externalNode->GetValueByPath("ExternalRows")) {
                        externalStage->EgressRows = std::make_shared<TSingleMetric>(ExternalRows, *externalRowsNode);
                        externalStage->Operators.front().OutputRows = std::make_shared<TSingleMetric>(OperatorOutputRows, *externalRowsNode);
                    }
                    if (auto* cpuTimeNode = externalNode->GetValueByPath("CpuTimeUs")) {
                        externalStage->CpuTime = std::make_shared<TSingleMetric>(ExternalCpuTime, *cpuTimeNode);
                        MergeTotalCpu(externalStage->CpuTime);
                    }
                    if (auto* partitionCountNode = externalNode->GetValueByPath("PartitionCount")) {
                        externalStage->Tasks = partitionCountNode->GetIntegerSafe();
                    }
                    if (auto* finishedPartitionCountNode = externalNode->GetValueByPath("FinishedPartitionCount")) {
                        externalStage->FinishedTasks = finishedPartitionCountNode->GetIntegerSafe();
                    }
                }
            }
        }

        if (auto* tasksNode = stage->StatsNode->GetValueByPath("Tasks")) {
            stage->Tasks = tasksNode->GetIntegerSafe();
            Tasks += stage->Tasks;
        }
        if (auto* finishedTasksNode = stage->StatsNode->GetValueByPath("FinishedTasks")) {
            stage->FinishedTasks = finishedTasksNode->GetIntegerSafe();
        }

        if (auto* physicalStageIdNode = stage->StatsNode->GetValueByPath("PhysicalStageId")) {
            stage->PhysicalStageId = physicalStageIdNode->GetIntegerSafe();
        }

        if (auto* baseTimeNode = stage->StatsNode->GetValueByPath("BaseTimeMs")) {
            stage->BaseTime = baseTimeNode->GetIntegerSafe();
            if (BaseTime == 0) {
                BaseTime = stage->BaseTime;
            } else {
                BaseTime = std::min(BaseTime, stage->BaseTime);
            }
        }

        if (auto* outputNode = stage->StatsNode->GetValueByPath("Output")) {
            for (const auto& subNode : outputNode->GetArray()) {
                if (auto* nameNode = subNode.GetValueByPath("Name")) {
                    auto name = nameNode->GetStringSafe();
                    if ((outputConnection && name == ToString(outputConnection->Stage.PlanNodeId)) || name == "RESULT") {
                        if (auto* statNode = GetOutputStatNode(subNode)) {
                            if (auto* bytesNode = statNode->GetValueByPath("Bytes")) {
                                stage->OutputBytes = std::make_shared<TSingleMetric>(OutputBytes,
                                    *bytesNode, 0, 0,
                                    statNode->GetValueByPath("FirstMessageMs"),
                                    statNode->GetValueByPath("LastMessageMs"),
                                    statNode->GetValueByPath("WaitTimeUs.History")
                                );
                                Min0(stage->MinTime, stage->OutputBytes->MinTime);
                                Max0(stage->MaxTime, stage->OutputBytes->MaxTime);
                            } else {
                                stage->OutputBytes = std::make_shared<TSingleMetric>(OutputBytes);
                            }
                            if (auto* rowsNode = statNode->GetValueByPath("Rows")) {
                                stage->OutputRows = std::make_shared<TSingleMetric>(OutputRows, *rowsNode);

                                if (!stage->Operators.front().OutputRows) {
                                    stage->Operators.front().OutputRows = std::make_shared<TSingleMetric>(OperatorOutputRows, *rowsNode);
                                }
                            } else {
                                stage->OutputRows = std::make_shared<TSingleMetric>(OutputRows);

                                if (!stage->Operators.front().OutputRows) {
                                    stage->Operators.front().OutputRows = std::make_shared<TSingleMetric>(OperatorOutputRows);
                                }
                            }
                            if (auto* chunksNode = statNode->GetValueByPath("Chunks")) {
                                if (auto* sumNode = chunksNode->GetValueByPath("Sum")) {
                                    stage->OutputChunks = sumNode->GetIntegerSafe();
                                    if (stage->OutputChunks) {
                                        stage->OutputChunkSize = std::make_shared<TScalarMetric>(OutputChunkSize,
                                            stage->OutputBytes->Details.Sum / stage->OutputChunks);
                                    }
                                }
                            }
                        }
                        if (auto* localBytesNode = subNode.GetValueByPath("LocalBytes")) {
                            stage->OutputLocalBytes = localBytesNode->GetIntegerSafe();
                        }
                    }
                }
            }
        }

        if (auto* spillingComputeBytesNode = stage->StatsNode->GetValueByPath("SpillingComputeBytes")) {
            stage->SpillingComputeBytes = std::make_shared<TSingleMetric>(SpillingComputeBytes, *spillingComputeBytesNode,
                stage->MinTime, stage->MaxTime);
        }

        if (auto* spillingComputeTimeNode = stage->StatsNode->GetValueByPath("SpillingComputeTimeUs")) {
            stage->SpillingComputeTime = std::make_shared<TSingleMetric>(SpillingComputeTime, *spillingComputeTimeNode,
                stage->MinTime, stage->MaxTime);
        }

        if (auto* spillingChannelBytesNode = stage->StatsNode->GetValueByPath("SpillingChannelBytes")) {
            stage->SpillingChannelBytes = std::make_shared<TSingleMetric>(SpillingChannelBytes, *spillingChannelBytesNode,
                stage->MinTime, stage->MaxTime);
        }

        if (auto* spillingChannelTimeNode = stage->StatsNode->GetValueByPath("SpillingChannelTimeUs")) {
            stage->SpillingChannelTime = std::make_shared<TSingleMetric>(SpillingChannelTime, *spillingChannelTimeNode,
                stage->MinTime, stage->MaxTime);
        }

        inputNode = stage->StatsNode->GetValueByPath("Input");
    }

    ui64 inputBytes = 0;

    if (auto* subNode = node.GetValueByPath("Plans")) {
        for (auto& plan : subNode->GetArray()) {
            TString subNodeType;
            if (auto* nodeTypeNode = plan.GetValueByPath("Node Type")) {
                subNodeType = nodeTypeNode->GetStringSafe();
            }
            TString planNodeType;
            if (auto* planNodeTypeNode = plan.GetValueByPath("PlanNodeType")) {
                planNodeType = planNodeTypeNode->GetStringSafe();
            }
            ui32 connectionPlanNodeId = 0;
            if (auto* planNodeIdNode = plan.GetValueByPath("PlanNodeId")) {
                connectionPlanNodeId = planNodeIdNode->GetIntegerSafe();
            }

            if (planNodeType == "Connection") {
                if (subNodeType == "TableLookup") {
                    subNodeType = "Lookup";
                } else if (subNodeType == "TableLookupJoin") {
                    subNodeType = "LookupJoin";
                } else if (subNodeType == "DqCnParallelUnionAll") {
                    subNodeType = "UnionAll";
                }

                std::shared_ptr<TConnection> connection;

                if (auto* subNode = plan.GetValueByPath("Plans")) {
                    for (auto& subPlan : subNode->GetArray()) {
                        TString nodeType;
                        if (auto* nodeTypeNode = subPlan.GetValueByPath("Node Type")) {
                            nodeType = nodeTypeNode->GetStringSafe();
                        }
                        if (auto* planNodeTypeNode = subPlan.GetValueByPath("PlanNodeType")) {
                            auto planNodeType = planNodeTypeNode->GetStringSafe();
                            if (planNodeType) {
                                ythrow yexception() << "Unexpected plan node type [" << planNodeType << "]";
                            }
                        }
                        connection = std::make_shared<TConnection>(Viz.NextGroupId(), *stage, subNodeType, connectionPlanNodeId);
                        if (auto* blocksNode = plan.GetValueByPath("Blocks")) {
                            connection->Blocks = blocksNode->GetStringSafe() == "True";
                        }
                        NodeToConnection[connectionPlanNodeId] = connection.get();
                        stage->Connections.push_back(connection);

                        if (auto* planNodeIdNode = subPlan.GetValueByPath("PlanNodeId")) {
                            auto planNodeId = planNodeIdNode->GetStringRobust();
                            if (inputNode) {
                                for (const auto& subNode : inputNode->GetArray()) {
                                    if (auto* nameNode = subNode.GetValueByPath("Name")) {
                                        if (planNodeId == nameNode->GetStringSafe()) {
                                            if (auto* statNode = GetInputStatNode(subNode)) {
                                                if (auto* bytesNode = statNode->GetValueByPath("Bytes")) {
                                                    connection->InputBytes = std::make_shared<TSingleMetric>(InputBytes,
                                                        *bytesNode, 0, 0,
                                                        statNode->GetValueByPath("FirstMessageMs"),
                                                        statNode->GetValueByPath("LastMessageMs"),
                                                        statNode->GetValueByPath("WaitTimeUs.History")
                                                    );
                                                    Min0(stage->MinTime, connection->InputBytes->MinTime);
                                                    Max0(stage->MaxTime, connection->InputBytes->MaxTime);
                                                    inputBytes += connection->InputBytes->Details.Sum;
                                                } else {
                                                    connection->InputBytes = std::make_shared<TSingleMetric>(InputBytes);
                                                }
                                                if (auto* rowsNode = statNode->GetValueByPath("Rows")) {
                                                    connection->InputRows = std::make_shared<TSingleMetric>(InputRows, *rowsNode);
                                                    for (auto& op : stage->Operators) {
                                                        for(auto& input : op.Inputs) {
                                                            if (input.PlanNodeId == connectionPlanNodeId) {
                                                                input.Rows = std::make_shared<TSingleMetric>(OperatorInputRows, *rowsNode);
                                                            }
                                                        }
                                                    }
                                                } else {
                                                    connection->InputRows = std::make_shared<TSingleMetric>(InputRows);
                                                }
                                                if (auto* chunksNode = statNode->GetValueByPath("Chunks")) {
                                                    if (auto* sumNode = chunksNode->GetValueByPath("Sum")) {
                                                        connection->InputChunks = sumNode->GetIntegerSafe();
                                                        if (connection->InputChunks) {
                                                            connection->InputChunkSize = std::make_shared<TScalarMetric>(InputChunkSize,
                                                                connection->InputBytes->Details.Sum / connection->InputChunks);
                                                        }
                                                    }
                                                }
                                            }
                                            if (auto* localBytesNode = subNode.GetValueByPath("LocalBytes")) {
                                                connection->InputLocalBytes = localBytesNode->GetIntegerSafe();
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        Stages.push_back(std::make_shared<TStage>(Viz.NextGroupId(), this, nodeType));
                        connection->FromStage = Stages.back();
                        Stages.back()->OutputPhysicalStageId = stage->PhysicalStageId;
                        LoadStage(Stages.back(), subPlan, connection.get());

                        if (subNodeType == "Lookup" || subNodeType == "LookupJoin") {
                            // auto stage = Stages.back();
                            auto connection = std::make_shared<TConnection>(Viz.NextGroupId(), *stage, "External", 0);
                            stage->Connections.push_back(connection);
                            Stages.push_back(std::make_shared<TStage>(Viz.NextGroupId(), this, "External"));
                            StageToExternalConnection[Stages.back().get()] = connection.get();
                            // stage->IngressConnection = connection.get();
                            connection->FromStage = Stages.back();
                            Stages.back()->External = true;
                            TStringBuilder builder;
                            if (auto* tableNode = plan.GetValueByPath("Table")) {
                                builder << ParseTableOrIndexName(tableNode->GetStringSafe());
                            }
                            builder << ParseColumns(plan.GetValueByPath("Columns")) << " by " << ParseColumns(plan.GetValueByPath("LookupKeyColumns"));
                            Stages.back()->Operators.emplace_back("TableLookup", builder);
                        }
                    }
                } else if (auto* cteNameNode = plan.GetValueByPath("CTE Name")) {
                    auto cteName = "CTE " + cteNameNode->GetStringSafe();
                    connection = std::make_shared<TConnection>(Viz.NextGroupId(), *stage, subNodeType, connectionPlanNodeId);
                    if (auto* blocksNode = plan.GetValueByPath("Blocks")) {
                        connection->Blocks = blocksNode->GetStringSafe() == "True";
                    }
                    NodeToConnection[connectionPlanNodeId] = connection.get();
                    connection->CteConnection = true;
                    stage->Connections.push_back(connection);
                    CteRefs.emplace_back(cteName, connection);
                    connection->StatsNode = stage->StatsNode;
                }

                if (connection) {
                    if (auto* keyColumnsNode = plan.GetValueByPath("KeyColumns")) {
                        for (auto& keyColumn : keyColumnsNode->GetArray()) {
                            connection->KeyColumns.push_back(keyColumn.GetStringSafe());
                        }
                    }
                    if (auto* sortColumnsNode = plan.GetValueByPath("SortColumns")) {
                        for (auto& sortColumn : sortColumnsNode->GetArray()) {
                            connection->SortColumns.push_back(sortColumn.GetStringSafe());
                        }
                    }
                    if (auto* hashFuncNode = plan.GetValueByPath("HashFunc")) {
                        connection->HashFunc = hashFuncNode->GetStringSafe();
                    }
                    if (auto* parallelNode = plan.GetValueByPath("Parallel")) {
                        connection->Parallel = parallelNode->GetStringSafe() == "True";
                    }
                }
            } else if (planNodeType == "") {
                if (subNodeType == "Source") {
                    if (stage->IngressName) {
                        ythrow yexception() << "Plan stage already has Ingress [" << stage->IngressName << "]";
                    }

                    NodeToSource.insert(connectionPlanNodeId);
                    stage->IngressName = subNodeType;
                    stage->BuiltInIngress = true;

                    const NJson::TJsonValue* ingressRowsNode = nullptr;
                    if (stage->StatsNode) {
                        if (auto* ingressTopNode = stage->StatsNode->GetValueByPath("Ingress")) {
                            // only 1 ingress node is possible (???)
                            auto& ingress0 = (*ingressTopNode)[0];
                            auto* ingressNode = ingress0.GetValueByPath("Ingress");
                            if (ingressNode) {
                                if (!ingressNode->GetValueByPath("Bytes.Sum")) {
                                    ingressNode = nullptr;
                                }
                            }
                            if (!ingressNode) {
                                ingressNode = GetInputStatNode(ingress0);
                            }
                            if (ingressNode) {
                                if (auto* bytesNode = ingressNode->GetValueByPath("Bytes")) {
                                    stage->IngressBytes = std::make_shared<TSingleMetric>(IngressBytes,
                                        *bytesNode, 0, 0,
                                        ingressNode->GetValueByPath("FirstMessageMs"),
                                        ingressNode->GetValueByPath("LastMessageMs"),
                                        ingressNode->GetValueByPath("WaitTimeUs.History")
                                    );
                                    Min0(stage->MinTime, stage->IngressBytes->MinTime);
                                    Max0(stage->MaxTime, stage->IngressBytes->MaxTime);
                                } else {
                                    stage->IngressBytes = std::make_shared<TSingleMetric>(IngressBytes);
                                }
                                if (auto* rowsNode = ingressNode->GetValueByPath("Rows")) {
                                    stage->IngressRows = std::make_shared<TSingleMetric>(IngressRows, *rowsNode);
                                    ingressRowsNode = rowsNode;
                                } else {
                                    stage->IngressRows = std::make_shared<TSingleMetric>(IngressRows);
                                }
                            }
                        }
                    }
                    LoadSource(plan, stage->Operators, ingressRowsNode);
                } else if (subNodeType == "TableFullScan" || subNodeType == "TablePointLookup" || subNodeType == "TableRangeScan") {
                    NodeToSource.insert(connectionPlanNodeId);
                    LoadStage(stage, plan, outputConnection);
                } else {
                    stage->Connections.push_back(std::make_shared<TConnection>(Viz.NextGroupId(), *stage, "Implicit", 0));
                    Stages.push_back(std::make_shared<TStage>(Viz.NextGroupId(), this, subNodeType));
                    stage->Connections.back()->FromStage = Stages.back();
                    Stages.back()->OutputPhysicalStageId = stage->PhysicalStageId;
                    LoadStage(Stages.back(), plan, nullptr);
                }
            } else {
                ythrow yexception() << "Unexpected plan node type [" << planNodeType << "]";
            }
        }
    }

    // CPU and MEM metrics use Min/Max time from Inputs and Outputs and should be processed after them
    if (stage->StatsNode) {

        // CTE Refs are NOT processed yet, so we don't know their Min/MaxTime - parse it explicitly
        if (inputNode) {
            for (const auto& subNode : inputNode->GetArray()) {
                if (auto* statNode = GetInputStatNode(subNode)) {
                    if (auto* firstMessageMaxNode = statNode->GetValueByPath("FirstMessageMs.Min")) {
                        Min0(stage->MinTime, firstMessageMaxNode->GetIntegerSafe());
                    }
                    if (auto* lastMessageMaxNode = statNode->GetValueByPath("LastMessageMs.Max")) {
                        Max0(stage->MaxTime, lastMessageMaxNode->GetIntegerSafe());
                    }
                }
            }
        }

        if (auto* cpuTimeNode = stage->StatsNode->GetValueByPath("CpuTimeUs")) {
            stage->CpuTime = std::make_shared<TSingleMetric>(CpuTime, *cpuTimeNode, stage->MinTime, stage->MaxTime);
            MergeTotalCpu(stage->CpuTime);
        }

        if (auto* mmuNode = stage->StatsNode->GetValueByPath("MemoryUsageMB")) {
            stage->MemoryUsage = std::make_shared<TSingleMetric>(MemoryUsage, *mmuNode, stage->MinTime, stage->MaxTime);
        }

        if (auto* mmuNode = stage->StatsNode->GetValueByPath("MaxMemoryUsage")) {
            stage->MaxMemoryUsage = std::make_shared<TSingleMetric>(MaxMemoryUsage, *mmuNode, stage->MinTime, stage->MaxTime);
        }

        if (auto* witNode = stage->StatsNode->GetValueByPath("WaitInputTimeUs")) {
            stage->WaitInputTime = std::make_shared<TSingleMetric>(WaitInputTime, *witNode, stage->MinTime, stage->MaxTime);
        }

        if (auto* wotNode = stage->StatsNode->GetValueByPath("WaitOutputTimeUs")) {
            stage->WaitOutputTime = std::make_shared<TSingleMetric>(WaitOutputTime, *wotNode, stage->MinTime, stage->MaxTime);
        }

        if (auto* updateTimeNode = stage->StatsNode->GetValueByPath("UpdateTimeMs")) {
            stage->UpdateTime = updateTimeNode->GetIntegerSafe();
        }
    }

    if (stage->IngressBytes) {
        inputBytes += stage->IngressBytes->Details.Sum;
    }
    auto stageDuration = stage->MaxTime - stage->MinTime;

    if (stageDuration && inputBytes) {
        stage->InputThroughput = std::make_shared<TSingleMetric>(StageInputThroughput, inputBytes * 1000 / stageDuration);
    }

    Max0(MaxTime, stage->MaxTime);
    Max0(UpdateTime, stage->UpdateTime);
}

void TPlan::LoadSource(const NJson::TJsonValue& node, std::vector<TOperatorInfo>& stageOperators, const NJson::TJsonValue* ingressRowsNode) {

    auto operators = node.GetValueByPath("Operators");

    if (operators) {
        for (const auto& subNode : operators->GetArray()) {
            TString name;
            TString info;
            {
                TStringBuilder builder;
                builder << "Source";
                if (auto* sourceTypeNode = subNode.GetValueByPath("SourceType")) {
                    builder << " " << sourceTypeNode->GetStringSafe();
                }
                name = builder;
            }
            {
                TStringBuilder builder;
                if (auto* nameNode = subNode.GetValueByPath("Name")) {
                    builder << nameNode->GetStringSafe();
                }
                builder << "(";
                if (auto* readColumnsNode = subNode.GetValueByPath("ReadColumns")) {
                    bool firstColumn = true;
                    for (const auto& subNode : readColumnsNode->GetArray()) {
                        if (firstColumn) {
                            firstColumn = false;
                        } else {
                            builder << ", ";
                        }
                        builder << subNode.GetStringSafe();
                    }
                }
                builder << ")";
                info = builder;
            }
            stageOperators.emplace_back(name, info);
            stageOperators.back().Estimations = GetEstimation(subNode);
            if (ingressRowsNode) {
                stageOperators.back().OutputRows = std::make_shared<TSingleMetric>(OperatorOutputRows, *ingressRowsNode);
            }
            break;
        }
    }
}

void TPlanVisualizer::LoadPlans(const TString& plans, bool simplified) {
    Config.Simplified = simplified;
    NJson::TJsonReaderConfig jsonConfig;
    NJson::TJsonValue jsonNode;
    if (NJson::ReadJsonTree(plans, &jsonConfig, &jsonNode)) {
        if (auto* topNode = jsonNode.GetValueByPath(simplified ? "SimplifiedPlan" : "Plan")) {
            LoadPlans(*topNode);
        } else if (auto* topNode = jsonNode.GetValueByPath(simplified ? "queries.[0].SimplifiedPlan" : "queries.[0].Plan")) {
            LoadPlans(*topNode);
        }
    }
}

void TPlanVisualizer::LoadPlans(const NJson::TJsonValue& root) {
    if (auto* subNode = root.GetValueByPath("Plans")) {
        for (auto& plan : subNode->GetArray()) {
            if (auto* typeNode = plan.GetValueByPath("Node Type")) {
                auto nodeType = typeNode->GetStringSafe();
                LoadPlan(nodeType, plan);
            }
        }
    }
    PostProcessPlans();
}

void TPlanVisualizer::LoadPlan(const TString& nodeType, const NJson::TJsonValue& node) {
    Plans.emplace_back(std::make_shared<TPlan>(NextGroupId(), nodeType, Config, *this));
    Plans.back()->Load(node);
}

void TPlanVisualizer::PostProcessPlans() {
    // Fix CTE Refs
    for (auto& p : Plans) {
        p->ResolveCteRefs();
        p->ResolveOperatorInputs();
    }
    // Fix Layouts
    for (auto& p : Plans) {
        p->MarkLayout();
        if (BaseTime == 0) {
            BaseTime = p->BaseTime;
        } else if (p->BaseTime) {
            BaseTime = std::min(BaseTime, p->BaseTime);
        }
    }
    for (auto& p : Plans) {
        if (p->BaseTime == 0) {
            p->BaseTime = BaseTime;
        }
    }
    // Fix time Offsets
    for (auto& p : Plans) {
        p->TimeOffset = p->BaseTime - BaseTime;
        MaxTime = std::max(MaxTime, p->TimeOffset + p->MaxTime);
        UpdateTime = std::max(UpdateTime, p->TimeOffset + p->UpdateTime);
    }
    // Calc hot path
    for (auto& p : Plans) {
        p->CalcHotPath();
    }
}

} // namespace NPlan2Svg
