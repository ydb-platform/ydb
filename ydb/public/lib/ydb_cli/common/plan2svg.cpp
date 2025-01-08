#include "plan2svg.h"

#include <util/stream/output.h>

constexpr ui32 INDENT_X = 8;
constexpr ui32 GAP_X = 3;
constexpr ui32 GAP_Y = 3;
constexpr ui32 TIME_HEIGHT = 10;
constexpr ui32 INTERNAL_GAP_Y = 2;
constexpr ui32 INTERNAL_GAP_X = 2;
constexpr ui32 INTERNAL_HEIGHT = 14;
constexpr ui32 INTERNAL_WIDTH = 16;
constexpr ui32 INTERNAL_TEXT_HEIGHT = 8;
constexpr ui32 TIME_SERIES_RANGES = 32;

TString FormatDurationMs(ui64 durationMs) {
    TStringBuilder builder;

    if (durationMs && durationMs < 100) {
        builder << durationMs << "ms";
    } else {
        auto seconds = durationMs / 1'000;
        if (seconds >= 60) {
            auto minutes = seconds / 60;
            if (minutes >= 60) {
                auto hours = minutes / 60;
                builder << hours << 'h';
                if (hours < 24) {
                    auto minutes60 = minutes % 60;
                    builder << ' ';
                    if (minutes60 < 10) {
                        builder << '0';
                    }
                    builder << minutes60 << 'm';
                }
            } else {
                auto seconds60 = seconds % 60;
                builder << minutes << "m ";
                if (seconds60 < 10) {
                    builder << '0';
                }
                builder << seconds60 << 's';
            }
        } else {
            auto hundredths = (durationMs % 1'000) / 10;
            builder << seconds << '.';
            if (hundredths < 10) {
                builder << '0';
            }
            builder << hundredths << 's';
        }
    }

    return builder;
}

TString FormatDurationUs(ui64 durationUs) {
    if (durationUs && durationUs < 1000) {
        return TStringBuilder() << durationUs << "us";
    }

    return FormatDurationMs(durationUs / 1000);
}

TString FormatUsage(ui64 usec) {
    return FormatDurationUs(usec);
}

TString FormatIntegerValue(ui64 i, ui32 scale = 1000, const TString& suffix = "") {
    if (i < scale) {
        return Sprintf("%lu%s", i, suffix.c_str());
    }
    for (auto c : "KMGTP") {
        auto pcs = (i % scale) * 100 / scale;
        i /= scale;
        if (i < scale || c == 'P') {
            return Sprintf("%lu.%.2lu%c%s", i, pcs, c, suffix.c_str());
        }
    }
    return "";
}

TString FormatBytes(ui64 bytes) {
    return FormatIntegerValue(bytes, 1024, "B");
}

TString FormatTimeMs(ui64 time, bool shortFormat) {
    if (shortFormat) {
        time /= 10;
        return Sprintf("%lu.%.2lu", time / 100, time % 100);
    } else {
        time /= 1000;
        return Sprintf("%lu:%.2lu", time / 60, time % 60);
    }
}

TString FormatTimeMs(ui64 time) {
    return FormatTimeMs(time, time < 60000);
}

TString FormatTimeAgg(const TAggregation& agg, bool shortFormat) {
    TStringBuilder result;
    result << FormatTimeMs(agg.Min, shortFormat) << " | " << FormatTimeMs(agg.Avg, shortFormat) << " | " << FormatTimeMs(agg.Max, shortFormat);
    return result;
}

TString FormatMCpu(ui64 mCpu) {
    mCpu /= 10;
    return Sprintf("%lu.%.2lu", mCpu / 100, mCpu % 100);
}

TString GetEstimation(const NJson::TJsonValue& node) {
    TStringBuilder ebuilder;
    auto* eCostNode = node.GetValueByPath("E-SelfCost");
    if (!eCostNode) {
        eCostNode = node.GetValueByPath("E-Cost");
    }
    if (eCostNode) {
        auto costString = eCostNode->GetStringSafe();
        if (costString != "No estimate") {
            ebuilder << "Est:";
            double cost;
            if (TryFromString(costString, cost)) {
                if (cost >= 1e+18) {
                    ebuilder << Sprintf(" %.2e", cost);
                } else {
                    ebuilder << ' ' << FormatIntegerValue(static_cast<ui64>(cost));
                }
            }
            if (auto* eRowsNode = node.GetValueByPath("E-Rows")) {
                double rows;
                if (TryFromString(eRowsNode->GetStringSafe(), rows)) {
                    if (rows >= 1e+18) {
                        ebuilder << Sprintf(" Rows: %.2e", rows);
                    } else {
                        ebuilder << " Rows: " << FormatIntegerValue(static_cast<ui64>(rows));
                    }
                }
            }
            if (auto* eSizeNode = node.GetValueByPath("E-Size")) {
                double size;
                if (TryFromString(eSizeNode->GetStringSafe(), size)) {
                    if (size >= 1e+18) {
                        ebuilder << Sprintf(" Size: %.2e", size);
                    } else {
                        ebuilder << " Size: " << FormatBytes(static_cast<ui64>(size));
                    }
                }
            }
        }
    }
    return ebuilder;
}

bool TAggregation::Load(const NJson::TJsonValue& node) {
    if (auto* countNode = node.GetValueByPath("Count")) {
        Count = countNode->GetIntegerSafe();

        if (Count == 0) {
            return false;
        }

        if (auto* sumNode = node.GetValueByPath("Sum")) {
            Sum = sumNode->GetIntegerSafe();
        }
        Avg = Sum / Count;
        if (auto* minNode = node.GetValueByPath("Min")) {
            Min = minNode->GetIntegerSafe();
        } else {
            Min = Avg;
        }
        if (auto* maxNode = node.GetValueByPath("Max")) {
            Max = maxNode->GetIntegerSafe();
        } else {
            Max = Avg;
        }

        return true;
    }
    return false;
}

void TMetricHistory::Load(const NJson::TJsonValue& node, ui64 explicitMinTime, ui64 explicitMaxTime) {
    std::vector<ui64> times;
    std::vector<ui64> values;

    bool even = true;

    for (const auto& subNode : node.GetArray()) {
        ui64 i = subNode.GetIntegerSafe();
        if (even) times.push_back(i);
        else values.push_back(i);
        even = !even;
    }

    if (times.size() > values.size()) {
        times.resize(values.size());
    }

    Load(times, values, explicitMinTime, explicitMaxTime);
}

void TMetricHistory::Load(std::vector<ui64>& times, std::vector<ui64>& values, ui64 explicitMinTime, ui64 explicitMaxTime) {
    if (times.size() < 2) {
        return;
    }
    auto itt = times.begin();
    auto itv = values.begin();

    MinTime = explicitMinTime ? explicitMinTime : *itt;
    MaxTime = explicitMaxTime ? explicitMaxTime : times.back();

    ui64 prevValue = *itv++;
    ui64 prevTime = *itt++;

    while (itt != times.end() && *itt <= MinTime) {
        prevValue = *itv++;
        prevTime = *itt++;
    }

    Deriv.resize(TIME_SERIES_RANGES + 1);
    Deriv[0].first = MinTime;

    ui64 timeLeft = MinTime;
    for (ui32 i = 1; i <= TIME_SERIES_RANGES; i++) {

        ui64 timeRight = MinTime + (MaxTime - MinTime) * i / TIME_SERIES_RANGES;
        Deriv[i].first = timeRight;
        while (itt != times.end() && *itt <= timeRight) {
            ui64 delta = (*itv - prevValue);
            if (prevTime >= timeLeft) {
                Deriv[i].second += delta;
            } else {
                ui64 currDelta = delta * (*itt - timeLeft) / (*itt - prevTime);
                Deriv[i].second += currDelta;
                if (i > 1) {
                    Deriv[i - 1].second += delta - currDelta;
                }
            }
            prevTime = *itt++;
            prevValue = *itv++;
        }

        timeLeft = timeRight;
    }

    if (itt != times.end()) {
        Deriv[TIME_SERIES_RANGES].second += (*itv - prevValue) * (*itt - MaxTime) / (*itt - prevTime);
    }
    for (ui32 i = 1; i <= TIME_SERIES_RANGES; i++) {
        MaxDeriv = std::max(MaxDeriv, Deriv[i].second);
    }
    bool missed = false;
    for (ui32 i = 0; i < times.size(); i++) {
        auto t = times[i];
        if (t < MinTime) {
            missed = true;
            continue;
        }
        if (missed && t > MinTime) {
            Values.emplace_back(MinTime, values[i - 1]);
        }
        if (t > MaxTime) {
            if (i && times[i - 1] < MaxTime) {
                Values.emplace_back(MaxTime, values[i - 1]);
            }
            break;
        }
        Values.emplace_back(t, values[i]);
    }
    for (auto& p : Values) {
        MaxValue = std::max(MaxValue, p.second);
    }
}

TSingleMetric::TSingleMetric(std::shared_ptr<TSummaryMetric> summary, const NJson::TJsonValue& node,
        const NJson::TJsonValue* firstMessageNode, const NJson::TJsonValue* lastMessageNode,
        const NJson::TJsonValue* waitTimeUsNode)
    : Summary(summary) {

    if (!Details.Load(node)) {
        return;
    }

    Summary->Add(Details.Sum);

    if (firstMessageNode) {
        FirstMessage.Load(*firstMessageNode);
    }

    if (lastMessageNode) {
        LastMessage.Load(*lastMessageNode);
    }

    if (auto* historyNode = node.GetValueByPath("History")) {
        History.Load(*historyNode, FirstMessage.Min, LastMessage.Max);
        MinTime = History.MinTime;
        MaxTime = History.MaxTime;
    }

    if (waitTimeUsNode) {
        WaitTime.Load(*waitTimeUsNode, FirstMessage.Min, LastMessage.Max);
        MinTime = MinTime ? std::min(MinTime, WaitTime.MinTime) : WaitTime.MinTime;
        MaxTime = MaxTime ? std::max(MaxTime, WaitTime.MaxTime) : WaitTime.MaxTime;
    } else if (FirstMessage.Min && LastMessage.Max) {
        MinTime = MinTime ? std::min(MinTime, FirstMessage.Min) : FirstMessage.Min;
        MaxTime = MaxTime ? std::max(MaxTime, LastMessage.Max) : LastMessage.Max;
    }
}

void TPlan::Load(const NJson::TJsonValue& node) {
    if (auto* subplanNameNode = node.GetValueByPath("Subplan Name")) {
        auto subplanName = subplanNameNode->GetStringSafe();
        if (subplanName.StartsWith("CTE ")) {
            if (auto* nodeTypeNode = node.GetValueByPath("Node Type")) {
                CteSubPlans[subplanName] = nodeTypeNode->GetStringSafe();
            }
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
                Stages.push_back(std::make_shared<TStage>(nodeType));
                LoadStage(Stages.back(), plan, 0);
            }
        }
    }

    if (!TotalCpuTimes.empty()) {
        TotalCpuTime.Load(TotalCpuTimes, TotalCpuValues, TotalCpuTimes.front(), TotalCpuTimes.back());
    }
}

void TPlan::ResolveCteRefs() {
    for (auto& memberRef : MemberRefs) {
        auto it = CteSubPlans.find(memberRef.first);
        if (it == CteSubPlans.end()) {
            ythrow yexception() << "Can not find CTE Ref " << memberRef.first;
        }
        memberRef.second.first->Info.at(memberRef.second.second) = "Reference: " + it->second;
    }
    for (auto& cteRef : CteRefs) {
        auto it = CteStages.find(cteRef.first);
        if (it == CteStages.end()) {
            ythrow yexception() << "Can not find CTE Ref " << cteRef.first;
        }

        cteRef.second->FromStage = it->second;
        if (cteRef.second->StatsNode) {
            if (auto* inputNode = cteRef.second->StatsNode->GetValueByPath("Input")) {
                for (const auto& subNode : inputNode->GetArray()) {
                    if (auto* nameNode = subNode.GetValueByPath("Name")) {
                        if (ToString(it->second->PlanNodeId) == nameNode->GetStringSafe()) {
                            if (auto* pushNode = subNode.GetValueByPath("Push")) {
                                if (auto* bytesNode = pushNode->GetValueByPath("Bytes")) {
                                    cteRef.second->InputBytes = std::make_shared<TSingleMetric>(InputBytes,
                                        *bytesNode,
                                        pushNode->GetValueByPath("FirstMessageMs"),
                                        pushNode->GetValueByPath("LastMessageMs"),
                                        pushNode->GetValueByPath("WaitTimeUs.History")
                                    );
                                    MaxTime = std::max(MaxTime, cteRef.second->InputBytes->MaxTime);
                                }
                                if (auto* rowsNode = pushNode->GetValueByPath("Rows")) {
                                    cteRef.second->InputRows = std::make_shared<TSingleMetric>(InputRows, *rowsNode);
                                }
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
                        if (ToString(cteRef.second->StagePlanNodeId) == nameNode->GetStringSafe()) {
                            if (auto* popNode = subNode.GetValueByPath("Pop")) {
                                if (auto* bytesNode = popNode->GetValueByPath("Bytes")) {
                                    cteRef.second->CteOutputBytes = std::make_shared<TSingleMetric>(OutputBytes,
                                        *bytesNode,
                                        popNode->GetValueByPath("FirstMessageMs"),
                                        popNode->GetValueByPath("LastMessageMs"),
                                        popNode->GetValueByPath("WaitTimeUs.History")
                                    );
                                    MaxTime = std::max(MaxTime, cteRef.second->CteOutputBytes->MaxTime);
                                }
                                if (auto* rowsNode = popNode->GetValueByPath("Rows")) {
                                    cteRef.second->CteOutputRows = std::make_shared<TSingleMetric>(OutputRows, *rowsNode);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

void TPlan::LoadStage(std::shared_ptr<TStage> stage, const NJson::TJsonValue& node, ui32 parentPlanNodeId) {

    if (auto* planNodeIdNode = node.GetValueByPath("PlanNodeId")) {
        stage->PlanNodeId = planNodeIdNode->GetIntegerSafe();
    }

    if (auto* subplanNameNode = node.GetValueByPath("Subplan Name")) {
        auto subplanName = subplanNameNode->GetStringSafe();
        if (subplanName.StartsWith("CTE ")) {
            CteStages[subplanName] = stage;
        }
    }

    stage->StatsNode = node.GetValueByPath("Stats");
    auto operators = node.GetValueByPath("Operators");

    if (operators) {
        TString prevFilter;
        std::set<std::string> references;
        for (const auto& subNode : operators->GetArray()) {
            if (auto* nameNode = subNode.GetValueByPath("Name")) {
                auto name = nameNode->GetStringSafe();

                if (name == "Iterator" || name == "Member") {
                    if (auto* referenceNode = subNode.GetValueByPath(name)) {
                        auto referenceName = referenceNode->GetStringSafe();
                        if (references.contains(referenceName)) {
                            continue;
                        }
                        if (name == "Iterator" && !referenceName.StartsWith("precompute_")) {
                            continue;
                        }
                    }
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

                if (name == "Iterator" || name == "Member" || name == "ToFlow") {
                    builder << "Reference";
                } else {
                    builder << name;
                }

                if (name == "Limit") {
                    if (auto* limitNode = subNode.GetValueByPath("Limit")) {
                        builder << ": " << limitNode->GetStringSafe();
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
                            builder << " by " << sortBy;
                        }
                    }
                } else if (name == "Filter") {
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
                        builder << ": " << filter;
                    }
                } else if (name == "Aggregate") {
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
                            builder << " " << aggr;
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
                            builder << ", Group By: " << groupBy;
                        }
                    }
                } else if (name == "TableFullScan") {
                    if (auto* tableNode = subNode.GetValueByPath("Table")) {
                        auto table = tableNode->GetStringSafe();
                        auto n = table.find_last_of('/');
                        if (n != table.npos) {
                            table = table.substr(n + 1);
                        }
                        builder << " " << table;
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
                } else if (name == "TopSort" || name == "Top") {
                    if (auto* limitNode = subNode.GetValueByPath("Limit")) {
                        auto limit = limitNode->GetStringSafe();
                        if (limit) {
                            builder << ", Limit: " << limit;
                        }
                    }
                    if (auto* topSortByNode = subNode.GetValueByPath("TopSortBy")) {
                        auto topSortBy = topSortByNode->GetStringSafe();
                        if (topSortBy) {
                            builder << ", TopSortBy: " << topSortBy;
                        }
                    }
                } else if (name == "Iterator" || name == "Member" || name == "ToFlow") {
                    if (auto* referenceNode = subNode.GetValueByPath(name)) {
                        auto referenceName = referenceNode->GetStringSafe();
                        references.insert(referenceName);
                        builder << ": " << referenceName;
                        auto cteRef = "CTE " + referenceName;
                        auto stageCopy = stage;
                        MemberRefs.emplace_back(cteRef, std::make_pair<std::shared_ptr<TStage>, ui32>(std::move(stageCopy), stage->Info.size()));
                    }
                } else if (name.Contains("Join")) {
                    if (auto* conditionNode = subNode.GetValueByPath("Condition")) {
                        builder << " on " << conditionNode->GetStringSafe();
                    }
                }
                stage->Info.push_back(builder);

                auto est = GetEstimation(subNode);
                if (est) {
                    stage->Info.push_back(est);
                }

                if (name == "TableFullScan" && stage->StatsNode) {
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
                        if (auto* tableArrayNode = stage->StatsNode->GetValueByPath("Table")) {
                            for (const auto& tableNode : tableArrayNode->GetArray()) {
                                if (auto* pathNode = tableNode.GetValueByPath("Path")) {
                                    if (tablePath == pathNode->GetStringSafe()) {
                                        if (auto* bytesNode = tableNode.GetValueByPath("ReadBytes")) {
                                            stage->IngressBytes = std::make_shared<TSingleMetric>(IngressBytes, *bytesNode);
                                        }
                                        if (auto* rowsNode = tableNode.GetValueByPath("ReadRows")) {
                                            stage->IngressRows = std::make_shared<TSingleMetric>(IngressRows, *rowsNode);
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    const NJson::TJsonValue* inputNode = nullptr;

    if (stage->StatsNode) {

        if (auto* tasksNode = stage->StatsNode->GetValueByPath("Tasks")) {
            stage->Tasks = tasksNode->GetIntegerSafe();
            Tasks += stage->Tasks;
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

        if (auto* cpuTimeNode = stage->StatsNode->GetValueByPath("CpuTimeUs")) {
            stage->CpuTime = std::make_shared<TSingleMetric>(CpuTime, *cpuTimeNode);

            std::vector<ui64> updatedCpuTimes;
            std::vector<ui64> updatedCpuValues;

            auto itt = TotalCpuTimes.begin();
            auto itv = TotalCpuValues.begin();
            auto ith = stage->CpuTime->History.Values.begin();

            ui64 v0 = 0;
            ui64 v1 = 0;
            ui64 t = 0;

            while (itt != TotalCpuTimes.end() || ith != stage->CpuTime->History.Values.end()) {

                if (itt == TotalCpuTimes.end()) {
                    t = ith->first;
                    v1 = ith->second;
                    ith++;
                } else if (ith == stage->CpuTime->History.Values.end()) {
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

        if (auto* mmuNode = stage->StatsNode->GetValueByPath("MaxMemoryUsage")) {
            stage->MaxMemoryUsage = std::make_shared<TSingleMetric>(MaxMemoryUsage, *mmuNode);
        }

        if (auto* spillingComputeBytesNode = stage->StatsNode->GetValueByPath("SpillingComputeBytes")) {
            stage->SpillingComputeBytes = std::make_shared<TSingleMetric>(SpillingComputeBytes, *spillingComputeBytesNode);
        }

        if (auto* spillingComputeTimeNode = stage->StatsNode->GetValueByPath("SpillingComputeTimeUs")) {
            stage->SpillingComputeTime = std::make_shared<TSingleMetric>(SpillingComputeTime, *spillingComputeTimeNode);
        }

        if (auto* spillingChannelBytesNode = stage->StatsNode->GetValueByPath("SpillingChannelBytes")) {
            stage->SpillingChannelBytes = std::make_shared<TSingleMetric>(SpillingChannelBytes, *spillingChannelBytesNode);
        }

        if (auto* spillingChannelTimeNode = stage->StatsNode->GetValueByPath("SpillingChannelTimeUs")) {
            stage->SpillingChannelTime = std::make_shared<TSingleMetric>(SpillingChannelTime, *spillingChannelTimeNode);
        }

        if (auto* outputNode = stage->StatsNode->GetValueByPath("Output")) {
            for (const auto& subNode : outputNode->GetArray()) {
                if (auto* nameNode = subNode.GetValueByPath("Name")) {
                    if (ToString(parentPlanNodeId) == nameNode->GetStringSafe()) {
                        if (auto* popNode = subNode.GetValueByPath("Pop")) {
                            if (auto* bytesNode = popNode->GetValueByPath("Bytes")) {
                                stage->OutputBytes = std::make_shared<TSingleMetric>(OutputBytes,
                                    *bytesNode,
                                    popNode->GetValueByPath("FirstMessageMs"),
                                    popNode->GetValueByPath("LastMessageMs"),
                                    popNode->GetValueByPath("WaitTimeUs.History")
                                );
                                MaxTime = std::max(MaxTime, stage->OutputBytes->MaxTime);
                            }
                            if (auto* rowsNode = popNode->GetValueByPath("Rows")) {
                                stage->OutputRows = std::make_shared<TSingleMetric>(OutputRows, *rowsNode);
                            }
                        }
                    }
                }
            }
        }

        inputNode = stage->StatsNode->GetValueByPath("Input");

        if (auto* operatorNode = stage->StatsNode->GetValueByPath("Operator")) {
            TStringBuilder builder;
            bool first = true;
            for (const auto& subNode : operatorNode->GetArray()) {
                TString id = "UNKNOWN_ID";
                if (auto* idNode = subNode.GetValueByPath("Id")) {
                    id = idNode->GetStringSafe();
                }
                TString type = "UNKNOWN_TYPE";
                if (auto* typeNode = subNode.GetValueByPath("Type")) {
                    type = typeNode->GetStringSafe();
                }
                if (first) {
                    first = false;
                } else {
                    builder << "; ";
                }
                builder << type << ':' << id;
                ui64 rows = 0;
                ui64 bytes = 0;
                if (auto* rowsSumNode = subNode.GetValueByPath("Rows.Sum")) {
                    rows = rowsSumNode->GetIntegerSafe();
                    builder << ", Rows: \u2211" << FormatIntegerValue(rows);
                }
                if (auto* bytesSumNode = subNode.GetValueByPath("Bytes.Sum")) {
                    bytes = bytesSumNode->GetIntegerSafe();
                    builder << ", Bytes: \u2211" << FormatBytes(bytes);
                }
                if (rows && bytes) {
                    builder << ", Width: " << FormatBytes(bytes / rows);
                }
            }
            stage->OperatorInfo = builder;
        }
    }

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
            if (planNodeType == "Connection") {
                auto* keyColumnsNode = plan.GetValueByPath("KeyColumns");
                auto* sortColumnsNode = plan.GetValueByPath("SortColumns");
                if (auto* subNode = plan.GetValueByPath("Plans")) {
                    for (auto& plan : subNode->GetArray()) {
                        TString nodeType;
                        if (auto* nodeTypeNode = plan.GetValueByPath("Node Type")) {
                            nodeType = nodeTypeNode->GetStringSafe();
                        }
                        if (auto* planNodeTypeNode = plan.GetValueByPath("PlanNodeType")) {
                            auto planNodeType = planNodeTypeNode->GetStringSafe();
                            if (planNodeType) {
                                ythrow yexception() << "Unexpected plan node type [" << planNodeType << "]";
                            }
                        }
                        auto connection = std::make_shared<TConnection>(subNodeType, stage->PlanNodeId);
                        stage->Connections.push_back(connection);
                        if (keyColumnsNode) {
                            for (auto& keyColumn : keyColumnsNode->GetArray()) {
                                stage->Connections.back()->KeyColumns.push_back(keyColumn.GetStringSafe());
                            }
                        }
                        if (sortColumnsNode) {
                            for (auto& sortColumn : sortColumnsNode->GetArray()) {
                                stage->Connections.back()->SortColumns.push_back(sortColumn.GetStringSafe());
                            }
                        }

                        if (auto* planNodeIdNode = plan.GetValueByPath("PlanNodeId")) {
                            auto planNodeId = planNodeIdNode->GetStringRobust();
                            if (inputNode) {
                                for (const auto& subNode : inputNode->GetArray()) {
                                    if (auto* nameNode = subNode.GetValueByPath("Name")) {
                                        if (planNodeId == nameNode->GetStringSafe()) {
                                            if (auto* pushNode = subNode.GetValueByPath("Push")) {
                                                if (auto* bytesNode = pushNode->GetValueByPath("Bytes")) {
                                                    connection->InputBytes = std::make_shared<TSingleMetric>(InputBytes,
                                                        *bytesNode,
                                                        pushNode->GetValueByPath("FirstMessageMs"),
                                                        pushNode->GetValueByPath("LastMessageMs"),
                                                        pushNode->GetValueByPath("WaitTimeUs.History")
                                                    );
                                                    MaxTime = std::max(MaxTime, connection->InputBytes->MaxTime);
                                                }
                                                if (auto* rowsNode = pushNode->GetValueByPath("Rows")) {
                                                    connection->InputRows = std::make_shared<TSingleMetric>(InputRows, *rowsNode);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        Stages.push_back(std::make_shared<TStage>(nodeType));
                        connection->FromStage = Stages.back();
                        LoadStage(Stages.back(), plan, stage->PlanNodeId);
                    }
                } else if (auto* cteNameNode = plan.GetValueByPath("CTE Name")) {
                    auto cteName = "CTE " + cteNameNode->GetStringSafe();
                    auto connection = std::make_shared<TConnection>(subNodeType, stage->PlanNodeId);
                    connection->CteConnection = true;
                    stage->Connections.push_back(connection);
                    if (keyColumnsNode) {
                        for (auto& keyColumn : keyColumnsNode->GetArray()) {
                            stage->Connections.back()->KeyColumns.push_back(keyColumn.GetStringSafe());
                        }
                    }
                    CteRefs.emplace_back(cteName, stage->Connections.back());
                    stage->Connections.back()->StatsNode = stage->StatsNode;
                }
            } else if (planNodeType == "") {
                if (subNodeType == "Source") {
                    if (stage->IngressName) {
                        ythrow yexception() << "Plan stage already has Ingress [" << stage->IngressName << "]";
                    }
                    stage->IngressName = subNodeType;
                    LoadSource(plan, stage->Info);

                    if (stage->StatsNode) {
                        if (auto* ingressTopNode = stage->StatsNode->GetValueByPath("Ingress")) {
                            if (auto* ingressNode = (*ingressTopNode)[0].GetValueByPath("Ingress")) {
                                if (auto* bytesNode = ingressNode->GetValueByPath("Bytes")) {
                                    stage->IngressBytes = std::make_shared<TSingleMetric>(IngressBytes,
                                        *bytesNode,
                                        ingressNode->GetValueByPath("FirstMessageMs"),
                                        ingressNode->GetValueByPath("LastMessageMs"),
                                        ingressNode->GetValueByPath("WaitTimeUs.History")
                                    );
                                    MaxTime = std::max(MaxTime, stage->IngressBytes->MaxTime);
                                }
                                if (auto* rowsNode = ingressNode->GetValueByPath("Rows")) {
                                    stage->IngressRows = std::make_shared<TSingleMetric>(IngressRows, *rowsNode);
                                }
                            }
                        }
                    }
                } else {
                    stage->Connections.push_back(std::make_shared<TConnection>("Implicit", stage->PlanNodeId));
                    Stages.push_back(std::make_shared<TStage>(subNodeType));
                    stage->Connections.back()->FromStage = Stages.back();
                    LoadStage(Stages.back(), plan, stage->PlanNodeId);
                }
            } else {
                ythrow yexception() << "Unexpected plan node type [" << planNodeType << "]";
            }
        }
    }
}

void TPlan::LoadSource(const NJson::TJsonValue& node, std::vector<std::string>& info) {

    auto operators = node.GetValueByPath("Operators");

    if (operators) {
        for (const auto& subNode : operators->GetArray()) {
            TStringBuilder builder;
            builder << "Source";
            if (auto* sourceTypeNode = subNode.GetValueByPath("SourceType")) {
                builder << " " << sourceTypeNode->GetStringSafe();
            }
            if (auto* nameNode = subNode.GetValueByPath("Name")) {
                builder << " " << nameNode->GetStringSafe();
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
            info.push_back(builder);

            auto est = GetEstimation(subNode);
            if (est) {
                info.push_back(est);
            }
            break;
        }
    }
}

void TPlan::MarkStageIndent(ui32 indent, ui32& offsetY, std::shared_ptr<TStage> stage) {
    if (stage->IndentX < indent) {
        stage->IndentX = indent;
    }

    stage->OffsetY = offsetY;
    ui32 height = std::max<ui32>(stage->Connections.size() + (stage->IngressName ? 1 : 0) + 3, 4) * (INTERNAL_HEIGHT + INTERNAL_GAP_Y) + INTERNAL_GAP_Y;
    stage->Height = height;
    stage->IndentY = stage->OffsetY + height;
    offsetY += (height + GAP_Y);

    if (stage->Connections.size() > 1) {
        indent += (INDENT_X + GAP_X);
    }

    for (auto c : stage->Connections) {
        if (c->CteConnection) {
            c->CteIndentX = indent;
            c->CteOffsetY = offsetY;
            offsetY += GAP_Y + INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2;
            stage->IndentY = std::max(stage->IndentY, offsetY);
        } else {
            MarkStageIndent(indent, offsetY, c->FromStage);
            stage->IndentY = std::max(stage->IndentY, c->FromStage->IndentY);
        }
    }
}

void TPlan::MarkLayout() {
    ui32 offsetY = 0;
    MarkStageIndent(0, offsetY, Stages.front());
    // Compress Reference(s)
    for (auto& stage : Stages) {
        auto& info = stage->Info;
        ui32 i = 0;
        while (i < info.size()) {
            auto& s = info[i];
            if (s.starts_with("Reference: ")) {
                auto next = i + 1;
                if (next < info.size()) {
                    auto& sn = info[next];
                    if (sn.starts_with("Reference: ")) {
                        s.insert(9, "s");
                        while (next < info.size()) {
                            auto& sn = info[next];
                            if (sn.starts_with("Reference: ")) {
                                s += ", " + sn.substr(11);
                                info.erase(info.begin() + next);
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
            i++;
        }
    }
}

void TPlan::PrintTimeline(TStringBuilder& background, TStringBuilder& canvas, const TString& title, TAggregation& firstMessage, TAggregation& lastMessage, ui32 x, ui32 y, ui32 w, ui32 h, const TString& color) {

    auto firstMin = firstMessage.Min * w / MaxTime;
    auto lastMax = lastMessage.Max * w / MaxTime;

    background
        << "<g><title>" << title << ", Duration: " << FormatTimeMs(lastMessage.Max - firstMessage.Min) << " (" << FormatTimeAgg(firstMessage, lastMessage.Max < 60000) << " - " << FormatTimeAgg(lastMessage, lastMessage.Max < 60000) << ")</title>"
        << "<rect x='" << x + firstMin << "' y='" << y
        << "' width='" << lastMax - firstMin << "' height='" << h
        << "' stroke-width='0' fill='" << color << "'/>" << Endl;

    if (firstMessage.Min < firstMessage.Max) {
        auto firstAvg = firstMessage.Avg * w / MaxTime;
        auto firstMax = firstMessage.Max * w / MaxTime;
        canvas
            << "  <line x1='" << x + firstMin << "' y1='" << y + 2
            << "' x2='" << x + firstMax << "' y2='" << y + 2
            << "' stroke-width='3' stroke='" << Config.Palette.MinMaxLine << "' stroke-dasharray='1,1' />" << Endl
            << "  <line x1='" << x + firstAvg << "' y1='" << y
            << "' x2='" << x + firstAvg << "' y2='" << y + h / 2
            << "' stroke-width='3' stroke='" << Config.Palette.MinMaxLine << "' />" << Endl;
    }

    if (lastMessage.Min < lastMessage.Max) {
        auto lastMin = lastMessage.Min * w / MaxTime;
        auto lastAvg = lastMessage.Avg * w / MaxTime;
        canvas
            << "  <line x1='" << x + lastMin << "' y1='" << y + h - 2
            << "' x2='" << x + lastMax << "' y2='" << y + h - 2
            << "' stroke-width='3' stroke='" << Config.Palette.MinMaxLine << "' stroke-dasharray='1,1' />" << Endl
            << "  <line x1='" << x + lastAvg << "' y1='" << y + h / 2
            << "' x2='" << x + lastAvg << "' y2='" << y + h
            << "' stroke-width='3' stroke='" << Config.Palette.MinMaxLine << "' />" << Endl;
    }

    background
        << "</g>" << Endl;
}

void TPlan::PrintWaitTime(TStringBuilder& background, std::shared_ptr<TSingleMetric> metric, ui32 x, ui32 y, ui32 w, ui32 h, const TString& fillColor) {

    if (metric->WaitTime.MaxDeriv == 0) {
        return;
    }

    background
        << "<polygon points='"
        << x + metric->WaitTime.Deriv.front().first * w / MaxTime << "," << y + (h - 1) << " ";

    for (auto& item : metric->WaitTime.Deriv) {
        background << x + item.first * w / MaxTime << "," << y + (h - std::max<ui32>(item.second * h / metric->WaitTime.MaxDeriv, 1)) << " ";
    }

    background
        << x + metric->WaitTime.Deriv.back().first * w / MaxTime << "," << y + (h - 1) << " "
        << "' stroke='none' fill='" << fillColor << "' />" << Endl;
}

void TPlan::PrintDeriv(TStringBuilder& canvas, TMetricHistory& history, ui32 x, ui32 y, ui32 w, ui32 h, const TString& title, const TString& lineColor, const TString& fillColor) {

    if (history.MaxDeriv == 0) {
        return;
    }

    if (title) {
        canvas << "<g><title>" << title << "</title>" << Endl;
    }

    canvas
        << (fillColor ? "<polygon points='" : "<polyline points='")
        << x + history.Deriv.front().first * w / MaxTime << "," << y + (h - 1) << " ";

    for (auto& item : history.Deriv) {
        canvas << x + item.first * w / MaxTime << "," << y + (h - std::max<ui32>(item.second * h / history.MaxDeriv, 1)) << " ";
    }

    canvas
        << x + history.Deriv.back().first * w / MaxTime << "," << y + (h - 1) << " "
        << "' stroke-width='1' stroke='" << lineColor << "' fill='" << (fillColor ? fillColor : "none") << "' />" << Endl;

    if (title) {
        canvas << "</g>" << Endl;
    }
}

void TPlan::PrintValues(TStringBuilder& canvas, std::shared_ptr<TSingleMetric> metric, ui32 x, ui32 y, ui32 w, ui32 h, const TString& title, const TString& lineColor, const TString& fillColor) {

    if (metric->History.MaxValue == 0) {
        return;
    }

    if (title) {
        canvas << "<g><title>" << title << "</title>" << Endl;
    }

    canvas
        << (fillColor ? "<polygon points='" : "<polyline points='")
        << x + metric->History.Values.front().first * w / MaxTime << "," << y + (h - 1) << " ";

    for (auto& item : metric->History.Values) {
        canvas << x + item.first * w / MaxTime << "," << y + (h - std::max<ui32>(item.second * h / metric->History.MaxValue, 1)) << " ";
    }

    canvas
        << x + metric->History.Values.back().first * w / MaxTime << "," << y + (h - 1) << " "
        << "' stroke-width='1' stroke='" << lineColor << "' fill='" << (fillColor ? fillColor : "none") << "' />" << Endl;

    if (title) {
        canvas << "</g>" << Endl;
    }
}

void TPlan::PrintStageSummary(TStringBuilder& background, TStringBuilder&, ui32 y0, std::shared_ptr<TSingleMetric> metric, const TString& mediumColor, const TString& lightColor, const TString& textSum, const TString& tooltip) {
    ui32 x0 = Config.HeaderWidth + GAP_X + INTERNAL_GAP_X;
    ui32 width = Config.SummaryWidth - INTERNAL_GAP_X * 2;
    if (metric->Summary && metric->Summary->Max) {
        width = metric->Details.Sum * width / metric->Summary->Max;
    }
    background
        << "<g><title>" << tooltip << "</title>" << Endl;
    if (metric->Details.Max) {
        auto wavg = width / 2;
        if (metric->Details.Max > metric->Details.Min) {
            wavg = (metric->Details.Avg - metric->Details.Min) * width / (metric->Details.Max - metric->Details.Min);
        }
        background
        << "  <rect x='" << x0 << "' y='" << y0
        << "' width='" << width << "' height='" << INTERNAL_HEIGHT
        << "' stroke-width='0' fill='" << lightColor << "'/>"
        << "  <polygon points='" << x0 << "," << y0 << " "
        << x0 + wavg << "," << y0 + INTERNAL_HEIGHT - metric->Details.Avg * INTERNAL_HEIGHT / metric->Details.Max << " "
        << x0 + width << "," << y0 + INTERNAL_HEIGHT - metric->Details.Min * INTERNAL_HEIGHT / metric->Details.Max << " "
        << x0 + width << "," << y0 + INTERNAL_HEIGHT << " "
        << x0 << "," << y0 + INTERNAL_HEIGHT
        << "' stroke='none' fill='" << mediumColor << "'/>" << Endl;
    } else {
        background
        << "  <rect x='" << x0 << "' y='" << y0
        << "' width='" << width << "' height='" << INTERNAL_HEIGHT
        << "' stroke-width='0' fill='" << mediumColor << "'/>" << Endl;
    }
    if (textSum) {
        background
        << "<rect x='" << x0 << "' y='" << y0 + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2
        << "' width='" << textSum.size() * INTERNAL_TEXT_HEIGHT * 7 / 10 << "' height='" << INTERNAL_TEXT_HEIGHT + 1
        << "' stroke-width='0' opacity='0.5' fill='" << Config.Palette.StageMain << "'/>" << Endl
        << "<text font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextSummary << "' x='" << x0
        << "' y='" << y0 + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << textSum << "</text>" << Endl;
    }
    background
        << "</g>" << Endl;
}

void TPlan::PrintSvg(ui64 maxTime, ui32& offsetY, TStringBuilder& background, TStringBuilder& canvas) {
    OffsetY = offsetY;
    ui32 planHeight = 0;

    for (auto& s : Stages) {
        planHeight = std::max(planHeight, s->IndentY);
        background
            << "<rect x='" << s->IndentX << "' y='" << s->OffsetY + offsetY
            << "' width='" << Config.HeaderWidth - s->IndentX - INTERNAL_WIDTH << "' height='" << s->Height
            << "' stroke-width='0' fill='" << Config.Palette.StageMain << "'/>" << Endl;
        auto x = Config.HeaderWidth + GAP_X;
        background
            << "<rect x='" << x << "' y='" << s->OffsetY + offsetY
            << "' width='" << Config.SummaryWidth << "' height='" << s->Height
            << "' stroke-width='0' fill='" << Config.Palette.StageMain << "'/>" << Endl;
        x += Config.SummaryWidth + GAP_X;
        background
            << "<rect x='" << x << "' y='" << s->OffsetY + offsetY
            << "' width='" << Config.Width - x << "' height='" << s->Height
            << "' stroke-width='0' fill='" << Config.Palette.StageMain << "'/>" << Endl;
        if (s->Connections.size() > 1) {
            ui32 y = s->OffsetY + s->Height;
            background
                << "<rect x='" << s->IndentX << "' y='" << y + offsetY
                << "' width='" << INDENT_X << "' height='" << s->IndentY - y
                << "' stroke-width='0' fill='" << Config.Palette.StageMain << "'/>" << Endl;
        }

        if (s->OperatorInfo) {
        background
            << "<g><title>" << s->OperatorInfo << "</title>" << Endl;
        }
        background
            << "<circle cx='" << s->IndentX + INTERNAL_WIDTH / 2
            << "' cy='" << s->OffsetY + s->Height / 2 + offsetY
            << "' r='" << INTERNAL_WIDTH / 2 - 1
            << "' stroke='" << Config.Palette.StageMain << "' stroke-width='1' fill='" << Config.Palette.StageClone << "' />" << Endl
            << "<text text-anchor='middle' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT
            << "px' fill='" << Config.Palette.StageText << "' x='" << s->IndentX + INTERNAL_WIDTH / 2
            << "' y='" << s->OffsetY + s->Height / 2 + offsetY + INTERNAL_TEXT_HEIGHT / 2
            << "'>" << ToString(s->PhysicalStageId) << "</text>" << Endl;
        if (s->OperatorInfo) {
        background
            << "</g>" << Endl;
        }

        {
            ui32 y0 = s->OffsetY + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 + offsetY;
            if (!s->Info.empty()) {
                for (auto text : s->Info) {
                    canvas
                        << "<g><title>" << text << "</title>"
                        << "<text clip-path='url(#clipTextPath)' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageText << "' x='" << s->IndentX + INTERNAL_WIDTH + 2
                        << "' y='" << y0 << "'>" << text << "</text>" << "</g>" << Endl;
                    y0 += (INTERNAL_TEXT_HEIGHT + INTERNAL_GAP_Y);
                }
            } else {
                canvas
                    << "<text clip-path='url(#clipTextPath)' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageText << "' x='" << s->IndentX + INTERNAL_WIDTH + 2
                    << "' y='" << y0 << "'>" << s->NodeType << "</text>" << Endl;
            }
        }


        ui32 y0 = s->OffsetY + offsetY + INTERNAL_GAP_Y;

        auto tx0 = Config.HeaderWidth + GAP_X + Config.SummaryWidth + GAP_X + INTERNAL_GAP_X;
        auto tx1 = Config.Width - INTERNAL_GAP_X;
        auto tw = tx1 - tx0;
        auto px = tx0 + TimeOffset * tw / maxTime;
        auto pw = MaxTime * tw / maxTime;

        auto taskCount = s->CpuTime ? s->CpuTime->Details.Count : 0;

        if (s->OutputBytes) {
            auto textSum = FormatBytes(s->OutputBytes->Details.Sum);
            TStringBuilder tooltip;
            tooltip
                << "Output "
                << s->OutputBytes->Details.Sum * 100 / s->OutputBytes->Summary->Value << "%, \u2211"
                << textSum << ", " << FormatBytes(s->OutputBytes->Details.Min) << " | "
                << FormatBytes(s->OutputBytes->Details.Avg) << " | " << FormatBytes(s->OutputBytes->Details.Max);
            if (s->OutputRows && s->OutputRows->Details.Sum) {
            tooltip
                << ", Rows \u2211"
                << FormatIntegerValue(s->OutputRows->Details.Sum) << ", " << FormatIntegerValue(s->OutputRows->Details.Min) << " | "
                << FormatIntegerValue(s->OutputRows->Details.Avg) << " | " << FormatIntegerValue(s->OutputRows->Details.Max)
                << ", Width " << FormatBytes(s->OutputBytes->Details.Sum / s->OutputRows->Details.Sum);
            }
            PrintStageSummary(background, canvas, y0, s->OutputBytes, Config.Palette.OutputMedium, Config.Palette.OutputLight, textSum, tooltip);

            if (s->SpillingChannelBytes && s->SpillingChannelBytes->Details.Sum) {
                auto textSum = FormatBytes(s->SpillingChannelBytes->Details.Sum);
                auto x1 = Config.HeaderWidth + GAP_X + Config.SummaryWidth + - INTERNAL_GAP_X;
                auto x0 = x1 - textSum.size() * INTERNAL_TEXT_HEIGHT * 7 / 10;
                background
                << "<g><title>" << "Channel Spilling \u2211" << textSum
                << ", " << FormatBytes(s->SpillingChannelBytes->Details.Min) << " | "
                << FormatBytes(s->SpillingChannelBytes->Details.Avg) << " | " << FormatBytes(s->SpillingChannelBytes->Details.Max)
                << "</title>" << Endl
                << "<rect x='" << x0 << "' y='" << y0 + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2
                << "' width='" << x1 - x0 << "' height='" << INTERNAL_TEXT_HEIGHT + 1
                << "' stroke-width='0' fill='" << Config.Palette.SpillingBytesLight << "'/>" << Endl
                << "<text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextSummary << "' x='" << x1 - 1
                << "' y='" << y0 + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << textSum << "</text>" << Endl
                << "</g>" << Endl;
            }

            if (s->OutputBytes->Details.Count != taskCount) {
                canvas
                << "<text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageTextHighlight << "' x='" << Config.HeaderWidth
                << "' y='" << y0 + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << s->OutputBytes->Details.Count << "</text>" << Endl;
            }

            auto d = s->OutputBytes->MaxTime - s->OutputBytes->MinTime;
            TStringBuilder title;
            title << "Output";
            if (d) {
                title << " " << FormatBytes(s->OutputBytes->Details.Sum * 1000 / d) << "/s";
                if (s->OutputRows) {
                    title << ", Rows " << FormatIntegerValue(s->OutputRows->Details.Sum * 1000 / d) << "/s";
                }
            }
            PrintTimeline(background, canvas, title, s->OutputBytes->FirstMessage, s->OutputBytes->LastMessage, px, y0, pw, INTERNAL_HEIGHT, Config.Palette.OutputMedium);

            if (!s->OutputBytes->WaitTime.Deriv.empty()) {
                PrintWaitTime(background, s->OutputBytes, px, y0, pw, INTERNAL_HEIGHT, Config.Palette.OutputLight);
            }

            if (!s->OutputBytes->History.Deriv.empty()) {
                PrintDeriv(canvas, s->OutputBytes->History, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.OutputDark);
            }
        }

        // Output is mandatory metric
        y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;

        if (s->MaxMemoryUsage) {
            auto textSum = FormatBytes(s->MaxMemoryUsage->Details.Sum);
            TStringBuilder tooltip;
            tooltip
                << "Memory "
                << s->MaxMemoryUsage->Details.Sum * 100 / s->MaxMemoryUsage->Summary->Value << "%, \u2211"
                << textSum << ", " << FormatBytes(s->MaxMemoryUsage->Details.Min) << " | "
                << FormatBytes(s->MaxMemoryUsage->Details.Avg) << " | " << FormatBytes(s->MaxMemoryUsage->Details.Max);
            PrintStageSummary(background, canvas, y0, s->MaxMemoryUsage, Config.Palette.MemMedium, Config.Palette.MemLight, textSum, tooltip);

            if (s->SpillingComputeBytes && s->SpillingComputeBytes->Details.Sum) {
                auto textSum = FormatBytes(s->SpillingComputeBytes->Details.Sum);
                auto x1 = Config.HeaderWidth + GAP_X + Config.SummaryWidth + - INTERNAL_GAP_X;
                auto x0 = x1 - textSum.size() * INTERNAL_TEXT_HEIGHT * 7 / 10;
                background
                << "<g><title>" << "Compute Spilling \u2211" << textSum
                << ", " << FormatBytes(s->SpillingComputeBytes->Details.Min) << " | "
                << FormatBytes(s->SpillingComputeBytes->Details.Avg) << " | " << FormatBytes(s->SpillingComputeBytes->Details.Max)
                << "</title>" << Endl
                << "<rect x='" << x0 << "' y='" << y0 + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2
                << "' width='" << x1 - x0 << "' height='" << INTERNAL_TEXT_HEIGHT + 1
                << "' stroke-width='0' fill='" << Config.Palette.SpillingBytesLight << "'/>" << Endl
                << "<text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextSummary << "' x='" << x1 - 1
                << "' y='" << y0 + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << textSum << "</text>" << Endl
                << "</g>" << Endl;
            }

            if (s->MaxMemoryUsage->Details.Count != taskCount) {
                canvas
                << "<text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageTextHighlight << "' x='" << Config.HeaderWidth
                << "' y='" << y0 + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << s->MaxMemoryUsage->Details.Count << "</text>" << Endl;
            }

            if (!s->MaxMemoryUsage->History.Values.empty()) {
                PrintValues(canvas, s->MaxMemoryUsage, px, y0, pw, INTERNAL_HEIGHT, "Max MEM " + FormatBytes(s->MaxMemoryUsage->History.MaxValue), Config.Palette.MemMedium, Config.Palette.MemMedium);
            }

            if (s->SpillingComputeBytes && !s->SpillingComputeBytes->History.Deriv.empty()) {
                PrintDeriv(canvas, s->SpillingComputeBytes->History, px, y0, pw, INTERNAL_HEIGHT, "Spilling Compute", Config.Palette.SpillingBytesMedium, Config.Palette.SpillingBytesLight);
            }
        }

        y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;

        if (s->CpuTime) {
            auto textSum = FormatUsage(s->CpuTime->Details.Sum);
            TStringBuilder tooltip;
            tooltip
                << "CPU Usage "
                << s->CpuTime->Details.Sum * 100 / s->CpuTime->Summary->Value << "%, \u2211"
                << textSum << ", " << FormatUsage(s->CpuTime->Details.Min) << " | "
                << FormatUsage(s->CpuTime->Details.Avg) << " | " << FormatUsage(s->CpuTime->Details.Max);
            PrintStageSummary(background, canvas, y0, s->CpuTime, Config.Palette.CpuMedium, Config.Palette.CpuLight, textSum, tooltip);

            if (taskCount) {
                canvas
                << "<text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageText << "' x='" << Config.HeaderWidth
                << "' y='" << y0 + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << taskCount << "</text>" << Endl;
            }

            if (!s->CpuTime->History.Deriv.empty()) {
                auto maxCpu = s->CpuTime->History.MaxDeriv * TIME_SERIES_RANGES / (s->CpuTime->History.MaxTime - s->CpuTime->History.MinTime);
                PrintDeriv(canvas, s->CpuTime->History, px, y0, pw, INTERNAL_HEIGHT, "Max CPU " + FormatMCpu(maxCpu), Config.Palette.CpuMedium, Config.Palette.CpuLight);
            }

            if (s->SpillingComputeTime && !s->SpillingComputeTime->History.Deriv.empty()) {
                PrintDeriv(canvas, s->SpillingComputeTime->History, px, y0, pw, INTERNAL_HEIGHT, "Spilling Compute", Config.Palette.SpillingTimeMedium);
            }
        }

        y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;

        for (auto& c : s->Connections) {

            auto x = c->CteConnection ? c->CteIndentX : c->FromStage->IndentX;
            auto y = (c->CteConnection ? c->CteOffsetY : c->FromStage->OffsetY) + offsetY;

            if (c->CteConnection) {
                auto xx = x;
                background
                    << "<rect x='" << xx << "' y='" << y
                    << "' width='" << Config.HeaderWidth - xx - INTERNAL_WIDTH<< "' height='" << INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2
                    << "' stroke-width='1' stroke='" << Config.Palette.StageMain << "' fill='" << Config.Palette.StageClone << "'/>" << Endl;
                xx = Config.HeaderWidth + GAP_X;
                background
                    << "<rect x='" << xx << "' y='" << y
                    << "' width='" << Config.SummaryWidth << "' height='" << INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2
                    << "' stroke-width='1' stroke='" << Config.Palette.StageMain << "' fill='" << Config.Palette.StageClone << "'/>" << Endl;
                xx += Config.SummaryWidth + GAP_X;
                background
                    << "<rect x='" << xx << "' y='" << y
                    << "' width='" << Config.Width - xx << "' height='" << INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2
                    << "' stroke-width='1' stroke='" << Config.Palette.StageMain << "' fill='" << Config.Palette.StageClone << "'/>" << Endl;
                background
                    << "<circle cx='" << c->CteIndentX + INTERNAL_WIDTH * 3 / 2
                    << "' cy='" << c->CteOffsetY + offsetY + INTERNAL_HEIGHT / 2 + INTERNAL_GAP_Y
                    << "' r='" << std::min(INTERNAL_HEIGHT, INTERNAL_WIDTH) / 2 - 1
                    << "' stroke='" << Config.Palette.StageMain << "' stroke-width='1' fill='" << Config.Palette.StageClone << "' />" << Endl
                    << "<text text-anchor='middle' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT
                    << "px' fill='" << Config.Palette.StageText << "' x='" << c->CteIndentX + INTERNAL_WIDTH * 3 / 2
                    << "' y='" << c->CteOffsetY + offsetY + INTERNAL_HEIGHT / 2 + INTERNAL_GAP_Y + INTERNAL_TEXT_HEIGHT / 2
                    << "'>" << ToString(c->FromStage->PhysicalStageId) << "</text>" << Endl;

                auto s = c->FromStage->Info.empty() ? c->FromStage->NodeType : c->FromStage->Info[0];
                canvas
                    << "<text clip-path='url(#clipTextPath)' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageText << "' x='" << x + INTERNAL_WIDTH * 2 + 2
                    << "' y='" << y + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << s << "</text>" << Endl;

                if (c->CteOutputBytes) {
                    auto textSum = FormatBytes(c->CteOutputBytes->Details.Sum);
                    TStringBuilder tooltip;
                    tooltip
                        << "Output "
                        << c->CteOutputBytes->Details.Sum * 100 / c->CteOutputBytes->Summary->Value << "%, \u2211"
                        << textSum << ", " << FormatBytes(c->CteOutputBytes->Details.Min) << " | "
                        << FormatBytes(c->CteOutputBytes->Details.Avg) << " | " << FormatBytes(c->CteOutputBytes->Details.Max);
                    if (c->CteOutputRows && c->CteOutputRows->Details.Sum) {
                    tooltip
                        << ", Rows \u2211"
                        << FormatIntegerValue(c->CteOutputRows->Details.Sum) << ", " << FormatIntegerValue(c->CteOutputRows->Details.Min) << " | "
                        << FormatIntegerValue(c->CteOutputRows->Details.Avg) << " | " << FormatIntegerValue(c->CteOutputRows->Details.Max)
                        << ", Width " << FormatBytes(c->CteOutputBytes->Details.Sum / c->CteOutputRows->Details.Sum);
                    }
                    PrintStageSummary(background, canvas, y + INTERNAL_GAP_Y, c->CteOutputBytes, Config.Palette.OutputMedium, Config.Palette.OutputLight, textSum, tooltip);

                    canvas
                        << "<text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageText << "' x='" << Config.HeaderWidth
                        << "' y='" << y + INTERNAL_GAP_Y + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << c->CteOutputBytes->Details.Count << "</text>" << Endl;

                    auto d = c->CteOutputBytes->MaxTime - c->CteOutputBytes->MinTime;
                    TStringBuilder title;
                    title << "Output";
                    if (d) {
                        title << " " << FormatBytes(c->CteOutputBytes->Details.Sum * 1000 / d) << "/s";
                        if (c->CteOutputRows) {
                            title << ", Rows " << FormatIntegerValue(c->CteOutputRows->Details.Sum * 1000 / d) << "/s";
                        }
                    }
                    PrintTimeline(background, canvas, title, c->CteOutputBytes->FirstMessage, c->CteOutputBytes->LastMessage, px, y + INTERNAL_GAP_Y, pw, INTERNAL_HEIGHT, Config.Palette.OutputMedium);

                    if (!c->CteOutputBytes->WaitTime.Deriv.empty()) {
                        PrintWaitTime(background, c->CteOutputBytes, px, y + INTERNAL_GAP_Y, pw, INTERNAL_HEIGHT, Config.Palette.OutputLight);
                    }

                    if (!c->CteOutputBytes->History.Deriv.empty()) {
                        PrintDeriv(canvas, c->CteOutputBytes->History, px, y + INTERNAL_GAP_Y, pw, INTERNAL_HEIGHT, "", Config.Palette.OutputDark);
                    }
                }
            }

            TString mark;
            if (c->NodeType == "HashShuffle")    mark = "H";
            else if (c->NodeType == "Merge")     mark = "Me";
            else if (c->NodeType == "Map")       mark = "Ma";
            else if (c->NodeType == "UnionAll")  mark = "U";
            else if (c->NodeType == "Broadcast") mark = "B";
            else                                 mark = "?";

            canvas
                << "<g><title>Connection: " << c->NodeType;
            if (!c->KeyColumns.empty()) {
                canvas << " KeyColumns: ";
                bool first = true;
                for (auto k : c->KeyColumns) {
                    if (first) {
                        first = false;
                    } else {
                        canvas << ", ";
                    }
                    canvas << k;
                }
            }
            if (!c->SortColumns.empty()) {
                canvas << " SortColumns: ";
                bool first = true;
                for (auto s : c->SortColumns) {
                    if (first) {
                        first = false;
                    } else {
                        canvas << ", ";
                    }
                    canvas << s;
                }
            }
            canvas
                << "</title>" << Endl
                << "  <polygon points='" << x + INTERNAL_WIDTH << "," << y + INTERNAL_HEIGHT << " "
                << x << "," << y + INTERNAL_HEIGHT << " ";
            if (s->Connections.size() >= 2) {
            canvas
                << x - INTERNAL_GAP_X * 2 << "," << y + INTERNAL_HEIGHT / 2 << " ";
            }
            canvas
                << x << "," << y << " ";
            if (s->Connections.size() == 1) {
            canvas
                << x + INTERNAL_WIDTH / 2 << "," << y - INTERNAL_GAP_Y * 2 << " ";
            }
            canvas
                << x + INTERNAL_WIDTH << "," << y << "' stroke-width='1' stroke='" << Config.Palette.ConnectionLine << "' fill='" << Config.Palette.ConnectionFill << "'/>" << Endl
                << "  <text text-anchor='middle' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT - 2 << "px' fill='" << Config.Palette.ConnectionText << "' x='" << x + INTERNAL_WIDTH / 2
                << "' y='" << y + INTERNAL_TEXT_HEIGHT  + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 - 1 << "'>" << mark << "</text>" << Endl
                << "</g>" << Endl;

            if (c->InputBytes) {
                auto textSum = FormatBytes(c->InputBytes->Details.Sum);
                TStringBuilder tooltip;
                tooltip
                    << "Input "
                    << c->InputBytes->Details.Sum * 100 / c->InputBytes->Summary->Value << "%, \u2211"
                    << textSum << ", " << FormatBytes(c->InputBytes->Details.Min) << " | "
                    << FormatBytes(c->InputBytes->Details.Avg) << " | " << FormatBytes(c->InputBytes->Details.Max);
                if (c->InputRows && c->InputRows->Details.Sum) {
                tooltip
                    << ", Rows \u2211"
                    << FormatIntegerValue(c->InputRows->Details.Sum) << ", " << FormatIntegerValue(c->InputRows->Details.Min) << " | "
                    << FormatIntegerValue(c->InputRows->Details.Avg) << " | " << FormatIntegerValue(c->InputRows->Details.Max)
                    << ", Width " << FormatBytes(c->InputBytes->Details.Sum / c->InputRows->Details.Sum);
                }
                PrintStageSummary(background, canvas, y0, c->InputBytes, Config.Palette.InputMedium, Config.Palette.InputLight, textSum, tooltip);

                if (c->InputBytes->Details.Count != taskCount) {
                    canvas
                    << "<text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageTextHighlight << "' x='" << Config.HeaderWidth
                    << "' y='" << y0 + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << c->InputBytes->Details.Count << "</text>" << Endl;
                }

                auto d = c->InputBytes->MaxTime - c->InputBytes->MinTime;
                TStringBuilder title;
                title << "Input";
                if (d) {
                    title << " " << FormatBytes(c->InputBytes->Details.Sum * 1000 / d) << "/s";
                    if (c->InputRows) {
                        title << ", Rows " << FormatIntegerValue(c->InputRows->Details.Sum * 1000 / d) << "/s";
                    }
                }
                PrintTimeline(background, canvas, title, c->InputBytes->FirstMessage, c->InputBytes->LastMessage, px, y0, pw, INTERNAL_HEIGHT, Config.Palette.InputMedium);

                if (!c->InputBytes->WaitTime.Deriv.empty()) {
                    PrintWaitTime(background, c->InputBytes, px, y0, pw, INTERNAL_HEIGHT, Config.Palette.InputLight);
                }

                if (!c->InputBytes->History.Deriv.empty()) {
                    PrintDeriv(canvas, c->InputBytes->History, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.InputDark);
                }

                y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;
            }
        }

        if (s->IngressBytes) {
            auto textSum = FormatBytes(s->IngressBytes->Details.Sum);
            TStringBuilder tooltip;
            tooltip
                << "Ingress "
                << s->IngressBytes->Details.Sum * 100 / s->IngressBytes->Summary->Value << "%, \u2211"
                << textSum << ", " << FormatBytes(s->IngressBytes->Details.Min) << " | "
                << FormatBytes(s->IngressBytes->Details.Avg) << " | " << FormatBytes(s->IngressBytes->Details.Max);
            if (s->IngressRows && s->IngressRows->Details.Sum) {
            tooltip
                << ", Rows \u2211"
                << FormatIntegerValue(s->IngressRows->Details.Sum) << ", " << FormatIntegerValue(s->IngressRows->Details.Min) << " | "
                << FormatIntegerValue(s->IngressRows->Details.Avg) << " | " << FormatIntegerValue(s->IngressRows->Details.Max)
                << ", Width " << FormatBytes(s->IngressBytes->Details.Sum / s->IngressRows->Details.Sum);
            }
            PrintStageSummary(background, canvas, y0, s->IngressBytes, Config.Palette.IngressMedium, Config.Palette.IngressLight, textSum, tooltip);

            if (s->IngressBytes->Details.Count != taskCount) {
                canvas
                << "<text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageTextHighlight << "' x='" << Config.HeaderWidth
                << "' y='" << y0 + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << s->IngressBytes->Details.Count << "</text>" << Endl;
            }

            auto d = s->IngressBytes->MaxTime - s->IngressBytes->MinTime;
            TStringBuilder title;
            title << "Ingress";
            if (d) {
                title << " " << FormatBytes(s->IngressBytes->Details.Sum * 1000 / d) << "/s";
                if (s->IngressRows) {
                    title << ", Rows " << FormatIntegerValue(s->IngressRows->Details.Sum / d) << "/s";
                }
            }
            PrintTimeline(background, canvas, title, s->IngressBytes->FirstMessage, s->IngressBytes->LastMessage, px, y0, pw, INTERNAL_HEIGHT, Config.Palette.IngressMedium);

            if (!s->IngressBytes->WaitTime.Deriv.empty()) {
                PrintWaitTime(background, s->IngressBytes, px, y0, pw, INTERNAL_HEIGHT, Config.Palette.IngressLight);
            }

            if (!s->IngressBytes->History.Deriv.empty()) {
                PrintDeriv(canvas, s->IngressBytes->History, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.IngressDark);
            }

            y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;
        }
    }

    offsetY += planHeight;
}

TColorPalette::TColorPalette() {
    StageMain     = "var(--stage-main, #F2F2F2)";
    StageClone    = "var(--stage-clone, #D9D9D9)";
    StageText     = "var(--stage-text, #262626)";
    StageTextHighlight = "var(--stage-texthl, #EA0703)";
    StageGrid     = "var(--stage-grid, #B2B2B2)";
    IngressDark   = "var(--ingress-dark, #574F38)";
    IngressMedium = "var(--ingress-medium, #82723C)";
    IngressLight  = "var(--ingress-light, #C0A645)";
    InputDark     = "var(--input-dark, #315B34)";
    InputMedium   = "var(--input-medium, #379A33)";
    InputLight    = "var(--input-light, #3AC936)";
    OutputDark    = "var(--output-dark, #3F5799)";
    OutputMedium  = "var(--output-medium, #4E79EB)";
    OutputLight   = "var(--output-light, #86A8FF)";
    MemMedium     = "var(--mem-medium, #543B70)";
    MemLight      = "var(--mem-light, #854EBD)";
    CpuMedium     = "var(--cpu-medium, #EA0703)";
    CpuLight      = "var(--cpu-light, #FF6866)";
    ConnectionFill= "var(--conn-fill, #BFBFBF)";
    ConnectionLine= "var(--conn-line, #BFBFBF)";
    ConnectionText= "var(--conn-text, #393939)";
    MinMaxLine    = "var(--minmax-line, #FFDB4D)";
    TextLight     = "var(--text-light, #FFFFFF)";
    TextInverted  = "var(--text-inv, #FFFFFF)";
    TextSummary   = "var(--text-summary, #262626)";
    SpillingBytesDark   = "var(--spill-dark, #406B61)";
    SpillingBytesMedium = "var(--spill-medium, #599587)";
    SpillingBytesLight  = "var(--spill-light, #72C0AE)";
    SpillingTimeDark    = "var(--spill-dark, #406B61)";
    SpillingTimeMedium  = "var(--spill-medium, #599587)";
    SpillingTimeLight   = "var(--spill-light, #72C0AE)";
}

TPlanViewConfig::TPlanViewConfig() {
    HeaderWidth = 300;
    SummaryWidth = 128;
    Width = 1024;
}


void TPlanVisualizer::LoadPlans(const TString& plans, bool simplified) {
    Config.Simplified = simplified;
    NJson::TJsonReaderConfig jsonConfig;
    NJson::TJsonValue jsonNode;
    if (NJson::ReadJsonTree(plans, &jsonConfig, &jsonNode)) {
        if (auto* topNode = jsonNode.GetValueByPath(simplified ? "SimplifiedPlan" : "Plan")) {
            if (auto* subNode = topNode->GetValueByPath("Plans")) {
                for (auto& plan : subNode->GetArray()) {
                    if (auto* typeNode = plan.GetValueByPath("Node Type")) {
                        auto nodeType = typeNode->GetStringSafe();
                        LoadPlan(nodeType, plan);
                    }
                }
            }
        }
    }
    PostProcessPlans();
}

void TPlanVisualizer::LoadPlan(const TString& nodeType, const NJson::TJsonValue& node) {
    Plans.emplace_back(nodeType, Config, CteStages, CteSubPlans);
    Plans.back().Load(node);
}

void TPlanVisualizer::PostProcessPlans() {
    // Fix CTE Refs
    for (auto& p : Plans) {
        p.ResolveCteRefs();
    }
    // Fix Layouts
    for (auto& p : Plans) {
        p.MarkLayout();
        if (BaseTime == 0) {
            BaseTime = p.BaseTime;
        } else {
            BaseTime = std::min(BaseTime, p.BaseTime);
        }
    }
    // Fix time Offsets
    for (auto& p : Plans) {
        p.TimeOffset = p.BaseTime - BaseTime;
        MaxTime = std::max(MaxTime, p.TimeOffset + p.MaxTime);
    }
}

TString TPlanVisualizer::PrintSvgSafe() {
    try {
        return PrintSvg();
    } catch (std::exception& e) {
        return Sprintf("<svg width='1024' height='256' xmlns='http://www.w3.org/2000/svg'><text>%s<text></svg>", e.what());
    }
}

TString TPlanVisualizer::PrintSvg() {
    TStringBuilder background;
    TStringBuilder canvas;
    TStringBuilder svg;

    ui32 offsetY = 0;

    ui32 summary3 = (Config.SummaryWidth - INTERNAL_GAP_X * 2) / 3;
    for (auto& p : Plans) {
        offsetY += GAP_Y;
        canvas
            << "<text font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageText
            << "' x='" << 0 << "' y='" << offsetY + INTERNAL_TEXT_HEIGHT << "'>"
            << p.NodeType << "</text>" << Endl;

        canvas
            << "<text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageText << "' x='" << Config.HeaderWidth
            << "' y='" << offsetY + INTERNAL_TEXT_HEIGHT << "'>" << ToString(p.Tasks) << "</text>" << Endl;

        canvas
            << "<g><title>Ingress "
            << FormatBytes(p.IngressBytes->Value) << ", Rows " << FormatIntegerValue(p.IngressRows->Value);
        if (p.IngressRows->Value) {
        canvas
            << ", Width " << p.IngressBytes->Value / p.IngressRows->Value << "B";
        }
        if (p.MaxTime) {
        canvas
            << ", Avg " << FormatBytes(p.IngressBytes->Value * 1000 / p.MaxTime) << "/s";
        }
        canvas
            << "</title>" << Endl
            << "  <rect x='" << Config.HeaderWidth + GAP_X << "' y='" << offsetY
            << "' width='" << summary3 << "' height='" << TIME_HEIGHT
            << "' stroke-width='0' fill='" << Config.Palette.IngressMedium << "'/>" << Endl
            << "  <text font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextLight << "' x='" << Config.HeaderWidth + GAP_X + 2
            << "' y='" << offsetY + INTERNAL_TEXT_HEIGHT << "'>" << FormatBytes(p.IngressBytes->Value) << "</text>" << Endl
            << "</g>" << Endl;

        canvas
            << "<g><title>CPU Usage " << FormatUsage(p.CpuTime->Value);
        if (p.MaxTime) {
            auto usagePS = p.CpuTime->Value / p.MaxTime;
            usagePS /= 10;
        canvas
            << ", Avg " << Sprintf("%lu.%.2lu", usagePS / 100, usagePS % 100) << " CPU/s";
        }
        canvas
            << "</title>" << Endl
            << "  <rect x='" << Config.HeaderWidth + GAP_X + INTERNAL_GAP_X + summary3 << "' y='" << offsetY
            << "' width='" << Config.SummaryWidth - (summary3 + INTERNAL_GAP_X) * 2 << "' height='" << TIME_HEIGHT
            << "' stroke-width='0' fill='" << Config.Palette.CpuMedium << "'/>" << Endl
            << "  <text font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextLight << "' x='" << Config.HeaderWidth + GAP_X + INTERNAL_GAP_X + summary3 + 2
            << "' y='" << offsetY + INTERNAL_TEXT_HEIGHT << "'>" << FormatUsage(p.CpuTime->Value) << "</text>" << Endl
            << "</g>" << Endl;

        canvas
            << "<g><title>Memory " << FormatBytes(p.MaxMemoryUsage->Value) << "</title>" << Endl
            << "  <rect x='" << Config.HeaderWidth + GAP_X + Config.SummaryWidth - summary3 << "' y='" << offsetY
            << "' width='" << summary3 << "' height='" << TIME_HEIGHT
            << "' stroke-width='0' fill='" << Config.Palette.MemMedium << "'/>" << Endl
            << "  <text font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextLight << "' x='" << Config.HeaderWidth + GAP_X + Config.SummaryWidth - summary3 + 2
            << "' y='" << offsetY + INTERNAL_TEXT_HEIGHT << "'>" << FormatBytes(p.MaxMemoryUsage->Value) << "</text>" << Endl
            << "</g>" << Endl;

        auto w = Config.Width - (Config.HeaderWidth + GAP_X + Config.SummaryWidth + GAP_X);
        auto x = (Config.HeaderWidth + GAP_X + Config.SummaryWidth + GAP_X) + w * (p.MaxTime + p.TimeOffset) / MaxTime;
        canvas
            << "<g><title>" << "Duration: " << FormatTimeMs(p.MaxTime) << ", Total " << FormatTimeMs(p.MaxTime + p.TimeOffset) << "</title>" << Endl
            << "  <rect x='" << x - summary3 << "' y='" << offsetY
            << "' width='" << summary3 << "' height='" << TIME_HEIGHT
            << "' stroke-width='0' fill='" << Config.Palette.StageGrid << "'/>" << Endl
            << "  <text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextInverted << "' x='" << x - 2
            << "' y='" << offsetY + INTERNAL_TEXT_HEIGHT << "'>" << FormatTimeMs(p.MaxTime + p.TimeOffset) << "</text>" << Endl
            << "</g>" << Endl;

        offsetY += TIME_HEIGHT;
        if (!p.TotalCpuTime.Deriv.empty()) {

            auto tx0 = Config.HeaderWidth + GAP_X + Config.SummaryWidth + GAP_X + INTERNAL_GAP_X;
            auto tx1 = Config.Width - INTERNAL_GAP_X;
            auto tw = tx1 - tx0;
            auto maxCpu = p.TotalCpuTime.MaxDeriv * TIME_SERIES_RANGES / (p.TotalCpuTime.MaxTime - p.TotalCpuTime.MinTime);
            p.PrintDeriv(canvas, p.TotalCpuTime, tx0, offsetY, tw, INTERNAL_HEIGHT, "Max CPU " + FormatMCpu(maxCpu), Config.Palette.CpuMedium, Config.Palette.CpuLight);
        }
        offsetY += INTERNAL_HEIGHT;
        p.PrintSvg(MaxTime, offsetY, background, canvas);
    }

    svg << "<svg width='" << Config.Width << "' height='" << offsetY << "' xmlns='http://www.w3.org/2000/svg'>" << Endl;
    svg << "<clipPath id='clipTextPath'><rect x='" << 0
        << "' y='0' width='" << Config.HeaderWidth - INTERNAL_WIDTH << "' height='" << offsetY << "'/>"
        << "</clipPath>" << Endl;
    svg << TString(background) << Endl;

    {
        ui64 maxSec = MaxTime / 1000;
        ui64 deltaSec = 0;

             if (maxSec <=  10) deltaSec = 1;
        else if (maxSec <=  20) deltaSec = 2;
        else if (maxSec <=  30) deltaSec = 3;
        else if (maxSec <=  40) deltaSec = 4;
        else if (maxSec <=  50) deltaSec = 5;
        else if (maxSec <=  60) deltaSec = 6;
        else if (maxSec <= 100) deltaSec = 10;
        else if (maxSec <= 150) deltaSec = 15;
        else if (maxSec <= 200) deltaSec = 20;
        else if (maxSec <= 300) deltaSec = 30;
        else if (maxSec <= 600) deltaSec = 60;
        else if (maxSec <= 1200) deltaSec = 120;
        else if (maxSec <= 1800) deltaSec = 180;
        else if (maxSec <= 3600) deltaSec = 360;
        else {
            ui64 stepSec = maxSec / 10;
            deltaSec = stepSec - (stepSec % 60);
        }

        auto x = Config.HeaderWidth + GAP_X + Config.SummaryWidth + GAP_X;
        auto w = Config.Width - x - INTERNAL_GAP_X * 2;

        for (ui64 t = 0; t < maxSec; t += deltaSec) {
            ui64 x1 = t * w / maxSec;
            svg
                << "<line x1='" << x + x1 << "' y1='0' x2='" << x + x1 << "' y2='" << offsetY
                << "' stroke-width='1' stroke='" << Config.Palette.StageGrid << "' stroke-dasharray='1,2'/>" << Endl;
            auto timeLabel = Sprintf("%lu:%.2lu", t / 60, t % 60);
            for (auto& p : Plans) {
                svg
                    << "<text font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageText
                    << "' x='" << x + x1 + 2 << "' y='" << p.OffsetY - INTERNAL_HEIGHT - (TIME_HEIGHT - INTERNAL_TEXT_HEIGHT) << "'>"
                    << timeLabel << "</text>" << Endl;
            }
        }
    }

    svg << TString(canvas) << Endl;
    svg << "</svg>" << Endl;

    return svg;
}
