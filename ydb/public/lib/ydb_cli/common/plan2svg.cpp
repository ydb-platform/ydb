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
                LoadStage(Stages.back(), plan);
            }
        }
    }

    if (!TotalCpuTimes.empty()) {
        TotalCpuTime.Load(TotalCpuTimes, TotalCpuValues, TotalCpuTimes.front(), TotalCpuTimes.back());
    }
}

void TPlan::ResolveCteRefs() {
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
    }
}

void TPlan::LoadStage(std::shared_ptr<TStage> stage, const NJson::TJsonValue& node) {

    if (auto* planNodeIdNode = node.GetValueByPath("PlanNodeId")) {
        stage->PlanNodeId = planNodeIdNode->GetIntegerSafe();
    }

    if (auto* subplanNameNode = node.GetValueByPath("Subplan Name")) {
        auto subplanName = subplanNameNode->GetStringSafe();
        if (subplanName.StartsWith("CTE ")) {
            CteStages[subplanName] = stage;
        }
    }

    auto operators = node.GetValueByPath("Operators");

    if (operators) {
        for (const auto& subNode : operators->GetArray()) {
            if (auto* nameNode = subNode.GetValueByPath("Name")) {
                auto name = nameNode->GetStringSafe();
                TStringBuilder builder;
                builder << name;
                if (name == "Limit") {
                    if (auto* limitNode = subNode.GetValueByPath("Limit")) {
                        builder << ": " << limitNode->GetStringSafe();
                    }
                } else if (name == "Filter") {
                    if (auto* predicateNode = subNode.GetValueByPath("Predicate")) {
                        auto filter = predicateNode->GetStringSafe();
                        while(true) {
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
                } else if (name == "TopSort") {
                    if (auto* limitNode = subNode.GetValueByPath("Limit")) {
                        builder << ", Limit: " << limitNode->GetStringSafe();
                    }
                    if (auto* topSortByNode = subNode.GetValueByPath("TopSortBy")) {
                        builder << ", TopSortBy: " << topSortByNode->GetStringSafe();
                    }
                } else if (name.Contains("Join")) {
                    if (auto* conditionNode = subNode.GetValueByPath("Condition")) {
                        builder << " on " << conditionNode->GetStringSafe();
                    }
                }
                stage->Info.push_back(builder);
                {
                    TStringBuilder ebuilder;
                    if (auto* eCostNode = subNode.GetValueByPath("E-Cost")) {
                        ebuilder << "E-Cost: " << eCostNode->GetStringSafe() << " ";
                    }
                    if (auto* eRowsNode = subNode.GetValueByPath("E-Rows")) {
                        ebuilder << "E-Rows: " << eRowsNode->GetStringSafe() << " ";
                    }
                    if (auto* eSizeNode = subNode.GetValueByPath("E-Size")) {
                        ebuilder << "E-Size: " << eSizeNode->GetStringSafe();
                    }
                    TString estring = ebuilder;
                    if (estring) {
                        stage->Info.push_back(estring);
                    }
                }
            }
        }
    }

    stage->StatsNode = node.GetValueByPath("Stats");

    const NJson::TJsonValue* inputNode;

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

        if (auto* outputNode = stage->StatsNode->GetValueByPath("Output")) {
            if (auto* popNode = (*outputNode)[0].GetValueByPath("Pop")) {
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

        inputNode = stage->StatsNode->GetValueByPath("Input");
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
                        auto connection = std::make_shared<TConnection>(subNodeType);
                        stage->Connections.push_back(connection);
                        if (keyColumnsNode) {
                            for (auto& keyColumn : keyColumnsNode->GetArray()) {
                                stage->Connections.back()->KeyColumns.push_back(keyColumn.GetStringSafe());
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
                        LoadStage(Stages.back(), plan);
                    }
                } else if (auto* cteNameNode = plan.GetValueByPath("CTE Name")) {
                    auto cteName = "CTE " + cteNameNode->GetStringSafe();
                    auto connection = std::make_shared<TConnection>(subNodeType);
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
                    if (stage->Source) {
                        ythrow yexception() << "Plan stage already has linked Source [" << stage->Source->NodeType << "]";
                    }
                    stage->Source = std::make_shared<TSource>(subNodeType);
                    LoadSource(stage->Source, plan);
                    if (!stage->Source->Info.empty()) {
                        stage->Info.insert(stage->Info.end(), stage->Source->Info.begin(), stage->Source->Info.end());
                    }

                    if (stage->StatsNode) {
                        if (auto* ingressTopNode = stage->StatsNode->GetValueByPath("Ingress")) {
                            if (auto* ingressNode = (*ingressTopNode)[0].GetValueByPath("Ingress")) {
                                if (auto* bytesNode = ingressNode->GetValueByPath("Bytes")) {
                                    stage->Source->IngressBytes = std::make_shared<TSingleMetric>(IngressBytes,
                                        *bytesNode,
                                        ingressNode->GetValueByPath("FirstMessageMs"),
                                        ingressNode->GetValueByPath("LastMessageMs"),
                                        ingressNode->GetValueByPath("WaitTimeUs.History")
                                    );
                                    MaxTime = std::max(MaxTime, stage->Source->IngressBytes->MaxTime);
                                }
                                if (auto* rowsNode = ingressNode->GetValueByPath("Rows")) {
                                    stage->Source->IngressRows = std::make_shared<TSingleMetric>(IngressRows, *rowsNode);
                                }
                            }
                        }
                    }

                } else {
                    stage->Connections.push_back(std::make_shared<TConnection>("Implicit"));
                    Stages.push_back(std::make_shared<TStage>(subNodeType));
                    stage->Connections.back()->FromStage = Stages.back();
                    LoadStage(Stages.back(), plan);
                }
            } else {
                ythrow yexception() << "Unexpected plan node type [" << planNodeType << "]";
            }
        }
    }
}

void TPlan::LoadSource(std::shared_ptr<TSource> source, const NJson::TJsonValue& node) {

    auto operators = node.GetValueByPath("Operators");

    if (operators) {
        for (const auto& subNode : operators->GetArray()) {
            TStringBuilder builder;
            builder << "Source";
            if (auto* sourceTypeNode = subNode.GetValueByPath("SourceType")) {
                builder << " " << sourceTypeNode->GetStringSafe();
            }
            if (auto* nameNode = subNode.GetValueByPath("Name")) {
                builder << " " << nameNode->GetStringSafe() << "(";
            }
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
            source->Info.push_back(builder);
            {
                TStringBuilder ebuilder;
                if (auto* eCostNode = subNode.GetValueByPath("E-Cost")) {
                    ebuilder << "E-Cost: " << eCostNode->GetStringSafe() << " ";
                }
                if (auto* eRowsNode = subNode.GetValueByPath("E-Rows")) {
                    ebuilder << "E-Rows: " << eRowsNode->GetStringSafe() << " ";
                }
                if (auto* eSizeNode = subNode.GetValueByPath("E-Size")) {
                    ebuilder << "E-Size: " << eSizeNode->GetStringSafe();
                }
                TString estring = ebuilder;
                if (estring) {
                    source->Info.push_back(estring);
                }
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
    ui32 height = std::max<ui32>(stage->Connections.size() + (stage->Source ? 1 : 0) + 3, 4) * (INTERNAL_HEIGHT + INTERNAL_GAP_Y) + INTERNAL_GAP_Y;
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
            offsetY += INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2;
            stage->IndentY = std::max(stage->IndentY, offsetY);
            stage->CteHeight += INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2;
            offsetY += GAP_Y;
        } else {
            MarkStageIndent(indent, offsetY, c->FromStage);
            stage->IndentY = std::max(stage->IndentY, c->FromStage->IndentY);
        }
    }
}

void TPlan::MarkLayout() {
    ui32 offsetY = 0;
    MarkStageIndent(0, offsetY, Stages.front());
}

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

TString FormatBytes(ui64 bytes) {
    ui64 pcs = 0;
    if (bytes < 1024) {
        return Sprintf("%luB", bytes);
    }
    pcs = (bytes % 1024) * 100 / 1024;
    bytes /= 1024;
    if (bytes < 1024) {
        return Sprintf("%lu.%.2luKB", bytes, pcs);
    }
    pcs = (bytes % 1024) * 100 / 1024;
    bytes /= 1024;
    if (bytes < 1024) {
        return Sprintf("%lu.%.2luMB", bytes, pcs);
    }
    pcs = (bytes % 1024) * 100 / 1024;
    bytes /= 1024;
    if (bytes < 1024) {
        return Sprintf("%lu.%.2luGB", bytes, pcs);
    }
    pcs = (bytes % 1024) * 100 / 1024;
    bytes /= 1024;
    return Sprintf("%lu.%.2luTB", bytes, pcs);
}

TString FormatRows(ui64 rows) {
    ui64 pcs = 0;
    if (rows < 1024) {
        return Sprintf("%lu", rows);
    }
    pcs = (rows % 1024) * 100 / 1024;
    rows /= 1024;
    if (rows < 1024) {
        return Sprintf("%lu.%.2luK", rows, pcs);
    }
    pcs = (rows % 1024) * 100 / 1024;
    rows /= 1024;
    if (rows < 1024) {
        return Sprintf("%lu.%.2luM", rows, pcs);
    }
    pcs = (rows % 1024) * 100 / 1024;
    rows /= 1024;
    if (rows < 1024) {
        return Sprintf("%lu.%.2luG", rows, pcs);
    }
    pcs = (rows % 1024) * 100 / 1024;
    rows /= 1024;
    return Sprintf("%lu.%.2luT", rows, pcs);
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

void TPlan::PrintSvg(ui64 maxTime, ui32& offsetY, TStringBuilder& background, TStringBuilder& canvas) {
    OffsetY = offsetY;
    ui32 planHeight = Stages.back()->OffsetY + Stages.back()->Height + Stages.back()->CteHeight;

    for (auto& s : Stages) {
        background
            << "<rect x='" << s->IndentX << "' y='" << s->OffsetY + offsetY
            << "' width='" << Config.HeaderWidth - s->IndentX << "' height='" << s->Height
            << "' stroke-width='0' fill='" << Config.Palette.StageDark << "'/>" << Endl;
        auto x = Config.HeaderWidth + GAP_X;
        background
            << "<rect x='" << x << "' y='" << s->OffsetY + offsetY
            << "' width='" << Config.SummaryWidth << "' height='" << s->Height
            << "' stroke-width='0' fill='" << Config.Palette.StageDark << "'/>" << Endl;
        x += Config.SummaryWidth + GAP_X;
        background
            << "<rect x='" << x << "' y='" << s->OffsetY + offsetY
            << "' width='" << Config.Width - x << "' height='" << s->Height
            << "' stroke-width='0' fill='" << Config.Palette.StageDark << "'/>" << Endl;
        if (s->Connections.size() > 1) {
            ui32 y = s->OffsetY + s->Height;
            background
                << "<rect x='" << s->IndentX << "' y='" << y + offsetY
                << "' width='" << INDENT_X << "' height='" << s->IndentY - y
                << "' stroke-width='0' fill='" << Config.Palette.StageDark << "'/>" << Endl;
        }
        background
            << "<circle cx='" << s->IndentX + INTERNAL_WIDTH / 2
            << "' cy='" << s->OffsetY + s->Height / 2 + offsetY
            << "' r='" << INTERNAL_WIDTH / 2 - 1
            << "' stroke='" << Config.Palette.StageDark << "' stroke-width='1' fill='" << Config.Palette.StageLight << "' />" << Endl
            << "<text text-anchor='middle' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT
            << "px' fill='" << Config.Palette.StageText << "' x='" << s->IndentX + INTERNAL_WIDTH / 2
            << "' y='" << s->OffsetY + s->Height / 2 + offsetY + INTERNAL_TEXT_HEIGHT / 2
            << "'>" << ToString(s->PhysicalStageId) << "</text>" << Endl;

        {
            ui32 y0 = s->OffsetY + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 + offsetY;
            if (!s->Info.empty()) {
                for (auto text : s->Info) {
                    canvas
                        << "<text clip-path='url(#clipTextPath)' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageText << "' x='" << s->IndentX + INTERNAL_WIDTH + 2
                        << "' y='" << y0 << "'>" << text << "</text>" << Endl;
                    y0 += (INTERNAL_TEXT_HEIGHT + INTERNAL_GAP_Y);
                }
            } else {
                canvas
                    << "<text clip-path='url(#clipTextPath)' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageText << "' x='" << s->IndentX + INTERNAL_WIDTH + 2
                    << "' y='" << y0 << "'>" << s->NodeType << "</text>" << Endl;
            }
        }


        ui32 x0 = Config.HeaderWidth + GAP_X + INTERNAL_GAP_X;
        ui32 y0 = s->OffsetY + offsetY + INTERNAL_GAP_Y;

        auto tx0 = Config.HeaderWidth + GAP_X + Config.SummaryWidth + GAP_X + INTERNAL_GAP_X;
        auto tx1 = Config.Width - INTERNAL_GAP_X;
        auto tw = tx1 - tx0;
        auto px = tx0 + TimeOffset * tw / maxTime;
        auto pw = MaxTime * tw / maxTime;

        if (s->OutputBytes) {
            ui32 width = 0;
            if (s->OutputBytes->Summary->Max) {
                width = s->OutputBytes->Details.Sum * (Config.SummaryWidth - INTERNAL_GAP_X * 2) / s->OutputBytes->Summary->Max;
            }
            canvas
                << "<text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageText << "' x='" << Config.HeaderWidth
                << "' y='" << y0 + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << s->OutputBytes->Details.Count << "</text>" << Endl;
            background
                << "<g><title>Output "
                << s->OutputBytes->Details.Sum * 100 / s->OutputBytes->Summary->Value << "%, \u2211"
                << FormatBytes(s->OutputBytes->Details.Sum) << ", " << FormatBytes(s->OutputBytes->Details.Min) << " | "
                << FormatBytes(s->OutputBytes->Details.Avg) << " | " << FormatBytes(s->OutputBytes->Details.Max);
            if (s->OutputRows && s->OutputRows->Details.Sum) {
            background
                << ", Rows \u2211"
                << FormatRows(s->OutputRows->Details.Sum) << ", " << FormatRows(s->OutputRows->Details.Min) << " | "
                << FormatRows(s->OutputRows->Details.Avg) << " | " << FormatRows(s->OutputRows->Details.Max)
                << ", Width " << FormatBytes(s->OutputBytes->Details.Sum / s->OutputRows->Details.Sum);
            }
            background
                << "</title>" << Endl;
            if (width < INTERNAL_HEIGHT) {
            background
                << "  <rect x='" << x0 << "' y='" << y0 << "' width='" << INTERNAL_HEIGHT << "' height='" << INTERNAL_HEIGHT << "' stroke='none' fill='" << Config.Palette.StageDark << "' />" << Endl;
            }
            if (s->OutputBytes->Details.Max) {
                ui32 minWidth = width * s->OutputBytes->Details.Min / s->OutputBytes->Details.Max;
                background
                    << "  <rect x='" << x0 + minWidth << "' y='" << y0
                    << "' width='" << width - minWidth << "' height='" << INTERNAL_HEIGHT
                    << "' stroke-width='0' fill='" << Config.Palette.OutputLight << "'/>" << Endl;

                ui32 avgX = width * s->OutputBytes->Details.Avg / s->OutputBytes->Details.Max;

                background << "  <line x1='" << x0 + avgX << "' y1='" << y0
                        << "' x2='" << x0 + avgX << "' y2='" << y0 + INTERNAL_HEIGHT
                        << "' stroke-width='1' stroke='" << Config.Palette.OutputDark << "' />" << Endl;

                width = minWidth;
            }
            background
                << "  <rect x='" << x0 << "' y='" << y0
                << "' width='" << width << "' height='" << INTERNAL_HEIGHT
                << "' stroke-width='0' fill='" << Config.Palette.OutputMedium << "'/>" << Endl;
            background
                << "</g>" << Endl;

            auto d = s->OutputBytes->MaxTime - s->OutputBytes->MinTime;
            TStringBuilder title;
            title << "Output";
            if (d) {
                title << " " << FormatBytes(s->OutputBytes->Details.Sum * 1000 / d) << "/s";
                if (s->OutputRows) {
                    title << ", Rows " << FormatRows(s->OutputRows->Details.Sum * 1000 / d) << "/s";
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
            ui32 width = 0;
            if (s->MaxMemoryUsage->Summary->Max) {
                width = s->MaxMemoryUsage->Details.Sum * (Config.SummaryWidth - INTERNAL_GAP_X * 2) / s->MaxMemoryUsage->Summary->Max;
            }
            canvas
                << "<text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageText << "' x='" << Config.HeaderWidth
                << "' y='" << y0 + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << s->MaxMemoryUsage->Details.Count << "</text>" << Endl;
            background
                << "<g><title>Memory "
                << s->MaxMemoryUsage->Details.Sum * 100 / s->MaxMemoryUsage->Summary->Value << "%, \u2211"
                << FormatBytes(s->MaxMemoryUsage->Details.Sum) << ", " << FormatBytes(s->MaxMemoryUsage->Details.Min) << " | "
                << FormatBytes(s->MaxMemoryUsage->Details.Avg) << " | " << FormatBytes(s->MaxMemoryUsage->Details.Max)
                << "</title>" << Endl;
            if (width < INTERNAL_HEIGHT) {
            background
                << "  <rect x='" << x0 << "' y='" << y0 << "' width='" << INTERNAL_HEIGHT << "' height='" << INTERNAL_HEIGHT / 2 << "' stroke='none' fill='" << Config.Palette.StageDark << "' />" << Endl;
            }
            if (s->MaxMemoryUsage->Details.Max) {
                ui32 minWidth = width * s->MaxMemoryUsage->Details.Min / s->MaxMemoryUsage->Details.Max;
                background
                    << "<rect x='" << x0 + minWidth << "' y='" << y0
                    << "' width='" << width - minWidth << "' height='" << INTERNAL_HEIGHT
                    << "' stroke-width='0' fill='" << Config.Palette.MemLight << "'/>" << Endl;

                ui32 avgX = width * s->MaxMemoryUsage->Details.Avg / s->MaxMemoryUsage->Details.Max;

                background << "<line x1='" << x0 + avgX << "' y1='" << y0
                        << "' x2='" << x0 + avgX << "' y2='" << y0 + INTERNAL_HEIGHT
                        << "' stroke-width='1' stroke='" << Config.Palette.MemMedium << "' />" << Endl;

                width = minWidth;
            }
            background
                << "<rect x='" << x0 << "' y='" << y0
                << "' width='" << width << "' height='" << INTERNAL_HEIGHT
                << "' stroke-width='0' fill='" << Config.Palette.MemMedium << "'/>" << Endl;
            background
                << "</g>" << Endl;


            if (!s->MaxMemoryUsage->History.Values.empty()) {
                PrintValues(canvas, s->MaxMemoryUsage, px, y0, pw, INTERNAL_HEIGHT, "Max MEM " + FormatBytes(s->MaxMemoryUsage->History.MaxValue), Config.Palette.MemMedium, Config.Palette.MemMedium);
            }

            if (s->SpillingComputeBytes && !s->SpillingComputeBytes->History.Deriv.empty()) {
                PrintDeriv(canvas, s->SpillingComputeBytes->History, px, y0, pw, INTERNAL_HEIGHT, "Spilling Compute", Config.Palette.SpillingBytesMedium, Config.Palette.SpillingBytesLight);
            }
        }

        // MEM is mandatory metric
        y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;

        if (s->CpuTime) {
            ui32 width = 0;
            if (s->CpuTime->Summary->Max) {
                width = s->CpuTime->Details.Sum * (Config.SummaryWidth - INTERNAL_GAP_X * 2) / s->CpuTime->Summary->Max;
            }
            canvas
                << "<text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageText << "' x='" << Config.HeaderWidth
                << "' y='" << y0 + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << s->CpuTime->Details.Count << "</text>" << Endl;
            background
                << "<g><title>CPU Usage "
                << s->CpuTime->Details.Sum * 100 / s->CpuTime->Summary->Value << "%, \u2211"
                << FormatUsage(s->CpuTime->Details.Sum) << ", " << FormatUsage(s->CpuTime->Details.Min) << " | "
                << FormatUsage(s->CpuTime->Details.Avg) << " | " << FormatUsage(s->CpuTime->Details.Max)
                << "</title>" << Endl;
            if (width < INTERNAL_HEIGHT) {
            background
                << "  <rect x='" << x0 << "' y='" << y0 << "' width='" << INTERNAL_HEIGHT << "' height='" << INTERNAL_HEIGHT << "' stroke='none' fill='" << Config.Palette.StageDark << "' />" << Endl;
            }
            if (s->CpuTime->Details.Max) {
                ui32 minWidth = width * s->CpuTime->Details.Min / s->CpuTime->Details.Max;
                background
                    << "<rect x='" << x0 + minWidth << "' y='" << y0
                    << "' width='" << width - minWidth << "' height='" << INTERNAL_HEIGHT
                    << "' stroke-width='0' fill='" << Config.Palette.CpuLight << "'/>" << Endl;

                ui32 avgX = width * s->CpuTime->Details.Avg / s->CpuTime->Details.Max;

                background << "<line x1='" << x0 + avgX << "' y1='" << y0
                        << "' x2='" << x0 + avgX << "' y2='" << y0 + INTERNAL_HEIGHT
                        << "' stroke-width='1' stroke='' />" << Endl;

                width = minWidth;
            }
            background
                << "<rect x='" << x0 << "' y='" << y0
                << "' width='" << width << "' height='" << INTERNAL_HEIGHT
                << "' stroke-width='0' fill='" << Config.Palette.CpuMedium << "'/>" << Endl;
            background
                << "</g>" << Endl;

            if (!s->CpuTime->History.Deriv.empty()) {
                auto maxCpu = s->CpuTime->History.MaxDeriv * TIME_SERIES_RANGES / (s->CpuTime->History.MaxTime - s->CpuTime->History.MinTime);
                PrintDeriv(canvas, s->CpuTime->History, px, y0, pw, INTERNAL_HEIGHT, "Max CPU " + FormatMCpu(maxCpu), Config.Palette.CpuMedium, Config.Palette.CpuLight);
            }

            if (s->SpillingComputeTime && !s->SpillingComputeTime->History.Deriv.empty()) {
                PrintDeriv(canvas, s->SpillingComputeTime->History, px, y0, pw, INTERNAL_HEIGHT, "Spilling Compute", Config.Palette.SpillingTimeDark);
            }
        }

        // CPU is mandatory metric
        y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;

        for (auto& c : s->Connections) {

            auto x = c->CteConnection ? c->CteIndentX : c->FromStage->IndentX;
            auto y = (c->CteConnection ? c->CteOffsetY : c->FromStage->OffsetY) + offsetY;

            if (c->CteConnection) {
                auto xx = x;
                background
                    << "<rect x='" << xx << "' y='" << y
                    << "' width='" << Config.HeaderWidth - xx << "' height='" << INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2
                    << "' stroke-width='1' stroke='" << Config.Palette.StageDark << "' fill='" << Config.Palette.StageLight << "'/>" << Endl;
                xx = Config.HeaderWidth + GAP_X;
                background
                    << "<rect x='" << xx << "' y='" << y
                    << "' width='" << Config.SummaryWidth << "' height='" << INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2
                    << "' stroke-width='1' stroke='" << Config.Palette.StageDark << "' fill='" << Config.Palette.StageLight << "'/>" << Endl;
                xx += Config.SummaryWidth + GAP_X;
                background
                    << "<rect x='" << xx << "' y='" << y
                    << "' width='" << Config.Width - xx << "' height='" << INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2
                    << "' stroke-width='1' stroke='" << Config.Palette.StageDark << "' fill='" << Config.Palette.StageLight << "'/>" << Endl;
                background
                    << "<circle cx='" << c->CteIndentX + INTERNAL_WIDTH * 3 / 2
                    << "' cy='" << c->CteOffsetY + offsetY + INTERNAL_HEIGHT / 2 + INTERNAL_GAP_Y
                    << "' r='" << std::min(INTERNAL_HEIGHT, INTERNAL_WIDTH) / 2 - 1
                    << "' stroke='" << Config.Palette.StageDark << "' stroke-width='1' fill='" << Config.Palette.StageLight << "' />" << Endl
                    << "<text text-anchor='middle' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT
                    << "px' fill='" << Config.Palette.StageText << "' x='" << c->CteIndentX + INTERNAL_WIDTH * 3 / 2
                    << "' y='" << c->CteOffsetY + offsetY + INTERNAL_HEIGHT / 2 + INTERNAL_GAP_Y + INTERNAL_TEXT_HEIGHT / 2
                    << "'>" << ToString(c->FromStage->PhysicalStageId) << "</text>" << Endl;

                auto s = c->FromStage->Info.empty() ? c->FromStage->NodeType : c->FromStage->Info[0];
                canvas
                    << "<text clip-path='url(#clipTextPath)' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageText << "' x='" << x + INTERNAL_WIDTH * 2 + 2
                    << "' y='" << y + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << s << "</text>" << Endl;

                if (c->FromStage->OutputBytes) {
                    auto d = c->FromStage->OutputBytes->MaxTime - c->FromStage->OutputBytes->MinTime;
                    TStringBuilder title;
                    title << "Output";
                    if (d) {
                        title << " " << FormatBytes(c->FromStage->OutputBytes->Details.Sum * 1000 / d) << "/s";
                        if (c->FromStage->OutputRows) {
                            title << ", Rows " << FormatRows(c->FromStage->OutputRows->Details.Sum * 1000 / d) << "/s";
                        }
                    }
                    PrintTimeline(background, canvas, title, c->FromStage->OutputBytes->FirstMessage, c->FromStage->OutputBytes->LastMessage, px, y + INTERNAL_GAP_Y, pw, INTERNAL_HEIGHT, Config.Palette.OutputMedium);

                    if (!c->FromStage->OutputBytes->WaitTime.Deriv.empty()) {
                        PrintWaitTime(background, c->FromStage->OutputBytes, px, y + INTERNAL_GAP_Y, pw, INTERNAL_HEIGHT, Config.Palette.OutputLight);
                    }

                    if (!c->FromStage->OutputBytes->History.Deriv.empty()) {
                        PrintDeriv(canvas, c->FromStage->OutputBytes->History, px, y + INTERNAL_GAP_Y, pw, INTERNAL_HEIGHT, "", Config.Palette.OutputDark);
                    }
                }
            }

            TString mark;
            if (c->NodeType == "HashShuffle")    mark = "\u2B82";
            else if (c->NodeType == "Merge")     mark = "M";
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
                ui32 width = 0;
                if (c->InputBytes->Summary->Max) {
                    width = c->InputBytes->Details.Sum * (Config.SummaryWidth - INTERNAL_GAP_X * 2) / c->InputBytes->Summary->Max;
                }
                canvas
                    << "<text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageText << "' x='" << Config.HeaderWidth
                    << "' y='" << y0 + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << c->InputBytes->Details.Count << "</text>" << Endl;
                background
                    << "<g><title>Input "
                    << c->InputBytes->Details.Sum * 100 / c->InputBytes->Summary->Value << "%, \u2211"
                    << FormatBytes(c->InputBytes->Details.Sum) << ", " << FormatBytes(c->InputBytes->Details.Min) << " | "
                    << FormatBytes(c->InputBytes->Details.Avg) << " | " << FormatBytes(c->InputBytes->Details.Max);
                if (c->InputRows && c->InputRows->Details.Sum) {
                background
                    << ", Rows \u2211"
                    << FormatRows(c->InputRows->Details.Sum) << ", " << FormatRows(c->InputRows->Details.Min) << " | "
                    << FormatRows(c->InputRows->Details.Avg) << " | " << FormatRows(c->InputRows->Details.Max)
                    << ", Width " << FormatBytes(c->InputBytes->Details.Sum / c->InputRows->Details.Sum);
                }
                background
                    << "</title>" << Endl;
                if (width < INTERNAL_HEIGHT) {
                background
                    << "  <rect x='" << x0 << "' y='" << y0 << "' width='" << INTERNAL_HEIGHT << "' height='" << INTERNAL_HEIGHT / 2 << "' stroke='none' fill='" << Config.Palette.StageDark << "' />" << Endl;
                }
                if (c->InputBytes->Details.Max) {
                    ui32 minWidth = width * c->InputBytes->Details.Min / c->InputBytes->Details.Max;
                    background
                        << "  <rect x='" << x0 + minWidth << "' y='" << y0
                        << "' width='" << width - minWidth << "' height='" << INTERNAL_HEIGHT
                        << "' stroke-width='0' fill='" << Config.Palette.InputLight << "'/>" << Endl;

                    ui32 avgX = width * c->InputBytes->Details.Avg / c->InputBytes->Details.Max;

                    background << "  <line x1='" << x0 + avgX << "' y1='" << y0
                            << "' x2='" << x0 + avgX << "' y2='" << y0 + INTERNAL_HEIGHT
                            << "' stroke-width='1' stroke='" << Config.Palette.InputDark << "' />" << Endl;

                    width = minWidth;
                }
                background
                    << "  <rect x='" << x0 << "' y='" << y0
                    << "' width='" << width << "' height='" << INTERNAL_HEIGHT
                    << "' stroke-width='0' fill='" << Config.Palette.InputMedium << "'/>" << Endl;
                background
                    << "</g>" << Endl;

                auto d = c->InputBytes->MaxTime - c->InputBytes->MinTime;
                TStringBuilder title;
                title << "Input";
                if (d) {
                    title << " " << FormatBytes(c->InputBytes->Details.Sum * 1000 / d) << "/s";
                    if (c->InputRows) {
                        title << ", Rows " << FormatRows(c->InputRows->Details.Sum * 1000 / d) << "/s";
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

        if (s->Source && s->Source->IngressBytes) {
            ui32 width = 0;
            if (s->Source->IngressBytes->Summary->Max) {
                width = s->Source->IngressBytes->Details.Sum * (Config.SummaryWidth - INTERNAL_GAP_X * 2) / s->Source->IngressBytes->Summary->Max;
            }
            canvas
                << "<text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageText << "' x='" << Config.HeaderWidth
                << "' y='" << y0 + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << s->Source->IngressBytes->Details.Count << "</text>" << Endl;
            background
                << "<g><title>Ingress "
                << s->Source->IngressBytes->Details.Sum * 100 / s->Source->IngressBytes->Summary->Value << "%, \u2211"
                << FormatBytes(s->Source->IngressBytes->Details.Sum) << ", " << FormatBytes(s->Source->IngressBytes->Details.Min) << " | "
                << FormatBytes(s->Source->IngressBytes->Details.Avg) << " | " << FormatBytes(s->Source->IngressBytes->Details.Max);
            if (s->Source->IngressRows && s->Source->IngressRows->Details.Sum) {
            background
                << ", Rows \u2211"
                << FormatRows(s->Source->IngressRows->Details.Sum) << ", " << FormatRows(s->Source->IngressRows->Details.Min) << " | "
                << FormatRows(s->Source->IngressRows->Details.Avg) << " | " << FormatRows(s->Source->IngressRows->Details.Max)
                << ", Width " << FormatBytes(s->Source->IngressBytes->Details.Sum / s->Source->IngressRows->Details.Sum);
            }
            background
                << "</title>" << Endl;
            if (width < INTERNAL_HEIGHT) {
            background
                << "  <rect x='" << x0 << "' y='" << y0 << "' width='" << INTERNAL_HEIGHT << "' height='" << INTERNAL_HEIGHT / 2 << "' stroke='none' fill='" << Config.Palette.StageDark << "' />" << Endl;
            }
            if (s->Source->IngressBytes->Details.Max) {
                ui32 minWidth = width * s->Source->IngressBytes->Details.Min / s->Source->IngressBytes->Details.Max;
                background
                    << "  <rect x='" << x0 + minWidth << "' y='" << y0
                    << "' width='" << width - minWidth << "' height='" << INTERNAL_HEIGHT
                    << "' stroke-width='0' fill='" << Config.Palette.IngressLight << "'/>" << Endl;

                ui32 avgX = width * s->Source->IngressBytes->Details.Avg / s->Source->IngressBytes->Details.Max;

                background << "  <line x1='" << x0 + avgX << "' y1='" << y0
                        << "' x2='" << x0 + avgX << "' y2='" << y0 + INTERNAL_HEIGHT
                        << "' stroke-width='1' stroke='" << Config.Palette.IngressDark << "' />" << Endl;

                width = minWidth;
            }
            background
                << "  <rect x='" << x0 << "' y='" << y0
                << "' width='" << width << "' height='" << INTERNAL_HEIGHT
                << "' stroke-width='0' fill='" << Config.Palette.IngressMedium << "'/>" << Endl;
            background
                << "</g>" << Endl;

            auto d = s->Source->IngressBytes->MaxTime - s->Source->IngressBytes->MinTime;
            TStringBuilder title;
            title << "Ingress";
            if (d) {
                title << " " << FormatBytes(s->Source->IngressBytes->Details.Sum * 1000 / d) << "/s";
                if (s->Source->IngressRows) {
                    title << ", Rows " << FormatRows(s->Source->IngressRows->Details.Sum / d) << "/s";
                }
            }
            PrintTimeline(background, canvas, title, s->Source->IngressBytes->FirstMessage, s->Source->IngressBytes->LastMessage, px, y0, pw, INTERNAL_HEIGHT, Config.Palette.IngressMedium);

            if (!s->Source->IngressBytes->WaitTime.Deriv.empty()) {
                PrintWaitTime(background, s->Source->IngressBytes, px, y0, pw, INTERNAL_HEIGHT, Config.Palette.IngressLight);
            }

            if (!s->Source->IngressBytes->History.Deriv.empty()) {
                PrintDeriv(canvas, s->Source->IngressBytes->History, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.IngressDark);
            }
            y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;
        }
    }

    offsetY += planHeight;
}

TColorPalette::TColorPalette() {
    StageDark     = "#B0C4DE";
    StageLight    = "#E6E6FA";
    StageText     = "#000000";
    StageGrid     = "#4682B4";
    IngressDark   = "#556B2F";
    IngressMedium = "#6B8E23";
    IngressLight  = "#9ACD32";
    InputDark     = "#006400";
    InputMedium   = "#2E8B57";
    InputLight    = "#3CB371";
    OutputDark    = "#0000FF";
    OutputMedium  = "#1E90FF";
    OutputLight   = "#00BFFF";
    MemMedium     = "#483D8B";
    MemLight      = "#6A5ACD";
    CpuMedium     = "#FF0000";
    CpuLight      = "#FF7777";
    ConnectionFill= "#FFD700";
    ConnectionLine= "#FF0000";
    ConnectionText= "#FF0000";
    MinMaxLine    = "#FFFF00";
    TextLight     = "#FFFFFF";
    SpillingBytesDark   = "#34495E";
    SpillingBytesMedium = "#5D6D7E";
    SpillingBytesLight  = "#85929E";
    SpillingTimeDark    = "#C0392B";
    SpillingTimeMedium  = "#CD6155";
    SpillingTimeLight   = "#D98880";
}

TPlanViewConfig::TPlanViewConfig() {
    HeaderWidth = 300;
    SummaryWidth = 128;
    Width = 1024;
}


void TPlanVisualizer::LoadPlans(const TString& plans) {
    NJson::TJsonReaderConfig jsonConfig;
    NJson::TJsonValue jsonNode;
    if (NJson::ReadJsonTree(plans, &jsonConfig, &jsonNode)) {
        if (auto* topNode = jsonNode.GetValueByPath("Plan")) {
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
    Plans.emplace_back(nodeType, Config, CteStages);
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
            << "<text font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT
            << "px' x='" << 0 << "' y='" << offsetY + INTERNAL_TEXT_HEIGHT << "'>"
            << p.NodeType << "</text>" << Endl;

        canvas
            << "<text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.StageText << "' x='" << Config.HeaderWidth
            << "' y='" << offsetY + INTERNAL_TEXT_HEIGHT << "'>" << ToString(p.Tasks) << "</text>" << Endl;

        canvas
            << "<g><title>Ingress "
            << FormatBytes(p.IngressBytes->Value) << ", Rows " << FormatRows(p.IngressRows->Value);
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
            << "  <text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextLight << "' x='" << x - 2
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
                    << "<text font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT
                    << "px' x='" << x + x1 + 2 << "' y='" << p.OffsetY - INTERNAL_HEIGHT - (TIME_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>"
                    << timeLabel << "</text>" << Endl;
            }
        }
    }

    svg << TString(canvas) << Endl;
    svg << "</svg>" << Endl;

    return svg;
}
