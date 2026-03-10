#include "plan2svg.h"

#include <util/stream/output.h>

constexpr ui32 INDENT_X = 8;
constexpr ui32 GAP_X = 3;
constexpr ui32 GAP_Y = 3;
constexpr ui32 TIME_HEIGHT = 10;
constexpr ui32 INTERNAL_GAP_Y = 2;
constexpr ui32 INTERNAL_GAP_X = 2;
constexpr ui32 CONN_SIZE = 14;
constexpr ui32 INTERNAL_HEIGHT = 14;
constexpr ui32 INTERNAL_WIDTH = 16;
constexpr ui32 INTERNAL_TEXT_HEIGHT = 8;
constexpr ui32 TIME_SERIES_RANGES = 32;
constexpr ui32 CONN_ARROW = 4;

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

TString FormatInteger(ui64 bytes) {
    return FormatIntegerValue(bytes);
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

TString FormatTooltip(TStringBuilder& builder, const TString& prefix, TSingleMetric* metric, TString (*format)(ui64), ui64 total = 0) {
    TString result;
    builder << prefix;
    if (metric) {
        result = format(metric->Details.Sum);
        if (!total) {
            total = metric->Summary->Value;
        }
        if (total) {
            builder << ' ' << metric->Details.Sum * 100 / total << "%,";
        }
        if (metric->Details.Count > 1) {
            builder << " \u2211" << result << ", " << format(metric->Details.Min) << " | "
            << format(metric->Details.Avg) << " | " << format(metric->Details.Max);
        } else {
            builder << ' ' << result;
        }
    }
    return result;
}

TString FormatTooltip(TString& tooltip, const TString& prefix, TSingleMetric* metric, TString (*format)(ui64), ui64 total = 0) {
    TStringBuilder builder;
    auto result = FormatTooltip(builder, prefix, metric, format, total);
    if (result) {
        tooltip = builder;
    }
    return result;
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

TString DivP1(ui32 divisible, ui32 divisor) {
    TStringBuilder result;
    ui32 p0 = divisible / divisor;
    result << p0;
    ui32 p1 = divisible * 10 / divisor - p0 * 10;
    if (p1) {
        result << '.' << p1;
    }
    return result;
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
            Avg = std::max(Avg, Min);
        } else {
            Min = Avg;
        }
        if (auto* maxNode = node.GetValueByPath("Max")) {
            Max = maxNode->GetIntegerSafe();
            Avg = std::min(Avg, Max);
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
    bool first_item = true;
    ui64 last_time = 0;

    for (const auto& subNode : node.GetArray()) {
        ui64 i = subNode.GetIntegerSafe();
        if (even) {
            if (first_item) {
                first_item = false;
            } else {
                // time should increase monotonously
                if (i <= last_time) {
                    // just ignore tail otherwise
                    break;
                }
            }
            times.push_back(i);
            last_time = i;
        } else {
            values.push_back(i);
        }
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
            missed = false;
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

void Min0(ui64& m, ui64 v) {
    if (v) {
        m = m ? std::min(m, v) : v;
    }
}

void Max0(ui64& m, ui64 v) {
    if (v) {
        m = m ? std::max(m, v) : v;
    }
}

TSingleMetric::TSingleMetric(std::shared_ptr<TSummaryMetric> summary, const NJson::TJsonValue& node,
        ui64 minTime, ui64 maxTime,
        const NJson::TJsonValue* firstMessageNode, const NJson::TJsonValue* lastMessageNode,
        const NJson::TJsonValue* waitTimeUsNode)
    : Summary(summary), MinTime(minTime), MaxTime(maxTime) {

    if (firstMessageNode) {
        FirstMessage.Load(*firstMessageNode);
        Min0(MinTime, FirstMessage.Min);
    }

    if (lastMessageNode) {
        LastMessage.Load(*lastMessageNode);
        Max0(MaxTime, LastMessage.Max);
    }

    if (waitTimeUsNode) {
        WaitTime.Load(*waitTimeUsNode, MinTime, MaxTime);
        Min0(MinTime, WaitTime.MinTime);
        Max0(MaxTime, WaitTime.MaxTime);
    }

    if (Details.Load(node)) {
        Summary->Add(Details.Sum);
        if (auto* historyNode = node.GetValueByPath("History")) {
            History.Load(*historyNode, MinTime, MaxTime);
            Min0(MinTime, History.MinTime);
            Max0(MaxTime, History.MaxTime);
        }
    }
}

TSingleMetric::TSingleMetric(std::shared_ptr<TSummaryMetric> summary, ui64 value)
    : Summary(summary), Details(value) {
    Summary->Add(Details.Sum);
}

TSingleMetric::TSingleMetric(std::shared_ptr<TSummaryMetric> summary)
    : Summary(summary) {
    Summary->Add(Details.Sum);
}

TScalarMetric::TScalarMetric(std::shared_ptr<TSummaryMetric> summary, ui64 value)
    : Summary(summary), Value(value) {
    Summary->Add(Value);
}

TString ParseTableOrIndexName(const TString& table) {
    auto n = table.find_last_of('/');
    if (n == table.npos) {
        return table;
    }

    auto tableName = table.substr(n + 1);
    if (n == 0 || tableName != "indexImplTable") {
        return tableName;
    }

    auto ni = table.find_last_of('/', n - 1);
    if (ni == table.npos) {
        return table.substr(0, n);
    }

    if (ni == 0) {
        return table.substr(ni + 1, n - ni - 1);
    }

    auto nt = table.find_last_of('/', ni - 1);
    if (nt == table.npos) {
        return table.substr(0, n);
    } else {
        return table.substr(nt + 1, nt - n - 1);
    }
}

TString ParseColumns(const NJson::TJsonValue* node) {
    TStringBuilder builder;
    builder << '(';
    if (node) {
        bool firstColumn = true;
        for (const auto& subNode : node->GetArray()) {
            if (firstColumn) {
                firstColumn = false;
            } else {
                builder << ", ";
            }
            builder << subNode.GetStringSafe();
        }
    }
    builder << ')';
    return builder;
}

TString SvgRect(ui32 x, ui32 y, ui32 w, const TString& h, const TString& cssClass) {
    return TStringBuilder()
        << "<rect x='" << x << "' y='" << y << "' width='" << w << "' height='" << h
        << "' class='" << cssClass << "'/>" << Endl;
}

TString SvgRect(ui32 x, ui32 y, ui32 w, ui32 h, const TString& cssClass) {
    return SvgRect(x, y, w, ToString(h), cssClass);
}

TString SvgText(const TString& x, const TString& y, const TString& cssClass, const TString& text) {
    return TStringBuilder() << "<text x='" << x << "' y='" << y << "' class='" << cssClass << "'>" << text << "</text>" << Endl;
}

TString SvgText(ui32 x, const TString& y, const TString& cssClass, const TString& text) {
    return SvgText(ToString(x), y, cssClass, text);
}

TString SvgText(ui32 x, ui32 y, const TString& cssClass, const TString& text) {
    return SvgText(ToString(x), ToString(y), cssClass, text);
}

TString SvgTextS(ui32 x, ui32 y, const TString& text) {
    return SvgText(x, y, "texts", text);
}

TString SvgTextM(ui32 x, ui32 y, const TString& text) {
    return SvgText(x, y, "textm", text);
}

TString SvgTextE(ui32 x, ui32 y, const TString& text) {
    return SvgText(x, y, "texte", text);
}

TString SvgCircle(ui32 x, ui32 y, const TString& cssClass, const TString& opacity = "") {
    TStringBuilder builder;
    builder << "<circle cx='" << x << "' cy='" << y << "' r='" << INTERNAL_WIDTH / 2 - 1 << "' class='" << cssClass;
    if (opacity) {
        builder << "' opacity='" << opacity;
    }
    builder << "' />" << Endl;
    return builder;
}

TString SvgStageId(ui32 x, ui32 y, const TString& id, const TString& opacity = "") {
    return TStringBuilder() << SvgCircle(x, y, "stage", opacity) <<  SvgTextM(x, y + INTERNAL_TEXT_HEIGHT / 2, id);
}

TString SvgLine(ui32 x1, ui32 y1, ui32 x2, ui32 y2, const TString& cssClass) {
    return TStringBuilder() << "<line x1='" << x1 << "' y1='" << y1 << "' x2='" << x2 << "' y2='" << y2 << "' class='" << cssClass << "' />" << Endl;
}

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
                            if (auto* pushNode = subNode.GetValueByPath("Push")) {
                                if (auto* bytesNode = pushNode->GetValueByPath("Bytes")) {
                                    cteRef.second->InputBytes = std::make_shared<TSingleMetric>(InputBytes,
                                        *bytesNode, 0, 0,
                                        pushNode->GetValueByPath("FirstMessageMs"),
                                        pushNode->GetValueByPath("LastMessageMs"),
                                        pushNode->GetValueByPath("WaitTimeUs.History")
                                    );
                                    Min0(cteRef.second->Stage.MinTime, cteRef.second->InputBytes->MinTime);
                                    Max0(cteRef.second->Stage.MaxTime, cteRef.second->InputBytes->MaxTime);
                                    Max0(MaxTime, cteRef.second->InputBytes->MaxTime);
                                } else {
                                    cteRef.second->InputBytes = std::make_shared<TSingleMetric>(InputBytes);
                                }
                                if (auto* rowsNode = pushNode->GetValueByPath("Rows")) {
                                    cteRef.second->InputRows = std::make_shared<TSingleMetric>(InputRows, *rowsNode);
                                } else {
                                    cteRef.second->InputRows = std::make_shared<TSingleMetric>(InputRows);
                                }
                                if (auto* chunksNode = pushNode->GetValueByPath("Chunks")) {
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
                            if (auto* popNode = subNode.GetValueByPath("Pop")) {
                                if (auto* bytesNode = popNode->GetValueByPath("Bytes")) {
                                    cteRef.second->CteOutputBytes = std::make_shared<TSingleMetric>(OutputBytes,
                                        *bytesNode, 0, 0,
                                        popNode->GetValueByPath("FirstMessageMs"),
                                        popNode->GetValueByPath("LastMessageMs"),
                                        popNode->GetValueByPath("WaitTimeUs.History")
                                    );
                                    Min0(cteRef.second->FromStage->MinTime, cteRef.second->CteOutputBytes->MinTime);
                                    Max0(cteRef.second->FromStage->MaxTime, cteRef.second->CteOutputBytes->MaxTime);
                                    Max0(MaxTime, cteRef.second->CteOutputBytes->MaxTime);
                                } else {
                                    cteRef.second->CteOutputBytes = std::make_shared<TSingleMetric>(OutputBytes);
                                }
                                if (auto* rowsNode = popNode->GetValueByPath("Rows")) {
                                    cteRef.second->CteOutputRows = std::make_shared<TSingleMetric>(OutputRows, *rowsNode);
                                    cteRef.second->CteOperatorOutputRows = std::make_shared<TSingleMetric>(OperatorOutputRows, *rowsNode);
                                } else {
                                    cteRef.second->CteOutputRows = std::make_shared<TSingleMetric>(OutputRows);
                                }
                                if (auto* chunksNode = popNode->GetValueByPath("Chunks")) {
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
                                    ingressNode = ingress0.GetValueByPath("Push");
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
                        if (auto* popNode = subNode.GetValueByPath("Pop")) {
                            if (auto* bytesNode = popNode->GetValueByPath("Bytes")) {
                                stage->OutputBytes = std::make_shared<TSingleMetric>(OutputBytes,
                                    *bytesNode, 0, 0,
                                    popNode->GetValueByPath("FirstMessageMs"),
                                    popNode->GetValueByPath("LastMessageMs"),
                                    popNode->GetValueByPath("WaitTimeUs.History")
                                );
                                Min0(stage->MinTime, stage->OutputBytes->MinTime);
                                Max0(stage->MaxTime, stage->OutputBytes->MaxTime);
                            } else {
                                stage->OutputBytes = std::make_shared<TSingleMetric>(OutputBytes);
                            }
                            if (auto* rowsNode = popNode->GetValueByPath("Rows")) {
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
                            if (auto* chunksNode = popNode->GetValueByPath("Chunks")) {
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
                                            if (auto* pushNode = subNode.GetValueByPath("Push")) {
                                                if (auto* bytesNode = pushNode->GetValueByPath("Bytes")) {
                                                    connection->InputBytes = std::make_shared<TSingleMetric>(InputBytes,
                                                        *bytesNode, 0, 0,
                                                        pushNode->GetValueByPath("FirstMessageMs"),
                                                        pushNode->GetValueByPath("LastMessageMs"),
                                                        pushNode->GetValueByPath("WaitTimeUs.History")
                                                    );
                                                    Min0(stage->MinTime, connection->InputBytes->MinTime);
                                                    Max0(stage->MaxTime, connection->InputBytes->MaxTime);
                                                    inputBytes += connection->InputBytes->Details.Sum;
                                                } else {
                                                    connection->InputBytes = std::make_shared<TSingleMetric>(InputBytes);
                                                }
                                                if (auto* rowsNode = pushNode->GetValueByPath("Rows")) {
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
                                                if (auto* chunksNode = pushNode->GetValueByPath("Chunks")) {
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
                                ingressNode = ingress0.GetValueByPath("Push");
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
                    // NodeToConnection[connectionPlanNodeId] = connection.get();
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
                if (auto* pushNode = subNode.GetValueByPath("Push")) {
                    if (auto* firstMessageMaxNode = pushNode->GetValueByPath("FirstMessageMs.Min")) {
                        Min0(stage->MinTime, firstMessageMaxNode->GetIntegerSafe());
                    }
                    if (auto* lastMessageMaxNode = pushNode->GetValueByPath("LastMessageMs.Max")) {
                        Max0(stage->MaxTime, lastMessageMaxNode->GetIntegerSafe());
                    }
                }
            }
        }

        if (auto* cpuTimeNode = stage->StatsNode->GetValueByPath("CpuTimeUs")) {
            stage->CpuTime = std::make_shared<TSingleMetric>(CpuTime, *cpuTimeNode, stage->MinTime, stage->MaxTime);
            MergeTotalCpu(stage->CpuTime);
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

void TPlan::MarkStageIndent(ui32 indent, ui32& offsetY, std::shared_ptr<TStage> stage) {
    if (stage->IndentX < indent) {
        stage->IndentX = indent;
    }

    stage->OffsetY = offsetY;
    ui32 height = std::max<ui32>(
        (   (stage->EgressBytes != nullptr) + (stage->OutputBytes != nullptr)
            + 2 /* MEM, CPU */
            + stage->Connections.size() + stage->BuiltInIngress
        ),
        stage->Operators.size()
    ) * (INTERNAL_HEIGHT + INTERNAL_GAP_Y) + INTERNAL_GAP_Y;

    stage->Height = height;
    stage->IndentY = stage->OffsetY + GAP_Y + height;
    offsetY += GAP_Y + height;

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

    Height = std::max(Height, stage->IndentY);
}

void TPlan::MarkLayout() {
    Height = 0;
    if (!Stages.empty()) {
        ui32 offsetY = 0;
        MarkStageIndent(0, offsetY, Stages.front());
    }
}

void TPlan::PrintTimeline(TStringBuilder& background, TStringBuilder& canvas, const TString& title, TAggregation& firstMessage, TAggregation& lastMessage, ui32 x, ui32 y, ui32 w, ui32 h, const TString& color, bool backgroundRect) {

    auto firstMin = firstMessage.Min * w / MaxTime;
    auto lastMax = lastMessage.Max * w / MaxTime;

    background
        << "<g><title>" << title << ", Duration: " << FormatTimeMs(lastMessage.Max - firstMessage.Min) << " (" << FormatTimeAgg(firstMessage, lastMessage.Max < 60000) << " - " << FormatTimeAgg(lastMessage, lastMessage.Max < 60000) << ")</title>";

    if (backgroundRect) {
        background << SvgRect(Config.TimelineLeft, y, Config.TimelineWidth, h, "background");
    }

    background
        << "<rect x='" << x + firstMin << "' y='" << y
        << "' width='" << lastMax - firstMin + 1 << "' height='" << h
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

void TPlan::PrintSeries(TStringBuilder& canvas, std::vector<std::pair<ui64, ui64>> series, ui64 maxValue, ui32 x, ui32 y, ui32 w, ui32 h, const TString& title, const TString& lineColor, const TString& fillColor) {
    if (title) {
        canvas << "<g><title>" << title << "</title>" << Endl;
    }
    i32 px0 = x + series.front().first * w / MaxTime;
    i32 py0 = y + (h - 1);
    canvas << "<path d='M" << px0 << ',' << py0;
    for (auto& item : series) {
        i32 px = x + item.first * w / MaxTime;
        i32 py = y + (h - std::max<ui32>(item.second * h / maxValue, 1));
        if (px != px0 || py != py0) {
            // we use integer arithmetics, ignore low-resolution spikes
            canvas
                << "c" << (px0 * 2 + px) / 3 - px0 << ',' << py0 - py0 << ',' << (px0 + px * 2) / 3 - px0 << ',' << py - py0 << ',' << px - px0 << ',' << py - py0;
            px0 = px;
            py0 = py;
        }
    }
    i32 px = x + series.back().first * w / MaxTime;
    i32 py = y + (h - 1);
    canvas
        << "c" << (px0 * 2 + px) / 3 - px0 << ',' << py0 - py0 << ',' << (px0 + px * 2) / 3 - px0 << ',' << py - py0 << ',' << px - px0 << ',' << py - py0 << 'z'
        << "' stroke-width='1' stroke='" << lineColor << "' fill='" << (fillColor ? fillColor : "none") << "' />" << Endl;

    if (title) {
        canvas << "</g>" << Endl;
    }
}


void TPlan::PrintDeriv(TStringBuilder& canvas, TMetricHistory& history, ui32 x, ui32 y, ui32 w, ui32 h, const TString& title, const TString& lineColor, const TString& fillColor) {
    if (history.MaxDeriv != 0) {
        PrintSeries(canvas, history.Deriv, history.MaxDeriv, x, y, w, h, title, lineColor, fillColor);
    }
}

void TPlan::PrintValues(TStringBuilder& canvas, TMetricHistory& history, ui32 x, ui32 y, ui32 w, ui32 h, const TString& title, const TString& lineColor, const TString& fillColor) {
    if (history.MaxValue != 0) {
        PrintSeries(canvas, history.Values, history.MaxValue, x, y, w, h, title, lineColor, fillColor);
    }
}

void TPlan::PrintStageSummary(TStringBuilder& background, ui32 viewLeft, ui32 viewWidth, ui32 y0, ui32 h, std::shared_ptr<TSingleMetric>& metric, const TString& mediumColor, const TString& lightColor, const TString& textSum, const TString& tooltip, ui32 taskCount, const TString& iconRef, const TString& iconColor, const TString& iconScale, bool backgroundRect, const TString& peerId, ui64 split, const std::shared_ptr<TScalarMetric>& scalar) {

    ui32 x0 = viewLeft + INTERNAL_GAP_X;
    ui32 width = viewWidth - INTERNAL_GAP_X * 2;
    if (iconRef) {
        x0 += INTERNAL_WIDTH;
        width -= INTERNAL_WIDTH;
    }
    if (metric->Details.Sum == 0) {
        width = 0;
    } else if (metric->Summary && metric->Summary->Max) {
        width = metric->Details.Sum * width / metric->Summary->Max;
    }
    if (width == 0) {
        width = 1;
    }
    if (tooltip) {
        background
        << "<g><title>" << tooltip << "</title>" << Endl;
    }
    if (backgroundRect) {
        background << SvgRect(viewLeft, y0, viewWidth, h, "background");
    }
    if (iconRef) {
        background
        << "<use href='" << iconRef << "' transform='translate(" << viewLeft << ' ' << y0 << ") scale(" << iconScale << ")' fill='" << iconColor << "'/>" << Endl;
    }
    if (peerId) {
        background
        << SvgTextM(viewLeft + INTERNAL_WIDTH / 2, y0 + INTERNAL_HEIGHT / 2 + INTERNAL_TEXT_HEIGHT / 2, peerId);
    }
    if (metric->Details.Max) {
        auto wavg = width / 2;
        if (metric->Details.Max > metric->Details.Min) {
            wavg = (metric->Details.Avg - metric->Details.Min) * width / (metric->Details.Max - metric->Details.Min);
        }
        background
        << "  <rect x='" << x0 << "' y='" << y0
        << "' width='" << width << "' height='" << h
        << "' stroke-width='0' fill='" << lightColor << "'/>"
        << "  <polygon points='" << x0 << "," << y0 << " "
        << x0 + wavg << "," << y0 + h - metric->Details.Avg * h / metric->Details.Max << " "
        << x0 + width << "," << y0 + h - metric->Details.Min * h / metric->Details.Max << " "
        << x0 + width << "," << y0 + h << " "
        << x0 << "," << y0 + h
        << "' stroke='none' fill='" << mediumColor << "'/>" << Endl;
    } else {
        background
        << "  <rect x='" << x0 << "' y='" << y0
        << "' width='" << width << "' height='" << h
        << "' stroke-width='0' fill='" << mediumColor << "'/>" << Endl;
    }
    if (split && split < metric->Details.Sum) {
        auto xs = x0 + width - split * width / metric->Details.Sum;
        background
        << "  <line x1='" << xs << "' y1='" << y0 << "' x2='" << xs << "' y2='" << y0 + h
        << "' stroke-width='2' stroke='" << lightColor << "'/>" << Endl;
    }
    if (scalar) {
        ui32 width = viewWidth - INTERNAL_GAP_X * 2;
        if (iconRef) {
            width -= INTERNAL_WIDTH;
        }
        auto x2 = x0 + width - scalar->Value * width / scalar->Summary->Max;
        background
        << "  <line x1='" << x0 << "' y1='" << y0 + h - 3 << "' x2='" << x2 << "' y2='" << y0 + h - 3
        << "' stroke-width='3' stroke='" << lightColor << "' stroke-dasharray='1,1'/>" << Endl;
    }
    if (textSum) {
        background
        << "<rect x='" << x0 << "' y='" << y0 + (h - INTERNAL_TEXT_HEIGHT) / 2
        << "' width='" << textSum.size() * INTERNAL_TEXT_HEIGHT * 7 / 10 << "' height='" << INTERNAL_TEXT_HEIGHT + 1
        << "' stroke-width='0' opacity='0.5' fill='" << Config.Palette.StageMain << "'/>" << Endl
        << "<text font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextSummary << "' x='" << x0
        << "' y='" << y0 + INTERNAL_TEXT_HEIGHT + (h - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << textSum << "</text>" << Endl;
    }
    if (tooltip) {
        background
        << "</g>" << Endl;
    }

    if (taskCount) {
        TStringBuilder warn;
        TString w = "";

        if (metric->Details.Count != taskCount && (metric->Details.Sum || metric->Details.Count)) {
            warn << "Only " << metric->Details.Count << " task(s) of " << taskCount << " reported this metric";
            w = ToString(metric->Details.Count);
        }

        // SKEW is not reported for small values (less than 10% of max per graph)
        if (metric->Summary && metric->Details.Sum * 10 >= metric->Summary->Max) {
            // Define SKEW as following:
            //   1. Max > 4 * Min, i.e. there is LARGE DIFFERENCE between minimal and maximal metric values
            // or
            //   1. Max > 2 * Min, i.e. there is SIGNIFICANT DIFFERENCE between minimal and maximal metric values
            //   2. (Max - Avg) > 2 * (Avg - Min), i.e. OVERLOADED tasks are in MINORITY
            // Skewing ratio (x2 and x4) may be tuned later
            if ((metric->Details.Max > 4 * metric->Details.Min) || (metric->Details.Max > 2 * metric->Details.Min
                && metric->Details.Max - metric->Details.Avg > 2 * (metric->Details.Avg - metric->Details.Min))) {
                if (w) {
                    warn << ", ";
                } else {
                    w = "S";
                }
                warn << "Significant skew in metric";
            }
        }

        if (w) {
            background
            << "<g><title>" << warn << "</title>" << Endl
            << "  <circle cx='" << (viewLeft + viewWidth) - INTERNAL_WIDTH / 2
            << "' cy='" << y0 + INTERNAL_WIDTH / 2
            << "' r='" << INTERNAL_WIDTH / 2 - 1
            << "' stroke='none' fill='" << Config.Palette.StageTextHighlight << "' />" << Endl
            << "  <text text-anchor='middle' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT
            << "px' fill='" << Config.Palette.TextLight
            << "' x='" << (viewLeft + viewWidth) - INTERNAL_WIDTH / 2
            << "' y='" << y0 + INTERNAL_WIDTH - (INTERNAL_WIDTH - INTERNAL_TEXT_HEIGHT) / 2
            << "'>" << w << "</text>" << Endl
            << "</g>" << Endl;
        }
    }
}

void TPlan::PrepareSvg(ui64 maxTime, ui32 timelineDelta, ui32& offsetY) {
    OffsetY = offsetY;

    auto* p = this;
    auto planName = NodeType;

    if (Stages.empty() && CtePlan != nullptr) {
        p = CtePlan;
        planName = planName + " (reference to " + p->NodeType + ')';
    }

    offsetY += GAP_Y;

    ui32 summary3 = (Config.SummaryWidth - INTERNAL_GAP_X * 2) / 3;
    auto titleHeight = INTERNAL_GAP_Y + (INTERNAL_HEIGHT + INTERNAL_TEXT_HEIGHT) / 2;

    _Builder
        << "<g data-group='g" << GroupId << "' class='selectable'><title> " << planName << "</title>" << Endl
        << SvgRect(Config.HeaderLeft, GAP_Y, Config.HeaderWidth, TIME_HEIGHT + INTERNAL_HEIGHT, "background")
        << SvgTextS(Config.HeaderLeft + INTERNAL_GAP_X + INTERNAL_WIDTH * 2 + 2, GAP_Y + titleHeight, planName)
        << "</g>" << Endl;

    _Builder
        << "<g class='ardn button'>"
        << SvgRect(INTERNAL_GAP_X, GAP_Y, CONN_SIZE, CONN_SIZE, "transparent")
        << "<use href='#icon_arrowdn' transform='translate(" << INTERNAL_GAP_X << ' ' << GAP_Y << ") scale(0.014, 0.014)' fill='" << Config.Palette.ConnectionText << "'/></g>" << Endl
        << "<g class='aruu button'>"
        << SvgRect(INTERNAL_GAP_X, GAP_Y + CONN_SIZE, CONN_SIZE, CONN_SIZE, "transparent")
        << "<use href='#icon_arrowup' transform='translate(" << INTERNAL_GAP_X << ' ' << GAP_Y + CONN_SIZE << ") scale(0.014, 0.014)' fill='" << Config.Palette.ConnectionText << "'/></g>" << Endl;

    _Builder
        << SvgTextS(Config.OperatorLeft + 2, GAP_Y + titleHeight, "Operators")
        << SvgTextS(Config.SummaryLeft + 2, GAP_Y + titleHeight, "Stages")
        << SvgTextE(Config.TaskLeft + Config.TaskWidth - 2, GAP_Y + titleHeight, "Tasks")
        << SvgTextE(Config.TaskLeft + Config.TaskWidth - 2, GAP_Y + titleHeight + INTERNAL_GAP_Y + INTERNAL_TEXT_HEIGHT, ToString(p->Tasks));

    _Builder
        << "<g><title>Ingress "
        << FormatBytes(p->IngressBytes->Value) << ", Rows " << FormatIntegerValue(p->IngressRows->Value);
    if (p->IngressRows->Value) {
    _Builder
        << ", Width " << p->IngressBytes->Value / p->IngressRows->Value << "B";
    }
    if (p->MaxTime) {
    _Builder
        << ", Avg " << FormatBytes(p->IngressBytes->Value * 1000 / p->MaxTime) << "/s";
    }
    _Builder
        << "</title>" << Endl
        << "  <rect x='" << Config.SummaryLeft << "' y='" << GAP_Y + titleHeight + INTERNAL_GAP_Y
        << "' width='" << summary3 << "' height='" << TIME_HEIGHT
        << "' stroke-width='0' fill='" << Config.Palette.IngressMedium << "'/>" << Endl
        << "  <text font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextLight
        << "' x='" << Config.SummaryLeft + 2
        << "' y='" << GAP_Y + titleHeight + INTERNAL_GAP_Y + INTERNAL_TEXT_HEIGHT << "'>" << FormatBytes(p->IngressBytes->Value) << "</text>" << Endl
        << "</g>" << Endl;

    _Builder
        << "<g><title>CPU Usage " << FormatUsage(p->CpuTime->Value);
    if (p->MaxTime) {
        auto usagePS = p->CpuTime->Value / p->MaxTime;
        usagePS /= 10;
    _Builder
        << ", Avg " << Sprintf("%lu.%.2lu", usagePS / 100, usagePS % 100) << " CPU/s";
    }
    _Builder
        << "</title>" << Endl
        << "  <rect x='" << Config.SummaryLeft + INTERNAL_GAP_X + summary3 << "' y='" << GAP_Y + titleHeight + INTERNAL_GAP_Y
        << "' width='" << Config.SummaryWidth - (summary3 + INTERNAL_GAP_X) * 2 << "' height='" << TIME_HEIGHT
        << "' stroke-width='0' fill='" << Config.Palette.CpuMedium << "'/>" << Endl
        << "  <text font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextLight
        << "' x='" << Config.SummaryLeft + INTERNAL_GAP_X + summary3 + 2
        << "' y='" << GAP_Y + titleHeight + INTERNAL_GAP_Y + INTERNAL_TEXT_HEIGHT << "'>" << FormatUsage(p->CpuTime->Value) << "</text>" << Endl
        << "</g>" << Endl;

    _Builder
        << "<g><title>Memory " << FormatBytes(p->MaxMemoryUsage->Value) << "</title>" << Endl
        << "  <rect x='" << Config.SummaryLeft + Config.SummaryWidth - summary3 << "' y='" << GAP_Y + titleHeight + INTERNAL_GAP_Y
        << "' width='" << summary3 << "' height='" << TIME_HEIGHT
        << "' stroke-width='0' fill='" << Config.Palette.MemMedium << "'/>" << Endl
        << "  <text font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextLight
        << "' x='" << Config.SummaryLeft + Config.SummaryWidth - summary3 + 2
        << "' y='" << GAP_Y + titleHeight + INTERNAL_GAP_Y + INTERNAL_TEXT_HEIGHT << "'>" << FormatBytes(p->MaxMemoryUsage->Value) << "</text>" << Endl
        << "</g>" << Endl;

    auto x = Config.TimelineLeft + (Config.TimelineWidth - timelineDelta) * (p->TimeOffset + p->MaxTime) / maxTime;
    _Builder
        << "<g><title>" << "Duration: " << FormatTimeMs(p->MaxTime) << ", Total " << FormatTimeMs(p->MaxTime + p->TimeOffset) << "</title>" << Endl
        << "  <rect x='" << x - summary3 << "' y='" << GAP_Y + INTERNAL_GAP_Y + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2
        << "' width='" << summary3 << "' height='" << TIME_HEIGHT
        << "' stroke-width='0' fill='" << Config.Palette.StageGrid << "'/>" << Endl
        << "  <text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextInverted << "' x='" << x - 2
        << "' y='" << GAP_Y + titleHeight << "'>" << FormatTimeMs(p->MaxTime + p->TimeOffset) << "</text>" << Endl
        << "</g>" << Endl;

    offsetY += titleHeight + INTERNAL_GAP_Y;
    if (!p->TotalCpuTime.Deriv.empty() && p->TotalCpuTime.MaxTime > p->TotalCpuTime.MinTime) {

        // auto tx0 = Config.TimelineLeft;
        // auto tw = Config.TimelineWidth;

        auto xmin = Config.TimelineLeft + (Config.TimelineWidth - timelineDelta) * (p->TotalCpuTime.MinTime + p->TimeOffset) / maxTime;
        auto xmax = Config.TimelineLeft + (Config.TimelineWidth - timelineDelta) * (p->TotalCpuTime.MaxTime + p->TimeOffset) / maxTime;

        auto maxCpu = p->TotalCpuTime.MaxDeriv * TIME_SERIES_RANGES / (p->TotalCpuTime.MaxTime - p->TotalCpuTime.MinTime);
        p->PrintDeriv(_Builder, p->TotalCpuTime, xmin, GAP_Y + titleHeight + INTERNAL_GAP_Y, xmax - xmin, TIME_HEIGHT, "Max CPU " + FormatMCpu(maxCpu), Config.Palette.CpuMedium, Config.Palette.CpuLight);
    }
    offsetY += TIME_HEIGHT;

    for (auto& s : Stages) {
        s->_Builder
            << "<g data-group='g" << s->GroupId << "' class='selectable'><title>Stage " << (s->External ? "E" : ToString(s->PhysicalStageId)) << "</title>" << Endl;
        auto stageClass = s->External ? "clone" : "stage";

        s->_Builder
            << SvgRect(Config.HeaderLeft + s->IndentX, 0, Config.HeaderWidth - s->IndentX, "100%", stageClass)
            << SvgRect(Config.OperatorLeft, 0, Config.OperatorWidth, "100%", stageClass)
            << SvgRect(Config.SummaryLeft, 0, Config.SummaryWidth, "100%", stageClass)
            << SvgRect(Config.TaskLeft, 0, Config.TaskWidth, "100%", stageClass)
            << SvgRect(Config.TimelineLeft, 0, Config.TimelineWidth, "100%", stageClass);

        {
            ui32 y0 = INTERNAL_GAP_Y;
            ui32 index = 0;
            for (auto op : s->Operators) {
                ui32 yt = y0 + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2;
                // if (index > 0) {
                //     s->_Builder << SvgLine(Config.HeaderLeft + s->IndentX + 2, yt, Config.HeaderLeft + Config.HeaderWidth, yt, "opdiv");
                // }
                s->_Builder
                    << "<g><title>" << op.Name << ": " << op.Info << (op.Blocks ? " Blocks: True" : "") << "</title>";
                if (op.Blocks) {
                    auto h = INTERNAL_TEXT_HEIGHT * 2 + INTERNAL_GAP_Y * 2;
                    if (index == s->Operators.size() - 1) {
                        h = s->Height - yt;
                    }
                    s->_Builder
                    << SvgRect(Config.HeaderLeft + s->IndentX, yt, INTERNAL_WIDTH * 2, h, "blocks");
                }

                s->_Builder
                    << SvgText(Config.HeaderLeft + s->IndentX + INTERNAL_GAP_X + INTERNAL_WIDTH * 2 + 2, yt + INTERNAL_TEXT_HEIGHT, "texts clipped", op.Name + ": " + op.Info);
                if (op.OutputRows) {
                    TStringBuilder tooltip;
                    auto textSum = FormatTooltip(tooltip, "Output Rows", op.OutputRows.get(), FormatInteger);
                    if (op.Estimations) {
                        tooltip
                        << ", " << op.Estimations;
                    }
                    PrintStageSummary(s->_Builder, Config.OperatorLeft, Config.OperatorWidth, y0, INTERNAL_HEIGHT, op.OutputRows, Config.Palette.OutputMedium, Config.Palette.OutputLight, textSum, tooltip, s->Tasks, "", "", "");
                }
                s->_Builder
                    << "</g>" << Endl;

                if (!op.Inputs.empty()) {
                    auto opX = Config.HeaderLeft + Config.HeaderWidth - INTERNAL_WIDTH * (1 + 2 * (op.Inputs.size() - 1)) / 2;
                    auto opY = y0 + INTERNAL_HEIGHT / 2;
                    for (auto& input : op.Inputs) {
                        if (input.StageId) {
                            s->_Builder
                                << "<g data-group='g" << NodeToConnection.at(input.PlanNodeId)->GroupId << "' class='selectable'><title>Input from Stage " << *input.StageId << "</title>" << Endl
                                << SvgStageId(opX, opY, ToString(*input.StageId))
                                << "</g>" << Endl;
                        } else if (input.PrecomputeRef) {
                            auto it = Viz.CteSubPlans.find(input.PrecomputeRef);
                            if (it != Viz.CteSubPlans.end()) {
                                s->_Builder
                                << "<g data-group='g" << it->second->GroupId << "' class='selectable'><title>Data from precompute " << it->second->NodeType << "</title>" << Endl
                                << SvgStageId(opX, opY, "P")
                                << "</g>" << Endl;
                            }
                        }
                        opX += INTERNAL_WIDTH;
                    }
                }

                y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;
                index++;
            }
        }

        s->_Builder
            << SvgStageId(Config.HeaderLeft + s->IndentX + INTERNAL_GAP_X + INTERNAL_WIDTH * 3 / 2, INTERNAL_GAP_Y + INTERNAL_HEIGHT / 2, s->External ? "E" : ToString(s->PhysicalStageId));

        ui32 y0 = INTERNAL_GAP_Y;

        auto tx0 = Config.TimelineLeft;
        auto px = tx0 + TimeOffset * (Config.TimelineWidth - timelineDelta) / maxTime;
        auto pw = MaxTime * (Config.TimelineWidth - timelineDelta) / maxTime;

        if (s->EgressBytes) {
            TStringBuilder& builder = s->_Builder;
            builder << "<g data-group='g" << (s->External ? StageToExternalConnection[s.get()]->GroupId : s->GroupId) << "' class='selectable'><title>Egress</title>" << Endl;

            TStringBuilder tooltip;
            auto textSum = FormatTooltip(tooltip, "Egress", s->EgressBytes.get(), FormatBytes);
            if (s->EgressRows) {
                FormatTooltip(tooltip, ", Rows", s->EgressRows.get(), FormatInteger);
            }
            PrintStageSummary(builder, Config.SummaryLeft, Config.SummaryWidth, y0, INTERNAL_HEIGHT, s->EgressBytes, Config.Palette.EgressMedium, Config.Palette.EgressLight, textSum, tooltip, s->Tasks, "#icon_egress", Config.Palette.EgressMedium, "0.9 0.9", s->External);

            auto d = s->EgressBytes->MaxTime - s->EgressBytes->MinTime;
            TStringBuilder title;
            title << "Egress";
            if (d) {
                title << " " << FormatBytes(s->EgressBytes->Details.Sum * 1000 / d) << "/s";
                if (s->EgressRows) {
                    title << ", Rows " << FormatInteger(s->EgressRows->Details.Sum * 1000 / d) << "/s";
                }
            }

            TStringBuilder connCanvas;

            PrintTimeline(builder, connCanvas, title, s->EgressBytes->FirstMessage, s->EgressBytes->LastMessage, px, y0, pw, INTERNAL_HEIGHT, Config.Palette.EgressMedium, s->External);

            if (!s->EgressBytes->WaitTime.Deriv.empty()) {
                PrintWaitTime(builder, s->EgressBytes, px, y0, pw, INTERNAL_HEIGHT, Config.Palette.EgressLight);
            }

            builder << connCanvas;

            if (!s->EgressBytes->History.Deriv.empty()) {
                PrintDeriv(builder, s->EgressBytes->History, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.EgressDark);
            }

            builder << "</g>" << Endl;
            y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;
        }

        if (s->OutputBytes) {
            TStringBuilder& builder = s->_Builder;
            builder << "<g data-group='g" << (s->OutputPlanNodeId ? NodeToConnection[s->OutputPlanNodeId]->GroupId : GroupId) << "' class='selectable'><title>Output</title>" << Endl;

            TStringBuilder tooltip;
            auto textSum = FormatTooltip(tooltip, "Output", s->OutputBytes.get(), FormatBytes);
            if (s->OutputLocalBytes && s->OutputBytes->Details.Sum) {
                tooltip << ", Local " << s->OutputLocalBytes * 100 / s->OutputBytes->Details.Sum << "%, \u2211" << FormatBytes(s->OutputLocalBytes);
            }
            if (s->OutputRows) {
                FormatTooltip(tooltip, ", Rows", s->OutputRows.get(), FormatInteger);
                if (s->OutputRows->Details.Sum) {
                    tooltip << ", Width " << FormatBytes(s->OutputBytes->Details.Sum / s->OutputRows->Details.Sum);
                }
            }
            if (s->OutputChunks) {
                tooltip << ", Chunks \u2211" << FormatInteger(s->OutputChunks);
                if (s->OutputChunkSize) {
                    tooltip << " ~ " << FormatBytes(s->OutputChunkSize->Value);
                }
            }
            PrintStageSummary(builder, Config.SummaryLeft, Config.SummaryWidth, y0, INTERNAL_HEIGHT, s->OutputBytes, Config.Palette.OutputMedium, Config.Palette.OutputLight, textSum, tooltip, s->Tasks, "#icon_output", Config.Palette.OutputLight, "0.0325 0.0325", true, s->OutputPhysicalStageId ? ToString(s->OutputPhysicalStageId) : "", s->OutputLocalBytes, s->OutputChunkSize);

            if (s->SpillingChannelBytes && s->SpillingChannelBytes->Details.Sum) {
                builder
                << "<g><title>";

                auto textSum = FormatTooltip(builder, "Channel Spilling", s->SpillingChannelBytes.get(), FormatBytes);
                auto x1 = Config.SummaryLeft + Config.SummaryWidth - INTERNAL_GAP_X;
                auto x0 = x1 - textSum.size() * INTERNAL_TEXT_HEIGHT * 7 / 10;

                builder
                << "</title>" << Endl
                << "  <rect x='" << x0 << "' y='" << y0 + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2
                << "' width='" << x1 - x0 << "' height='" << INTERNAL_TEXT_HEIGHT + 1
                << "' stroke-width='0' fill='" << Config.Palette.SpillingBytesLight << "'/>" << Endl
                << "  <text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextSummary << "' x='" << x1 - 1
                << "' y='" << y0 + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << textSum << "</text>" << Endl
                << "</g>" << Endl;
            }

            auto d = s->OutputBytes->MaxTime - s->OutputBytes->MinTime;
            TStringBuilder title;
            title << "Output";
            if (d) {
                title << " " << FormatBytes(s->OutputBytes->Details.Sum * 1000 / d) << "/s";
                if (s->OutputRows) {
                    title << ", Rows " << FormatInteger(s->OutputRows->Details.Sum * 1000 / d) << "/s";
                }
            }

            TStringBuilder connCanvas;

            PrintTimeline(builder, connCanvas, title, s->OutputBytes->FirstMessage, s->OutputBytes->LastMessage, px, y0, pw, INTERNAL_HEIGHT, Config.Palette.OutputMedium, s->OutputPlanNodeId);

            if (!s->OutputBytes->WaitTime.Deriv.empty()) {
                PrintWaitTime(builder, s->OutputBytes, px, y0, pw, INTERNAL_HEIGHT, Config.Palette.OutputLight);
            }

            builder << connCanvas;

            if (!s->OutputBytes->History.Deriv.empty()) {
                PrintDeriv(builder, s->OutputBytes->History, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.OutputDark);
            }

            builder << "</g>" << Endl;
            y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;
        }

        if (s->MaxMemoryUsage) {
            TString tooltip;
            auto textSum = FormatTooltip(tooltip, "Memory", s->MaxMemoryUsage.get(), FormatBytes);
            PrintStageSummary(s->_Builder, Config.SummaryLeft, Config.SummaryWidth, y0, INTERNAL_HEIGHT, s->MaxMemoryUsage, Config.Palette.MemMedium, Config.Palette.MemLight, textSum, tooltip, s->Tasks, "#icon_memory", Config.Palette.MemMedium, "0.6 0.6");

            if (s->SpillingComputeBytes && s->SpillingComputeBytes->Details.Sum) {
                s->_Builder
                << "<g><title>";

                auto textSum = FormatTooltip(s->_Builder, "Compute Spilling", s->SpillingComputeBytes.get(), FormatBytes);
                auto x1 = Config.SummaryLeft + Config.SummaryWidth - INTERNAL_GAP_X;
                auto x0 = x1 - textSum.size() * INTERNAL_TEXT_HEIGHT * 7 / 10;

                s->_Builder
                << "</title>" << Endl
                << "<rect x='" << x0 << "' y='" << y0 + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2
                << "' width='" << x1 - x0 << "' height='" << INTERNAL_TEXT_HEIGHT + 1
                << "' stroke-width='0' fill='" << Config.Palette.SpillingBytesLight << "'/>" << Endl
                << "<text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextSummary << "' x='" << x1 - 1
                << "' y='" << y0 + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2 << "'>" << textSum << "</text>" << Endl
                << "</g>" << Endl;
            }

            if (!s->MaxMemoryUsage->History.Values.empty()) {
                PrintValues(s->_Builder, s->MaxMemoryUsage->History, px, y0, pw, INTERNAL_HEIGHT, "Max MEM " + FormatBytes(s->MaxMemoryUsage->History.MaxValue), Config.Palette.MemMedium, Config.Palette.MemMedium);
            }

            if (s->SpillingComputeBytes && !s->SpillingComputeBytes->History.Deriv.empty()) {
                PrintDeriv(s->_Builder, s->SpillingComputeBytes->History, px, y0, pw, INTERNAL_HEIGHT, "Spilling Compute", Config.Palette.SpillingBytesMedium, Config.Palette.SpillingBytesLight);
            }
        }

        y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;

        if (s->CpuTime) {
            TString tooltip;
            auto textSum = FormatTooltip(tooltip, "CPU Usage", s->CpuTime.get(), FormatUsage);
            PrintStageSummary(s->_Builder, Config.SummaryLeft, Config.SummaryWidth, y0, INTERNAL_HEIGHT, s->CpuTime, Config.Palette.CpuMedium, Config.Palette.CpuLight, textSum, tooltip, s->Tasks, "#icon_cpu", Config.Palette.CpuMedium, "0.6 0.6");

            auto totalTime = s->CpuTime->Details.Sum;
            if (s->WaitInputTime) {
                totalTime += s->WaitInputTime->Details.Sum;
            }
            if (s->WaitOutputTime) {
                totalTime += s->WaitOutputTime->Details.Sum;
            }

            ui32 activePercentsMin = 0;
            ui32 activePercentsMax = 100;

            if (s->WaitInputTime) {
                if (totalTime) {
                    auto heightPercents = s->WaitInputTime->Details.Sum * 100 / totalTime;
                    activePercentsMax -= heightPercents;
                s->_Builder
                    << "<g><title>";
                    FormatTooltip(s->_Builder, "Wait Input Time", s->WaitInputTime.get(), FormatUsage, totalTime);
                s->_Builder
                    << "</title>" << Endl
                    << "  <rect x='" << Config.TaskLeft << "' y='" << activePercentsMax
                    << "%' width='" << Config.TaskWidth << "' height='" << heightPercents
                    << "%' stroke-width='0' fill='" << Config.Palette.InputLight << "'/>" << Endl
                    << "</g>" << Endl;
                }
                if(!s->WaitInputTime->History.Deriv.empty()) {
                    PrintDeriv(s->_Builder, s->WaitInputTime->History, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.InputMedium, Config.Palette.InputLight);
                }

                // consider only 10% or more waiting times
                if (totalTime && s->WaitInputTime->Details.Sum * 10 > totalTime) {
                    TStringBuilder waitOutputPeers;
                    for (auto& c : s->Connections) {
                        if (c->FromStage && c->FromStage->WaitOutputTime) {
                            auto peerTotalTime = c->FromStage->WaitOutputTime->Details.Sum;
                            if (c->FromStage->WaitInputTime) {
                                peerTotalTime += c->FromStage->WaitInputTime->Details.Sum;
                            }
                            if (c->FromStage->CpuTime) {
                                peerTotalTime += c->FromStage->CpuTime->Details.Sum;
                            }
                            if (peerTotalTime && c->FromStage->WaitOutputTime->Details.Sum * 10 > peerTotalTime) {
                                if (waitOutputPeers) {
                                    waitOutputPeers << ", ";
                                }
                                waitOutputPeers << c->FromStage->PhysicalStageId;
                            }
                        }
                    }
                    if (waitOutputPeers) {
                    s->_Builder
                        << "<g><title>" << "Wait input with peer stage(s) " << waitOutputPeers << " wait output" << "</title>" << Endl
                        << "  <circle cx='" << Config.TaskLeft + Config.TaskWidth / 2
                        << "' cy='" << s->OffsetY + offsetY + s->Height - INTERNAL_WIDTH / 2
                        << "' r='" << INTERNAL_WIDTH / 2 - 1
                        << "' stroke='none' fill='" << Config.Palette.StageTextHighlight << "' />" << Endl
                        << "  <text text-anchor='middle' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT
                        << "px' fill='" << Config.Palette.TextLight
                        << "' x='" << Config.TaskLeft + Config.TaskWidth / 2
                        << "' y='" << s->OffsetY + offsetY + s->Height - (INTERNAL_WIDTH - INTERNAL_TEXT_HEIGHT) / 2
                        << "'>" << "W" << "</text>" << Endl
                        << "</g>" << Endl;
                    }
                }
            }

            if (s->WaitOutputTime) {
                if (totalTime) {
                    auto heightPercents = s->WaitOutputTime->Details.Sum * s->Height / totalTime;
                    activePercentsMin += heightPercents;
                s->_Builder
                    << "<g><title>";
                    FormatTooltip(s->_Builder, "Wait Output Time", s->WaitOutputTime.get(), FormatUsage, totalTime);
                s->_Builder
                    << "</title>" << Endl
                    << "  <rect x='" << Config.TaskLeft << "' y='0%' stroke-width='0' fill='" << Config.Palette.OutputLight << "'/>" << Endl
                    << "</g>" << Endl;
                }
                if (!s->WaitOutputTime->History.Deriv.empty()) {
                    PrintDeriv(s->_Builder, s->WaitOutputTime->History, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.OutputMedium, Config.Palette.OutputLight);
                }
            }

            if (activePercentsMax > activePercentsMin && s->InputThroughput) {
                auto opacity = s->InputThroughput->Details.Sum / static_cast<double>(s->InputThroughput->Summary->Max * 2);
                s->_Builder
                << "<g><title>Input Throughput " << FormatInteger(s->InputThroughput->Details.Sum) << "/s</title>" << Endl
                << "  <rect x='" << Config.TaskLeft << "' y='" << activePercentsMin
                << "%' width='" << Config.TaskWidth << "' height='" << activePercentsMax - activePercentsMin
                << "%' stroke-width='0' fill='" << Config.Palette.CpuLight << "' opacity='" << opacity  << "'/>" << Endl
                << "</g>" << Endl;
            }

            if (!s->CpuTime->History.Deriv.empty() && s->CpuTime->History.MaxTime > s->CpuTime->History.MinTime) {
                auto maxCpu = s->CpuTime->History.MaxDeriv * TIME_SERIES_RANGES / (s->CpuTime->History.MaxTime - s->CpuTime->History.MinTime);
                PrintDeriv(s->_Builder, s->CpuTime->History, px, y0, pw, INTERNAL_HEIGHT, "Max CPU " + FormatMCpu(maxCpu), Config.Palette.CpuMedium, Config.Palette.CpuLight);
            }

            if (s->SpillingComputeTime && !s->SpillingComputeTime->History.Deriv.empty()) {
                PrintDeriv(s->_Builder, s->SpillingComputeTime->History, px, y0, pw, INTERNAL_HEIGHT, "Spilling Compute", Config.Palette.SpillingTimeMedium);
            }
        }

        if (s->Tasks) {
            if (s->External) {
                s->_Builder
                << "<g><title>External Source, partitions: " << s->Tasks << ", finished: " << s->FinishedTasks << "</title>" << Endl;
                if (s->FinishedTasks && s->FinishedTasks <= s->Tasks) {
                    auto unfinishedPercent = 100 * (s->Tasks - s->FinishedTasks) / s->Tasks;
                    auto xx = Config.TaskLeft + Config.TaskWidth / 8;
                    s->_Builder
                    << "<line x1='" << xx << "' y1='" << unfinishedPercent << "%' x2='" << xx << "' y2='100%'"
                    << " stroke-width='" << Config.TaskWidth / 4 << "' stroke='" << Config.Palette.StageText << "' stroke-dasharray='1,1' />" << Endl;
                }
                s->_Builder
                << "  " << SvgText(Config.TaskLeft + Config.TaskWidth - 2, "50%", "textc", ToString(s->Tasks))
                << "</g>" << Endl;
            } else {
                s->_Builder
                << "<g><title>Stage " << s->PhysicalStageId << ", tasks: " << s->Tasks << ", finished: " << s->FinishedTasks << "</title>" << Endl;
                if (s->FinishedTasks && s->FinishedTasks <= s->Tasks) {
                    auto unfinishedPercent = 100 * (s->Tasks - s->FinishedTasks) / s->Tasks;
                    auto xx = Config.TaskLeft + Config.TaskWidth / 8;
                    s->_Builder
                    << "<line x1='" << xx << "' y1='" << unfinishedPercent << "%' x2='" << xx << "' y2='100%'"
                    << " stroke-width='" << Config.TaskWidth / 4 << "' stroke='" << Config.Palette.StageText << "' stroke-dasharray='1,1' />" << Endl;
                }
                s->_Builder
                << "  " << SvgText(Config.TaskLeft + Config.TaskWidth - 2, "50%", "textc", ToString(s->Tasks))
                << "</g>" << Endl;
            }
        }

        if (!s->Connections.empty()) {
            s->_Builder
            << "<g class='plus button'>"
            << SvgRect(s->IndentX + INTERNAL_GAP_X, INTERNAL_GAP_Y * 3 + INTERNAL_HEIGHT, CONN_SIZE, CONN_SIZE, "transparent")
            << "<use href='#icon_minus' class='icon_minus' transform='translate(" << s->IndentX + INTERNAL_GAP_X << ' ' << INTERNAL_GAP_Y * 3 + INTERNAL_HEIGHT << ") scale(0.014, 0.014)' fill='" << Config.Palette.ConnectionText << "'/>" << Endl
            << "<use href='#icon_plus' class='icon_plus' transform='translate(" << s->IndentX + INTERNAL_GAP_X << ' ' << INTERNAL_GAP_Y * 3 + INTERNAL_HEIGHT << ") scale(0.014, 0.014)' fill='" << Config.Palette.ConnectionText << "'/></g>" << Endl;
        }
        s->_Builder
            << "<g class='arup button'>"
            << SvgRect(s->IndentX + INTERNAL_GAP_X, s->Height - (INTERNAL_GAP_Y + CONN_SIZE), CONN_SIZE, CONN_SIZE, "transparent")
            << "<use href='#icon_arrowup' transform='translate(" << s->IndentX + INTERNAL_GAP_X << ' ' << s->Height - (INTERNAL_GAP_Y + CONN_SIZE) << ") scale(0.014, 0.014)' fill='" << Config.Palette.ConnectionText << "'/></g>" << Endl;

        y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;

        for (auto& c : s->Connections) {

            auto x = c->CteConnection ? c->CteIndentX : c->FromStage->IndentX;
            auto y = 0;

            c->_Builder << "<g data-group='g" << c->GroupId << "' class='selectable'><title>Connection: " << c->NodeType;
            if (!c->KeyColumns.empty()) {
                c->_Builder << " KeyColumns: ";
                bool first = true;
                for (auto k : c->KeyColumns) {
                    if (first) {
                        first = false;
                    } else {
                        c->_Builder << ", ";
                    }
                    c->_Builder << k;
                }
            }
            if (!c->SortColumns.empty()) {
                c->_Builder << " SortColumns: ";
                bool first = true;
                for (auto s : c->SortColumns) {
                    if (first) {
                        first = false;
                    } else {
                        c->_Builder << ", ";
                    }
                    c->_Builder << s;
                }
            }
            if (c->Blocks) {
                c->_Builder << " Blocks: True";
            }
            if (c->HashFunc) {
                c->_Builder << " HashFunc: " << c->HashFunc;
            }
            if (c->Parallel) {
                c->_Builder << " Parallel: True";
            }
            c->_Builder
                << "</title>" << Endl;

            if (c->CteConnection) {
                c->_CteBuilder
                    << "<g data-group='g" << c->FromStage->GroupId << "' class='selectable'><title>Stage " << (c->FromStage->External ? "E" : ToString(c->FromStage->PhysicalStageId)) << "</title>" << Endl
                    << SvgRect(Config.TaskLeft, y, Config.TaskWidth, INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2, "clone")
                    << SvgRect(Config.HeaderLeft + x, y, Config.HeaderWidth - x, INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2, "clone")
                    << SvgRect(Config.SummaryLeft, y, Config.SummaryWidth, INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2, "clone")
                    << SvgRect(Config.OperatorLeft, y, Config.OperatorWidth, INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2, "clone");

                if (c->CteOperatorOutputRows) {
                    TStringBuilder tooltip;
                    auto textSum = FormatTooltip(tooltip, "Output Rows", c->CteOperatorOutputRows.get(), FormatInteger);
                    PrintStageSummary(c->_CteBuilder, Config.OperatorLeft, Config.OperatorWidth, y, INTERNAL_HEIGHT, c->CteOperatorOutputRows, Config.Palette.OutputMedium, Config.Palette.OutputLight, textSum, tooltip, 0, "", "", "");
                }

                c->_CteBuilder
                    << SvgRect(Config.TimelineLeft, y, Config.TimelineWidth, INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2, "clone")
                    << SvgStageId(Config.HeaderLeft + x + INTERNAL_GAP_X + INTERNAL_WIDTH * 3 / 2, y + INTERNAL_GAP_Y + INTERNAL_HEIGHT / 2, ToString(c->FromStage->PhysicalStageId))
                    << SvgText(Config.HeaderLeft + x + INTERNAL_GAP_X + INTERNAL_WIDTH * 2 + 2, y + INTERNAL_GAP_Y + INTERNAL_TEXT_HEIGHT + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2, "texts clipped", c->FromStage->Operators[0].Name + ": " + c->FromStage->Operators[0].Info)
                    << "</g>" << Endl;

                if (c->CteOutputBytes) {
                    c->_CteBuilder << "<g data-group='g" << c->GroupId << "' class='selectable'><title>Output</title>" << Endl;

                    TStringBuilder tooltip;
                    auto textSum = FormatTooltip(tooltip, "Output", c->CteOutputBytes.get(), FormatBytes);
                    if (c->CteOutputLocalBytes && c->CteOutputBytes->Details.Sum) {
                        tooltip << ", Local " << c->CteOutputLocalBytes * 100 / c->CteOutputBytes->Details.Sum << "%, \u2211" << FormatBytes(c->CteOutputLocalBytes);
                    }
                    if (c->CteOutputRows) {
                        FormatTooltip(tooltip, ", Rows", c->CteOutputRows.get(), FormatInteger);
                        if (c->CteOutputRows->Details.Sum) {
                            tooltip << ", Width " << FormatBytes(c->CteOutputBytes->Details.Sum / c->CteOutputRows->Details.Sum);
                        }
                    }
                    if (c->CteOutputChunks) {
                        tooltip << ", Chunks \u2211" << FormatInteger(c->CteOutputChunks);
                        if (c->CteOutputChunkSize) {
                            tooltip << " ~ " << FormatBytes(c->CteOutputChunkSize->Value);
                        }
                    }
                    PrintStageSummary(c->_CteBuilder, Config.SummaryLeft, Config.SummaryWidth, y + INTERNAL_GAP_Y, INTERNAL_HEIGHT, c->CteOutputBytes, Config.Palette.OutputMedium, Config.Palette.OutputLight, textSum, tooltip, 0, "#icon_output", Config.Palette.OutputLight, "0.0325 0.0325", true, ToString(s->PhysicalStageId), c->CteOutputLocalBytes, c->CteOutputChunkSize);

                    auto d = c->CteOutputBytes->MaxTime - c->CteOutputBytes->MinTime;
                    TStringBuilder title;
                    title << "Output";
                    if (d) {
                        title << " " << FormatBytes(c->CteOutputBytes->Details.Sum * 1000 / d) << "/s";
                        if (c->CteOutputRows) {
                            title << ", Rows " << FormatInteger(c->CteOutputRows->Details.Sum * 1000 / d) << "/s";
                        }
                    }

                    TStringBuilder connCanvas;

                    PrintTimeline(c->_CteBuilder, connCanvas, title, c->CteOutputBytes->FirstMessage, c->CteOutputBytes->LastMessage, px, y + INTERNAL_GAP_Y, pw, INTERNAL_HEIGHT, Config.Palette.OutputMedium, true);

                    if (!c->CteOutputBytes->WaitTime.Deriv.empty()) {
                        PrintWaitTime(c->_CteBuilder, c->CteOutputBytes, px, y + INTERNAL_GAP_Y, pw, INTERNAL_HEIGHT, Config.Palette.OutputLight);
                    }

                    c->_CteBuilder << connCanvas;

                    if (!c->CteOutputBytes->History.Deriv.empty()) {
                        PrintDeriv(c->_CteBuilder, c->CteOutputBytes->History, px, y + INTERNAL_GAP_Y, pw, INTERNAL_HEIGHT, "", Config.Palette.OutputDark);
                    }
                    c->_CteBuilder << "</g>" << Endl;
                }
            }

            TString mark;
            if (c->NodeType == "HashShuffle")     mark = "H";
            else if (c->NodeType == "Merge")      mark = "Me";
            else if (c->NodeType == "Map")        mark = "Ma";
            else if (c->NodeType == "UnionAll")   mark = "U";
            else if (c->NodeType == "Broadcast")  mark = "B";
            else if (c->NodeType == "External")   mark = "E";
            else if (c->NodeType == "Table")      mark = "T";
            else if (c->NodeType == "Lookup")     mark = "L";
            else if (c->NodeType == "LookupJoin") mark = "LJ";
            else                                  mark = "?";

            if (s->Connections.size() == 1) {
                c->_Builder
                << "  <path d='M" << Config.HeaderLeft + x + INTERNAL_WIDTH << ',' << y + GAP_Y + INTERNAL_GAP_Y + INTERNAL_HEIGHT << "l-" << CONN_SIZE << ",0"
                << "l0,-" << CONN_SIZE << "l" << CONN_SIZE / 2 << ",-" << CONN_ARROW << 'l' << CONN_SIZE / 2 << ',' << CONN_ARROW
                << "z' class='" << (c->Blocks ? "conn blocks": "conn") << "' />" << Endl;
            } else {
                c->_Builder
                << "  <path d='M" << Config.HeaderLeft + x + INTERNAL_WIDTH << ',' << y + GAP_Y + INTERNAL_GAP_Y + INTERNAL_HEIGHT << "l-" << CONN_SIZE << ",0"
                << "l-" << CONN_ARROW << ",-" << CONN_SIZE / 2 << 'l' << CONN_ARROW << ",-" << CONN_SIZE / 2 << 'l' << CONN_SIZE << ",0"
                << "z' class='" << (c->Blocks ? "conn blocks": "conn") << "' />" << Endl;
            }

            c->_Builder
                << SvgText(Config.HeaderLeft + x + INTERNAL_WIDTH - CONN_SIZE / 2, y + GAP_Y + INTERNAL_GAP_Y + INTERNAL_TEXT_HEIGHT  + (INTERNAL_HEIGHT - INTERNAL_TEXT_HEIGHT) / 2, "conn", mark);

            c->_Builder << "</g>" << Endl;

            if (c->InputBytes) {

                s->_Builder << "<g data-group='g" << c->GroupId << "' class='selectable'><title>Input</title>" << Endl;

                TStringBuilder tooltip;
                auto textSum = FormatTooltip(tooltip, "Input", c->InputBytes.get(), FormatBytes);
                if (c->InputLocalBytes && c->InputBytes->Details.Sum) {
                    tooltip << ", Local " << c->InputLocalBytes * 100 / c->InputBytes->Details.Sum << "%, \u2211" << FormatBytes(c->InputLocalBytes);
                }
                if (c->InputRows) {
                    FormatTooltip(tooltip, ", Rows", c->InputRows.get(), FormatInteger);
                    if (c->InputRows->Details.Sum) {
                        tooltip << ", Width " << FormatBytes(c->InputBytes->Details.Sum / c->InputRows->Details.Sum);
                    }
                }
                if (c->InputChunks) {
                    tooltip << ", Chunks \u2211" << FormatInteger(c->InputChunks);
                    if (c->InputChunkSize) {
                        tooltip << " ~ " << FormatBytes(c->InputChunkSize->Value);
                    }
                }
                PrintStageSummary(s->_Builder, Config.SummaryLeft, Config.SummaryWidth, y0, INTERNAL_HEIGHT, c->InputBytes, Config.Palette.InputMedium, Config.Palette.InputLight, textSum, tooltip, s->Tasks, "#icon_input", Config.Palette.InputLight, "0.0325 0.0325", true, ToString(c->FromStage->PhysicalStageId), c->InputLocalBytes, c->InputChunkSize);

                auto d = c->InputBytes->MaxTime - c->InputBytes->MinTime;
                TStringBuilder title;
                title << "Input";
                if (d) {
                    title << " " << FormatBytes(c->InputBytes->Details.Sum * 1000 / d) << "/s";
                    if (c->InputRows) {
                        title << ", Rows " << FormatInteger(c->InputRows->Details.Sum * 1000 / d) << "/s";
                    }
                }

                TStringBuilder connCanvas;

                PrintTimeline(s->_Builder, connCanvas, title, c->InputBytes->FirstMessage, c->InputBytes->LastMessage, px, y0, pw, INTERNAL_HEIGHT, Config.Palette.InputMedium, true);

                if (!c->InputBytes->WaitTime.Deriv.empty()) {
                    PrintWaitTime(s->_Builder, c->InputBytes, px, y0, pw, INTERNAL_HEIGHT, Config.Palette.InputLight);
                }

                s->_Builder << connCanvas;

                if (!c->InputBytes->History.Deriv.empty()) {
                    PrintDeriv(s->_Builder, c->InputBytes->History, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.InputDark);
                }

                y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;

                s->_Builder << "</g>" << Endl;
            }
        }

        if (s->IngressBytes) {
            TStringBuilder& builder = s->_Builder;
            builder << "<g data-group='g" << (s->IngressConnection ? s->IngressConnection->GroupId : s->GroupId) << "' class='selectable'><title>Ingress</title>" << Endl;

            TStringBuilder tooltip;
            auto textSum = FormatTooltip(tooltip, "Ingress", s->IngressBytes.get(), FormatBytes);
            if (s->IngressRows) {
                FormatTooltip(tooltip, ", Rows", s->IngressRows.get(), FormatInteger);
                if (s->IngressRows->Details.Sum) {
                    tooltip << ", Width " << FormatBytes(s->IngressBytes->Details.Sum / s->IngressRows->Details.Sum);
                }
            }
            PrintStageSummary(builder, Config.SummaryLeft, Config.SummaryWidth, y0, INTERNAL_HEIGHT, s->IngressBytes, Config.Palette.IngressMedium, Config.Palette.IngressLight, textSum, tooltip, s->Tasks, "#icon_ingress", Config.Palette.IngressMedium, "0.9 0.9", s->IngressConnection);

            auto d = s->IngressBytes->MaxTime - s->IngressBytes->MinTime;
            TStringBuilder title;
            title << "Ingress";
            if (d) {
                title << " " << FormatBytes(s->IngressBytes->Details.Sum * 1000 / d) << "/s";
                if (s->IngressRows) {
                    title << ", Rows " << FormatInteger(s->IngressRows->Details.Sum / d) << "/s";
                }
            }

            TStringBuilder connCanvas;

            PrintTimeline(builder, connCanvas, title, s->IngressBytes->FirstMessage, s->IngressBytes->LastMessage, px, y0, pw, INTERNAL_HEIGHT, Config.Palette.IngressMedium, s->IngressConnection);

            if (!s->IngressBytes->WaitTime.Deriv.empty()) {
                PrintWaitTime(builder, s->IngressBytes, px, y0, pw, INTERNAL_HEIGHT, Config.Palette.IngressLight);
            }

            builder << connCanvas;

            if (!s->IngressBytes->History.Deriv.empty()) {
                PrintDeriv(builder, s->IngressBytes->History, px, y0, pw, INTERNAL_HEIGHT, "", Config.Palette.IngressDark);
            }

            builder << "</g>" << Endl;
            y0 += INTERNAL_HEIGHT + INTERNAL_GAP_Y;
        }
        s->_Builder << "</g>" << Endl;
    }

    offsetY += Height;
}

void TPlan::PrintStage(TStringBuilder& builder, std::shared_ptr<TStage>& stage, TConnection* c) {

    if (stage->Connections.size() > 1) {
        builder << SvgRect(Config.HeaderLeft + stage->IndentX, GAP_Y, INDENT_X, "100%", "stage");
    }

    builder << "<svg class='slimable' data-stage='inner " << stage->PhysicalStageId << "' data-height='" << stage->Height << "' width='" << Config.Width << "' height='" << stage->Height << "' x='0' y='" << GAP_Y << "'>" << Endl;
    builder << stage->_Builder;
    builder << "</svg>" << Endl;

    auto y = stage->Height + GAP_Y;
    for (auto c : stage->Connections) {
        if (c->CteConnection) {
            builder << "<svg data-stage='outer cte' data-height='" << GAP_Y + INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2 << "' width='" << Config.Width << "' height='" << GAP_Y + INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2 << "' x='0' y='" << y << "'>" << Endl;
            builder << "<svg data-stage='inner cte' data-height='" << INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2 << "' width='" << Config.Width << "' height='" << INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2 << "' x='0' y='" << GAP_Y << "'>" << Endl;
            builder << c->_CteBuilder;
            builder << "</svg>" << Endl;
            builder << c->_Builder;
            builder << "</svg>" << Endl;
            y += INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2 + GAP_Y;
        } else {
            builder << "<svg data-stage='outer " << c->FromStage->PhysicalStageId << "' data-height='" << c->FromStage->IndentY - c->FromStage->OffsetY << "' width='" << Config.Width << "' height='" << c->FromStage->IndentY - c->FromStage->OffsetY << "' x='0' y='" << y << "'>" << Endl;
            PrintStage(builder, c->FromStage, c.get());
            builder << "</svg>" << Endl;
            y += (c->FromStage->IndentY - c->FromStage->OffsetY); // GAP_Y included
        }
    }

    if (c) {
        builder << c->_Builder;
    }
}

void TPlan::PrintSvg(TStringBuilder& builder) {
    auto headerHeight = GAP_Y + TIME_HEIGHT + INTERNAL_HEIGHT;
    builder << "<svg data-height='" << Height + headerHeight << "' width='" << Config.Width << "' height='" << Height + headerHeight << "' x='0' y='" << OffsetY << "'>" << Endl;
    builder << _Builder;
    if (!Stages.empty()) {
        auto& stage = Stages.front();
        builder << "<svg data-stage='outer " << stage->PhysicalStageId << "' data-height='" << stage->IndentY - stage->OffsetY << "' width='" << Config.Width << "' height='" << stage->IndentY - stage->OffsetY << "' x='0' y='" << headerHeight << "'>" << Endl;
        PrintStage(builder, stage, nullptr);
        builder << "</svg>" << Endl;
    }
    builder << "</svg>" << Endl;
}

TColorPalette::TColorPalette() {
    StageMain     = "var(--stage-main, #F2F2F2)";
    StageClone    = "var(--stage-clone, #D9D9D9)";
    StageText     = "var(--stage-text, #262626)";
    StageTextHighlight = "var(--stage-texthl, #FC2824)";
    StageGrid     = "var(--stage-grid, #B2B2B2)";

    IngressDark   = "var(--ingress-dark, #384F50)";
    IngressMedium = "var(--ingress-medium, #466364)";
    IngressLight  = "var(--ingress-light, #5A8183)";
    InputDark     = "var(--input-dark, #466364)";
    InputMedium   = "var(--input-medium, #5A8183)";
    InputLight    = "var(--input-light, #7CA3A5)";

    EgressDark    = "var(--egress-dark, #2D486C)";
    EgressMedium  = "var(--egress-medium, #3C6090)";
    EgressLight   = "var(--egress-light, #4B78B4)";
    OutputDark    = "var(--output-dark, #41689C)";
    OutputMedium  = "var(--output-medium, #5781B9)";
    OutputLight   = "var(--output-light, #6F93C3)";

    MemMedium     = "var(--mem-medium, #7E4E5B)";
    MemLight      = "var(--mem-light, #AA7785)";
    CpuMedium     = "var(--cpu-medium, #A36D7B)";
    CpuLight      = "var(--cpu-light, #B78C98)";

    ConnectionFill= "var(--conn-fill, #BFBFBF)";
    ConnectionLine= "var(--conn-line, #BFBFBF)";
    ConnectionText= "var(--conn-text, #393939)";
    MinMaxLine    = "var(--minmax-line, #FFDB4D)";
    TextLight     = "var(--text-light, #FFFFFF)";
    TextInverted  = "var(--text-inv, #FFFFFF)";
    TextSummary   = "var(--text-summary, #262626)";

    SpillingBytesDark   = "var(--spill-dark, #CC9700)";
    SpillingBytesMedium = "var(--spill-medium, #FFC522)";
    SpillingBytesLight  = "var(--spill-light, #FFD766)";
    SpillingTimeDark    = "var(--spill-dark, #CC9700)";
    SpillingTimeMedium  = "var(--spill-medium, #FFC522)";
    SpillingTimeLight   = "var(--spill-light, #FFD766)";

    BlockMedium = "var(--block-medium, #EACB68)";
}

TPlanViewConfig::TPlanViewConfig() {
    Width = 1280;
    HeaderLeft = 0;
    HeaderWidth = 300 - INTERNAL_GAP_X;
    OperatorLeft = HeaderLeft + HeaderWidth + GAP_X;
    OperatorWidth = 64;
    TaskLeft = OperatorLeft + OperatorWidth + GAP_X;
    TaskWidth = 24;
    SummaryLeft = TaskLeft + TaskWidth + GAP_X;
    SummaryWidth = 200;
    TimelineLeft = SummaryLeft + SummaryWidth + GAP_X;
    TimelineWidth = Width - TimelineLeft;
}


void TPlanVisualizer::LoadPlans(const TString& plans, bool simplified) {
    Config.Simplified = simplified;
    NJson::TJsonReaderConfig jsonConfig;
    NJson::TJsonValue jsonNode;
    if (NJson::ReadJsonTree(plans, &jsonConfig, &jsonNode)) {
        if (auto* topNode = jsonNode.GetValueByPath(simplified ? "SimplifiedPlan" : "Plan")) {
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
    ui32 timelineDelta = (UpdateTime > MaxTime) ? std::min<ui32>(Config.TimelineWidth * (UpdateTime - MaxTime) / UpdateTime, Config.TimelineWidth / 10) : 0;

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

    auto x = Config.TimelineLeft + INTERNAL_GAP_X;
    auto w = Config.TimelineWidth - timelineDelta - INTERNAL_GAP_X * 2;

    for (auto plan : Plans) {
        for (ui64 t = 0; t <= maxSec; t += deltaSec) {
            ui64 x1 = t * w * 1000 / MaxTime;
            auto timeLabel = Sprintf("%lu:%.2lu", t / 60, t % 60);
            plan->_Builder << SvgTextS(x + x1 + 2, GAP_Y + INTERNAL_GAP_Y + (INTERNAL_HEIGHT + INTERNAL_TEXT_HEIGHT) / 2, timeLabel);
        }
        plan->PrepareSvg(MaxTime, timelineDelta, offsetY);
    }

    for (auto plan : Plans) {
        plan->PrintSvg(background);
    }

    svg << "<svg width='" << Config.Width << "' height='" << offsetY << "' xmlns='http://www.w3.org/2000/svg'>" << Endl;
    svg << "<clipPath id='clipTextPath'><rect x='" << Config.HeaderLeft
        << "' y='0' width='" << Config.HeaderWidth << "' height='" << offsetY << "'/>"
        << "</clipPath>" << Endl;
    svg << R"(
<defs>
  <g id='icon_memory'>
    <path fill-rule='evenodd' clip-rule='evenodd' d='M5 3C3.89543 3 3 3.89543 3 5H1V7H3V9H1V11H3V13H1V15H3V17H1V19H3C3 20.1046 3.89543 21 5 21H9C10.1046 21 11 20.1046 11 19H13C13 20.1046 13.8954 21 15 21H19C20.1046 21 21 20.1046 21 19H23V17H21V15H23V13H21V11H23V9H21V7H23V5H21C21 3.89543 20.1046 3 19 3H15C13.8954 3 13 3.89543 13 5H11C11 3.89543 10.1046 3 9 3H5ZM11 7V9H13V7H11ZM11 11V13H13V11H11ZM11 15V17H13V15H11ZM5 5H9V19H5V5ZM15 5H19V19H15V5Z' />
  </g>

  <g id='icon_cpu'>
    <path d='M13.9802 7.75H10.0102C8.76023 7.75 7.74023 8.76 7.74023 10.02V13.99C7.74023 15.24 8.75023 16.26 10.0102 16.26H13.9802C15.2302 16.26 16.2502 15.25 16.2502 13.99V10.02C16.2502 8.76 15.2402 7.75 13.9802 7.75ZM13.5002 12.98L12.6102 14.53C12.4802 14.76 12.2402 14.88 12.0002 14.88C11.8802 14.88 11.7502 14.85 11.6502 14.79C11.3102 14.6 11.1902 14.17 11.3902 13.83L12.0302 12.72H11.4702C11.0202 12.72 10.6502 12.52 10.4502 12.18C10.2502 11.84 10.2702 11.42 10.5002 11.03L11.3902 9.48C11.5902 9.14 12.0202 9.03 12.3502 9.22C12.6902 9.41 12.8102 9.84 12.6102 10.18L11.9702 11.29H12.5302C12.9802 11.29 13.3502 11.49 13.5502 11.83C13.7502 12.17 13.7302 12.59 13.5002 12.98Z' />
    <path d='M21.25 12.75C21.67 12.75 22 12.41 22 12C22 11.58 21.67 11.25 21.25 11.25H20V9.05H21.25C21.67 9.05 22 8.72 22 8.3C22 7.89 21.67 7.55 21.25 7.55H19.77C19.29 5.96 18.04 4.71 16.45 4.23V2.75C16.45 2.34 16.11 2 15.7 2C15.29 2 14.95 2.34 14.95 2.75V4H12.75V2.75C12.75 2.34 12.41 2 12 2C11.59 2 11.25 2.34 11.25 2.75V4H9.06V2.75C9.06 2.34 8.72 2 8.31 2C7.89 2 7.56 2.34 7.56 2.75V4.23C5.96 4.71 4.71 5.96 4.23 7.55H2.75C2.34 7.55 2 7.89 2 8.3C2 8.72 2.34 9.05 2.75 9.05H4V11.25H2.75C2.34 11.25 2 11.58 2 12C2 12.41 2.34 12.75 2.75 12.75H4V14.95H2.75C2.34 14.95 2 15.28 2 15.7C2 16.11 2.34 16.45 2.75 16.45H4.23C4.7 18.04 5.96 19.29 7.56 19.77V21.25C7.56 21.66 7.89 22 8.31 22C8.72 22 9.06 21.66 9.06 21.25V20H11.26V21.25C11.26 21.66 11.59 22 12.01 22C12.42 22 12.76 21.66 12.76 21.25V20H14.95V21.25C14.95 21.66 15.29 22 15.7 22C16.11 22 16.45 21.66 16.45 21.25V19.77C18.04 19.29 19.29 18.04 19.77 16.45H21.25C21.67 16.45 22 16.11 22 15.7C22 15.28 21.67 14.95 21.25 14.95H20V12.75H21.25ZM17.26 14.26C17.26 15.91 15.91 17.26 14.26 17.26H9.74C8.09 17.26 6.74 15.91 6.74 14.26V9.74C6.74 8.09 8.09 6.74 9.74 6.74H14.26C15.91 6.74 17.26 8.09 17.26 9.74V14.26Z' />
 </g>

  <g id='icon_ingress'>
    <path d='M3 6L6 6L6 12L10 12L10 6L13 6V5L8 0L3 5L3 6Z' />
    <path d='M2 16L14 16V14L2 14V16Z' />
  </g>

  <g id='icon_egress'>
    <path d='M13 9H10V16H6V9L3 9V8L8 3L13 8V9Z' />
    <path d='M14 2H2V0H14V2Z' />
  </g>

  <g id='icon_input'>
    <path d="M456.49,264.459L295.206,103.183l56.572-56.572c1.084-1.08,1.456-2.696,0.948-4.14c-0.504-1.448-1.792-2.48-3.316-2.656
        L4.654,0.027c-1.22-0.144-2.428,0.284-3.292,1.148c-0.864,0.86-1.284,2.068-1.144,3.28L40.01,349.208
        c0.176,1.52,1.208,2.812,2.656,3.316c1.448,0.508,3.068,0.14,4.144-0.948l56.18-56.18L264.274,456.68
        c0.748,0.748,1.764,1.172,2.828,1.172c1.06,0,2.076-0.424,2.828-1.172l186.556-186.568
        C458.05,268.555,458.05,266.027,456.49,264.459z"/>
  </g>

  <g id='icon_output'>
	<path d="M456.494,1.168c-0.864-0.86-2.064-1.284-3.288-1.14l-344.76,39.78c-1.524,0.18-2.812,1.216-3.32,2.656
        c-0.504,1.456-0.132,3.068,0.948,4.148l56.572,56.576L1.362,264.464c-1.564,1.564-1.564,4.092,0,5.656l186.56,186.556
        c0.78,0.784,1.804,1.172,2.828,1.172c1.024,0,2.048-0.388,2.828-1.172l161.28-161.28l56.18,56.18
        c1.084,1.088,2.7,1.452,4.144,0.952c1.448-0.504,2.48-1.796,2.656-3.32L457.63,4.456C457.774,3.248,457.354,2.036,456.494,1.168z"/>
  </g>

  <g id='icon_plus'>
    <path d="M328 544h152v152c0 4.4 3.6 8 8 8h48c4.4 0 8-3.6 8-8V544h152c4.4 0 8-3.6 8-8v-48c0-4.4-3.6-8-8-8H544V328c0-4.4-3.6-8-8-8h-48c-4.4 0-8 3.6-8 8v152H328c-4.4 0-8 3.6-8 8v48c0 4.4 3.6 8 8 8"/>
    <path d="M880 112H144c-17.7 0-32 14.3-32 32v736c0 17.7 14.3 32 32 32h736c17.7 0 32-14.3 32-32V144c0-17.7-14.3-32-32-32m-40 728H184V184h656z"/>
  </g>

  <g id='icon_minus'>
    <path d="M328 544h368c4.4 0 8-3.6 8-8v-48c0-4.4-3.6-8-8-8H328c-4.4 0-8 3.6-8 8v48c0 4.4 3.6 8 8 8"/>
    <path d="M880 112H144c-17.7 0-32 14.3-32 32v736c0 17.7 14.3 32 32 32h736c17.7 0 32-14.3 32-32V144c0-17.7-14.3-32-32-32m-40 728H184V184h656z"/>
  </g>

  <g id='icon_arrowup'>
    <path d="M334 624h46.9c10.2 0 19.9-4.9 25.9-13.2L512 465.4l105.2 145.4c6 8.3 15.6 13.2 25.9 13.2H690c6.5 0 10.3-7.4 6.5-12.7l-178-246a7.95 7.95 0 0 0-12.9 0l-178 246A7.96 7.96 0 0 0 334 624"/>
    <path d="M880 112H144c-17.7 0-32 14.3-32 32v736c0 17.7 14.3 32 32 32h736c17.7 0 32-14.3 32-32V144c0-17.7-14.3-32-32-32m-40 728H184V184h656z"/>
  </g>

  <g id='icon_arrowdn'>
    <path d="M505.5 658.7c3.2 4.4 9.7 4.4 12.9 0l178-246c3.8-5.3 0-12.7-6.5-12.7H643c-10.2 0-19.9 4.9-25.9 13.2L512 558.6L406.8 413.2c-6-8.3-15.6-13.2-25.9-13.2H334c-6.5 0-10.3 7.4-6.5 12.7z"/>
    <path d="M880 112H144c-17.7 0-32 14.3-32 32v736c0 17.7 14.3 32 32 32h736c17.7 0 32-14.3 32-32V144c0-17.7-14.3-32-32-32m-40 728H184V184h656z"/>
  </g>

</defs>
)";
    svg << "<style type='text/css'>" << Endl
        << "  rect.stage { stroke-width:0; fill:" << Config.Palette.StageMain << "; }" << Endl
        << "  rect.clone { stroke-width:0; fill:" << Config.Palette.StageClone << "; }" << Endl
        << "  rect.blocks { stroke-width:0; fill:" << Config.Palette.BlockMedium << "; }" << Endl
        << "  .texts { text-anchor:start; font-family:Verdana; font-size:" << INTERNAL_TEXT_HEIGHT << "px; fill:" << Config.Palette.StageText << "; }" << Endl
        << "  .textm { text-anchor:middle; font-family:Verdana; font-size:" << INTERNAL_TEXT_HEIGHT << "px; fill:" << Config.Palette.StageText << "; }" << Endl
        << "  .texte { text-anchor:end; font-family:Verdana; font-size:" << INTERNAL_TEXT_HEIGHT << "px; fill:" << Config.Palette.StageText << "; }" << Endl
        << "  .textc { text-anchor:end; dominant-baseline:middle; font-family:Verdana; font-size:" << INTERNAL_TEXT_HEIGHT << "px; fill:" << Config.Palette.StageText << "; }" << Endl
        << "  circle.stage { stroke:" << Config.Palette.StageMain << "; stroke-width:1; fill:" << Config.Palette.StageClone << "; }" << Endl
        << "  line.opdiv { stroke-width:1; stroke:" << Config.Palette.StageGrid << "; stroke-dasharray:1,2; }" << Endl
        << "  text.clipped { clip-path:url(#clipTextPath); }" << Endl
        << "  polygon.conn { stroke-width:0; fill:" << Config.Palette.ConnectionFill << "; }" << Endl
        << "  path.conn { stroke-width:1; stroke:" << Config.Palette.ConnectionLine << "; fill:" << Config.Palette.ConnectionFill << "; }" << Endl
        << "  rect.conn { stroke-width:1; stroke:" << Config.Palette.ConnectionLine << "; fill:none; }" << Endl
        << "  path.conn.blocks { stroke-width:1; stroke:" << Config.Palette.ConnectionLine << "; fill:" << Config.Palette.BlockMedium << "; }" << Endl
        << "  text.conn { text-anchor:middle; font-family:Verdana; font-size:" << INTERNAL_TEXT_HEIGHT << "px; fill:" << Config.Palette.ConnectionText << "; }" << Endl
        << "  rect.background { stroke-width:0; fill:#33FFFF; opacity:0; }" << Endl
        << "  rect.transparent { stroke-width:0; fill:#33FFFF; opacity:0; }" << Endl
        << "  g.selected circle.stage { fill:#33FFFF; }" << Endl
        << "  g.selected polygon.conn { fill:#33FFFF; }" << Endl
        << "  g.selected path.conn { stroke:#33FFFF; fill:#33FFFF; }" << Endl
        << "  g.selected rect.stage { fill:#33FFFF; }" << Endl
        << "  g.selected rect.clone { fill:#33FFFF; }" << Endl
        << "  g.selected rect.background { opacity:1; }" << Endl
        << "  g.selected path.blocks { stroke:#33FFFF; }" << Endl
        << "  g.selected circle.stage.selected { fill:#33FFFF; }" << Endl
        << "  polygon.conn.selected { fill:#33FFFF; }" << Endl
        << "  path.conn.selected { stroke:#33FFFF; fill:#33FFFF; }" << Endl
        << "  rect.stage.selected { fill:#33FFFF; }" << Endl
        << "  rect.clone.selected { fill:#33FFFF; }" << Endl
        << "  rect.background.selected { opacity:1; }" << Endl
        << "  path.blocks.selected { stroke:#33FFFF; }" << Endl
        << "  svg:not(.folded) > g > g.button .icon_plus { opacity:0; }" << Endl
        << "  svg.folded > g > g.button .icon_minus { opacity:0; }" << Endl
        << "</style>" << Endl;
    svg << R"(
<script type="text/ecmascript">
<![CDATA[
    var selectedGroup;

    function shift_nodes(node, delta) {
        while(node && node.tagName == "svg") {
            node.setAttribute("y", Number(node.getAttribute("y")) + delta);
            node = node.nextElementSibling;
        }
    }

    function resize_nodes(node, delta) {
        while(node && node.tagName == "svg") {
            node.setAttribute("height", Number(node.getAttribute("height")) + delta);
            shift_nodes(node.nextElementSibling, delta);
            node = node.parentElement;
        }
    }

    function calc_delta(node) {
        var delta = 0;
        while(node && node.tagName == "svg") {
            delta += Number(node.getAttribute("height"));
            node = node.nextElementSibling;
        }
        return delta;
    }

    function find_parent_svg(node) {
        while (node && node.tagName != "svg") {
            node = node.parentElement;
        }
        return node;
    }

    function toggle_fold(node) {
        if (node) {
            var delta = calc_delta(node.nextElementSibling);
            if (delta) {
                if (node.classList.contains("folded")) {
                    resize_nodes(node.parentElement, delta);
                    node.classList.remove("folded");
                } else {
                    resize_nodes(node.parentElement, -delta);
                    node.classList.add("folded");
                }
            }
        }
    }

    function toggle_slim_on(node) {
        if (node && node.classList.contains("slimable") && !node.classList.contains("slim")) {
            node.classList.add("slim");
            var delta = 18 - Number(node.getAttribute("height"));
            if (delta) {
                resize_nodes(node, delta);
            }
        }
    }

    function toggle_slim_off(node) {
        if (node && node.classList.contains("slim")) {
            node.classList.remove("slim");
            var delta = Number(node.getAttribute("data-height")) - Number(node.getAttribute("height"));
            if (delta) {
                resize_nodes(node, delta);
            }
        }
    }

    function expand_tree(node) {
        if (node) {
            for (var i = 0; i < node.children.length; i++) {
                var child = node.children[i];
                if (child.tagName == "svg") {
                    toggle_slim_off(child);
                    if (child.classList.contains("folded")) {
                        toggle_fold(child);
                    }
                    expand_tree(child)
                }
            }
        }
    }

    function tree_slim_on(node) {
        if (node) {
            for (var i = 0; i < node.children.length; i++) {
                var child = node.children[i];
                if (child.tagName == "svg") {
                    toggle_slim_on(child);
                    tree_slim_on(child)
                }
            }
        }
    }

    function toggle_selection(node) {
        var g = node.getAttribute("data-group");
        if (g) {
            if (selectedGroup) {
                var e = document.querySelectorAll("[data-group='" + selectedGroup + "']");
                for (var i = 0; i < e.length; i++) {
                    e[i].classList.remove("selected");
                }
            }
            if (g == selectedGroup) {
                selectedGroup = null;
            } else {
                selectedGroup = g;
                var e = document.querySelectorAll("[data-group='" + selectedGroup + "']");
                for (var i = 0; i < e.length; i++) {
                    e[i].classList.add("selected");
                    toggle_slim_off(find_parent_svg(e[i]));
                }
            }
        }
    }

    window.onload = function() {
        var node = document.querySelector(".selected");
        if (node) {
            selectedGroup = node.getAttribute("data-group");
        }
    }

    window.addEventListener("click", function(e) {
        var node = e.target;
        while (node) {
            if (node.classList.contains("button") && node.classList.contains("plus")) {
                toggle_fold(find_parent_svg(node));
                return;
            }
            if (node.classList.contains("button") && node.classList.contains("arup")) {
                toggle_slim_on(find_parent_svg(node));
                return;
            }
            if (node.classList.contains("button") && node.classList.contains("ardn")) {
                expand_tree(find_parent_svg(node));
                return;
            }
            if (node.classList.contains("button") && node.classList.contains("aruu")) {
                tree_slim_on(find_parent_svg(node));
                return;
            }
            if (node.classList.contains("selectable")) {
                toggle_slim_off(find_parent_svg(node));
                toggle_selection(node);
                return;
            }
            node = node.parentElement;
        }
	}, false)
]]>
</script>
)";
    if (timelineDelta) {
        ui32 summary3 = (Config.SummaryWidth - INTERNAL_GAP_X * 2) / 3;
        svg
        << "<g><title>" << "Last Update: " << FormatTimeMs(UpdateTime) << "</title>" << Endl
        << "  <rect x='" << Config.TimelineLeft + Config.TimelineWidth - summary3 << "' y='" << GAP_Y
        << "' width='" << summary3 << "' height='" << TIME_HEIGHT
        << "' stroke-width='0' fill='" << Config.Palette.StageTextHighlight << "'/>" << Endl
        << "  <text text-anchor='end' font-family='Verdana' font-size='" << INTERNAL_TEXT_HEIGHT << "px' fill='" << Config.Palette.TextInverted << "' x='" << Config.TimelineLeft + Config.TimelineWidth - 2
        << "' y='" << GAP_Y + INTERNAL_TEXT_HEIGHT << "'>" << FormatTimeMs(UpdateTime) << "</text>" << Endl
        << "</g>" << Endl;
    }

    svg << TString(background) << Endl;

    for (ui64 t = 0; t <= maxSec; t += deltaSec) {
        ui64 x1 = t * w * 1000 / MaxTime;
        svg
            << "<line x1='" << x + x1 << "' y1='0' x2='" << x + x1 << "' y2='" << "100%" // offsetY
            << "' stroke-width='1' stroke='" << Config.Palette.StageGrid << "' stroke-dasharray='1,2'/>" << Endl;
    }

    if (timelineDelta) {
        auto opacity = MaxTime ? std::min(0.5, static_cast<double>(UpdateTime - MaxTime) / (2 * MaxTime)) : 0.5;
        svg
        << "<rect x='" << Config.TimelineLeft + Config.TimelineWidth - timelineDelta << "' y='" << 0
        << "' width='" << timelineDelta << "' height='" << offsetY
        << "' stroke-width='0' opacity='" << opacity << "' fill='" << Config.Palette.StageTextHighlight << "'/>" << Endl;
    }

    svg << TString(canvas) << Endl;
    svg << "</svg>" << Endl;

    return svg;
}
