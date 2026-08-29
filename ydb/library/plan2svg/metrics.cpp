#include "metrics.h"

#include "config.h"

#include <util/string/builder.h>

#include <algorithm>

namespace NPlan2Svg {

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

void TMetricHistory::Load(std::vector<ui64>& times, const NJson::TJsonValue& node, ui64 explicitMinTime, ui64 explicitMaxTime) {
    std::vector<ui64> values;

    for (const auto& subNode : node.GetArray()) {
        values.push_back(subNode.GetIntegerSafe());
    }

    if (values.size() > times.size()) {
        values.resize(times.size());
    } else while (values.size() && values.size() < times.size()) {
        values.push_back(values.back());
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

ui64 TMetricHistory::Integrate() {
    ui64 result = 0;
    for (ui32 i = 1; i < Values.size(); i++) {
        result += (Values[i - 1].second + Values[i].second) / 2 * (Values[i].first - Values[i - 1].first);
    }
    return result;
}

ui64 TMetricHistory::Average() {
    if (!AvgValue) {
        ui64 dt = Values.empty() ? 0 : Values.back().first - Values.front().first;
        AvgValue = dt ? Integrate() / dt : 0;
    }
    return AvgValue;
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

} // namespace NPlan2Svg
