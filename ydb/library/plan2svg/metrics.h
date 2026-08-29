#pragma once

#include <library/cpp/json/json_reader.h>

#include <util/generic/string.h>
#include <util/system/types.h>

#include <memory>
#include <vector>

namespace NPlan2Svg {

class TSummaryMetric {

public:
    ui64 Value = 0;
    ui32 Count = 0;
    ui64 Min = 0;
    ui64 Max = 0;

    void Add(ui64 value) {
        if (Count) {
            Min = std::min(Min, value);
            Max = std::max(Max, value);
        } else {
            Min = value;
            Max = value;
        }
        Value += value;
        Count++;
    }

    ui64 Average() {
        return Count ? (Value / Count) : 0;
    }
};

struct TAggregation {
    ui64 Min = 0;
    ui64 Max = 0;
    ui64 Avg = 0;
    ui64 Sum = 0;
    ui32 Count = 0;

    TAggregation() {}
    TAggregation(ui64 value) : Min(value), Max(value), Avg(value), Sum(value), Count(1) {}
    bool Load(const NJson::TJsonValue& node);
};

struct TMetricHistory {
    std::vector<std::pair<ui64, ui64>> Deriv;
    ui64 MaxDeriv = 0;
    std::vector<std::pair<ui64, ui64>> Values;
    ui64 MaxValue = 0;
    ui64 MinTime = 0;
    ui64 MaxTime = 0;
    ui64 AvgValue = 0;

    void Load(const NJson::TJsonValue& node, ui64 explicitMinTime, ui64 explicitMaxTime); // time + value
    void Load(std::vector<ui64>& times, const NJson::TJsonValue& node, ui64 explicitMinTime, ui64 explicitMaxTime); // value only
    void Load(std::vector<ui64>& times, std::vector<ui64>& values, ui64 explicitMinTime, ui64 explicitMaxTime); // explicit
    ui64 Integrate();
    ui64 Average();
};

struct TSingleMetric {
    TSingleMetric(std::shared_ptr<TSummaryMetric> summary, const NJson::TJsonValue& node,
        ui64 minTime = 0, ui64 maxTime = 0,
        const NJson::TJsonValue* firstMessageNode = nullptr,
        const NJson::TJsonValue* lastMessageNode = nullptr,
        const NJson::TJsonValue* waitTimeUsNode = nullptr);
    TSingleMetric(std::shared_ptr<TSummaryMetric> summary, ui64 value);
    TSingleMetric(std::shared_ptr<TSummaryMetric> summary);

    std::shared_ptr<TSummaryMetric> Summary;
    TAggregation Details;

    TMetricHistory History;
    TMetricHistory WaitTime;
    ui64 MinTime = 0;
    ui64 MaxTime = 0;
    TAggregation FirstMessage;
    TAggregation LastMessage;
    bool MinMaxDistribution = true;
};

struct TScalarMetric {
    TScalarMetric(std::shared_ptr<TSummaryMetric> summary, ui64 value);

    std::shared_ptr<TSummaryMetric> Summary;
    ui64 Value = 0;
};

struct TMutableMetric : public TMetricHistory {
    TMutableMetric(const TString& title, bool isLine = false) : Title(title), IsLine(isLine) {}
    const TString Title;
    const bool IsLine;
    ui64 DisplayMaxValue = 0;
};

void UpdateMin(ui64& m, ui64 v);
void UpdateMax(ui64& m, ui64 v);

} // namespace NPlan2Svg
