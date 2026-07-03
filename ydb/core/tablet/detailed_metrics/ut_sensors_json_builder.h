#pragma once

#include "ut_helpers.h"

#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>

#include <unordered_map>

namespace NKikimr {

namespace NDetailedMetricsTests {

// JSON sensors array builder.
class TSensorsJsonBuilder {
public:
    static TSensorsJsonBuilder Start() {
        return TSensorsJsonBuilder();
    }

    // Finalize the building process and create the final JSON of sensors.
    TString BuildJson() {
        Y_ABORT_UNLESS(ParentBuilder == nullptr, "The sensors JSON should be created at the root level");
        return NJson::WriteJson(RootJson);
    }

    // Start a new nested level of sensors with the given label.
    TSensorsJsonBuilder StartNested(const TString& labelKey, const TString& labelValue) {
        return TSensorsJsonBuilder(*this, labelKey, labelValue);
    }

    // Finish the current level of sensors and return back to the parent level.
    TSensorsJsonBuilder& EndNested() {
        Y_ABORT_UNLESS(ParentBuilder != nullptr, "Unable to finish the root level of sensors");
        return *ParentBuilder;
    }

    // Add a new GAUGE sensor at the current level of sensors.
    TSensorsJsonBuilder& AddGauge(const TString& name, ui64 value) {
        AddSimpleSensor(TSensorKind::GAUGE, name, value);
        return *this;
    }

    // Add a new RATE sensor at the current level of sensors.
    TSensorsJsonBuilder& AddRate(const TString& name, ui64 value) {
        AddSimpleSensor(TSensorKind::RATE, name, value);
        return *this;
    }

    // Add a new HIST sensor for the CPU usage at the current level of sensors.
    TSensorsJsonBuilder& AddCpuHistogram(
        const TString& name,
        const std::unordered_map<ui64, ui64>& buckets,
        ui64 infValue = 0
    ) {
        auto &record = SensorsJson.AppendValue(NJson::JSON_MAP);
        record["kind"] = TSensorKind::HIST;

        auto& histRecord = record.InsertValue("hist", NJson::JSON_MAP);
        histRecord["inf"] = infValue;

        auto& boundsRecord = histRecord.InsertValue("bounds", NJson::JSON_ARRAY);
        auto& bucketsRecord = histRecord.InsertValue("buckets", NJson::JSON_ARRAY);

        // First, copy the values into the full list of buckets to assign defaults
        // and verify all bucket keys
        std::unordered_map<ui64, ui64> fullBuckets = {
            {0, 0},
            {10, 0},
            {20, 0},
            {30, 0},
            {40, 0},
            {50, 0},
            {60, 0},
            {70, 0},
            {80, 0},
            {90, 0},
            {100, 0},
        };

        for (auto [key, value] : buckets) {
            Y_ABORT_UNLESS(fullBuckets.contains(key), "Invalid bucket key %" PRIu64, key);
            fullBuckets[key] = value;
        }

        // Second, copy the bucket boundaries and values into the JSON values
        for (ui64 i = 0; i <= 100; i += 10) {
            boundsRecord.AppendValue(i);
            bucketsRecord.AppendValue(fullBuckets.at(i));
        }

        auto& labelsRecord = record.InsertValue("labels", NJson::JSON_MAP);
        labelsRecord["name"] = name;

        for (const auto& [key, value] : Labels ) {
            labelsRecord[key] = value;
        }

        return *this;
    }

    // Add a group of sensors at the current level.
    TSensorsJsonBuilder& AddGroup(const TExpectedSensorGroup& group) {
        for (const auto& [name, item] : group) {
            switch (item.Type) {
                case TExpectedSensorValue::Undefined:
                    Y_ABORT("Undefined sensor type");
                case TExpectedSensorValue::Gauge:
                    AddGauge(name, item.Value);
                    break;
                case TExpectedSensorValue::Rate:
                    AddRate(name, item.Value);
                    break;
                case TExpectedSensorValue::Hist:
                    AddCpuHistogram(name, item.Buckets, item.InfValue);
                    break;
                }
        }
        return *this;
    }

    // Add a nested group of sensors.
    TSensorsJsonBuilder& AddNestedGroup(const TString &labelKey, const TString &labelValue, const TExpectedSensorGroup& group) {
        if (group.empty()) {
            return *this;
        }
        return StartNested(labelKey, labelValue).AddGroup(group).EndNested();
    }

    // Add a nested group of sensors using generator functor.
    template<typename Pred, typename Gen>
    TSensorsJsonBuilder& AddNestedIf(const TString &labelKey, const TString &labelValue, Pred pred, Gen gen) {
        if (pred()) {
            return gen(StartNested(labelKey, labelValue)).EndNested();
        }
        return *this;
    }

private:
    TSensorsJsonBuilder()
        : ParentBuilder(nullptr)
        , RootJson(NJson::JSON_MAP)
        , SensorsJson(RootJson.InsertValue("sensors", NJson::JSON_ARRAY)) {
    }

    // Constructor for the nested level builder.
    TSensorsJsonBuilder(
        TSensorsJsonBuilder& parentBuilder,
        const TString& nestedLabelKey,
        const TString& nestedLabelValue
    ) : ParentBuilder(&parentBuilder)
      , SensorsJson(parentBuilder.SensorsJson)
      , Labels(parentBuilder.Labels) {
        // Also include the nested label for all nested sensors
        Labels[nestedLabelKey] = nestedLabelValue;
    }

    // Add a new simple sensor (RATE or GAUGE) at the current level of sensors.
    void AddSimpleSensor(const char* kind, const TString& name, ui64 value) {
        auto& record = SensorsJson.AppendValue(NJson::JSON_MAP);

        record["kind"] = kind;
        record["value"] = value;

        auto& labelsRecord = record.InsertValue("labels", NJson::JSON_MAP);
        labelsRecord["name"] = name;

        for (const auto& [key, value] : Labels ) {
            labelsRecord[key] = value;
        }
    }

    class TSensorKind {
    public:
        static constexpr const char* RATE = "RATE";
        static constexpr const char* GAUGE = "GAUGE";
        static constexpr const char* HIST = "HIST";
    };

    TSensorsJsonBuilder* ParentBuilder;             // Parent JSON builder (used for nested arrays of sensors).
    NJson::TJsonValue RootJson;                     // Holder for the root level JSON value (populated only for the root builder).
    NJson::TJsonValue& SensorsJson;                 // The JSON value, which accumulates sensors for all levels.
    std::unordered_map<TString, TString> Labels;    // Labels for sensors at this level and below.
};

} // namespace NDetailedMetricsTests

} // namespace NKikimr
