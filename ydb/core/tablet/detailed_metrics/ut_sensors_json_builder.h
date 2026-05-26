#pragma once

#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>

#include <unordered_map>

namespace NKikimr {

namespace NDetailedMetricsTests {

/**
 * The builder for an array of JSON sensors, potentially nested.
 */
class TSensorsJsonBuilder {
public:
    /**
     * Start the building process for a new JSON of sensors.
     *
     * @return The corresponding instance of the builder to use
     */
    static TSensorsJsonBuilder Start() {
        return TSensorsJsonBuilder();
    }

    /**
     * Finalize the building process and create the final JSON of sensors.
     *
     * @return The corresponding JSON of sensors
     */
    TString BuildJson() {
        Y_ABORT_UNLESS(ParentBuilder == nullptr, "The sensors JSON should be created at the root level");
        return NJson::WriteJson(RootJson);
    }

    /**
     * Start a new nested level of sensors with the given label.
     *
     * @param[in] labelKey The key for the nested label
     * @param[in] labelValue The value for the nested label
     *
     * @return The corresponding instance of the builder to use
     */
    TSensorsJsonBuilder StartNested(const TString& labelKey, const TString& labelValue) {
        return TSensorsJsonBuilder(*this, labelKey, labelValue);
    }

    /**
     * Finish the current level of sensors and return back to the parent level.
     *
     * @return The corresponding instance of the builder to use
     */
    TSensorsJsonBuilder& EndNested() {
        Y_ABORT_UNLESS(ParentBuilder != nullptr, "Unable to finish the root level of sensors");
        return *ParentBuilder;
    }

    /**
     * Add a new GAUGE sensor at the current level of sensors.
     *
     * @param[in] name The name of the GAUGE sensor to add
     * @param[in] value The value of the new GAUGE sensor
     *
     * @return The corresponding instance of the builder to use
     */
    TSensorsJsonBuilder& AddGauge(const TString& name, ui64 value) {
        AddSimpleSensor(TSensorKind::GAUGE, name, value);
        return *this;
    }

    /**
     * Add a new RATE sensor at the current level of sensors.
     *
     * @param[in] name The name of the RATE sensor to add
     * @param[in] value The value of the new RATE sensor
     *
     * @return The corresponding instance of the builder to use
     */
    TSensorsJsonBuilder& AddRate(const TString& name, ui64 value) {
        AddSimpleSensor(TSensorKind::RATE, name, value);
        return *this;
    }

    /**
     * Add a new HIST sensor for the CPU usage at the current level of sensors.
     *
     * @warning This function creates buckets with the 10% step (0%, 10% ... 100%).
     *          The keys in the specified buckets parameter must use one of these
     *          values. Unspecified buckets are assumed to have zero values.
     *
     * @param[in] name The name of the HIST sensor to add
     * @param[in] buckets The list of buckets for the new HIST sensor
     * @param[in] infValue The value for the infinity bucket
     *
     * @return The corresponding instance of the builder to use
     */
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

private:
    /**
     * The constructor for the root level builder.
     */
    TSensorsJsonBuilder()
        : ParentBuilder(nullptr)
        , RootJson(NJson::JSON_MAP)
        , SensorsJson(RootJson.InsertValue("sensors", NJson::JSON_ARRAY)) {
    }

    /**
     * The constructor for the nested level builder.
     *
     * @param[in] parentBuilder The builder for the parent level of sensors
     * @param[in] nestedLabelKey The key for the nested label for this level
     * @param[in] nestedLabelValue The value for the nestd label for this level
     */
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

    /**
     * Add a new simple sensor (RATE or GAUGE) at the current level of sensors.
     *
     * @param[in] kind The kind of the sensor to add
     * @param[in] name The name of the sensor to add
     * @param[in] value The value of the new sensor
     */
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

    /**
     * The holder for all sensor kinds.
     */
    class TSensorKind {
    public:
        static constexpr const char* RATE = "RATE";
        static constexpr const char* GAUGE = "GAUGE";
        static constexpr const char* HIST = "HIST";
    };

    /**
     * The parent JSON builder (used for nested arrays of sensors).
     */
    TSensorsJsonBuilder* ParentBuilder;

    /**
     * The holder for the root level JSON value (populated only for the root builder).
     */
    NJson::TJsonValue RootJson;

    /**
     * The JSON value, which accumulates sensors for all levels.
     */
    NJson::TJsonValue& SensorsJson;

    /**
     * The full set of labels, which should be assigned to all sensors at this level.
     */
    std::unordered_map<TString, TString> Labels;
};

} // namespace NDetailedMetricsTests

} // namespace NKikimr
