#pragma once

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/string.h>

#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <optional>
#include <map>
#include <unordered_map>
#include <unordered_set>

namespace NKikimr {

namespace NDetailedMetricsTests {

// Expected sensor value
struct TExpectedSensorValue {
    enum Type {
        Undefined,
        Gauge,
        Rate,
        Hist,
    };
    Type Type = Undefined;
    ui64 Value = 0;
    std::unordered_map<ui64, ui64> Buckets;
    ui64 InfValue;

    // Merge sensor with a new value, isDelta true for value updates and false for aggregations.
    void Merge(const TExpectedSensorValue& other, bool isDelta) {
        UNIT_ASSERT(Type == other.Type || Type == Undefined);
        if (isDelta && Type == Gauge) {
            Value = other.Value;
        } else {
            Value += other.Value;
        }
        Type = other.Type;
        if (isDelta) {
            Buckets.clear();
            InfValue = 0;
        }
        for (const auto& [key, value] : other.Buckets) {
            Buckets[key] += value;
        }
        InfValue += other.InfValue;
    }
};

// A group of named sensors with expected values.
struct TExpectedSensorGroup : public std::map<TString, TExpectedSensorValue> {
    void Add(const TString& name, const TTabletSimpleCounter& counter) {
        (*this)[name].Merge({.Type = TExpectedSensorValue::Gauge, .Value = counter.Get()}, true);
    }

    void Add(const TString& name, const TTabletCumulativeCounter& counter) {
        (*this)[name].Merge({.Type = TExpectedSensorValue::Rate, .Value = counter.Get()}, true);
    }

    void AddHist(const TString& name, const std::unordered_map<ui64, ui64>& buckets, ui64 infValue = 0) {
        (*this)[name].Merge({.Type = TExpectedSensorValue::Hist, .Buckets = buckets, .InfValue = infValue}, true);
    }

    // Apply new sensor values.
    TExpectedSensorGroup& operator+=(const TExpectedSensorGroup& other) {
        for (const auto& [name, value] : other) {
            (*this)[name].Merge(value, true);
        }
        return *this;
    }

    TExpectedSensorGroup operator+(const TExpectedSensorGroup& other) const {
        TExpectedSensorGroup result(*this);
        result += other;
        return result;
    }

    // Aggregate sensor values from underlying levels.
    TExpectedSensorGroup& operator|=(const TExpectedSensorGroup& other) {
        for (const auto& [name, value] : other) {
            (*this)[name].Merge(value, false);
        }
        return *this;
    }

    TExpectedSensorGroup operator|(const TExpectedSensorGroup& other) const {
        TExpectedSensorGroup result(*this);
        result |= other;
        return result;
    }
};

// Normalize JSON to be well formatted with all keys sorted. Removes sensors with labels matching pairs from removeSensorsByLabels.
TString NormalizeJson(const TString& jsonString,
    const std::unordered_map<TString, std::unordered_set<TString>>& removeSensorsByLabels = {});

// Create and initialize the Tablet Counters Aggregator actor.
NActors::TActorId InitializeTabletCountersAggregator(NActors::TTestBasicRuntime& runtime,
    bool forFollowers,
    bool enableDetailedMetrics = true); // The value of EnableDetailedMetrics feature flag

// Send low level metrics to the Tablet Counters Aggregator for the specified DataShard tablet.
TExpectedSensorGroup SendDataShardMetrics(NActors::TTestBasicRuntime& runtime,
    const NActors::TActorId& aggregatorId,
    const NActors::TActorId& edgeActorId,
    ui64 tabletId,
    ui32 followerId,
    ui32 cpuLoadPercentage,
    TEvTabletCounters::TTableMetricsConfig* tableMetricsConfig = nullptr,
    std::optional<ui64> metricsOffset = {});

// Create a group of DataShard sensors with default values.
TExpectedSensorGroup EmptyDataShardMetrics();

// Send a message to the Tablet Counters Aggregator to forget the specified Data Shard tablet.
void SendForgetDataShardTablet(NActors::TTestBasicRuntime& runtime,
    const NActors::TActorId& aggregatorId,
    const NActors::TActorId& edgeActorId,
    ui64 tabletId,
    ui32 followerId);

} // namespace NDetailedMetricsTests

} // namespace NKikimr
