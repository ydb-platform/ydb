#pragma once

#include <ydb/core/graph/api/events.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

namespace NKikimr {
namespace NGraph {

enum class EBackendType {
    Memory = 0,
    Local = 1,
    External = 2,
};

struct TMetricsData {
    TInstant Timestamp;
    std::unordered_map<TString, double> Values;
    std::unordered_map<TString, std::map<ui64, ui64>> HistogramValues;

    struct TArithmeticMetric {
        char Op = 0;
        double ValueA = 0;
        double ValueB = 0;
    };

    std::unordered_map<TString, TArithmeticMetric> ArithmeticValues;
};

struct TAggregateSettings {
    TDuration PeriodToStart;
    TDuration SampleSize = TDuration::Zero(); // clear
    TDuration MinimumStep = TDuration::Minutes(5);
    mutable TInstant StartTimestamp = {};

    bool IsItTimeToAggregate(TInstant now) const {
        return (now - StartTimestamp > PeriodToStart + MinimumStep);
    }

    TString ToString() const {
        return TStringBuilder() << "start=" << StartTimestamp.ToStringUpToSeconds() << " period=" << PeriodToStart << " sample=" << SampleSize << " min=" << MinimumStep;
    }
};

class TBaseBackend {
public:
    struct TMetricsValues {
        std::vector<TInstant> Timestamps;
        std::vector<std::vector<double>> Values; // columns

        void Clear() {
            Timestamps.clear();
            for (auto& value : Values) {
                value.clear();
            }
        }
    };

    static void NormalizeAndDownsample(TMetricsValues& values, size_t maxPoints);
    static void FillResult(TMetricsValues& values, const NKikimrGraph::TEvGetMetrics& get, NKikimrGraph::TEvMetricsResult& result);
    static double GetTimingForPercentile(double percentile, const TVector<ui64>& values, const TVector<ui64>& /*upper*/bounds, ui64 total);

    static TString GetLogPrefix() { return {}; }

    std::unordered_map<TString, ui64> MetricsIndex; // mapping name -> id
};

class TMemoryBackend : public TBaseBackend {
public:
    void StoreMetrics(TMetricsData&& data);
    void GetMetrics(const NKikimrGraph::TEvGetMetrics& get, NKikimrGraph::TEvMetricsResult& result) const;
    void ClearData(TInstant now, const TAggregateSettings& settings);
    void DownsampleData(TInstant now, const TAggregateSettings& settings);
    void AggregateData(TInstant now, const TAggregateSettings& settings);

    TString GetLogPrefix() const;

    struct TMetricsRecord {
        TInstant Timestamp;
        std::vector<double> Values; // rows

        std::strong_ordering operator <=>(const TMetricsRecord& rec) const {
            return Timestamp.GetValue() <=> rec.Timestamp.GetValue();
        }

        std::strong_ordering operator <=>(TInstant time) const {
            return Timestamp.GetValue() <=> time.GetValue();
        }
    };

    std::deque<TMetricsRecord> MetricsValues;
};

class TLocalBackend : public TBaseBackend {
public:
    static constexpr ui64 MAX_ROWS_TO_DELETE = 1000;

    bool StoreMetrics(NTabletFlatExecutor::TTransactionContext& txc, TMetricsData&& data);
    bool GetMetrics(NTabletFlatExecutor::TTransactionContext& txc, const NKikimrGraph::TEvGetMetrics& get, NKikimrGraph::TEvMetricsResult& result) const;
    bool ClearData(NTabletFlatExecutor::TTransactionContext& txc, TInstant now, const TAggregateSettings& settings);
    bool DownsampleData(NTabletFlatExecutor::TTransactionContext& txc, TInstant now, const TAggregateSettings& settings);
    bool AggregateData(NTabletFlatExecutor::TTransactionContext& txc, TInstant now, const TAggregateSettings& settings);

    TString GetLogPrefix() const;
};

} // NGraph
} // NKikimr
