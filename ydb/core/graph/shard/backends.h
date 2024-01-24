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
};

class TBaseBackend {
public:
    struct TMetricsValues {
        std::vector<TInstant> Timestamps;
        std::vector<std::vector<double>> Values;
    };

    static void NormalizeAndDownsample(TMetricsValues& values, size_t maxPoints);
    static void FillResult(TMetricsValues& values, const NKikimrGraph::TEvGetMetrics& get, NKikimrGraph::TEvMetricsResult& result);

    std::unordered_map<TString, ui64> MetricsIndex; // mapping name -> id
};

class TMemoryBackend : public TBaseBackend {
public:
    void StoreMetrics(TMetricsData&& data);
    void GetMetrics(const NKikimrGraph::TEvGetMetrics& get, NKikimrGraph::TEvMetricsResult& result) const;
    void ClearData(TInstant cutline, TInstant& newStartTimestamp);

    TString GetLogPrefix() const;

    struct TMetricsRecord {
        TInstant Timestamp;
        TSmallVec<double> Values;

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
    bool ClearData(NTabletFlatExecutor::TTransactionContext& txc, TInstant cutline, TInstant& newStartTimestamp);

    TString GetLogPrefix() const;
};

} // NGraph
} // NKikimr
