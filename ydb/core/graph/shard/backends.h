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

class TMemoryBackend {
public:
    void StoreMetrics(TMetricsData&& data);
    void GetMetrics(const NKikimrGraph::TEvGetMetrics& get, NKikimrGraph::TEvMetricsResult& result) const;
    void ClearData(TInstant cutline, TInstant& newStartTimestamp);

    template<typename ValueType>
    static std::vector<ValueType> Downsample(const std::vector<ValueType>& data, size_t maxPoints);

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

    std::unordered_map<TString, size_t> MetricsIndex; // mapping name -> id
    std::deque<TMetricsRecord> MetricsValues;
};

class TLocalBackend {
public:
    static constexpr ui64 MAX_ROWS_TO_DELETE = 1000;

    bool StoreMetrics(NTabletFlatExecutor::TTransactionContext& txc, TMetricsData&& data);
    bool GetMetrics(NTabletFlatExecutor::TTransactionContext& txc, const NKikimrGraph::TEvGetMetrics& get, NKikimrGraph::TEvMetricsResult& result) const;
    bool ClearData(NTabletFlatExecutor::TTransactionContext& txc, TInstant cutline, TInstant& newStartTimestamp);

    TString GetLogPrefix() const;

    std::unordered_map<TString, ui64> MetricsIndex; // mapping name -> id
};

} // NGraph
} // NKikimr
