#pragma once

#include <ydb/core/graph/api/events.h>

namespace NKikimr {
namespace NGraph {

enum class EBackendType {
    EMemory,
    ELocal,
    EExternal,
};

struct TMetricsData {
    std::unordered_map<TString, double> Values;
};

class TMemoryBackend {
public:
    void StoreMetrics(TInstant timestamp, const TMetricsData& data);
    NKikimrGraph::TEvMetricsResult GetMetrics(const NKikimrGraph::TEvGetMetrics& get) const;

    template<typename ValueType>
    static std::vector<ValueType> Downsample(const std::vector<ValueType>& data, size_t maxPoints);

private:
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

} // NGraph
} // NKikimr
