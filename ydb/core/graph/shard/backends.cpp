#include "log.h"
#include "backends.h"

namespace NKikimr {
namespace NGraph {

template<>
std::vector<TInstant> TMemoryBackend::Downsample<TInstant>(const std::vector<TInstant>& data, size_t maxPoints) {
    if (data.size() <= maxPoints) {
        return data;
    }
    std::vector<TInstant> result;
    double coeff = (double)maxPoints / data.size();
    result.resize(maxPoints);
    size_t ltrg = maxPoints;
    for (size_t src = 0; src < data.size(); ++src) {
        size_t trg = floor(coeff * src);
        if (trg != ltrg) {
            result[trg] = data[src]; // we expect sorted data so we practically use min() here
            ltrg = trg;
        }
    }
    return result;
}

template<>
std::vector<double> TMemoryBackend::Downsample<double>(const std::vector<double>& data, size_t maxPoints) {
    if (data.size() <= maxPoints) {
        return data;
    }
    std::vector<double> result;
    double coeff = (double)maxPoints / data.size();
    result.resize(maxPoints);
    size_t ltrg = 0;
    long cnt = 0;
    for (size_t src = 0; src < data.size(); ++src) {
        if (isnan(data[src])) {
            continue;
        }
        size_t trg = floor(coeff * src);
        if (trg != ltrg && cnt > 0) {
            if (cnt > 1) {
                result[ltrg] /= cnt;
            }
            cnt = 0;
        }
        result[trg] += data[src];
        ++cnt;
        ltrg = trg;
    }
    if (cnt > 1) {
        result[ltrg] /= cnt;
    }
    return result;
}


void TMemoryBackend::StoreMetrics(TInstant timestamp, const TMetricsData& data) {
    if (!MetricsValues.empty() && MetricsValues.back().Timestamp >= timestamp) {
        BLOG_ERROR("Invalid timestamp ordering for " << timestamp << " and " << MetricsValues.back().Timestamp);
    }
    TMetricsRecord& record = MetricsValues.emplace_back();
    record.Timestamp = timestamp;
    for (const auto& [name, value] : data.Values) {
        auto itMetricsIndex = MetricsIndex.find(name);
        if (itMetricsIndex == MetricsIndex.end()) {
            itMetricsIndex = MetricsIndex.emplace(name, MetricsIndex.size()).first;
        }
        size_t idx = itMetricsIndex->second;
        if (idx <= record.Values.size()) {
            record.Values.resize(idx + 1, NAN);
        }
        record.Values[idx] = value;
    }
    BLOG_TRACE("Stored metrics");
    if (MetricsValues.size() > 86400) {
        MetricsValues.erase(std::prev(MetricsValues.end()));
        BLOG_TRACE("Removed old metrics");
    }
}

NKikimrGraph::TEvMetricsResult TMemoryBackend::GetMetrics(const NKikimrGraph::TEvGetMetrics& get) const {
    NKikimrGraph::TEvMetricsResult result;
    auto itLeft = get.HasTimeFrom()
        ? std::lower_bound(MetricsValues.begin(), MetricsValues.end(), TInstant::Seconds(get.GetTimeFrom()))
        : MetricsValues.begin();
    auto itRight = get.HasTimeTo()
        ? std::upper_bound(itLeft, MetricsValues.end(), TInstant::Seconds(get.GetTimeTo()))
        : MetricsValues.end();
    std::vector<size_t> indexes;
    for (const TString& metric : get.GetMetrics()) {
        auto itMetricsIndex = MetricsIndex.find(metric);
        size_t idx;
        if (itMetricsIndex != MetricsIndex.end()) {
            idx = itMetricsIndex->second;
        } else {
            idx = MetricsIndex.size(); // non-existent index
        }
        indexes.push_back(idx);
    }
    std::vector<TInstant> timestamps;
    std::vector<std::vector<double>> values;
    values.resize(indexes.size());
    for (auto it = itLeft; it != itRight; ++it) {
        timestamps.push_back(it->Timestamp);
        for (size_t num = 0; num < indexes.size(); ++num) {
            size_t idx = indexes[num];
            if (idx < it->Values.size()) {
                values[num].push_back(it->Values[idx]);
            } else {
                values[num].push_back(NAN);
            }
        }
    }
    if (get.HasMaxPoints() && timestamps.size() > get.GetMaxPoints()) {
        timestamps = Downsample(timestamps, get.GetMaxPoints());
        BLOG_TRACE("GetMetrics timestamps=" << timestamps.size());
        for (std::vector<double>& values : values) {
            values = Downsample(values, get.GetMaxPoints());
            BLOG_TRACE("GetMetrics values=" << values.size());
        }
    }
    auto time = result.MutableTime();
    time->Reserve(timestamps.size());
    for (const TInstant t : timestamps) {
        time->Add(t.Seconds());
    }
    for (std::vector<double>& values : values) {
        result.AddData()->MutableValues()->Add(values.begin(), values.end());
    }
    BLOG_TRACE("GetMetrics returned " << result.TimeSize() << " points");
    return result;
}

TString TMemoryBackend::GetLogPrefix() const {
    return "MEM ";
}


} // NGraph
} // NKikimr

