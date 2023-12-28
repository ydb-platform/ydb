#include "log.h"
#include "backends.h"
#include "schema.h"

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

void TMemoryBackend::StoreMetrics(TMetricsData&& data) {
    if (!MetricsValues.empty() && MetricsValues.back().Timestamp >= data.Timestamp) {
        BLOG_ERROR("Invalid timestamp ordering for " << data.Timestamp << " and " << MetricsValues.back().Timestamp);
    }
    TMetricsRecord& record = MetricsValues.emplace_back();
    record.Timestamp = data.Timestamp;
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
}

void TMemoryBackend::GetMetrics(const NKikimrGraph::TEvGetMetrics& get, NKikimrGraph::TEvMetricsResult& result) const {
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
    result.Clear();
    auto time = result.MutableTime();
    time->Reserve(timestamps.size());
    for (const TInstant t : timestamps) {
        time->Add(t.Seconds());
    }
    for (std::vector<double>& values : values) {
        result.AddData()->MutableValues()->Add(values.begin(), values.end());
    }
}

void TMemoryBackend::ClearData(TInstant cutline, TInstant& newStartTimestamp) {
    auto itCutLine = std::lower_bound(MetricsValues.begin(), MetricsValues.end(), cutline);
    MetricsValues.erase(MetricsValues.begin(), itCutLine);
    if (!MetricsValues.empty()) {
        newStartTimestamp = MetricsValues.front().Timestamp;
    } else {
        newStartTimestamp = {};
    }
}

TString TMemoryBackend::GetLogPrefix() const {
    return "MEM ";
}

bool TLocalBackend::StoreMetrics(NTabletFlatExecutor::TTransactionContext& txc, TMetricsData&& data) {
    NIceDb::TNiceDb db(txc.DB);
    for (const auto& [name, value] : data.Values) {
        auto itId = MetricsIndex.find(name);
        if (itId == MetricsIndex.end()) {
            itId = MetricsIndex.emplace(name, MetricsIndex.size()).first;
            db.Table<Schema::MetricsIndex>().Key(name).Update<Schema::MetricsIndex::Id>(itId->second);
        }
        ui64 id = itId->second;
        db.Table<Schema::MetricsValues>().Key(data.Timestamp.Seconds(), id).Update<Schema::MetricsValues::Value>(value);
    }
    BLOG_TRACE("Stored metrics");
    return true;
}

bool TLocalBackend::GetMetrics(NTabletFlatExecutor::TTransactionContext& txc, const NKikimrGraph::TEvGetMetrics& get, NKikimrGraph::TEvMetricsResult& result) const {
    NIceDb::TNiceDb db(txc.DB);
    ui64 minTime = std::numeric_limits<ui64>::min();
    ui64 maxTime = std::numeric_limits<ui64>::max();
    std::unordered_map<ui64, ui64> metricIdx;
    if (get.HasTimeFrom()) {
        minTime = get.GetTimeFrom();
    }
    if (get.HasTimeTo()) {
        maxTime = get.GetTimeTo();
    }
    for (size_t nMetric = 0; nMetric < get.MetricsSize(); ++nMetric) {
        TString name = get.GetMetrics(nMetric);
        auto itMetricIdx = MetricsIndex.find(name);
        if (itMetricIdx != MetricsIndex.end()) {
            metricIdx[itMetricIdx->second] = nMetric;
        }
    }
    std::vector<TInstant> timestamps;
    std::vector<std::vector<double>> values;
    auto rowset = db.Table<Schema::MetricsValues>().GreaterOrEqual(minTime).LessOrEqual(maxTime).Select();
    if (!rowset.IsReady()) {
        return false;
    }
    ui64 lastTime = 0;
    values.resize(get.MetricsSize());
    while (!rowset.EndOfSet()) {
        ui64 time = rowset.GetValue<Schema::MetricsValues::Timestamp>();
        if (time != lastTime) {
            lastTime = time;
            timestamps.push_back(TInstant::Seconds(time));
            for (auto& vals : values) {
                vals.emplace_back(NAN);
            }
        }
        ui64 id = rowset.GetValue<Schema::MetricsValues::Id>();
        auto itIdx = metricIdx.find(id);
        if (itIdx != metricIdx.end()) {
            values.back()[itIdx->second] = rowset.GetValue<Schema::MetricsValues::Value>();
        }
        if (!rowset.Next()) {
            return false;
        }
    }
    if (get.HasMaxPoints() && timestamps.size() > get.GetMaxPoints()) {
        timestamps = TMemoryBackend::Downsample(timestamps, get.GetMaxPoints());
        BLOG_TRACE("GetMetrics timestamps=" << timestamps.size());
        for (std::vector<double>& values : values) {
            values = TMemoryBackend::Downsample(values, get.GetMaxPoints());
            BLOG_TRACE("GetMetrics values=" << values.size());
        }
    }
    result.Clear();
    auto time = result.MutableTime();
    time->Reserve(timestamps.size());
    for (const TInstant t : timestamps) {
        time->Add(t.Seconds());
    }
    for (std::vector<double>& values : values) {
        result.AddData()->MutableValues()->Add(values.begin(), values.end());
    }
    return true;
}

bool TLocalBackend::ClearData(NTabletFlatExecutor::TTransactionContext& txc, TInstant cutline, TInstant& newStartTimestamp) {
    NIceDb::TNiceDb db(txc.DB);
    ui64 rows = 0;
    auto rowset = db.Table<Schema::MetricsValues>().LessOrEqual(cutline.Seconds()).Select();
    if (!rowset.IsReady()) {
        return false;
    }
    while (!rowset.EndOfSet()) {
        ui64 timestamp = rowset.GetValue<Schema::MetricsValues::Timestamp>();
        ui64 id = rowset.GetValue<Schema::MetricsValues::Id>();
        db.Table<Schema::MetricsValues>().Key(timestamp, id).Delete();
        newStartTimestamp = TInstant::Seconds(timestamp);
        if (++rows >= MAX_ROWS_TO_DELETE) {
            break;
        }
        if (!rowset.Next()) {
            return false;
        }
    }
    return true;
}

TString TLocalBackend::GetLogPrefix() const {
    return "DB ";
}


} // NGraph
} // NKikimr

