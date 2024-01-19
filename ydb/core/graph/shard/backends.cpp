#include "log.h"
#include "backends.h"
#include "schema.h"

namespace NKikimr {
namespace NGraph {

void TBaseBackend::NormalizeAndDownsample(TMetricsValues& values, size_t maxPoints) {
    if (values.Timestamps.size() <= maxPoints) {
        return;
    }

    TMetricsValues result;
    result.Timestamps.resize(maxPoints);
    result.Values.resize(values.Values.size());
    for (auto& values : result.Values) {
        values.resize(maxPoints);
    }

    size_t srcSize = values.Timestamps.size();
    size_t trgSize = result.Timestamps.size();
    TInstant frontTs = values.Timestamps.front();
    TInstant backTs = values.Timestamps.back();
    TDuration distanceTs = backTs - frontTs;
    // normalize timestamps
    size_t src = 0;
    size_t trg = 0;
    size_t trgSrc = 0;
    size_t trgRest = trgSize - 1;
    while (src < srcSize && trg < trgSize) {
        TInstant expected = frontTs + TDuration::Seconds((trg - trgSrc) * distanceTs.Seconds() / trgRest);
        if (expected < values.Timestamps[src]) {
            result.Timestamps[trg] = values.Timestamps[src];
            frontTs = values.Timestamps[src];
            distanceTs = backTs - frontTs;
            trgSrc = trg;
            trgRest = trgSize - trg - 1;
            ++src;
            ++trg;
        } else if (expected == values.Timestamps[src]) {
            result.Timestamps[trg] = values.Timestamps[src];
            ++src;
            ++trg;
        } else if (expected > values.Timestamps[src]) {
            result.Timestamps[trg] = expected;
            ++trg;
            do {
                ++src;
                if (src >= srcSize) {
                    break;
                }
            } while (values.Timestamps[src] < expected);
        }
    }
    // aggregate values
    for (size_t numVal = 0; numVal < result.Values.size(); ++numVal) {
        double accm = NAN; // avg impl
        long cnt = 0;
        const std::vector<double>& srcValues(values.Values[numVal]);
        std::vector<double>& trgValues(result.Values[numVal]);
        size_t trgPos = 0;
        for (size_t srcPos = 0; srcPos < srcValues.size(); ++srcPos) {
            double srcValue = srcValues[srcPos];
            if (!isnan(srcValue)) {
                if (isnan(accm)) { // avg impl
                    accm = srcValue;
                    cnt = 1;
                } else {
                    accm += srcValue;
                    cnt += 1;
                }
            }
            if (values.Timestamps[srcPos] >= result.Timestamps[trgPos]) {
                if (isnan(accm)) { // avg impl
                    trgValues[trgPos] = NAN;
                } else {
                    trgValues[trgPos] = accm / cnt;
                }
                ++trgPos;
                accm = NAN;
                cnt = 0;
            }
        }
    }
    values = std::move(result);
}

void TBaseBackend::FillResult(TMetricsValues& values, const NKikimrGraph::TEvGetMetrics& get, NKikimrGraph::TEvMetricsResult& result) {
    if (get.HasMaxPoints() && values.Timestamps.size() > get.GetMaxPoints()) {
        NormalizeAndDownsample(values, get.GetMaxPoints());
    }
    result.Clear();
    auto time = result.MutableTime();
    time->Reserve(values.Timestamps.size());
    for (const TInstant t : values.Timestamps) {
        time->Add(t.Seconds());
    }
    for (std::vector<double>& values : values.Values) {
        result.AddData()->MutableValues()->Add(values.begin(), values.end());
    }
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
    TMetricsValues metricValues;
    metricValues.Values.resize(indexes.size());
    for (auto it = itLeft; it != itRight; ++it) {
        metricValues.Timestamps.push_back(it->Timestamp);
        for (size_t num = 0; num < indexes.size(); ++num) {
            size_t idx = indexes[num];
            if (idx < it->Values.size()) {
                metricValues.Values[num].push_back(it->Values[idx]);
            } else {
                metricValues.Values[num].push_back(NAN);
            }
        }
    }
    FillResult(metricValues, get, result);
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
    TMetricsValues metricValues;
    BLOG_D("Querying from " << minTime << " to " << maxTime);
    auto rowset = db.Table<Schema::MetricsValues>().GreaterOrEqual(minTime).LessOrEqual(maxTime).Select();
    if (!rowset.IsReady()) {
        return false;
    }
    ui64 lastTime = 0;
    metricValues.Values.resize(get.MetricsSize());
    while (!rowset.EndOfSet()) {
        ui64 time = rowset.GetValue<Schema::MetricsValues::Timestamp>();
        if (time != lastTime) {
            lastTime = time;
            metricValues.Timestamps.push_back(TInstant::Seconds(time));
            for (auto& vals : metricValues.Values) {
                vals.emplace_back(NAN);
            }
        }
        ui64 id = rowset.GetValue<Schema::MetricsValues::Id>();
        auto itIdx = metricIdx.find(id);
        if (itIdx != metricIdx.end()) {
            metricValues.Values[itIdx->second].back() = rowset.GetValue<Schema::MetricsValues::Value>();
        }
        if (!rowset.Next()) {
            return false;
        }
    }
    FillResult(metricValues, get, result);
    return true;
}

bool TLocalBackend::ClearData(NTabletFlatExecutor::TTransactionContext& txc, TInstant cutline, TInstant& newStartTimestamp) {
    NIceDb::TNiceDb db(txc.DB);
    ui64 rows = 0;
    auto rowset = db.Table<Schema::MetricsValues>().LessOrEqual(cutline.Seconds()).Select();
    if (!rowset.IsReady()) {
        return false;
    }
    ui64 prevTimestamp = 0;
    while (!rowset.EndOfSet()) {
        ui64 timestamp = rowset.GetValue<Schema::MetricsValues::Timestamp>();
        ui64 id = rowset.GetValue<Schema::MetricsValues::Id>();
        db.Table<Schema::MetricsValues>().Key(timestamp, id).Delete();
        newStartTimestamp = TInstant::Seconds(timestamp);
        if (timestamp != prevTimestamp && ++rows >= MAX_ROWS_TO_DELETE) { // we count as a logical row every unique timestamp
            break;                                                        // so for database it will be MAX_ROWS * NUM_OF_METRICS rows
        }
        prevTimestamp = timestamp;
        if (!rowset.Next()) {
            return false;
        }
    }
    BLOG_D("Cleared " << rows << " logical rows");
    return true;
}

TString TLocalBackend::GetLogPrefix() const {
    return "DB ";
}


} // NGraph
} // NKikimr

