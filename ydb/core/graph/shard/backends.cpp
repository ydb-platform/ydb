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
        TInstant expected = trgRest != 0 ? (frontTs + TDuration::Seconds((trg - trgSrc) * distanceTs.Seconds() / trgRest)) : backTs;
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

double TBaseBackend::GetTimingForPercentile(double percentile, const TVector<ui64>& values, const TVector<ui64>& /*upper*/bounds, ui64 total) {
    ui64 ppMark = total * percentile;
    ui64 accm = 0;
    ui32 n = 0;
    while (n < bounds.size() && accm < ppMark) {
        if (accm + values[n] >= ppMark) {
            ui64 lowerBound = 0;
            if (n > 0) {
                lowerBound = bounds[n - 1];
            }
            ui64 upperBound = bounds[n];
            if (upperBound == std::numeric_limits<ui64>::max()) {
                return lowerBound; // workaround for INF bucket
            }
            ui64 currentValue = values[n];
            ui64 ppValue = ppMark - accm;
            if (currentValue == 0) {
                return NAN;
            }
            return (static_cast<double>(ppValue) / currentValue) * (upperBound - lowerBound) + lowerBound;
        }
        accm += values[n];
        n++;
    }
    return NAN;
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
        if (idx >= record.Values.size()) {
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
    if (!get.GetSkipBorders()) {
        if (get.HasTimeFrom()) {
            TInstant from(TInstant::Seconds(get.GetTimeFrom()));
            if (itLeft == MetricsValues.end() || itLeft->Timestamp > from) {
                metricValues.Timestamps.push_back(from);
                for (size_t num = 0; num < indexes.size(); ++num) {
                    metricValues.Values[num].push_back(NAN);
                }
            }
        }
    }
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
    if (!get.GetSkipBorders()) {
        if (get.HasTimeTo()) {
            TInstant to(TInstant::Seconds(get.GetTimeTo()));
            if (metricValues.Timestamps.empty() || (metricValues.Timestamps.back() < to)) {
                metricValues.Timestamps.push_back(to);
                for (size_t num = 0; num < indexes.size(); ++num) {
                    metricValues.Values[num].push_back(NAN);
                }
            }
        }
    }
    FillResult(metricValues, get, result);
}

void TMemoryBackend::ClearData(TInstant now, const TAggregateSettings& settings) {
    TInstant cutline = now - settings.PeriodToStart;
    BLOG_D("Clear data to " << cutline.ToStringUpToSeconds());
    auto itCutLine = std::upper_bound(MetricsValues.begin(), MetricsValues.end(), cutline);
    size_t before = MetricsValues.size();
    MetricsValues.erase(MetricsValues.begin(), itCutLine);
    if (!MetricsValues.empty()) {
        settings.StartTimestamp = MetricsValues.front().Timestamp;
    } else {
        settings.StartTimestamp = {};
    }
    BLOG_D("Cleared " << before - MetricsValues.size() << " logical rows");
}

void TMemoryBackend::DownsampleData(TInstant now, const TAggregateSettings& settings) {
    const TInstant startTimestamp = TInstant::Seconds(settings.StartTimestamp.Seconds() / settings.SampleSize.Seconds() * settings.SampleSize.Seconds());
    const TInstant endTimestamp = now - settings.PeriodToStart;
    BLOG_D("Downsample data from " << settings.StartTimestamp.ToStringUpToSeconds() << " to " << endTimestamp.ToStringUpToSeconds());

    const auto itStart = std::lower_bound(MetricsValues.begin(), MetricsValues.end(), startTimestamp);
    if (itStart == MetricsValues.end()) {
        BLOG_TRACE("StartTimestamp beyond the range");
        return;
    }

    const auto itStop = std::upper_bound(MetricsValues.begin(), MetricsValues.end(), endTimestamp);
    if (itStop == MetricsValues.begin()) {
        BLOG_TRACE("EndTimestamp beyond the range");
        return;
    }

    const size_t before = MetricsValues.size();

    TMetricsValues values;
    auto itEraseStart = itStart;
    auto itEraseStop = itEraseStart;
    std::optional<TInstant> beginSampleTimestamp;
    TInstant endSampleTimestamp;
    values.Values.resize(MetricsIndex.size());
    std::vector<TMetricsRecord> insertedValues;
    for (auto it = itStart; it != itStop; ++it) {
        const TInstant timestamp = it->Timestamp;
        if (!beginSampleTimestamp) {
            beginSampleTimestamp = TInstant::FromValue(timestamp.GetValue() / settings.SampleSize.GetValue() * settings.SampleSize.GetValue());
            endSampleTimestamp = *beginSampleTimestamp + settings.SampleSize;
        }

        if (std::prev(itStop)->Timestamp < endSampleTimestamp) {
            break;
        }

        if (timestamp >= endSampleTimestamp) {
            if (values.Timestamps.size() > 1) {
                itEraseStop = it;

                BLOG_TRACE("Normalizing " << values.Timestamps.size() << " values from " << values.Timestamps.front().Seconds()
                    << " to " << values.Timestamps.back().Seconds());
                NormalizeAndDownsample(values, 1);
            } else {
                if (itEraseStart + 1 == it) {
                    itEraseStop = ++itEraseStart;
                }
            }

            if (!values.Timestamps.empty()) {
                BLOG_TRACE("Result time is " << values.Timestamps.front().Seconds());
                if (it != itEraseStart) {
                    insertedValues.emplace_back(TMetricsRecord{values.Timestamps.front(), std::move(values.Values.front())});
                }
            }

            settings.StartTimestamp = endSampleTimestamp;
            values.Clear();
            beginSampleTimestamp = TInstant::FromValue(timestamp.GetValue() / settings.SampleSize.GetValue() * settings.SampleSize.GetValue());
            endSampleTimestamp = *beginSampleTimestamp + settings.SampleSize;
        }

        values.Timestamps.emplace_back(timestamp);
        for (size_t nVal = 0; nVal < MetricsIndex.size(); ++nVal) {
            if (nVal < it->Values.size()) {
                values.Values[nVal].push_back(it->Values[nVal]);
            } else {
                values.Values[nVal].push_back(NAN);
            }
        }
    }

    insertedValues.reserve(insertedValues.size() + MetricsValues.end() - itEraseStop);
    for (auto it = itEraseStop; it != MetricsValues.end(); ++it) {
        insertedValues.emplace_back(std::move(*it));
    }
    MetricsValues.erase(itEraseStart, MetricsValues.end());
    for (auto& val : insertedValues) {
        MetricsValues.emplace_back(std::move(val));
    }

    BLOG_D("Downsampled " << before - MetricsValues.size() << " logical rows");
}

void TMemoryBackend::AggregateData(TInstant now, const TAggregateSettings& settings) {
    if (settings.SampleSize == TDuration::Zero()) {
        ClearData(now, settings);
    } else {
        DownsampleData(now, settings);
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
            BLOG_TRACE("Metric " << name << " has id " << itId->second);
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
    std::optional<ui64> lastTime;
    metricValues.Values.resize(get.MetricsSize());
    if (get.HasTimeFrom() && !get.GetSkipBorders() && (rowset.EndOfSet() || rowset.GetValue<Schema::MetricsValues::Timestamp>() > minTime)) {
        metricValues.Timestamps.push_back(TInstant::Seconds(minTime));
        for (auto& vals : metricValues.Values) {
            vals.emplace_back(NAN);
        }
    }
    while (!rowset.EndOfSet()) {
        ui64 time = rowset.GetValue<Schema::MetricsValues::Timestamp>();
        if (!lastTime || time != *lastTime) {
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
    if (get.HasTimeTo() && !get.GetSkipBorders() && (!lastTime || *lastTime < maxTime)) {
        metricValues.Timestamps.push_back(TInstant::Seconds(maxTime));
        for (auto& vals : metricValues.Values) {
            vals.emplace_back(NAN);
        }
    }
    FillResult(metricValues, get, result);
    return true;
}

bool TLocalBackend::ClearData(NTabletFlatExecutor::TTransactionContext& txc, TInstant now, const TAggregateSettings& settings) {
    TInstant cutline = now - settings.PeriodToStart;
    BLOG_D("Clear data to " << cutline.ToStringUpToSeconds());
    NIceDb::TNiceDb db(txc.DB);
    ui64 rows = 0;
    auto rowset = db.Table<Schema::MetricsValues>().LessOrEqual(cutline.Seconds()).Select();
    if (!rowset.IsReady()) {
        return false;
    }
    std::optional<ui64> prevTimestamp;
    while (!rowset.EndOfSet()) {
        ui64 timestamp = rowset.GetValue<Schema::MetricsValues::Timestamp>();
        ui64 id = rowset.GetValue<Schema::MetricsValues::Id>();
        db.Table<Schema::MetricsValues>().Key(timestamp, id).Delete();
        settings.StartTimestamp = TInstant::Seconds(timestamp);
        if ((!prevTimestamp || timestamp != *prevTimestamp) // we count as a logical row every unique timestamp
            && ++rows >= MAX_ROWS_TO_DELETE) {              // so for database it will be MAX_ROWS * NUM_OF_METRICS rows
            break;
        }
        prevTimestamp = timestamp;
        if (!rowset.Next()) {
            return false;
        }
    }
    BLOG_D("Cleared " << rows << " logical rows");
    return true;
}

bool TLocalBackend::DownsampleData(NTabletFlatExecutor::TTransactionContext& txc, TInstant now, const TAggregateSettings& settings) {
    const TInstant startTimestamp = TInstant::Seconds(settings.StartTimestamp.Seconds() / settings.SampleSize.Seconds() * settings.SampleSize.Seconds());
    const TInstant endTimestamp = now - settings.PeriodToStart;
    BLOG_D("Downsample data from " << startTimestamp.ToStringUpToSeconds() << " to " << endTimestamp.ToStringUpToSeconds());
    NIceDb::TNiceDb db(txc.DB);
    ui64 rows = 0;
    auto rowset = db.Table<Schema::MetricsValues>().GreaterOrEqual(startTimestamp.Seconds()).LessOrEqual(endTimestamp.Seconds()).Select();
    if (!rowset.IsReady()) {
        return false;
    }
    TMetricsValues values;
    std::unordered_set<ui64> ids;
    std::optional<ui64> beginSampleTimestamp;
    ui64 endSampleTimestamp;
    std::optional<ui64> prevTimestamp;
    values.Values.resize(MetricsIndex.size());
    while (!rowset.EndOfSet()) {
        const ui64 timestamp = rowset.GetValue<Schema::MetricsValues::Timestamp>();
        const ui64 metricId = rowset.GetValue<Schema::MetricsValues::Id>();
        const double value = rowset.GetValue<Schema::MetricsValues::Value>();
        if (!beginSampleTimestamp) {
            beginSampleTimestamp = timestamp / settings.SampleSize.Seconds() * settings.SampleSize.Seconds();
            endSampleTimestamp = *beginSampleTimestamp + settings.SampleSize.Seconds();
        }
        if (timestamp >= endSampleTimestamp) {
            if (values.Timestamps.size() > 1) {
                for (TInstant ts : values.Timestamps) {
                    for (ui64 id : ids) {
                        db.Table<Schema::MetricsValues>().Key(ts.Seconds(), id).Delete();
                    }
                    ++rows;
                }

                BLOG_TRACE("Normalizing " << values.Timestamps.size() << " values from " << values.Timestamps.front().Seconds()
                    << " to " << values.Timestamps.back().Seconds());
                NormalizeAndDownsample(values, 1);
            }
            if (!values.Timestamps.empty()) {
                BLOG_TRACE("Result time is " << values.Timestamps.front().Seconds());
                for (ui64 id : ids) {
                    if (!values.Values[id].empty()) {
                        BLOG_TRACE("Updating values with id " << id);
                        db.Table<Schema::MetricsValues>().Key(values.Timestamps.front().Seconds(), id).Update<Schema::MetricsValues::Value>(values.Values[id].front());
                    }
                }
            }
            settings.StartTimestamp = TInstant::Seconds(endSampleTimestamp);
            ids.clear();
            values.Clear();
            beginSampleTimestamp = timestamp / settings.SampleSize.Seconds() * settings.SampleSize.Seconds();
            endSampleTimestamp = *beginSampleTimestamp + settings.SampleSize.Seconds();
            prevTimestamp = std::nullopt;
        }
        if (!prevTimestamp || timestamp != *prevTimestamp) {
            for (auto& val : values.Values) {
                if (val.size() < values.Timestamps.size()) {
                    val.push_back(NAN);
                }
            }
            values.Timestamps.emplace_back(TInstant::Seconds(timestamp));
        }
        values.Values[metricId].push_back(value);
        ids.insert(metricId);
        Y_VERIFY(values.Values[metricId].size() <= values.Timestamps.size());
        prevTimestamp = timestamp;
        if (!rowset.Next()) {
            return false;
        }
    }
    BLOG_D("Downsampled " << rows << " logical rows");
    return true;
}

bool TLocalBackend::AggregateData(NTabletFlatExecutor::TTransactionContext& txc, TInstant now, const TAggregateSettings& settings) {
    if (settings.SampleSize == TDuration::Zero()) {
        return ClearData(txc, now, settings);
    } else {
        return DownsampleData(txc, now, settings);
    }
}

TString TLocalBackend::GetLogPrefix() const {
    return "DB ";
}


} // NGraph
} // NKikimr

