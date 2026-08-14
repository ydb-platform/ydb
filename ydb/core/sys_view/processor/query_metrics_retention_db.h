#pragma once

#include "schema.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NSysView {

struct TStoredQueryMetricsOneHour {
    ui64 HourEnd = 0;
    ui32 Rank = 0;
    TString Text;
    TString Data;
};

struct TQueryMetricsOneHourLoadResult {
    TVector<TStoredQueryMetricsOneHour> Rows;
    ui64 RetainedBytes = 0;
    ui64 EvictBeforeHourEnd = 0;
};

inline bool LoadQueryMetricsOneHour(
    NIceDb::TNiceDb& db,
    ui64 activeHourEnd,
    ui64 byteLimit,
    ui64 persistentCutoff,
    TQueryMetricsOneHourLoadResult& result)
{
    result = {};
    result.EvictBeforeHourEnd = persistentCutoff;

    auto rowset = db.Table<TProcessorSchema::MetricsOneHour>().Reverse().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    TVector<TStoredQueryMetricsOneHour> bucket;
    ui64 bucketHourEnd = 0;
    ui64 bucketBytes = 0;
    ui64 oldestRetainedHourEnd = 0;
    bool byteLimitReached = false;

    auto flushBucket = [&]() {
        if (bucket.empty()) {
            return true;
        }

        const bool isActive = bucketHourEnd == activeHourEnd;
        if (!isActive && result.RetainedBytes + bucketBytes > byteLimit) {
            result.EvictBeforeHourEnd = oldestRetainedHourEnd
                ? oldestRetainedHourEnd
                : activeHourEnd;
            return false;
        }

        result.RetainedBytes += bucketBytes;
        oldestRetainedHourEnd = bucketHourEnd;
        for (auto& row : bucket) {
            result.Rows.emplace_back(std::move(row));
        }
        bucket.clear();
        bucketBytes = 0;
        return true;
    };

    while (!rowset.EndOfSet()) {
        const ui64 hourEnd =
            rowset.GetValue<TProcessorSchema::MetricsOneHour::IntervalEnd>();
        if (persistentCutoff && hourEnd < persistentCutoff) {
            break;
        }

        if (!bucket.empty() && bucketHourEnd != hourEnd) {
            if (!flushBucket()) {
                byteLimitReached = true;
                break;
            }
        }
        if (bucket.empty()) {
            bucketHourEnd = hourEnd;
        }

        TStoredQueryMetricsOneHour row;
        row.HourEnd = hourEnd;
        row.Rank = rowset.GetValue<TProcessorSchema::MetricsOneHour::Rank>();
        row.Text = rowset.GetValue<TProcessorSchema::MetricsOneHour::Text>();
        row.Data = rowset.GetValue<TProcessorSchema::MetricsOneHour::Data>();
        bucketBytes += row.Text.size() + row.Data.size();
        bucket.emplace_back(std::move(row));

        if (!rowset.Next()) {
            return false;
        }
    }

    if (!bucket.empty() && !byteLimitReached) {
        flushBucket();
    }

    return true;
}

struct TQueryMetricsOneHourCleanupResult {
    size_t Deleted = 0;
    ui64 EvictedBuckets = 0;
    ui64 NewCutoff = 0;
    bool More = false;
};

inline bool CleanupQueryMetricsOneHour(
    NIceDb::TNiceDb& db,
    ui64 cutoff,
    size_t batchSize,
    TQueryMetricsOneHourCleanupResult& result)
{
    result = {};
    result.NewCutoff = cutoff;
    if (!cutoff || !batchSize) {
        return true;
    }

    auto rowset = db.Table<TProcessorSchema::MetricsOneHour>().Range().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        const ui64 hourEnd =
            rowset.GetValue<TProcessorSchema::MetricsOneHour::IntervalEnd>();
        if (hourEnd >= cutoff) {
            result.NewCutoff = 0;
            break;
        }

        const ui32 rank =
            rowset.GetValue<TProcessorSchema::MetricsOneHour::Rank>();
        db.Table<TProcessorSchema::MetricsOneHour>().Key(hourEnd, rank).Delete();
        result.EvictedBuckets += rank == 1;

        if (++result.Deleted == batchSize) {
            result.More = true;
            break;
        }

        if (!rowset.Next()) {
            return false;
        }
    }

    if (rowset.EndOfSet()) {
        result.NewCutoff = 0;
    }
    return true;
}

} // namespace NKikimr::NSysView
