#include "time_predictor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <util/generic/algorithm.h>

#include <algorithm>
#include <cmath>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {

TDuration PercentileAt(
    const TVector<TDuration>& durations,
    size_t count,
    double percentile)
{
    Y_ABORT_UNLESS(count > 0);
    // Nearest-rank: ceil(p * n) - 1, clamped to [0, n).
    const size_t idx =
        Min(count - 1, static_cast<size_t>(std::ceil(percentile * count)) - 1);
    return durations[idx];
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TTimePredictor::THistory::THistory(size_t capacity)
    : History(capacity)
    , Durations(capacity)
{}

void TTimePredictor::THistory::Add(TDuration time)
{
    if (Durations.empty()) {
        // capacity == 0 - nothing is ever remembered, and Predict returns
        // zero unconditionally.
        return;
    }

    const auto begin = Durations.begin();
    const auto end = begin + Count;
    const auto insertAt = LowerBound(begin, end, time);

    const auto extracted = History.PushBack(time);
    if (!extracted) {
        std::move_backward(insertAt, end, end + 1);
        *insertAt = time;
        ++Count;
        return;
    }

    const auto eraseAt = LowerBound(begin, end, *extracted);
    Y_DEBUG_ABORT_UNLESS(eraseAt != end);
    if (insertAt <= eraseAt) {
        std::move_backward(insertAt, eraseAt, eraseAt + 1);
        *insertAt = time;
    } else {
        // Close the gap at eraseAt with an overlap-safe rotate, then place
        // the new value in the vacated slot at insertAt - 1.
        std::rotate(eraseAt, eraseAt + 1, insertAt);
        *(insertAt - 1) = time;
    }
}

TDuration TTimePredictor::THistory::Predict(size_t nthFromEnd) const
{
    return nthFromEnd >= Count ? TDuration()
                               : Durations[Count - 1 - nthFromEnd];
}

TLatencyStats TTimePredictor::THistory::GetLatencyStats() const
{
    TLatencyStats stats;
    if (Count == 0) {
        return stats;
    }
    stats.Count = Count;
    stats.Min = Durations.front();
    stats.Max = Durations[Count - 1];
    stats.P50 = PercentileAt(Durations, Count, 0.50);
    stats.P90 = PercentileAt(Durations, Count, 0.90);
    stats.P99 = PercentileAt(Durations, Count, 0.99);
    return stats;
}

////////////////////////////////////////////////////////////////////////////////

TTimePredictor::TTimePredictor(size_t capacity, size_t nthFromEnd)
    : Capacity(capacity)
    , NthFromEnd(nthFromEnd)
    , History(DirectBlockGroupHostCount, THistory(capacity))
{}

void TTimePredictor::Add(THostIndex host, TDuration time)
{
    if (host >= History.size()) {
        History.resize(host + 1, THistory(Capacity));
    }
    History[host].Add(time);
}

void TTimePredictor::Add(THostMask hostMask, TDuration time)
{
    for (auto host: hostMask) {
        Add(host, time);
    }
}

TDuration TTimePredictor::Predict(THostIndex host) const
{
    if (host >= History.size()) {
        return {};
    }
    return History[host].Predict(NthFromEnd);
}

TDuration TTimePredictor::Predict(THostMask hostMask) const
{
    if (NthFromEnd >= Capacity) {
        return {};
    }

    TDuration result;
    for (auto host: hostMask) {
        result = Max(result, Predict(host));
    }
    return result;
}

TLatencyStats TTimePredictor::GetLatencyStats(THostIndex host) const
{
    if (host >= History.size()) {
        return {};
    }
    return History[host].GetLatencyStats();
}

size_t TTimePredictor::GetCapacity() const
{
    return Capacity;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
