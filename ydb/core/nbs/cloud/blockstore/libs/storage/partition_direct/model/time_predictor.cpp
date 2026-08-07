#include "time_predictor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <util/generic/algorithm.h>

#include <cmath>
#include <iterator>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {

TDuration PercentileAt(const TMultiSet<TDuration>& durations, double percentile)
{
    const size_t n = durations.size();
    Y_ABORT_UNLESS(n > 0);
    // Nearest-rank: ceil(p * n) - 1, clamped to [0, n).
    const size_t idx =
        Min(n - 1, static_cast<size_t>(std::ceil(percentile * n)) - 1);
    return *std::next(durations.begin(), static_cast<std::ptrdiff_t>(idx));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TTimePredictor::THistory::THistory(size_t capacity)
    : History(capacity)
{}

void TTimePredictor::THistory::Add(TDuration time)
{
    Durations.insert(time);
    if (auto extracted = History.PushBack(time)) {
        Durations.erase(Durations.find(*extracted));
    }
}

TDuration TTimePredictor::THistory::Predict(size_t nthFromEnd) const
{
    auto it = Durations.rbegin();
    for (size_t i = 0; i < nthFromEnd; ++i) {
        if (it == Durations.rend()) {
            return {};
        }
        ++it;
    }
    return it == Durations.rend() ? TDuration() : *it;
}

TLatencyStats TTimePredictor::THistory::GetLatencyStats() const
{
    TLatencyStats stats;
    if (Durations.empty()) {
        return stats;
    }
    stats.Count = Durations.size();
    stats.Min = *Durations.begin();
    stats.Max = *Durations.rbegin();
    stats.P50 = PercentileAt(Durations, 0.50);
    stats.P90 = PercentileAt(Durations, 0.90);
    stats.P99 = PercentileAt(Durations, 0.99);
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
