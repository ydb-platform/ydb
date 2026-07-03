#include "time_predictor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <util/generic/algorithm.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TTimePredictor::THistory::THistory(size_t capacity)
    : History(capacity)
{}

void TTimePredictor::THistory::Add(TDuration time)
{
    auto extractred = History.PushBack(time);
    if (extractred) {
        Durations.erase(Durations.find(*extractred));
    }
    Durations.insert(time);
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

////////////////////////////////////////////////////////////////////////////////

TTimePredictor::TTimePredictor(size_t capacity, size_t nthFromEnd)
    : Capacity(capacity)
    , NthFromEnd(nthFromEnd)
    , History(DirectBlockGroupHostCount, THistory(capacity))
{
    Y_ABORT_UNLESS(Capacity > 0);
    Y_ABORT_UNLESS(NthFromEnd < Capacity);
}

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
    TDuration result;
    for (auto host: hostMask) {
        result = Max(result, Predict(host));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
