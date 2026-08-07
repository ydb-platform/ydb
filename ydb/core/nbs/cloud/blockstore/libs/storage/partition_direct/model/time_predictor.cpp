#include "time_predictor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <util/generic/algorithm.h>

#include <algorithm>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

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

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
