#pragma once

#include <library/cpp/containers/ring_buffer/ring_buffer.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

enum EDowntimeStateChange
{
    DOWN = 0,
    UP = 1
};

using TDowntimeChange = std::pair<TInstant, EDowntimeStateChange>;
using TDowntimeHistory = TVector<TDowntimeChange>;

////////////////////////////////////////////////////////////////////////////////

class TDowntimeHistoryHolder
{
public:
    using TDowntimeEvents = TSimpleRingBuffer<TDowntimeChange>;

private:
    // Max change frequency is 5 sec, here we hold info at least for one hour
    static constexpr size_t MAX_HISTORY_DEPTH = 12 * 60;
    static constexpr TDuration MAX_HISTORY_DURATION = TDuration::Hours(1);
    TDowntimeEvents DownEvents;

public:
    TDowntimeHistoryHolder();

public:
    bool Empty() const;
    void PushBack(TInstant now, EDowntimeStateChange state);
    TDowntimeHistory RecentEvents(TInstant now) const;
    bool HasRecentState(TInstant now, EDowntimeStateChange state) const;
};

}   // namespace NYdb::NBS::NBlockStore
