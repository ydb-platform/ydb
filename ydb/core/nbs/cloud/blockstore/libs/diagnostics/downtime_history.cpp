#include "downtime_history.h"

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TDowntimeHistoryHolder::TDowntimeHistoryHolder()
    : DownEvents(MAX_HISTORY_DEPTH)
{}

bool TDowntimeHistoryHolder::Empty() const
{
    return DownEvents.TotalSize() == 0;
}

void TDowntimeHistoryHolder::PushBack(TInstant now, EDowntimeStateChange state)
{
    if (DownEvents.TotalSize() == 0 ||
        (DownEvents[DownEvents.TotalSize() - 1].second != state))
    {
        DownEvents.PushBack(std::make_pair(now, state));
    }
}

TDowntimeHistory TDowntimeHistoryHolder::RecentEvents(TInstant now) const
{
    if (DownEvents.TotalSize() == 0) {
        return {};
    }

    size_t i = DownEvents.FirstIndex();
    for (; i + 1 < DownEvents.TotalSize(); ++i) {
        if ((now - DownEvents[i + 1].first) < MAX_HISTORY_DURATION) {
            break;
        }
    }
    TDowntimeHistory result;
    result.reserve(DownEvents.TotalSize() - i);
    for (; i < DownEvents.TotalSize(); ++i) {
        result.push_back(DownEvents[i]);
    }
    return result;
}

bool TDowntimeHistoryHolder::HasRecentState(
    TInstant now,
    EDowntimeStateChange state) const
{
    if (DownEvents.TotalSize() == 0) {
        return {};
    }

    size_t i = DownEvents.FirstIndex();
    for (; i + 1 < DownEvents.TotalSize(); ++i) {
        if ((now - DownEvents[i + 1].first) < MAX_HISTORY_DURATION) {
            break;
        }
    }
    for (; i < DownEvents.TotalSize(); ++i) {
        if (DownEvents[i].second == state) {
            return true;
        }
    }
    return false;
}

}   // namespace NYdb::NBS::NBlockStore
