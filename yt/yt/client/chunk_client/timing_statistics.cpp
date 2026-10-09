#include "timing_statistics.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NChunkClient {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TDuration TTimingStatistics::GetTotalTime() const
{
    return WaitTime + ReadTime + IdleTime;
}

TTimingStatistics& operator+=(TTimingStatistics& lhs, const TTimingStatistics& rhs)
{
    lhs.IdleTime += rhs.IdleTime;
    lhs.ReadTime += rhs.ReadTime;
    lhs.WaitTime += rhs.WaitTime;

    return lhs;
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TTimingStatistics& statistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("wait_time").Value(statistics.WaitTime)
            .Item("read_time").Value(statistics.ReadTime)
            .Item("idle_time").Value(statistics.IdleTime)
        .EndMap();
}

void FormatValue(TStringBuilderBase* builder, const TTimingStatistics& statistics, TStringBuf /*spec*/)
{
    Format(
        builder,
        "{Wait: %v, Read: %v, Idle: %v}",
        statistics.WaitTime,
        statistics.ReadTime,
        statistics.IdleTime);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
