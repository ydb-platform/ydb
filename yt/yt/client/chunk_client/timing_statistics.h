#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

#include <library/cpp/yt/string/format.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TTimingStatistics
{
    //! Time spent while waiting on reader ready event.
    TDuration WaitTime;
    //! Time spent in synchronous manner in Read().
    TDuration ReadTime;
    //! Time of not waiting and not reading.
    TDuration IdleTime;

    //! Returns the sum of all components.
    TDuration GetTotalTime() const;
};

TTimingStatistics& operator+=(TTimingStatistics& lhs, const TTimingStatistics& rhs);

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TTimingStatistics& statistics, NYson::IYsonConsumer* consumer);

void FormatValue(TStringBuilderBase* builder, const TTimingStatistics& statistics, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
