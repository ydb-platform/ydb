#pragma once

#include <util/generic/fwd.h>
#include <util/system/yassert.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

// represents [Start, End) interval in bytes.
struct TSizeInterval
{
    ui64 Start;
    ui64 End;

    TSizeInterval(ui64 start, ui64 end)
        : Start(start)
        , End(end)
    {
        Y_ABORT_UNLESS(start < end);
    }
};

TString ToString(const TSizeInterval& interval);

}   // namespace NYdb::NBS
