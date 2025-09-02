#pragma once

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EOperationIncarnationSwitchReason,
    (JobAborted)
    (JobFailed)
    (JobInterrupted)
    (JobLackAfterRevival)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
