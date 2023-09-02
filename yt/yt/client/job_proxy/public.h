#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((MemoryLimitExceeded)       (1200))
    ((MemoryCheckFailed)         (1201))
    ((JobTimeLimitExceeded)      (1202))
    ((UnsupportedJobType)        (1203))
    ((JobNotPrepared)            (1204))
    ((UserJobFailed)             (1205))
    ((UserJobProducedCoreFiles)  (1206))
    ((ShallowMergeFailed)        (1207))
    ((JobNotRunning)             (1208))
    ((InterruptionUnsupported)   (1209))
    ((InterruptionTimeout)       (1210))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
