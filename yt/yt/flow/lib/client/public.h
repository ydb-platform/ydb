#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

/*
    TODO(mikari): naming
    Pipeline State
    1) Stopped - everything stopped and all internal queues is empty.
    2) Paused - everything stopped but some internal queues may not be empty.
    3) Working - usual work.
    4) Draining - preparing to stop pipeline.
        Injection of new messages and trigger activation disabled, waiting for emptiness of internal queues.
    5) Pausing - preparing to pause pipeline.
    6) Completed - everything has been processed.
*/
DEFINE_ENUM(EPipelineState,
    ((Unknown)        (0))
    ((Stopped)        (1))
    ((Paused)         (2))
    ((Working)        (3))
    ((Draining)       (4))
    ((Pausing)        (5))
    ((Completed)      (6))
);

YT_DEFINE_ERROR_ENUM(
    ((SpecVersionMismatch)    (3300))
);

YT_DEFINE_STRONG_TYPEDEF(TVersion, i64);

////////////////////////////////////////////////////////////////////////////////

inline const TString PipelineFormatVersionAttribute("pipeline_format_version");
inline const TString LeaderControllerAddressAttribute("leader_controller_address");

constexpr int CurrentPipelineFormatVersion = 1;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
