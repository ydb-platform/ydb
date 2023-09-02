#pragma once

// TODO(ignat): migrate to enum class
#include <library/cpp/yt/misc/enum.h>

#include <yt/yt/core/misc/error_code.h>

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESchedulingMode,
    (Fifo)
    (FairShare)
);

DEFINE_ENUM(EIntegralGuaranteeType,
    (None)
    (Burst)
    (Relaxed)
);

YT_DEFINE_ERROR_ENUM(
    ((PoolTreeGuaranteesOvercommit) (29000))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf

