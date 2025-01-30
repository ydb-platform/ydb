#pragma once

#include <yt/yt/core/misc/error_code.h>

#include <library/cpp/yt/misc/enum.h>
#include <library/cpp/yt/misc/guid.h>

#include <library/cpp/yt/string/guid.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

using TEpochId = TGuid;
using TPeerPriority = std::pair<i64, i64>;

constexpr int InvalidPeerId = -1;

using TCellId = TGuid;
extern const TCellId NullCellId;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((InvalidState)  (800))
    ((InvalidLeader) (801))
    ((InvalidEpoch)  (802))
);

DEFINE_ENUM(EPeerState,
    (Stopped)
    (Voting)
    (Leading)
    (Following)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
