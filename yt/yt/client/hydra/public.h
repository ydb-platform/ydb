#pragma once

#include <yt/yt/client/election/public.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPeerState,
    ((None)                       (0))
    ((Stopped)                    (1))
    ((Elections)                  (2))
    ((FollowerRecovery)           (3))
    ((Following)                  (4))
    ((LeaderRecovery)             (5))
    ((Leading)                    (6))
);

YT_DEFINE_ERROR_ENUM(
    ((NoSuchSnapshot)              (600))
    ((NoSuchChangelog)             (601))
    ((InvalidEpoch)                (602))
    ((InvalidVersion)              (603))
    ((OutOfOrderMutations)         (609))
    ((InvalidSnapshotVersion)      (610))
    ((ReadOnlySnapshotBuilt)       (611))
    ((ReadOnlySnapshotBuildFailed) (612))
    ((BrokenChangelog)             (613))
    ((ChangelogIOError)            (614))
    ((InvalidChangelogState)       (615))
    ((ReadOnly)                    (616))
);

DEFINE_ENUM(EPeerKind,
    ((Leader)            (0))
    ((Follower)          (1))
    ((LeaderOrFollower)  (2))
);

using TRevision = ui64;
constexpr TRevision NullRevision = 0;

struct TVersion;
struct TReachableState;
struct TElectionPriority;

////////////////////////////////////////////////////////////////////////////////

using NElection::TCellId;
using NElection::NullCellId;
using NElection::InvalidPeerId;
using NElection::TPeerPriority;
using NElection::TEpochId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
