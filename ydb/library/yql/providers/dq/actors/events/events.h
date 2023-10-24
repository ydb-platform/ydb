#pragma once

namespace NYql {

    struct TDqEvents {
        enum {
            ES_BECOME_LEADER = EventSpaceBegin(NActors::TEvents::EEventSpace::ES_USERSPACE) + 10100,
            ES_BECOME_FOLLOWER,

            ES_TICK,

            ES_OTHER1,
            ES_OTHER2,
            ES_OTHER3,
            ES_OTHER4
        };
    };

    struct TEvBecomeLeader: NActors::TEventLocal<TEvBecomeLeader, TDqEvents::ES_BECOME_LEADER> {
        TEvBecomeLeader() = default;

        TEvBecomeLeader(ui32 leaderEpoch, const TString& leaderTransaction, const TString& attributes)
            : LeaderEpoch(leaderEpoch)
            , LeaderTransaction(leaderTransaction)
            , Attributes(attributes)
        { }

        const ui32 LeaderEpoch;
        const TString LeaderTransaction;
        const TString Attributes;
    };

    struct TEvBecomeFollower: NActors::TEventLocal<TEvBecomeFollower, TDqEvents::ES_BECOME_FOLLOWER> {
        TEvBecomeFollower() = default;

        TEvBecomeFollower(const TString& attributes)
            : Attributes(attributes)
        { }

        const TString Attributes;
    };

    struct TEvTick
        : NActors::TEventLocal<TEvTick, TDqEvents::ES_TICK> {
        TEvTick() = default;
    };

    struct TEvUploadComplete
        : NActors::TEventLocal<TEvTick, TDqEvents::ES_OTHER2> {
        TEvUploadComplete() = default;
    };

    struct TEvDownloadComplete
        : NActors::TEventLocal<TEvTick, TDqEvents::ES_OTHER3> {
        TVector<TString> FailedObjectIds;
        TEvDownloadComplete() = default;
    };

} // namespace NYql
