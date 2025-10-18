#pragma once

#include <ydb/core/fq/libs/events/event_subspace.h>

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/interconnect/events_local.h>

#include <yql/essentials/public/issue/yql_issue.h>

namespace NFq {

struct TEvCheckpointCoordinator {
    // Event ids.
    enum EEv : ui32 {
        EvScheduleCheckpointing = YqEventSubspaceBegin(TYqEventSubspace::CheckpointCoordinator),
        EvCoordinatorRegistered,
        EvZeroCheckpointDone,
        EvRunGraph,
        EvReadyState,
        EvRaiseTransientIssues,
        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(TYqEventSubspace::CheckpointCoordinator), "All events must be in their subspace");

    // Events.

    struct TEvScheduleCheckpointing : NActors::TEventLocal<TEvScheduleCheckpointing, EvScheduleCheckpointing> {
    };

    struct TEvCoordinatorRegistered : NActors::TEventLocal<TEvCoordinatorRegistered, EvCoordinatorRegistered> {
    };

    // Checkpoint coordinator sends this event to run actor when it initializes a new zero checkpoint.
    // Run actor saves that next time we need to restore from checkpoint.
    struct TEvZeroCheckpointDone : public NActors::TEventLocal<TEvZeroCheckpointDone, EvZeroCheckpointDone> {
    };

    // When run actor saved restore info after zero checkpoint, it sends this event to checkpoint coordinator.
    struct TEvRunGraph : public NActors::TEventLocal<TEvRunGraph, EvRunGraph> {
    };

    struct TEvReadyState : public NActors::TEventLocal<TEvReadyState, EvReadyState> {
        struct TTask {
            ui64 Id = 0;
            bool IsCheckpointingEnabled = false;
            bool IsIngress = false;
            bool IsEgress = false;
            bool HasState = false;
            NActors::TActorId ActorId;
        };
        std::vector<TTask> Tasks;
    };

    struct TEvRaiseTransientIssues : public NActors::TEventLocal<TEvRaiseTransientIssues, EvRaiseTransientIssues> {
        TEvRaiseTransientIssues() = default;

        explicit TEvRaiseTransientIssues(NYql::TIssues issues)
            : TransientIssues(std::move(issues))
        {
        }

        NYql::TIssues TransientIssues;
    };
};

} // namespace NFq
