#pragma once
#include <ydb/core/fq/libs/checkpointing_common/defs.h>
#include <ydb/core/fq/libs/events/event_subspace.h>

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/interconnect/events_local.h>

namespace NFq {

struct TEvCheckpointCoordinator {
    // Event ids.
    enum EEv : ui32 {
        EvScheduleCheckpointing = YqEventSubspaceBegin(TYqEventSubspace::CheckpointCoordinator),
        EvCoordinatorRegistered,
        EvZeroCheckpointDone,
        EvRunGraph,

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
};

} // namespace NFq
