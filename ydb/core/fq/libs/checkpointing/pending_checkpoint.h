#pragma once
#include <ydb/core/fq/libs/checkpointing_common/defs.h>

#include <ydb/library/actors/core/actor.h>

#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>

namespace NFq {

struct TPendingCheckpointStats {
    const TInstant CreatedAt = TInstant::Now();
    ui64 StateSize = 0;
};

class TPendingCheckpoint {
    THashSet<NActors::TActorId> NotYetAcknowledged;
    NYql::NDqProto::ECheckpointType Type;
    TPendingCheckpointStats Stats;

public:
    explicit TPendingCheckpoint(
        THashSet<NActors::TActorId> toBeAcknowledged,
        NYql::NDqProto::ECheckpointType type,
        TPendingCheckpointStats stats = TPendingCheckpointStats());

    void Acknowledge(const NActors::TActorId& actorId);

    void Acknowledge(const NActors::TActorId& actorId, ui64 stateSize);

    [[nodiscard]]
    bool GotAllAcknowledges() const;

    [[nodiscard]]
    size_t NotYetAcknowledgedCount() const;

    [[nodiscard]]
    const TPendingCheckpointStats& GetStats() const;

    [[nodiscard]]
    NYql::NDqProto::ECheckpointType GetType() const;
};

class TPendingRestoreCheckpoint {
public:
    TPendingRestoreCheckpoint(TCheckpointId checkpointId, bool commitAfterRestore, THashSet<NActors::TActorId> toBeAcknowledged);

    void Acknowledge(const NActors::TActorId& actorId);

    [[nodiscard]]
    bool GotAllAcknowledges() const;

    [[nodiscard]]
    size_t NotYetAcknowledgedCount() const;

public:
    TCheckpointId CheckpointId;
    bool CommitAfterRestore;

private:
    THashSet<NActors::TActorId> NotYetAcknowledged;
};

// Class for storing all variables that are needed to make coordinator know
// when it can inject zero checkpoint after its initialization process.
// If there is no need to inject zero checkpoint this struct can be freed.
class TPendingInitCoordinator {
public:
    explicit TPendingInitCoordinator(size_t actorsCount)
        : ActorsCount(actorsCount)
    {
    }

    void OnNewCheckpointCoordinatorAck();
    bool AllNewCheckpointCoordinatorAcksProcessed() const;

    bool CanInjectCheckpoint() const;

public:
    const size_t ActorsCount;
    size_t NewCheckpointCoordinatorAcksGot = 0;
    TMaybe<TCheckpointId> CheckpointId;
};

} // namespace NFq
