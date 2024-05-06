#include "pending_checkpoint.h"

namespace NFq {

TPendingCheckpoint::TPendingCheckpoint(
    THashSet<NActors::TActorId> toBeAcknowledged,
    NYql::NDqProto::ECheckpointType type,
    TPendingCheckpointStats stats)
    : NotYetAcknowledged(std::move(toBeAcknowledged))
    , Type(type)
    , Stats(std::move(stats)) {
}

void TPendingCheckpoint::Acknowledge(const NActors::TActorId& actorId) {
    Acknowledge(actorId, 0);
}

void TPendingCheckpoint::Acknowledge(const NActors::TActorId& actorId, ui64 stateSize) {
    NotYetAcknowledged.erase(actorId);
    Stats.StateSize += stateSize;
}

bool TPendingCheckpoint::GotAllAcknowledges() const {
    return NotYetAcknowledged.empty();
}

size_t TPendingCheckpoint::NotYetAcknowledgedCount() const {
    return NotYetAcknowledged.size();
}

NYql::NDqProto::ECheckpointType TPendingCheckpoint::GetType() const {
    return Type;   
}

const TPendingCheckpointStats& TPendingCheckpoint::GetStats() const {
    return Stats;
}

TPendingRestoreCheckpoint::TPendingRestoreCheckpoint(TCheckpointId checkpointId, bool commitAfterRestore, THashSet<NActors::TActorId> toBeAcknowledged)
    : CheckpointId(checkpointId)
    , CommitAfterRestore(commitAfterRestore)
    , NotYetAcknowledged(std::move(toBeAcknowledged)) {
}

void TPendingRestoreCheckpoint::Acknowledge(const NActors::TActorId& actorId) {
    NotYetAcknowledged.erase(actorId);
}

bool TPendingRestoreCheckpoint::GotAllAcknowledges() const {
    return NotYetAcknowledged.empty();
}

size_t TPendingRestoreCheckpoint::NotYetAcknowledgedCount() const {
    return NotYetAcknowledged.size();
}

void TPendingInitCoordinator::OnNewCheckpointCoordinatorAck() {
    ++NewCheckpointCoordinatorAcksGot;
    Y_ABORT_UNLESS(NewCheckpointCoordinatorAcksGot <= ActorsCount);
}

bool TPendingInitCoordinator::AllNewCheckpointCoordinatorAcksProcessed() const {
    return NewCheckpointCoordinatorAcksGot == ActorsCount;
}

bool TPendingInitCoordinator::CanInjectCheckpoint() const {
    return AllNewCheckpointCoordinatorAcksProcessed() && CheckpointId;
}

} // namespace NFq
