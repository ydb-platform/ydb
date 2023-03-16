#include "checkpoint_id_generator.h"

#include <ydb/core/fq/libs/checkpointing_common/defs.h>

namespace NFq {

TCheckpointIdGenerator::TCheckpointIdGenerator(TCoordinatorId coordinatorId, TCheckpointId lastCheckpoint)
    : CoordinatorId(std::move(coordinatorId)) {
    if (CoordinatorId.Generation > lastCheckpoint.CoordinatorGeneration) {
        NextNumber = 1;
    } else if (CoordinatorId.Generation == lastCheckpoint.CoordinatorGeneration) {
        NextNumber = lastCheckpoint.SeqNo + 1;
    } else {
        ythrow yexception() << "Unexpected CheckpointCoordinator generation: " << CoordinatorId.Generation
                            << " while last checkpoint has " << lastCheckpoint.CoordinatorGeneration;
    }
}

TCheckpointId NFq::TCheckpointIdGenerator::NextId() {
    return TCheckpointId(CoordinatorId.Generation, NextNumber++);
}

} // namespace NFq
