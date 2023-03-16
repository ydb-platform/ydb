#pragma once

#include <ydb/core/fq/libs/checkpointing_common/defs.h>

namespace NFq {

class TCheckpointIdGenerator {
private:
    TCoordinatorId CoordinatorId;
    ui64 NextNumber;

public:
    explicit TCheckpointIdGenerator(TCoordinatorId id, TCheckpointId lastCheckpoint = TCheckpointId(0, 0));

    TCheckpointId NextId();
};

} // namespace NFq
