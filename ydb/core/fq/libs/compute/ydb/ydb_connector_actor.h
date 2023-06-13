#pragma once

#include <ydb/core/fq/libs/compute/common/run_actor_params.h>

namespace NFq {

std::unique_ptr<NActors::IActor> CreateConnectorActor(const TRunActorParams& params);

}
