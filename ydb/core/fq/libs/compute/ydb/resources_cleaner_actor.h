#pragma once

#include <ydb/core/fq/libs/compute/common/run_actor_params.h>

#include <ydb/library/yql/providers/common/metrics/service_counters.h>

#include <ydb/library/actors/core/actor.h>

namespace NFq {

std::unique_ptr<NActors::IActor> CreateResourcesCleanerActor(const TRunActorParams& params,
                                                             const NActors::TActorId& parent,
                                                             const NActors::TActorId& connector,
                                                             const NYdb::TOperation::TOperationId& operationId,
                                                             const ::NYql::NCommon::TServiceCounters& queryCounters);

}
