#pragma once

#include <ydb/core/fq/libs/compute/common/run_actor_params.h>
#include <ydb/core/fq/libs/compute/common/utils.h>
#include <ydb/core/fq/libs/metrics/status_code_counters.h>

#include <ydb/library/yql/providers/common/metrics/service_counters.h>

#include <ydb/library/actors/core/actor.h>

namespace NFq {

std::unique_ptr<NActors::IActor> CreateStatusTrackerActor(const TRunActorParams& params,
                                                          const NActors::TActorId& parent,
                                                          const NActors::TActorId& connector,
                                                          const NActors::TActorId& pinger,
                                                          const NYdb::TOperation::TOperationId& operationId,
                                                          std::unique_ptr<IPlanStatProcessor>&& processor,
                                                          const ::NYql::NCommon::TServiceCounters& queryCounters,
                                                          const NFq::TStatusCodeByScopeCounters::TPtr& failedStatusCodeCounters);

}
