#pragma once

#include <ydb/core/fq/libs/compute/common/run_actor_params.h>

#include <yql/essentials/providers/common/metrics/service_counters.h>

#include <ydb-cpp-sdk/client/query/query.h>

#include <ydb/library/actors/core/actor.h>

namespace NFq {

std::unique_ptr<NActors::IActor> CreateInitializerActor(const TRunActorParams& params,
                                                        const NActors::TActorId& parent,
                                                        const NActors::TActorId& pinger,
                                                        const ::NYql::NCommon::TServiceCounters& queryCounters);

}
