#pragma once

#include <ydb/core/fq/libs/compute/common/run_actor_params.h>

#include <yql/essentials/providers/common/metrics/service_counters.h>

#include <ydb/library/actors/core/actor.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/query.h>

namespace NFq {

std::unique_ptr<NActors::IActor> CreateExecuterActor(const TRunActorParams& params,
                                                     NYdb::NQuery::EStatsMode statsMode,
                                                     const NActors::TActorId& parent,
                                                     const NActors::TActorId& connector,
                                                     const NActors::TActorId& pinger,
                                                     const ::NYql::NCommon::TServiceCounters& queryCounters);

}
