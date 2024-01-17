#pragma once

#include "actors_factory.h"

#include <ydb/core/mon/mon.h>

#include <ydb/core/fq/libs/compute/common/run_actor_params.h>
#include <ydb/core/fq/libs/config/protos/pinger.pb.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/private_client/private_client.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>
#include <ydb/core/fq/libs/signer/signer.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/providers/dq/worker_manager/interface/counters.h>
#include <ydb/library/yql/providers/dq/actors/proto_builder.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/common/metrics/service_counters.h>
#include <ydb/library/yql/providers/pq/cm_client/client.h>

#include <ydb/public/lib/fq/scope.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/datetime/base.h>

namespace NFq {

NActors::IActor* CreateYdbRunActor(
    const NActors::TActorId& fetcherId,
    const ::NYql::NCommon::TServiceCounters& serviceCounters,
    TRunActorParams&& params,
    const IActorFactory::TPtr& actorFactory);

} /* NFq */
