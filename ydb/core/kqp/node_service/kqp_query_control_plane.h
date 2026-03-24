#pragma once

#include <memory>

#include "kqp_node_state.h"

#include <ydb/library/actors/core/actor.h>

#include <ydb/core/kqp/compute_actor/kqp_compute_actor_factory.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>

namespace NKikimr::NKqp {

NActors::IActor* CreateKqpQueryManager(TIntrusivePtr<TKqpCounters>& counters, std::shared_ptr<TNodeState>& state,
    std::shared_ptr<NRm::IKqpResourceManager>& resourceManager, std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory>& caFactory);

} // namespace NKikimr::NKqp
