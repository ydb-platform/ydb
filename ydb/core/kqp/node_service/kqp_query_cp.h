#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NKqp {

NActors::IActor* CreateKqpQueryManager(std::shared_ptr<TNodeState>& state, std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory>& caFactory);

} // namespace NKikimr::NKqp
