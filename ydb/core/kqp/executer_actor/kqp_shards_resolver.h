#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NKqp {

NActors::IActor* CreateKqpShardsResolver(const NActors::TActorId& owner, ui64 txId, bool useFollowers, TSet<ui64>&& shardIds);

} // namespace NKikimr::NKqp
