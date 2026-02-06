#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NKqp {

// Creates the per-node TKqpQueryTextCacheService actor.
// This service replicates TNodeQueryTextCache entries across cluster nodes
// to enable cross-node breaker query text lookups for TLI deferred lock logging.
NActors::IActor* CreateKqpQueryTextCacheService();

} // namespace NKikimr::NKqp
