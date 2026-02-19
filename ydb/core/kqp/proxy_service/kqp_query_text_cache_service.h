#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NKqp {

// Creates the per-node TKqpQueryTextCacheService actor.
// Handles cross-node query text lookups for TLI deferred lock logging.
NActors::IActor* CreateKqpQueryTextCacheService();

} // namespace NKikimr::NKqp
