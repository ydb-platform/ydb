#pragma once

#include <library/cpp/actors/core/actor.h>
#include "pg_proxy_config.h"
#include "pg_proxy_impl.h"

namespace NPG {

NActors::IActor* CreatePGConnection(TIntrusivePtr<TSocketDescriptor> socket, TNetworkConfig::TSocketAddressType address, const NActors::TActorId& databaseProxy);

}
