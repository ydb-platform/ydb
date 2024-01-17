#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/raw_socket/sock_config.h>
#include <ydb/core/raw_socket/sock_impl.h>

namespace NPG {

using namespace NKikimr::NRawSocket;

NActors::IActor* CreatePGConnection(const TActorId& listenerActorId, 
                                    TIntrusivePtr<TSocketDescriptor> socket,
                                    TNetworkConfig::TSocketAddressType address,
                                    const NActors::TActorId& databaseProxy);

} // namespace NPG
