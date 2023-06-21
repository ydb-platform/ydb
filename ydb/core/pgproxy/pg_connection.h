#pragma once

#include <library/cpp/actors/core/actor.h>
#include <ydb/core/raw_socket/sock_config.h>
#include <ydb/core/raw_socket/sock_impl.h>

namespace NPG {

using namespace NKikimr::NRawSocket;

NActors::IActor* CreatePGConnection(TIntrusivePtr<TSocketDescriptor> socket, TNetworkConfig::TSocketAddressType address,
                                    const NActors::TActorId& databaseProxy);

} // namespace NPG
