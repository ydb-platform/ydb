#pragma once

#include <ydb/core/raw_socket/sock_impl.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NSql {

NActors::IActor* CreateSqlConnection(const NActors::TActorId& listenerActorId,
                                     TIntrusivePtr<NRawSocket::TSocketDescriptor> socket,
                                     NRawSocket::TNetworkConfig::TSocketAddressType address,
                                     ui32 internalErrorRetries);

} // namespace NKikimr::NSql
