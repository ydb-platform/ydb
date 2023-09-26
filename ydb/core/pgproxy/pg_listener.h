#pragma once

#include <ydb/core/raw_socket/sock_listener.h>
#include "pg_connection.h"

namespace NPG {

using namespace NKikimr::NRawSocket;

inline NActors::IActor* CreatePGListener(const NActors::TActorId& poller, const NActors::TActorId databaseProxy,
                                         const TListenerSettings& settings = {.Port = 5432}) {
    return CreateSocketListener(
        poller, settings,
        [=](const TActorId& listenerActorId, TIntrusivePtr<TSocketDescriptor> socket, TNetworkConfig::TSocketAddressType address) {
            return CreatePGConnection(listenerActorId, socket, address, databaseProxy);
        },
        NKikimrServices::EServiceKikimr::PGWIRE);
}

} // namespace NPG
