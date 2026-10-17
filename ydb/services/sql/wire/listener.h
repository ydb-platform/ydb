#pragma once

#include <ydb/core/raw_socket/sock_listener.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NSql {

NActors::IActor* CreateSqlListener(const NActors::TActorId& poller,
                                   const NRawSocket::TListenerSettings& settings);

} // namespace NKikimr::NSql
