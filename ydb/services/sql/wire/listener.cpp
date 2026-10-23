#include "listener.h"

#include "connection.h"

#include <ydb/library/services/services.pb.h>

namespace NKikimr::NSql {

NActors::IActor* CreateSqlListener(const NActors::TActorId& poller,
                                   const NRawSocket::TListenerSettings& settings,
                                   ui32 internalErrorRetries,
                                   ui32 recursionLimit) {
    return NRawSocket::CreateSocketListener(
        poller,
        settings,
        [internalErrorRetries, recursionLimit](const NActors::TActorId& listenerActorId,
           TIntrusivePtr<NRawSocket::TSocketDescriptor> socket,
           NRawSocket::TNetworkConfig::TSocketAddressType address) {
            return CreateSqlConnection(listenerActorId, std::move(socket), std::move(address), internalErrorRetries, recursionLimit);
        },
        NKikimrServices::PGWIRE);
}

} // namespace NKikimr::NSql
