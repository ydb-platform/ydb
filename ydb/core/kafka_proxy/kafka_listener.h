#pragma once

#include <ydb/core/raw_socket/sock_listener.h>
#include "kafka_connection.h"

namespace NKafka {

using namespace NKikimr::NRawSocket;

inline NActors::IActor* CreateKafkaListener(
        const NActors::TActorId& poller, const TListenerSettings& settings, const NKikimrConfig::TKafkaProxyConfig& config,
        std::shared_ptr<NKafka::TInet64SecureStreamSocket::TServerMtlsCreds> serverCreds = nullptr
) {
    return CreateSocketListener(
        poller, settings,
        [=](const TActorId& listenerActorId, TIntrusivePtr<TSocketDescriptor> socket, TNetworkConfig::TSocketAddressType address) {
            return CreateKafkaConnection(listenerActorId, socket, address, config, serverCreds);
        },
        NKikimrServices::EServiceKikimr::KAFKA_PROXY, EErrorAction::Abort);
}

} // namespace NKafka
