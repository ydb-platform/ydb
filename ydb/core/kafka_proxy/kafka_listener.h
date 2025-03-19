#pragma once

#include <ydb/core/raw_socket/sock_listener.h>
#include "kafka_connection.h"

namespace NKafka {

using namespace NKikimr::NRawSocket;


TActorId MakeKafkaDiscoveryCacheID();

inline NActors::IActor* CreateKafkaListener(
        const NActors::TActorId& poller, const TListenerSettings& settings, const NKikimrConfig::TKafkaProxyConfig& config,
        const TActorId& discoveryCacheActorId
) {
    return CreateSocketListener(
        poller, settings,
        [=](const TActorId& listenerActorId, TIntrusivePtr<TSocketDescriptor> socket, TNetworkConfig::TSocketAddressType address) {
            return CreateKafkaConnection(listenerActorId, socket, address, config, discoveryCacheActorId);
        },
        NKikimrServices::EServiceKikimr::KAFKA_PROXY, EErrorAction::Abort);
}

} // namespace NKafka
