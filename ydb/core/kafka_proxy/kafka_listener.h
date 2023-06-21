#pragma once

#include <ydb/core/raw_socket/sock_listener.h>
#include "kafka_connection.h"

namespace NKafka {

using namespace NKikimr::NRawSocket;

inline NActors::IActor* CreateKafkaListener(const NActors::TActorId& poller, const TListenerSettings& settings = {.Port = 9092}) {
    return CreateSocketListener(
        poller, settings,
        [](TIntrusivePtr<TSocketDescriptor> socket, TNetworkConfig::TSocketAddressType address) {
            return CreateKafkaConnection(socket, address);
        },
        NKikimrServices::EServiceKikimr::KAFKA_PROXY);
}

} // namespace NKafka
