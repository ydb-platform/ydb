#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/raw_socket/sock_config.h>
#include <ydb/core/raw_socket/sock_impl.h>

namespace NKafka {

using namespace NKikimr::NRawSocket;

NActors::IActor* CreateKafkaConnection(const TActorId& listenerActorId,
                                       TIntrusivePtr<TSocketDescriptor> socket,
                                       TNetworkConfig::TSocketAddressType address,
                                       const NKikimrConfig::TKafkaProxyConfig& config,
                                       const TActorId& discoveryCacheActorId);

} // namespace NKafka
