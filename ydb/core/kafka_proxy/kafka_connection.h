#pragma once

#include <library/cpp/actors/core/actor.h>
#include <ydb/core/raw_socket/sock_config.h>
#include <ydb/core/raw_socket/sock_impl.h>

namespace NKafka {

using namespace NKikimr::NRawSocket;    

NActors::IActor* CreateKafkaConnection(TIntrusivePtr<TSocketDescriptor> socket, TNetworkConfig::TSocketAddressType address);

}
