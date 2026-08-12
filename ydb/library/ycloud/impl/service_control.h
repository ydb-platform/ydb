#pragma once

#include <ydb/library/grpc/actor_client/grpc_service_settings.h>
#include <ydb/library/ycloud/api/service_control.h>

namespace NCloud {

struct TServiceControlSettings : NGrpcActorClient::TGrpcClientSettings {};

NActors::IActor* CreateServiceControl(const TServiceControlSettings& settings);

} // namespace NCloud
