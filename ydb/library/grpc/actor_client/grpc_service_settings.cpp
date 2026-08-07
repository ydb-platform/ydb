#include <ydb/library/grpc/actor_client/grpc_service_settings.h>

namespace NGrpcActorClient {

TGrpcClientSettings::TGrpcClientSettings(TString userAgentHint)
    : UserAgentHint(std::move(userAgentHint))
{}

} // namespace NGrpcActorClient
