#pragma once

#include <ydb/core/protos/serverless_proxy_config.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

namespace NKikimr::NHttpProxy {

    struct TDiscoverySettings {
        TString DiscoveryEndpoint;
        TMaybe<TString> CaCert;
        TString Database;
    };

    NActors::IActor* CreateDiscoveryProxyActor(std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider, const NKikimrConfig::TServerlessProxyConfig& config);

} // namespace NKikimr::NHttpProxy
