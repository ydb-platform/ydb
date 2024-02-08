#pragma once

#include <ydb/core/protos/serverless_proxy_config.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>
#include <ydb/library/actors/core/actor.h>


namespace NKikimr::NHttpProxy {

    struct THttpProxyConfig {
        NKikimrConfig::TServerlessProxyConfig Config;
        std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
        bool UseSDK{false};
    };

    NActors::IActor* CreateHttpProxy(const THttpProxyConfig& config);

} // namespace NKikimr::NHttpProxy
