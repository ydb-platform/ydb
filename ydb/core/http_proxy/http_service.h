#pragma once

#include <ydb/core/protos/serverless_proxy_config.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

namespace NKikimr::NHttpProxy {

    struct THttpProxyConfig {
        NKikimrConfig::TServerlessProxyConfig Config;
        // Declared before CredentialsProvider: the provider stores a weak_ptr and must stop
        // while this facility is still alive.
        std::shared_ptr<NYdb::ICoreFacility> CoreFacility;
        std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
        bool UseSDK{false};
    };

    NActors::IActor* CreateHttpProxy(const THttpProxyConfig& config);

} // namespace NKikimr::NHttpProxy
