#pragma once

#include "events.h"

#include <ydb/core/protos/serverless_proxy_config.pb.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NHttpProxy {

    struct THttpProxyConfig {
        NKikimrConfig::TServerlessProxyConfig Config;
        std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
        bool UseSDK = false;
    };

    NActors::IActor* CreateHttpProxy(const THttpProxyConfig& config);

} // namespace
