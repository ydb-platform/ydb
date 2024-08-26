#pragma once

#include "public.h"

#include <yt/yt/client/api/rpc_proxy/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <util/generic/vector.h>

namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct TClientConfig
    : public virtual NYTree::TYsonStruct
{
    NApi::NRpcProxy::TConnectionConfigPtr Connection;
    TDuration InitialPenalty;

    REGISTER_YSON_STRUCT(TClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClientConfig)

////////////////////////////////////////////////////////////////////////////////

//! The options for hedging client.
struct THedgingClientOptions
    : public virtual NYTree::TYsonStructLite
{
    std::vector<TClientConfigPtr> ClientConfigs;

    struct TClientOptions
    {
        TClientOptions(
            NApi::IClientPtr client,
            const std::string& clusterName,
            TDuration initialPenalty,
            TCounterPtr counter = {});

        TClientOptions(
            NApi::IClientPtr client,
            TDuration initialPenalty,
            TCounterPtr counter = {});

        NApi::IClientPtr Client;
        std::string ClusterName;
        TDuration InitialPenalty;
        TCounterPtr Counter;
    };

    TDuration BanPenalty;
    TDuration BanDuration;
    THashMap<TString, TString> Tags;

    // This parameter is set on postprocessor.
    TVector<TClientOptions> Clients;

    REGISTER_YSON_STRUCT_LITE(THedgingClientOptions);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
