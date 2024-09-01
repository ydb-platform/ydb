#pragma once

#include "public.h"

#include <yt/yt/client/api/rpc_proxy/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <util/generic/vector.h>

namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct TConnectionWithPenaltyConfig
    : public virtual NApi::NRpcProxy::TConnectionConfig
{
    TDuration InitialPenalty;

    REGISTER_YSON_STRUCT(TConnectionWithPenaltyConfig);

    static void Register(TRegistrar registrar);
};


////////////////////////////////////////////////////////////////////////////////

//! The options for hedging client.
//! TODO(bulatman) Rename to `THedgingClientConfig`.
struct THedgingClientOptions
    : public virtual NYTree::TYsonStruct
{
    std::vector<TIntrusivePtr<TConnectionWithPenaltyConfig>> Connections;
    TDuration BanPenalty;
    TDuration BanDuration;
    THashMap<TString, TString> Tags;

    REGISTER_YSON_STRUCT(THedgingClientOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THedgingClientOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
