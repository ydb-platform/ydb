#pragma once

#include "public.h"

#include <yt/yt/client/api/rpc_proxy/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <util/generic/vector.h>

namespace NYT::NClient::NCache {

////////////////////////////////////////////////////////////////////////////////

struct TClientsCacheConfig
    : public virtual NYTree::TYsonStruct
{
    NApi::NRpcProxy::TConnectionConfigPtr DefaultConfig;

    THashMap<TString, NApi::NRpcProxy::TConnectionConfigPtr> ClusterConfigs;

    REGISTER_YSON_STRUCT(TClientsCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClientsCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
