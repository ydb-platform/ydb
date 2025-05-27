#pragma once

#include "public.h"

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct TChaosCacheChannelConfig
    : public NRpc::TRetryingChannelConfig
    , public NRpc::TBalancingChannelConfig
{
    REGISTER_YSON_STRUCT(TChaosCacheChannelConfig);

    static void Register(TRegistrar /*registrar*/)
    { }
};

DEFINE_REFCOUNTED_TYPE(TChaosCacheChannelConfig)

////////////////////////////////////////////////////////////////////////////////

struct TReplicationCardCacheConfig
    : public TAsyncExpiringCacheConfig
    , public TChaosCacheChannelConfig
{
    bool EnableWatching;

    REGISTER_YSON_STRUCT(TReplicationCardCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicationCardCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TReplicationCardCacheDynamicConfig
    : public virtual NYTree::TYsonStruct
{
    std::optional<bool> EnableWatching;

    REGISTER_YSON_STRUCT(TReplicationCardCacheDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicationCardCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

