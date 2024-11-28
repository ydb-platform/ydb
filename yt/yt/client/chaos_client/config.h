#pragma once

#include "public.h"

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

class TChaosCacheChannelConfig
    : public NRpc::TRetryingChannelConfig
    , public NRpc::TBalancingChannelConfig
{
public:
    REGISTER_YSON_STRUCT(TChaosCacheChannelConfig);

    static void Register(TRegistrar /*registrar*/)
    { }
};

DEFINE_REFCOUNTED_TYPE(TChaosCacheChannelConfig)

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardCacheConfig
    : public TAsyncExpiringCacheConfig
    , public TChaosCacheChannelConfig
{
public:
    bool EnableWatching;

    REGISTER_YSON_STRUCT(TReplicationCardCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicationCardCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardCacheDynamicConfig
    : public virtual NYTree::TYsonStruct
{
public:
    std::optional<bool> EnableWatching;

    REGISTER_YSON_STRUCT(TReplicationCardCacheDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicationCardCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

