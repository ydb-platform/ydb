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

struct TWatchedReplicationCardCacheConfig
    : public TAsyncExpiringCacheConfig
{
    TWatchedReplicationCardCacheConfigPtr ApplyDynamic(
        const TAsyncExpiringCacheDynamicConfigPtr& dynamicConfig) const;

    REGISTER_YSON_STRUCT(TWatchedReplicationCardCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TWatchedReplicationCardCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TReplicationCardCacheConfig
    : public TAsyncExpiringCacheConfig
    , public TChaosCacheChannelConfig
{
    bool EnableWatching;
    TWatchedReplicationCardCacheConfigPtr WatchedCacheConfig;

    TReplicationCardCacheConfigPtr ApplyDynamic(const TReplicationCardCacheDynamicConfigPtr& dynamicConfig) const;

    REGISTER_YSON_STRUCT(TReplicationCardCacheConfig);

    static void Register(TRegistrar registrar);

protected:
    void ApplyDynamicInplace(const TReplicationCardCacheDynamicConfigPtr& dynamicConfig);
};

DEFINE_REFCOUNTED_TYPE(TReplicationCardCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TReplicationCardCacheDynamicConfig
    : public TAsyncExpiringCacheDynamicConfig
{
    std::optional<bool> EnableWatching;
    TAsyncExpiringCacheDynamicConfigPtr WatchedCacheConfig;

    REGISTER_YSON_STRUCT(TReplicationCardCacheDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicationCardCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
