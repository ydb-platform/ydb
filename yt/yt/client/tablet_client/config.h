#pragma once

#include "public.h"

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

class TTableMountCacheConfig
    : public TAsyncExpiringCacheConfig
{
public:
    //! If entry is requested for the first time then allow only client who requested the entry to wait for it.
    bool RejectIfEntryIsRequestedButNotReady;

    TTableMountCacheConfigPtr ApplyDynamic(const TTableMountCacheDynamicConfigPtr& dynamicConfig) const;

    REGISTER_YSON_STRUCT(TTableMountCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableMountCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TTableMountCacheDynamicConfig
    : public TAsyncExpiringCacheDynamicConfig
{
public:
    std::optional<bool> RejectIfEntryIsRequestedButNotReady;

    REGISTER_YSON_STRUCT(TTableMountCacheDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableMountCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TRemoteDynamicStoreReaderConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TDuration ClientReadTimeout;
    TDuration ServerReadTimeout;
    TDuration ClientWriteTimeout;
    TDuration ServerWriteTimeout;
    i64 MaxRowsPerServerRead;

    ssize_t WindowSize;

    // Testing option.
    double StreamingSubrequestFailureProbability;

    REGISTER_YSON_STRUCT(TRemoteDynamicStoreReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRemoteDynamicStoreReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TRetryingRemoteDynamicStoreReaderConfig
    : public TRemoteDynamicStoreReaderConfig
{
public:
    //! Maximum number of locate requests.
    int RetryCount;

    //! Time to wait between making another locate request.
    TDuration LocateRequestBackoffTime;

    REGISTER_YSON_STRUCT(TRetryingRemoteDynamicStoreReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRetryingRemoteDynamicStoreReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TReplicationCollocationOptions
    : public NYTree::TYsonStruct
{
public:
    std::optional<std::vector<TString>> PreferredSyncReplicaClusters;

    REGISTER_YSON_STRUCT(TReplicationCollocationOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicationCollocationOptions)

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableOptions
    : public TReplicationCollocationOptions
{
public:
    bool EnableReplicatedTableTracker;

    std::optional<int> MaxSyncReplicaCount;
    std::optional<int> MinSyncReplicaCount;

    TDuration SyncReplicaLagThreshold;

    // TODO(akozhikhov): We probably do not need these in this per-table config.
    TDuration TabletCellBundleNameTtl;
    TDuration RetryOnFailureInterval;

    bool EnablePreloadStateCheck;
    TDuration IncompletePreloadGracePeriod;

    std::tuple<int, int> GetEffectiveMinMaxReplicaCount(int replicaCount) const;

    REGISTER_YSON_STRUCT(TReplicatedTableOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicatedTableOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
