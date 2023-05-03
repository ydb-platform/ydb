#pragma once

#include <ydb/public/api/grpc/draft/ydb_dynamic_config_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_types/ydb.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/settings.h>
#include <ydb/public/sdk/cpp/client/ydb_types/request_settings.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <util/generic/string.h>
#include <util/generic/map.h>
#include <util/system/types.h>

#include <memory>

namespace NYdb::NDynamicConfig {

struct TGetConfigResult : public TStatus {
    TGetConfigResult(
        TStatus status,
        TString clusterName,
        ui64 version,
        TString config,
        TMap<ui64, TString> volatileConfigs)
        : TStatus(std::move(status))
        , ClusterName_(std::move(clusterName))
        , Version_(version)
        , Config_(std::move(config))
        , VolatileConfigs_(std::move(volatileConfigs))
    {}

    const TString& GetClusterName() const {
        return ClusterName_;
    }

    ui64 GetVersion() const {
        return Version_;
    }

    const TString& GetConfig() const {
        return Config_;
    }

    const TMap<ui64, TString>& GetVolatileConfigs() const {
        return VolatileConfigs_;
    }

private:
    TString ClusterName_;
    ui64 Version_;
    TString Config_;
    TMap<ui64, TString> VolatileConfigs_;
};

using TAsyncGetConfigResult = NThreading::TFuture<TGetConfigResult>;

struct TResolveConfigResult : public TStatus {
    TResolveConfigResult(
        TStatus status,
        TString config)
        : TStatus(std::move(status))
        , Config_(std::move(config))
    {}

    const TString& GetConfig() const {
        return Config_;
    }

private:
    TString Config_;
};

using TAsyncResolveConfigResult = NThreading::TFuture<TResolveConfigResult>;

struct TDynamicConfigClientSettings : public TCommonClientSettingsBase<TDynamicConfigClientSettings> {
    using TSelf = TDynamicConfigClientSettings;
};

struct TClusterConfigSettings : public NYdb::TOperationRequestSettings<TClusterConfigSettings> {};

class TDynamicConfigClient {
public:
    class TImpl;

    explicit TDynamicConfigClient(const TDriver& driver);

    // Apply config
    TAsyncStatus ApplyConfig(const TString& config, const TClusterConfigSettings& settings = {});

    // Drop config
    TAsyncStatus DropConfig(
        const TString& cluster,
        ui64 version,
        const TClusterConfigSettings& settings = {});

    // Add volatile config
    TAsyncStatus AddVolatileConfig(
        const TString& config,
        ui64 id, ui64 version,
        const TString& cluster,
        const TClusterConfigSettings& settings = {});

    // Remove specific volatile config or all of them
    TAsyncStatus RemoveVolatileConfig(
        const TString& cluster,
        ui64 version,
        const TVector<ui64>& ids,
        const TClusterConfigSettings& settings = {});

    // Get current cluster configs
    TAsyncGetConfigResult GetConfig(const TClusterConfigSettings& settings = {});

    // Resolve arbitrary config for specific labels
    TAsyncResolveConfigResult ResolveConfig(
        const TString& config,
        const TMap<ui64, TString>& volatileConfigs,
        const TMap<TString, TString>& labels,
        const TClusterConfigSettings& settings = {});

    // Resolve arbitrary config for all label combinations
    TAsyncResolveConfigResult ResolveConfig(
        const TString& config,
        const TMap<ui64, TString>& volatileConfigs,
        const TClusterConfigSettings& settings = {});

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NDynamicConfig
