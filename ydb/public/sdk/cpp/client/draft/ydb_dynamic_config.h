#pragma once

#include <ydb/public/api/grpc/draft/ydb_dynamic_config_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_types/ydb.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/settings.h>
#include <ydb/public/sdk/cpp/client/ydb_types/request_settings.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <util/generic/string.h>
#include <util/generic/set.h>
#include <util/generic/map.h>
#include <util/system/types.h>

#include <memory>

namespace NYdb::NDynamicConfig {

struct TGetConfigResult : public TStatus {
    TGetConfigResult(
        TStatus&& status,
        TString&& clusterName,
        ui64 version,
        TString&& config,
        TMap<ui64, TString>&& volatileConfigs)
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

struct TGetMetadataResult : public TStatus {
    TGetMetadataResult(
        TStatus&& status,
        TString&& metadata,
        TMap<ui64, TString>&& volatileConfigs)
        : TStatus(std::move(status))
        , Metadata_(std::move(metadata))
        , VolatileConfigs_(std::move(volatileConfigs))
    {}

    const TString& GetMetadata() const {
        return Metadata_;
    }

    const TMap<ui64, TString>& GetVolatileConfigs() const {
        return VolatileConfigs_;
    }

private:
    TString Metadata_;
    TMap<ui64, TString> VolatileConfigs_;
};

using TAsyncGetMetadataResult = NThreading::TFuture<TGetMetadataResult>;

struct TResolveConfigResult : public TStatus {
    TResolveConfigResult(
        TStatus&& status,
        TString&& config)
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

struct TGetNodeLabelsResult : public TStatus {
    TGetNodeLabelsResult(
        TStatus&& status,
        TMap<TString, TString>&& labels)
        : TStatus(std::move(status))
        , Labels_(std::move(labels))
    {}

    const TMap<TString, TString>& GetLabels() const {
        return Labels_;
    }

private:
    TMap<TString, TString> Labels_;
};

using TAsyncGetNodeLabelsResult = NThreading::TFuture<TGetNodeLabelsResult>;

struct TVerboseResolveConfigResult : public TStatus {
    struct TLabel {
        enum class EType {
            Negative = 0,
            Empty,
            Common,
        };

        EType Type;
        TString Value;

        bool operator<(const TLabel& other) const {
            int lhs = static_cast<int>(Type);
            int rhs = static_cast<int>(other.Type);
            return std::tie(lhs, Value) < std::tie(rhs, other.Value);
        }

        bool operator==(const TLabel& other) const {
            int lhs = static_cast<int>(Type);
            int rhs = static_cast<int>(other.Type);
            return std::tie(lhs, Value) == std::tie(rhs, other.Value);
        }
    };

    using ConfigByLabelSet = TMap<TSet<TVector<TLabel>>, TString>;

    TVerboseResolveConfigResult(
        TStatus&& status,
        TSet<TString>&& labels,
        ConfigByLabelSet&& configs)
        : TStatus(std::move(status))
        , Labels_(std::move(labels))
        , Configs_(std::move(configs))
    {}

    const TSet<TString>& GetLabels() const {
        return Labels_;
    }

    const ConfigByLabelSet& GetConfigs() const {
        return Configs_;
    }

private:
    TSet<TString> Labels_;
    ConfigByLabelSet Configs_;
};

using TAsyncVerboseResolveConfigResult = NThreading::TFuture<TVerboseResolveConfigResult>;


struct TDynamicConfigClientSettings : public TCommonClientSettingsBase<TDynamicConfigClientSettings> {
    using TSelf = TDynamicConfigClientSettings;
};

struct TClusterConfigSettings : public NYdb::TOperationRequestSettings<TClusterConfigSettings> {};

class TDynamicConfigClient {
public:
    class TImpl;

    explicit TDynamicConfigClient(const TDriver& driver);

    // Set config
    TAsyncStatus SetConfig(const TString& config, bool dryRun = false, bool allowUnknownFields = false, const TClusterConfigSettings& settings = {});

    // Replace config
    TAsyncStatus ReplaceConfig(const TString& config, bool dryRun = false, bool allowUnknownFields = false, const TClusterConfigSettings& settings = {});

    // Drop config
    TAsyncStatus DropConfig(
        const TString& cluster,
        ui64 version,
        const TClusterConfigSettings& settings = {});

    // Add volatile config
    TAsyncStatus AddVolatileConfig(
        const TString& config,
        const TClusterConfigSettings& settings = {});

    // Remove specific volatile configs
    TAsyncStatus RemoveVolatileConfig(
        const TString& cluster,
        ui64 version,
        const TVector<ui64>& ids,
        const TClusterConfigSettings& settings = {});

    // Remove all volatile config
    TAsyncStatus RemoveAllVolatileConfigs(
        const TString& cluster,
        ui64 version,
        const TClusterConfigSettings& settings = {});

    // Remove specific volatile configs
    TAsyncStatus ForceRemoveVolatileConfig(
        const TVector<ui64>& ids,
        const TClusterConfigSettings& settings = {});

    // Remove all volatile config
    TAsyncStatus ForceRemoveAllVolatileConfigs(
        const TClusterConfigSettings& settings = {});

    // Get current cluster configs metadata
    TAsyncGetMetadataResult GetMetadata(const TClusterConfigSettings& settings = {});

    // Get current cluster configs
    TAsyncGetConfigResult GetConfig(const TClusterConfigSettings& settings = {});

    // Get current cluster configs
    TAsyncGetNodeLabelsResult GetNodeLabels(ui64 nodeId, const TClusterConfigSettings& settings = {});

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

    // Resolve arbitrary config for all label combinations
    TAsyncVerboseResolveConfigResult VerboseResolveConfig(
        const TString& config,
        const TMap<ui64, TString>& volatileConfigs,
        const TClusterConfigSettings& settings = {});

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NDynamicConfig
