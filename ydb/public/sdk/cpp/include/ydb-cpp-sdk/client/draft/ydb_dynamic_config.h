#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/ydb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <memory>

namespace NYdb::inline Dev::NDynamicConfig {

struct TGetConfigResult : public TStatus {
    TGetConfigResult(
        TStatus&& status,
        std::string&& clusterName,
        uint64_t version,
        std::string&& config,
        std::map<uint64_t, std::string>&& volatileConfigs)
        : TStatus(std::move(status))
        , ClusterName_(std::move(clusterName))
        , Version_(version)
        , Config_(std::move(config))
        , VolatileConfigs_(std::move(volatileConfigs))
    {}

    const std::string& GetClusterName() const {
        return ClusterName_;
    }

    uint64_t GetVersion() const {
        return Version_;
    }

    const std::string& GetConfig() const {
        return Config_;
    }

    const std::map<uint64_t, std::string>& GetVolatileConfigs() const {
        return VolatileConfigs_;
    }

private:
    std::string ClusterName_;
    uint64_t Version_;
    std::string Config_;
    std::map<uint64_t, std::string> VolatileConfigs_;
};

using TAsyncGetConfigResult = NThreading::TFuture<TGetConfigResult>;

struct TGetMetadataResult : public TStatus {
    TGetMetadataResult(
        TStatus&& status,
        std::string&& metadata,
        std::map<uint64_t, std::string>&& volatileConfigs)
        : TStatus(std::move(status))
        , Metadata_(std::move(metadata))
        , VolatileConfigs_(std::move(volatileConfigs))
    {}

    const std::string& GetMetadata() const {
        return Metadata_;
    }

    const std::map<uint64_t, std::string>& GetVolatileConfigs() const {
        return VolatileConfigs_;
    }

private:
    std::string Metadata_;
    std::map<uint64_t, std::string> VolatileConfigs_;
};

using TAsyncGetMetadataResult = NThreading::TFuture<TGetMetadataResult>;

struct TResolveConfigResult : public TStatus {
    TResolveConfigResult(
        TStatus&& status,
        std::string&& config)
        : TStatus(std::move(status))
        , Config_(std::move(config))
    {}

    const std::string& GetConfig() const {
        return Config_;
    }

private:
    std::string Config_;
};

using TAsyncResolveConfigResult = NThreading::TFuture<TResolveConfigResult>;

struct TGetNodeLabelsResult : public TStatus {
    TGetNodeLabelsResult(
        TStatus&& status,
        std::map<std::string, std::string>&& labels)
        : TStatus(std::move(status))
        , Labels_(std::move(labels))
    {}

    const std::map<std::string, std::string>& GetLabels() const {
        return Labels_;
    }

private:
    std::map<std::string, std::string> Labels_;
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
        std::string Value;

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

    using ConfigByLabelSet = std::map<std::set<std::vector<TLabel>>, std::string>;

    TVerboseResolveConfigResult(
        TStatus&& status,
        std::set<std::string>&& labels,
        ConfigByLabelSet&& configs)
        : TStatus(std::move(status))
        , Labels_(std::move(labels))
        , Configs_(std::move(configs))
    {}

    const std::set<std::string>& GetLabels() const {
        return Labels_;
    }

    const ConfigByLabelSet& GetConfigs() const {
        return Configs_;
    }

private:
    std::set<std::string> Labels_;
    ConfigByLabelSet Configs_;
};

using TAsyncVerboseResolveConfigResult = NThreading::TFuture<TVerboseResolveConfigResult>;

struct TFetchStartupConfigResult : public TStatus {
    TFetchStartupConfigResult(TStatus&& status, std::string&& config)
        : TStatus(std::move(status))
        , Config_(std::move(config))
    {}

    const std::string& GetConfig() const {
        return Config_;
    }

private:
    std::string Config_;
};

using TAsyncFetchStartupConfigResult = NThreading::TFuture<TFetchStartupConfigResult>;

struct TDynamicConfigClientSettings : public TCommonClientSettingsBase<TDynamicConfigClientSettings> {
    using TSelf = TDynamicConfigClientSettings;
};

struct TClusterConfigSettings : public NYdb::TOperationRequestSettings<TClusterConfigSettings> {};

class TDynamicConfigClient {
public:
    class TImpl;

    explicit TDynamicConfigClient(const TDriver& driver);

    // Set config
    TAsyncStatus SetConfig(const std::string& config, bool dryRun = false, bool allowUnknownFields = false, const TClusterConfigSettings& settings = {});

    // Replace config
    TAsyncStatus ReplaceConfig(const std::string& config, bool dryRun = false, bool allowUnknownFields = false, const TClusterConfigSettings& settings = {});

    // Drop config
    TAsyncStatus DropConfig(
        const std::string& cluster,
        uint64_t version,
        const TClusterConfigSettings& settings = {});

    // Add volatile config
    TAsyncStatus AddVolatileConfig(
        const std::string& config,
        const TClusterConfigSettings& settings = {});

    // Remove specific volatile configs
    TAsyncStatus RemoveVolatileConfig(
        const std::string& cluster,
        uint64_t version,
        const std::vector<uint64_t>& ids,
        const TClusterConfigSettings& settings = {});

    // Remove all volatile config
    TAsyncStatus RemoveAllVolatileConfigs(
        const std::string& cluster,
        uint64_t version,
        const TClusterConfigSettings& settings = {});

    // Remove specific volatile configs
    TAsyncStatus ForceRemoveVolatileConfig(
        const std::vector<uint64_t>& ids,
        const TClusterConfigSettings& settings = {});

    // Remove all volatile config
    TAsyncStatus ForceRemoveAllVolatileConfigs(
        const TClusterConfigSettings& settings = {});

    // Get current cluster configs metadata
    TAsyncGetMetadataResult GetMetadata(const TClusterConfigSettings& settings = {});

    // Get current cluster configs
    TAsyncGetConfigResult GetConfig(const TClusterConfigSettings& settings = {});

    // Get current cluster configs
    TAsyncGetNodeLabelsResult GetNodeLabels(uint64_t nodeId, const TClusterConfigSettings& settings = {});

    // Resolve arbitrary config for specific labels
    TAsyncResolveConfigResult ResolveConfig(
        const std::string& config,
        const std::map<uint64_t, std::string>& volatileConfigs,
        const std::map<std::string, std::string>& labels,
        const TClusterConfigSettings& settings = {});

    // Resolve arbitrary config for all label combinations
    TAsyncResolveConfigResult ResolveConfig(
        const std::string& config,
        const std::map<uint64_t, std::string>& volatileConfigs,
        const TClusterConfigSettings& settings = {});

    // Resolve arbitrary config for all label combinations
    TAsyncVerboseResolveConfigResult VerboseResolveConfig(
        const std::string& config,
        const std::map<uint64_t, std::string>& volatileConfigs,
        const TClusterConfigSettings& settings = {});

    // Fetch startup config
    TAsyncFetchStartupConfigResult FetchStartupConfig(const TClusterConfigSettings& settings = {});

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NDynamicConfig
