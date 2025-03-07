#pragma once

#include <ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/types/status/status.h>
#include <ydb-cpp-sdk/client/types/request_settings.h>
#include <ydb-cpp-sdk/client/types/ydb.h>

#include <memory>
#include <string>

namespace NYdb::inline Dev::NConfig {

struct TReplaceConfigSettings : public NYdb::TOperationRequestSettings<TReplaceConfigSettings> {
    FLUENT_SETTING_FLAG(DryRun);
    FLUENT_SETTING_FLAG(AllowUnknownFields);
    FLUENT_SETTING_FLAG(BypassChecks);
};

enum class EFetchAllConfigsTransform {
    NONE,
    DETACH_STORAGE_CONFIG_SECTION,
    ATTACH_STORAGE_CONFIG_SECTION,
};

struct TFetchAllConfigsSettings : public NYdb::TOperationRequestSettings<TFetchAllConfigsSettings> {
    FLUENT_SETTING(EFetchAllConfigsTransform, Transform);
};

struct TBootstrapClusterSettings : public NYdb::TOperationRequestSettings<TBootstrapClusterSettings> {};

struct TMainConfigIdentity {
    std::uint64_t Version;
    std::string Cluster;
};

struct TStorageConfigIdentity {
    std::uint64_t Version;
    std::string Cluster;
};

struct TDatabaseConfigIdentity {
    std::uint64_t Version;
    std::string Cluster;
    std::string Database;
};

using TIdentityTypes = std::variant<
      std::monostate
    , TMainConfigIdentity
    , TStorageConfigIdentity
    , TDatabaseConfigIdentity
    >;

struct TConfig {
    TIdentityTypes Identity;
    std::string Config;
};

struct TFetchConfigResult : public TStatus {
    TFetchConfigResult(
            TStatus&& status,
            std::vector<TConfig>&& configs)
        : TStatus(std::move(status))
        , Configs_(std::move(configs))
    {}

    const std::vector<TConfig>& GetConfigs() const {
        return Configs_;
    }

private:
    std::vector<TConfig> Configs_;
};

using TAsyncFetchConfigResult = NThreading::TFuture<TFetchConfigResult>;

class TConfigClient {
public:
    explicit TConfigClient(const TDriver& driver, const TCommonClientSettings& settings = {});

    ~TConfigClient();

    // Replace config
    TAsyncStatus ReplaceConfig(
        const std::string& mainConfig,
        const TReplaceConfigSettings& settings = {});

    // Replace config
    TAsyncStatus ReplaceConfig(
        const std::string& mainConfig,
        const std::string& storageConfig,
        const TReplaceConfigSettings& settings = {});

    // Replace config
    TAsyncStatus ReplaceConfigDisableDedicatedStorageSection(
        const std::string& mainConfig,
        const TReplaceConfigSettings& settings = {});

    // Replace config
    TAsyncStatus ReplaceConfigEnableDedicatedStorageSection(
        const std::string& mainConfig,
        const std::string& storageConfig,
        const TReplaceConfigSettings& settings = {});

    // Fetch current cluster storage config
    TAsyncFetchConfigResult FetchAllConfigs(const TFetchAllConfigsSettings& settings = {});

    // Bootstrap cluster with automatic configuration
    TAsyncStatus BootstrapCluster(
        const std::string& selfAssemblyUUID,
        const TBootstrapClusterSettings& settings = {});

private:
    class TImpl;

    std::unique_ptr<TImpl> Impl_;
};

}
