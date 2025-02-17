#pragma once

#include <ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/types/status/status.h>
#include <ydb-cpp-sdk/client/types/request_settings.h>
#include <ydb-cpp-sdk/client/types/ydb.h>

#include <memory>
#include <string>

namespace NYdb::inline V3::NConfig {

struct TReplaceConfigSettings : public NYdb::TOperationRequestSettings<TReplaceConfigSettings> {
    FLUENT_SETTING_FLAG(DryRun);
    FLUENT_SETTING_FLAG(AllowUnknownFields);
    FLUENT_SETTING_FLAG(BypassChecks);
};

struct TFetchConfigSettings : public NYdb::TOperationRequestSettings<TFetchConfigSettings> {};

struct TBootstrapClusterSettings : public NYdb::TOperationRequestSettings<TBootstrapClusterSettings> {};

struct TMainConfigIdentity {
    ui64 Version;
    TString Cluster;
};

struct TStorageConfigIdentity {
    ui64 Version;
    TString Cluster;
};

struct TDatabaseConfigIdentity {
    ui64 Version;
    TString Cluster;
    TString Database;
};

using TKnownIdentitiyTypes = std::variant<
      std::monostate
    , TMainConfigMetadata
    , TStorageConfigMetadata
    , TDatabaseConfigMetadata
    >;

struct TConfig {
    TKnownIdentityTypes Identity;
    TString Config;
};

struct TFetchConfigResult : public TStatus {
    TFetchConfigResult(
            TStatus&& status,
            std::vector<TConfig>&& configs)
        : TStatus(std::move(status))
        , Config_(std::move(config))
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
        const TString& mainConfig,
        const TReplaceConfigSettings& settings = {});

    // Replace config
    TAsyncStatus ReplaceConfig(
        const TString& mainConfig,
        const TString& storageConfig,
        const TReplaceConfigSettings& settings = {});

    // Replace config
    TAsyncStatus ReplaceConfigDisableDedicatedStorageSection(
        const TString& mainConfig,
        const TReplaceConfigSettings& settings = {});

    // Replace config
    TAsyncStatus ReplaceConfigEnableDedicatedStorageSection(
        const TString& mainConfig,
        const TString& storageConfig,
        const TReplaceConfigSettings& settings = {});

    // Fetch current cluster storage config
    TAsyncFetchConfigResult FetchConfig(const TFetchConfigSettings& settings = {});

    // Bootstrap cluster with automatic configuration
    TAsyncStatus BootstrapCluster(
        const TString& selfAssemblyUUID,
        const TBootstrapClusterSettings& settings = {});

private:
    class TImpl;

    std::unique_ptr<TImpl> Impl_;
};

}
