#pragma once

#include <ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/types/status/status.h>
#include <ydb-cpp-sdk/client/types/request_settings.h>
#include <ydb-cpp-sdk/client/types/ydb.h>

#include <memory>
#include <string>

namespace NYdb::inline V3::NConfig {

struct TFetchConfigResult : public TStatus {
    TFetchConfigResult(
            TStatus&& status,
            std::string&& main_config,
            std::string&& storage_config)
        : TStatus(std::move(status))
        , MainConfig_(std::move(main_config))
        , StorageConfig_(std::move(storage_config))
    {}

    const std::string& GetMainConfig() const {
        return MainConfig_;
    }

    const std::string& GetStorageConfig() const {
        return StorageConfig_;
    }

private:
    std::string MainConfig_;
    std::string StorageConfig_;
};

using TAsyncFetchConfigResult = NThreading::TFuture<TFetchConfigResult>;

struct TReplaceConfigSettings : public NYdb::TOperationRequestSettings<TReplaceConfigSettings> {
    FLUENT_SETTING_OPTIONAL(bool, SwitchDedicatedStorageSection);
    FLUENT_SETTING_FLAG(DedicatedConfigMode);
    FLUENT_SETTING_FLAG(DryRun);
    FLUENT_SETTING_FLAG(AllowUnknownFields);
    FLUENT_SETTING_FLAG(AllowAbsentDatabase);
    FLUENT_SETTING_FLAG(AllowIncorrectVersion);
    FLUENT_SETTING_FLAG(AllowIncorrectCluster);
};

struct TFetchConfigSettings : public NYdb::TOperationRequestSettings<TFetchConfigSettings> {};

struct TBootstrapClusterSettings : public NYdb::TOperationRequestSettings<TBootstrapClusterSettings> {};

class TConfigClient {
public:
    explicit TConfigClient(const TDriver& driver, const TCommonClientSettings& settings = {});

    ~TConfigClient();

    // Replace config
    TAsyncStatus ReplaceConfig(
        const std::optional<std::string>& main_config,
        const std::optional<std::string>& storage_config,
        const TReplaceConfigSettings& settings = {});

    // Fetch current cluster storage config
    TAsyncFetchConfigResult FetchConfig(bool dedicated_storage_section, bool dedicated_cluster_section,
        const TFetchConfigSettings& settings = {});

    // Bootstrap cluster with automatic configuration
    TAsyncStatus BootstrapCluster(const std::string& selfAssemblyUUID, const TBootstrapClusterSettings& settings = {});

private:
    class TImpl;

    std::unique_ptr<TImpl> Impl_;
};

}
