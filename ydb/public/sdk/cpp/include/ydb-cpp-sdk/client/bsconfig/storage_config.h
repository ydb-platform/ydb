#pragma once

#include <ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/types/status/status.h>
#include <ydb-cpp-sdk/client/types/request_settings.h>
#include <ydb-cpp-sdk/client/types/ydb.h>

#include <memory>
#include <string>

namespace NYdb::inline V3::NStorageConfig {

struct TFetchStorageConfigResult : public TStatus {
    TFetchStorageConfigResult(
            TStatus&& status,
            std::string&& config,
            std::string&& storage_config)
        : TStatus(std::move(status))
        , Config_(std::move(config))
        , StorageConfig_(std::move(storage_config))
    {}

    const std::string& GetConfig() const {
        return Config_;
    }

    const std::string& GetStorageConfig() const {
        return StorageConfig_;
    }

private:
    std::string Config_;
    std::string StorageConfig_;
};

using TAsyncFetchStorageConfigResult = NThreading::TFuture<TFetchStorageConfigResult>;

struct TReplaceStorageConfigSettings : public NYdb::TOperationRequestSettings<TReplaceStorageConfigSettings> {};
struct TFetchStorageConfigSettings : public NYdb::TOperationRequestSettings<TFetchStorageConfigSettings> {};
struct TBootstrapClusterSettings : public NYdb::TOperationRequestSettings<TBootstrapClusterSettings> {};

class TStorageConfigClient {
public:
    explicit TStorageConfigClient(const TDriver& driver, const TCommonClientSettings& settings = {});

    ~TStorageConfigClient();

    // Replace config
    TAsyncStatus ReplaceStorageConfig(const std::optional<std::string>& yaml_config,
        const std::optional<std::string>& storage_yaml_config,
        std::optional<bool> switch_dedicated_storage_section,
        bool dedicated_config_mode,
        const TReplaceStorageConfigSettings& settings = {});

    // Fetch current cluster storage config
    TAsyncFetchStorageConfigResult FetchStorageConfig(bool dedicated_storage_section, bool dedicated_cluster_section,
        const TFetchStorageConfigSettings& settings = {});

    // Bootstrap cluster with automatic configuration
    TAsyncStatus BootstrapCluster(const std::string& selfAssemblyUUID, const TBootstrapClusterSettings& settings = {});

private:
    class TImpl;

    std::unique_ptr<TImpl> Impl_;
};

}
