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

using TAsyncFetchStorageConfigResult = NThreading::TFuture<TFetchStorageConfigResult>;

struct TStorageConfigSettings : public NYdb::TOperationRequestSettings<TStorageConfigSettings> {};

class TStorageConfigClient {
public:

    explicit TStorageConfigClient(const TDriver& driver, const TCommonClientSettings& settings = {});

    ~TStorageConfigClient();

    // Replace config
    TAsyncStatus ReplaceStorageConfig(const std::string& config);

    // Fetch current cluster storage config
    TAsyncFetchStorageConfigResult FetchStorageConfig(const TStorageConfigSettings& settings = {});

    // Bootstrap cluster with automatic configuration
    TAsyncStatus BootstrapCluster(const std::string& selfAssemblyUUID);

private:
    class TImpl;

    std::unique_ptr<TImpl> Impl_;
};

}
