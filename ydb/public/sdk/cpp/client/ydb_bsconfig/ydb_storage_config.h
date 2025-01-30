#pragma once

#include <ydb/public/api/grpc/ydb_bsconfig_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_types/ydb.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/settings.h>
#include <ydb/public/sdk/cpp/client/ydb_types/request_settings.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <util/generic/string.h>

#include <memory>

namespace NYdb::NStorageConfig {

struct TFetchStorageConfigResult : public TStatus {
    TFetchStorageConfigResult(
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

using TAsyncFetchStorageConfigResult = NThreading::TFuture<TFetchStorageConfigResult>;

struct TStorageConfigSettings : public NYdb::TOperationRequestSettings<TStorageConfigSettings> {};

class TStorageConfigClient {
public:

    explicit TStorageConfigClient(const TDriver& driver, const TCommonClientSettings& settings = {});

    ~TStorageConfigClient();

    // Replace config
    TAsyncStatus ReplaceStorageConfig(const TString& config);

    // Fetch current cluster storage config
    TAsyncFetchStorageConfigResult FetchStorageConfig(const TStorageConfigSettings& settings = {});

    // Bootstrap cluster with automatic configuration
    TAsyncStatus BootstrapCluster(const TString& selfAssemblyUUID);

private:
    class TImpl;

    std::unique_ptr<TImpl> Impl_;
};

}
