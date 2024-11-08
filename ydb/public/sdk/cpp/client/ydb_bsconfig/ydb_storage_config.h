#pragma once

#include <ydb/public/api/grpc/ydb_bsconfig_v1.grpc.pb.h>
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

struct TStorageConfigClientSettings : public TCommonClientSettingsBase<TStorageConfigClientSettings> {
    using TSelf = TStorageConfigClientSettings;
};

struct TClusterConfigSettings : public NYdb::TOperationRequestSettings<TClusterConfigSettings> {};

class TStorageConfigClient {
public:
    class TImpl;

    explicit TStorageConfigClient(const TDriver& driver);

    // Replace config
    TAsyncStatus ReplaceStorageConfig(const TString& config, const TClusterConfigSettings& settings = {});

    // Fetch current cluster storage config
    TAsyncFetchStorageConfigResult FetchStorageConfig(const TClusterConfigSettings& settings = {});

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NStorageConfig
