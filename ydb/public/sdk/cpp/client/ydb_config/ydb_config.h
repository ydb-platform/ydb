#pragma once

#include <ydb/public/api/grpc/ydb_config_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_types/ydb.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/settings.h>
#include <ydb/public/sdk/cpp/client/ydb_types/request_settings.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <util/generic/string.h>

#include <memory>
#include <vector>
#include <variant>

namespace NYdb::NConfig {

struct TMainConfigIdentity {

};

struct TStorageConfigIdentity {

};

struct TDatabaseConfigIdentity {

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

struct TConfigSettings : public NYdb::TOperationRequestSettings<TConfigSettings> {};

class TConfigClient {
public:

    explicit TConfigClient(const TDriver& driver, const TCommonClientSettings& settings = {});

    ~TConfigClient();

    // Replace config
    TAsyncStatus ReplaceConfig(const TString& mainConfig);

    // Replace config
    TAsyncStatus ReplaceConfig(const TString& mainConfig, const TString& storageConfig);

    // Replace config
    TAsyncStatus ReplaceConfigDisableDedicatedStorageSection(const TString& mainConfig);

    // Replace config
    TAsyncStatus ReplaceConfigEnableDedicatedStorageSection(const TString& mainConfig, const TString& storageConfig);

    // Fetch current cluster storage config
    TAsyncFetchConfigResult FetchConfig(const TConfigSettings& settings = {});

    // Bootstrap cluster with automatic configuration
    TAsyncStatus BootstrapCluster(const TString& selfAssemblyUUID);

private:
    class TImpl;

    std::unique_ptr<TImpl> Impl_;
};

}
