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

using TKnownIdentityTypes = std::variant<
      std::monostate
    , TMainConfigIdentity
    , TStorageConfigIdentity
    , TDatabaseConfigIdentity
    >;

struct TConfig {
    TIdentityTypes Identity;
    TString Config;
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
