#include <ydb-cpp-sdk/client/config/config.h>

#include <src/client/common_client/impl/client.h>
#include <src/client/impl/ydb_internal/make_request/make.h>

#include <ydb/public/api/grpc/ydb_config_v1.grpc.pb.h>

namespace NYdb::inline Dev::NConfig {

class TConfigClient::TImpl : public TClientImplCommon<TConfigClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {
    }

    TAsyncStatus ReplaceConfig(const std::string& mainConfig, const TReplaceConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::Config::ReplaceConfigRequest>(settings);
        request.set_replace(mainConfig);

        ApplyReplaceSettings(request, settings);

        return RunSimple<Ydb::Config::V1::ConfigService, Ydb::Config::ReplaceConfigRequest, Ydb::Config::ReplaceConfigResponse>(
            std::move(request),
            &Ydb::Config::V1::ConfigService::Stub::AsyncReplaceConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus ReplaceConfig(const std::string& mainConfig, const std::string& storageConfig, const TReplaceConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::Config::ReplaceConfigRequest>(settings);
        auto& replace = *request.mutable_replace_with_dedicated_storage_section();
        replace.set_main_config(mainConfig);
        replace.set_storage_config(storageConfig);

        ApplyReplaceSettings(request, settings);

        return RunSimple<Ydb::Config::V1::ConfigService, Ydb::Config::ReplaceConfigRequest, Ydb::Config::ReplaceConfigResponse>(
            std::move(request),
            &Ydb::Config::V1::ConfigService::Stub::AsyncReplaceConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus ReplaceConfigDisableDedicatedStorageSection(const std::string& mainConfig, const TReplaceConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::Config::ReplaceConfigRequest>(settings);
        request.set_replace_disable_dedicated_storage_section(mainConfig);

        ApplyReplaceSettings(request, settings);

        return RunSimple<Ydb::Config::V1::ConfigService, Ydb::Config::ReplaceConfigRequest, Ydb::Config::ReplaceConfigResponse>(
            std::move(request),
            &Ydb::Config::V1::ConfigService::Stub::AsyncReplaceConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus ReplaceConfigEnableDedicatedStorageSection(const std::string& mainConfig, const std::string& storageConfig, const TReplaceConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::Config::ReplaceConfigRequest>(settings);
        auto& replace = *request.mutable_replace_enable_dedicated_storage_section();
        replace.set_main_config(mainConfig);
        replace.set_storage_config(storageConfig);

        ApplyReplaceSettings(request, settings);

        return RunSimple<Ydb::Config::V1::ConfigService, Ydb::Config::ReplaceConfigRequest, Ydb::Config::ReplaceConfigResponse>(
            std::move(request),
            &Ydb::Config::V1::ConfigService::Stub::AsyncReplaceConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncFetchConfigResult FetchAllConfigs(const TFetchAllConfigsSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::Config::FetchConfigRequest>(settings);
        request.mutable_all();

        auto promise = NThreading::NewPromise<TFetchConfigResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
                std::vector<TConfig> configs;
                if (Ydb::Config::FetchConfigResult result; any && any->UnpackTo(&result)) {
                    for (const auto& entry : result.config()) {
                        TIdentityTypes identity;

                        switch (entry.identity().type_case()) {
                        case Ydb::Config::ConfigIdentity::TypeCase::kMain:
                            identity = TMainConfigIdentity {
                                .Version = entry.identity().version(),
                                .Cluster = entry.identity().cluster(),
                            };
                            break;
                        case Ydb::Config::ConfigIdentity::TypeCase::kStorage:
                            identity = TStorageConfigIdentity {
                                .Version = entry.identity().version(),
                                .Cluster = entry.identity().cluster(),
                            };
                            break;
                        case Ydb::Config::ConfigIdentity::TypeCase::kDatabase:
                            identity = TDatabaseConfigIdentity {
                                .Version = entry.identity().version(),
                                .Cluster = entry.identity().cluster(),
                                .Database = entry.identity().database().database(),
                            };
                            break;
                        case Ydb::Config::ConfigIdentity::TypeCase::TYPE_NOT_SET:
                            break; // leave in monostate; uknown identity
                        }

                        configs.push_back(TConfig{
                                .Identity = identity,
                                .Config = entry.config(),
                            });
                    }
                }

                TFetchConfigResult val(TStatus(std::move(status)), std::move(configs));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::Config::V1::ConfigService, Ydb::Config::FetchConfigRequest, Ydb::Config::FetchConfigResponse>(
            std::move(request),
            extractor,
            &Ydb::Config::V1::ConfigService::Stub::AsyncFetchConfig,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));
        return promise.GetFuture();
    }

    TAsyncStatus BootstrapCluster(const std::string& selfAssemblyUUID, const TBootstrapClusterSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::Config::BootstrapClusterRequest>(settings);
        request.set_self_assembly_uuid(selfAssemblyUUID);

        return RunSimple<Ydb::Config::V1::ConfigService, Ydb::Config::BootstrapClusterRequest,
            Ydb::Config::BootstrapClusterResponse>(std::move(request),
            &Ydb::Config::V1::ConfigService::Stub::AsyncBootstrapCluster);
    }
private:
    static void ApplyReplaceSettings(auto& request, const TReplaceConfigSettings& settings) {
        request.set_dry_run(settings.DryRun_);
        request.set_allow_unknown_fields(settings.AllowUnknownFields_);
        request.set_bypass_checks(settings.BypassChecks_);
    }
};

TConfigClient::TConfigClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TConfigClient::TImpl(CreateInternalInterface(driver), settings))
{}

TConfigClient::~TConfigClient() = default;

TAsyncStatus TConfigClient::ReplaceConfig(
    const std::string& mainConfig,
    const TReplaceConfigSettings& settings)
{
    return Impl_->ReplaceConfig(mainConfig, settings);
}

TAsyncStatus TConfigClient::ReplaceConfig(
    const std::string& mainConfig,
    const std::string& storageConfig,
    const TReplaceConfigSettings& settings)
{
    return Impl_->ReplaceConfig(mainConfig, storageConfig, settings);
}

TAsyncStatus TConfigClient::ReplaceConfigDisableDedicatedStorageSection(
    const std::string& mainConfig,
    const TReplaceConfigSettings& settings)
{
    return Impl_->ReplaceConfigDisableDedicatedStorageSection(mainConfig, settings);
}

TAsyncStatus TConfigClient::ReplaceConfigEnableDedicatedStorageSection(
    const std::string& mainConfig,
    const std::string& storageConfig,
    const TReplaceConfigSettings& settings)
{
    return Impl_->ReplaceConfigEnableDedicatedStorageSection(mainConfig, storageConfig, settings);
}

TAsyncFetchConfigResult TConfigClient::FetchAllConfigs(const TFetchAllConfigsSettings& settings) {
    return Impl_->FetchAllConfigs(settings);
}

TAsyncStatus TConfigClient::BootstrapCluster(
    const std::string& selfAssemblyUUID,
    const TBootstrapClusterSettings& settings)
{
    return Impl_->BootstrapCluster(selfAssemblyUUID, settings);
}

}
