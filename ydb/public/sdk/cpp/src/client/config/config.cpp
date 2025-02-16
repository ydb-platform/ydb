#include <ydb-cpp-sdk/client/config/config.h>

#include <src/client/common_client/impl/client.h>
#include <src/client/impl/ydb_internal/make_request/make.h>

#include <ydb/public/api/grpc/ydb_config_v1.grpc.pb.h>

namespace NYdb::inline V3::NConfig {

class TConfigClient::TImpl : public TClientImplCommon<TConfigClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {
    }

    TAsyncStatus ReplaceConfig(
            const std::optional<std::string>& main_config,
            const std::optional<std::string>& storage_config,
            const TReplaceConfigSettings& settings)
    {
        auto request = MakeRequest<Ydb::Config::ReplaceConfigRequest>();

        Y_UNUSED(main_config, storage_config, settings); // FIXME

        // if (yaml_config) {
        //     request.set_main_config(*yaml_config);
        // }

        // if (storage_yaml_config) {
        //     request.set_storage_config(*storage_yaml_config);
        // }

        // if (settings.SwitchDedicatedStorageSection_) {
        //     request.set_switch_dedicated_storage_section(*settings.SwitchDedicatedStorageSection_);
        // }

        // request.set_dedicated_config_mode(settings.DedicatedConfigMode_);
        // request.set_dry_run(settings.DryRun_);
        // request.set_allow_unknown_fields(settings.AllowUnknownFields_);
        // request.set_allow_absent_database(settings.AllowAbsentDatabase_);
        // request.set_allow_incorrect_version(settings.AllowIncorrectVersion_);
        // request.set_allow_incorrect_cluster(settings.AllowIncorrectCluster_);

        return RunSimple<Ydb::Config::V1::ConfigService, Ydb::Config::ReplaceConfigRequest, Ydb::Config::ReplaceConfigResponse>(
            std::move(request),
            &Ydb::Config::V1::ConfigService::Stub::AsyncReplaceConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncFetchConfigResult FetchConfig(bool dedicated_storage_section, bool dedicated_cluster_section,
            const TFetchConfigSettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::Config::FetchConfigRequest>(settings);
        Y_UNUSED(dedicated_storage_section, dedicated_cluster_section, settings); // FIXME
        // if (dedicated_storage_section) {
        //     request.set_dedicated_storage_section(true);
        // }
        // if (dedicated_cluster_section) {
        //     request.set_dedicated_cluster_section(true);
        // }
        auto promise = NThreading::NewPromise<TFetchConfigResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
            NYdb::TStringType config;
            NYdb::TStringType storage_config;
            Y_UNUSED(any);
            // if (Ydb::Config::FetchConfigResult result; any && any->UnpackTo(&result)) {
            //     config = result.main_config();
            //     storage_config = result.storage_config();
            // }

            TFetchConfigResult val(TStatus(std::move(status)), std::string{std::move(config)},
                std::string{std::move(storage_config)});
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

    TAsyncStatus BootstrapCluster(const std::string& selfAssemblyUUID, const TBootstrapClusterSettings& settings) {
        auto request = MakeRequest<Ydb::Config::BootstrapClusterRequest>();
        request.set_self_assembly_uuid(selfAssemblyUUID);
        return RunSimple<Ydb::Config::V1::ConfigService, Ydb::Config::BootstrapClusterRequest,
            Ydb::Config::BootstrapClusterResponse>(std::move(request),
            &Ydb::Config::V1::ConfigService::Stub::AsyncBootstrapCluster,
            TRpcRequestSettings::Make(settings));
    }
};

TConfigClient::TConfigClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TConfigClient::TImpl(CreateInternalInterface(driver), settings))
{}

TConfigClient::~TConfigClient() = default;

TAsyncStatus TConfigClient::ReplaceConfig(
        const std::optional<std::string>& yaml_config,
        const std::optional<std::string>& storage_yaml_config,
        const TReplaceConfigSettings& settings)
{
    return Impl_->ReplaceConfig(yaml_config, storage_yaml_config, settings);
}

TAsyncFetchConfigResult TConfigClient::FetchConfig(bool dedicated_storage_section,
        bool dedicated_cluster_section, const TFetchConfigSettings& settings)
{
    return Impl_->FetchConfig(dedicated_storage_section, dedicated_cluster_section, settings);
}

TAsyncStatus TConfigClient::BootstrapCluster(const std::string& selfAssemblyUUID, const TBootstrapClusterSettings& settings) {
    return Impl_->BootstrapCluster(selfAssemblyUUID, settings);
}


}
