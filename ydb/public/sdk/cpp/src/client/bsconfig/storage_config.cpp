#include <ydb-cpp-sdk/client/bsconfig/storage_config.h>

#include <src/client/common_client/impl/client.h>
#include <src/client/impl/ydb_internal/make_request/make.h>

#include <ydb/public/api/grpc/ydb_bsconfig_v1.grpc.pb.h>

namespace NYdb::inline V3::NStorageConfig {

class TStorageConfigClient::TImpl : public TClientImplCommon<TStorageConfigClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {
    }

    TAsyncStatus ReplaceStorageConfig(const std::optional<std::string>& yaml_config,
            const std::optional<std::string>& storage_yaml_config,
            std::optional<bool> switch_dedicated_storage_section,
            bool dedicated_config_mode,
            const TReplaceStorageConfigSettings& settings) {
        auto request = MakeRequest<Ydb::BSConfig::ReplaceStorageConfigRequest>();

        if (yaml_config) {
            request.set_yaml_config(*yaml_config);
        }
        if (storage_yaml_config) {
            request.set_storage_yaml_config(*storage_yaml_config);
        }
        if (switch_dedicated_storage_section) {
            request.set_switch_dedicated_storage_section(*switch_dedicated_storage_section);
        }
        request.set_dedicated_config_mode(dedicated_config_mode);

        return RunSimple<Ydb::BSConfig::V1::BSConfigService, Ydb::BSConfig::ReplaceStorageConfigRequest, Ydb::BSConfig::ReplaceStorageConfigResponse>(
            std::move(request),
            &Ydb::BSConfig::V1::BSConfigService::Stub::AsyncReplaceStorageConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncFetchStorageConfigResult FetchStorageConfig(bool dedicated_storage_section, bool dedicated_cluster_section,
			const TFetchStorageConfigSettings& settings) {
        auto request = MakeOperationRequest<Ydb::BSConfig::FetchStorageConfigRequest>(settings);
        if (dedicated_storage_section) {
            request.set_dedicated_storage_section(true);
        }
        if (dedicated_cluster_section) {
            request.set_dedicated_cluster_section(true);
        }
        auto promise = NThreading::NewPromise<TFetchStorageConfigResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
            NYdb::TStringType config;
            NYdb::TStringType storage_config;
            if (Ydb::BSConfig::FetchStorageConfigResult result; any && any->UnpackTo(&result)) {
                config = result.yaml_config();
                storage_config = result.storage_yaml_config();
            }

            TFetchStorageConfigResult val(TStatus(std::move(status)), std::string{std::move(config)},
                std::string{std::move(storage_config)});
            promise.SetValue(std::move(val));
        };

        Connections_->RunDeferred<Ydb::BSConfig::V1::BSConfigService, Ydb::BSConfig::FetchStorageConfigRequest, Ydb::BSConfig::FetchStorageConfigResponse>(
            std::move(request),
            extractor,
            &Ydb::BSConfig::V1::BSConfigService::Stub::AsyncFetchStorageConfig,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));
        return promise.GetFuture();
    }

    TAsyncStatus BootstrapCluster(const std::string& selfAssemblyUUID, const TBootstrapClusterSettings& settings) {
        auto request = MakeRequest<Ydb::BSConfig::BootstrapClusterRequest>();
        request.set_self_assembly_uuid(selfAssemblyUUID);
        return RunSimple<Ydb::BSConfig::V1::BSConfigService, Ydb::BSConfig::BootstrapClusterRequest,
            Ydb::BSConfig::BootstrapClusterResponse>(std::move(request),
            &Ydb::BSConfig::V1::BSConfigService::Stub::AsyncBootstrapCluster,
            TRpcRequestSettings::Make(settings));
    }
};

TStorageConfigClient::TStorageConfigClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TStorageConfigClient::TImpl(CreateInternalInterface(driver), settings))
{}

TStorageConfigClient::~TStorageConfigClient() = default;

TAsyncStatus TStorageConfigClient::ReplaceStorageConfig(const std::optional<std::string>& yaml_config,
        const std::optional<std::string>& storage_yaml_config, std::optional<bool> switch_dedicated_storage_section,
        bool dedicated_config_mode, const TReplaceStorageConfigSettings& settings) {
    return Impl_->ReplaceStorageConfig(yaml_config, storage_yaml_config, switch_dedicated_storage_section,
        dedicated_config_mode, settings);
}

TAsyncFetchStorageConfigResult TStorageConfigClient::FetchStorageConfig(bool dedicated_storage_section,
        bool dedicated_cluster_section, const TFetchStorageConfigSettings& settings) {
    return Impl_->FetchStorageConfig(dedicated_storage_section, dedicated_cluster_section, settings);
}

TAsyncStatus TStorageConfigClient::BootstrapCluster(const std::string& selfAssemblyUUID, const TBootstrapClusterSettings& settings) {
    return Impl_->BootstrapCluster(selfAssemblyUUID, settings);
}


}
