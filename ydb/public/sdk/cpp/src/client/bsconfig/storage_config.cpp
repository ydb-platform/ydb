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

    TAsyncStatus ReplaceStorageConfig(const std::string& config) {
        auto request = MakeRequest<Ydb::BSConfig::ReplaceStorageConfigRequest>();
        request.set_yaml_config(config);

        return RunSimple<Ydb::BSConfig::V1::BSConfigService, Ydb::BSConfig::ReplaceStorageConfigRequest, Ydb::BSConfig::ReplaceStorageConfigResponse>(
            std::move(request),
            &Ydb::BSConfig::V1::BSConfigService::Stub::AsyncReplaceStorageConfig);
    }

    TAsyncFetchStorageConfigResult FetchStorageConfig(const TStorageConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::BSConfig::FetchStorageConfigRequest>(settings);
        auto promise = NThreading::NewPromise<TFetchStorageConfigResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
                NYdb::TStringType config;
                if (Ydb::BSConfig::FetchStorageConfigResult result; any && any->UnpackTo(&result)) {
                    config = result.yaml_config();
                }

                TFetchStorageConfigResult val(TStatus(std::move(status)), std::string{std::move(config)});
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

    TAsyncStatus BootstrapCluster(const std::string& selfAssemblyUUID) {
        auto request = MakeRequest<Ydb::BSConfig::BootstrapClusterRequest>();
        request.set_self_assembly_uuid(selfAssemblyUUID);
        return RunSimple<Ydb::BSConfig::V1::BSConfigService, Ydb::BSConfig::BootstrapClusterRequest,
            Ydb::BSConfig::BootstrapClusterResponse>(std::move(request),
            &Ydb::BSConfig::V1::BSConfigService::Stub::AsyncBootstrapCluster);
    }
};

TStorageConfigClient::TStorageConfigClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TStorageConfigClient::TImpl(CreateInternalInterface(driver), settings))
{}

TStorageConfigClient::~TStorageConfigClient() = default;

TAsyncStatus TStorageConfigClient::ReplaceStorageConfig(const std::string& config) {
    return Impl_->ReplaceStorageConfig(config);
}

TAsyncFetchStorageConfigResult TStorageConfigClient::FetchStorageConfig(const TStorageConfigSettings& settings) {
    return Impl_->FetchStorageConfig(settings);
}

TAsyncStatus TStorageConfigClient::BootstrapCluster(const std::string& selfAssemblyUUID) {
    return Impl_->BootstrapCluster(selfAssemblyUUID);
}


}
