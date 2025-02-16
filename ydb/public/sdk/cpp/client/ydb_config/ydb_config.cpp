#include "ydb_storage_config.h"

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>

namespace NYdb::NConfig {

class TConfigClient::TImpl : public TClientImplCommon<TConfigClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {
    }

    TAsyncStatus ReplaceConfig(const TString& config) {
        auto request = MakeRequest<Ydb::Config::ReplaceConfigRequest>();
        request.set_yaml_config(config);

        return RunSimple<Ydb::Config::V1::BSConfigService, Ydb::Config::ReplaceConfigRequest, Ydb::Config::ReplaceConfigResponse>(
            std::move(request),
            &Ydb::Config::V1::BSConfigService::Stub::AsyncReplaceConfig);
    }

    TAsyncFetchConfigResult FetchConfig(const TConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::Config::FetchConfigRequest>(settings);
        auto promise = NThreading::NewPromise<TFetchConfigResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
                TString config;
                if (Ydb::Config::FetchConfigResult result; any && any->UnpackTo(&result)) {
                    config = result.yaml_config();
                }

                TFetchConfigResult val(TStatus(std::move(status)), std::move(config));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::Config::V1::BSConfigService, Ydb::Config::FetchConfigRequest, Ydb::Config::FetchConfigResponse>(
            std::move(request),
            extractor,
            &Ydb::Config::V1::BSConfigService::Stub::AsyncFetchConfig,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));
        return promise.GetFuture();
    }

    TAsyncStatus BootstrapCluster(const TString& selfAssemblyUUID) {
        auto request = MakeRequest<Ydb::Config::BootstrapClusterRequest>();
        request.set_self_assembly_uuid(selfAssemblyUUID);

        return RunSimple<Ydb::Config::V1::BSConfigService, Ydb::Config::BootstrapClusterRequest,
            Ydb::Config::BootstrapClusterResponse>(std::move(request),
            &Ydb::Config::V1::BSConfigService::Stub::AsyncBootstrapCluster);
    }
};

TConfigClient::TConfigClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TConfigClient::TImpl(CreateInternalInterface(driver), settings))
{}

TConfigClient::~TConfigClient() = default;

TAsyncStatus TConfigClient::ReplaceConfig(const TString& config) {
    return Impl_->ReplaceConfig(config);
}

TAsyncFetchConfigResult TConfigClient::FetchConfig(const TConfigSettings& settings) {
    return Impl_->FetchConfig(settings);
}

TAsyncStatus TConfigClient::BootstrapCluster(const TString& selfAssemblyUUID) {
    return Impl_->BootstrapCluster(selfAssemblyUUID);
}

}
