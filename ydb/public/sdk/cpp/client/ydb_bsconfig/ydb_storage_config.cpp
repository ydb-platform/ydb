#include "ydb_storage_config.h"

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>

namespace NYdb::NStorageConfig {

class TStorageConfigClient::TImpl : public TClientImplCommon<TStorageConfigClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections)
        : TClientImplCommon(std::move(connections), TStorageConfigClientSettings{})
    {
    }

    TAsyncStatus ReplaceStorageConfig(const TString& config, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::BSConfig::ReplaceStorageConfigRequest>(settings);
        request.set_yaml_config(config);

        return RunSimple<Ydb::BSConfig::V1::BSConfigService, Ydb::BSConfig::ReplaceStorageConfigRequest, Ydb::BSConfig::ReplaceStorageConfigResponse>(
            std::move(request),
            &Ydb::BSConfig::V1::BSConfigService::Stub::AsyncReplaceStorageConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncFetchStorageConfigResult FetchStorageConfig(const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::BSConfig::FetchStorageConfigRequest>(settings);
        auto promise = NThreading::NewPromise<TFetchStorageConfigResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
                TString config;
                if (Ydb::BSConfig::FetchStorageConfigResult result; any && any->UnpackTo(&result)) {
                    config = result.yaml_config();
                }

                TFetchStorageConfigResult val(TStatus(std::move(status)), std::move(config));
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

};

TStorageConfigClient::TStorageConfigClient(const TDriver& driver)
    : Impl_(new TStorageConfigClient::TImpl(CreateInternalInterface(driver)))
{}

TAsyncStatus TStorageConfigClient::ReplaceStorageConfig(
    const TString& config,
    const TClusterConfigSettings& settings) {
    return Impl_->ReplaceStorageConfig(config, settings);
}


TAsyncFetchStorageConfigResult TStorageConfigClient::FetchStorageConfig(const TClusterConfigSettings& settings) {
    return Impl_->FetchStorageConfig(settings);
}


} // namespace NYdb::NStorageConfig
