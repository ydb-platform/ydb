#include "ydb_dynamic_config.h"

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>

namespace NYdb::NDynamicConfig {

class TDynamicConfigClient::TImpl : public TClientImplCommon<TDynamicConfigClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections)
        : TClientImplCommon(std::move(connections), TDynamicConfigClientSettings{})
    {
    }

    TAsyncStatus ApplyConfig(const TString& config, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::ApplyConfigRequest>(settings);
        request.set_config(config);

        return RunSimple<Ydb::DynamicConfig::V1::DynamicConfigService, Ydb::DynamicConfig::ApplyConfigRequest, Ydb::DynamicConfig::ApplyConfigResponse>(
            std::move(request),
            &Ydb::DynamicConfig::V1::DynamicConfigService::Stub::AsyncApplyConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus DropConfig(const TString& cluster, ui64 version, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::DropConfigRequest>(settings);

        request.set_cluster(cluster);
        request.set_version(version);

        return RunSimple<Ydb::DynamicConfig::V1::DynamicConfigService, Ydb::DynamicConfig::DropConfigRequest, Ydb::DynamicConfig::DropConfigResponse>(
            std::move(request),
            &Ydb::DynamicConfig::V1::DynamicConfigService::Stub::AsyncDropConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus AddVolatileConfig(const TString& config, ui64 id, ui64 version, const TString& cluster, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::AddVolatileConfigRequest>(settings);
        request.set_config(config);
        request.set_id(id);
        request.set_version(version);
        request.set_cluster(cluster);

        return RunSimple<Ydb::DynamicConfig::V1::DynamicConfigService, Ydb::DynamicConfig::AddVolatileConfigRequest, Ydb::DynamicConfig::AddVolatileConfigResponse>(
            std::move(request),
            &Ydb::DynamicConfig::V1::DynamicConfigService::Stub::AsyncAddVolatileConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus RemoveVolatileConfig(const TString& cluster, ui64 version, const TVector<ui64>& ids, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::RemoveVolatileConfigRequest>(settings);

        request.set_cluster(cluster);
        request.set_version(version);

        for (auto& id: ids) {
            request.add_ids(id);
        }

        return RunSimple<Ydb::DynamicConfig::V1::DynamicConfigService, Ydb::DynamicConfig::RemoveVolatileConfigRequest, Ydb::DynamicConfig::RemoveVolatileConfigResponse>(
            std::move(request),
            &Ydb::DynamicConfig::V1::DynamicConfigService::Stub::AsyncRemoveVolatileConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncGetConfigResult GetConfig(const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::GetConfigRequest>(settings);

        auto promise = NThreading::NewPromise<TGetConfigResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
                TString clusterName = "<unknown>";
                ui64 version = 0;
                TString config;
                TMap<ui64, TString> volatileConfigs;
                if (Ydb::DynamicConfig::GetConfigResponse result; any && any->UnpackTo(&result)) {
                    clusterName = result.cluster();
                    version = result.version();
                    config = result.config();
                    for (const auto& config : result.volatile_configs()) {
                        volatileConfigs.emplace(config.id(), config.config());
                    }
                }

                TGetConfigResult val(TStatus(std::move(status)), std::move(clusterName), version, std::move(config), std::move(volatileConfigs));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::DynamicConfig::V1::DynamicConfigService, Ydb::DynamicConfig::GetConfigRequest, Ydb::DynamicConfig::GetConfigResponse>(
            std::move(request),
            extractor,
            &Ydb::DynamicConfig::V1::DynamicConfigService::Stub::AsyncGetConfig,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncResolveConfigResult ResolveConfig(const TString& config, const TMap<ui64, TString>& volatileConfigs, const TMap<TString, TString>& labels, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::ResolveConfigRequest>(settings);
        request.set_config(config);
        for (auto& [id, volatileConfig] : volatileConfigs) {
            auto* proto = request.add_volatile_configs();
            proto->set_id(id);
            proto->set_config(volatileConfig);
        }
        for (auto& [name, value] : labels) {
            auto* proto = request.add_labels();
            proto->set_label(name);
            proto->set_value(value);
        }

        auto promise = NThreading::NewPromise<TResolveConfigResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
                TString config;
                if (Ydb::DynamicConfig::ResolveConfigResponse result; any && any->UnpackTo(&result)) {
                    config = result.config();
                }

                TResolveConfigResult val(TStatus(std::move(status)), std::move(config));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::DynamicConfig::V1::DynamicConfigService, Ydb::DynamicConfig::ResolveConfigRequest, Ydb::DynamicConfig::ResolveConfigResponse>(
            std::move(request),
            extractor,
            &Ydb::DynamicConfig::V1::DynamicConfigService::Stub::AsyncResolveConfig,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncResolveConfigResult ResolveConfig(const TString& config, const TMap<ui64, TString>& volatileConfigs, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::ResolveAllConfigRequest>(settings);
        request.set_config(config);
        for (auto& [id, volatileConfig] : volatileConfigs) {
            auto* proto = request.add_volatile_configs();
            proto->set_id(id);
            proto->set_config(volatileConfig);
        }

        auto promise = NThreading::NewPromise<TResolveConfigResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
                TString config;
                if (Ydb::DynamicConfig::ResolveAllConfigResponse result; any && any->UnpackTo(&result)) {
                    config = result.config();
                }

                TResolveConfigResult val(TStatus(std::move(status)), std::move(config));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::DynamicConfig::V1::DynamicConfigService, Ydb::DynamicConfig::ResolveAllConfigRequest, Ydb::DynamicConfig::ResolveAllConfigResponse>(
            std::move(request),
            extractor,
            &Ydb::DynamicConfig::V1::DynamicConfigService::Stub::AsyncResolveAllConfig,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }
};

TDynamicConfigClient::TDynamicConfigClient(const TDriver& driver)
    : Impl_(new TDynamicConfigClient::TImpl(CreateInternalInterface(driver)))
{}

TAsyncStatus TDynamicConfigClient::ApplyConfig(
    const TString& config,
    const TClusterConfigSettings& settings) {
    return Impl_->ApplyConfig(config, settings);
}

TAsyncStatus TDynamicConfigClient::DropConfig(
    const TString& cluster,
    ui64 version,
    const TClusterConfigSettings& settings) {
    return Impl_->DropConfig(cluster, version, settings);
}

TAsyncStatus TDynamicConfigClient::AddVolatileConfig(
    const TString& config,
    ui64 id,
    ui64 version,
    const TString& cluster,
    const TClusterConfigSettings& settings) {
    return Impl_->AddVolatileConfig(config, id, version, cluster, settings);
}

TAsyncStatus TDynamicConfigClient::RemoveVolatileConfig(
    const TString& cluster,
    ui64 version,
    const TVector<ui64>& ids,
    const TClusterConfigSettings& settings) {
    return Impl_->RemoveVolatileConfig(cluster, version, ids, settings);
}

TAsyncGetConfigResult TDynamicConfigClient::GetConfig(const TClusterConfigSettings& settings) {
    return Impl_->GetConfig(settings);
}

TAsyncResolveConfigResult TDynamicConfigClient::ResolveConfig(
    const TString& config,
    const TMap<ui64, TString>& volatileConfigs,
    const TMap<TString, TString>& labels,
    const TClusterConfigSettings& settings) {
    return Impl_->ResolveConfig(config, volatileConfigs, labels, settings);
}

TAsyncResolveConfigResult TDynamicConfigClient::ResolveConfig(
    const TString& config,
    const TMap<ui64, TString>& volatileConfigs,
    const TClusterConfigSettings& settings) {
    return Impl_->ResolveConfig(config, volatileConfigs, settings);
}

} // namespace NYdb::NDynamicConfig
