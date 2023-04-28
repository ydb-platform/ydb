#include "ydb_console.h"

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>

namespace NYdb::NConsole {

class TConsoleClient::TImpl : public TClientImplCommon<TConsoleClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections)
        : TClientImplCommon(std::move(connections), TConsoleClientSettings{})
    {
    }

    TAsyncStatus ApplyConfig(const TString& config, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::Console::ApplyConfigRequest>(settings);
        request.set_config(config);

        return RunSimple<Ydb::Console::V1::ConsoleService, Ydb::Console::ApplyConfigRequest, Ydb::Console::ApplyConfigResponse>(
            std::move(request),
            &Ydb::Console::V1::ConsoleService::Stub::AsyncApplyConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus DropConfig(const TString& cluster, ui64 version, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::Console::DropConfigRequest>(settings);

        request.set_cluster(cluster);
        request.set_version(version);

        return RunSimple<Ydb::Console::V1::ConsoleService, Ydb::Console::DropConfigRequest, Ydb::Console::DropConfigResponse>(
            std::move(request),
            &Ydb::Console::V1::ConsoleService::Stub::AsyncDropConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus AddVolatileConfig(const TString& config, ui64 id, ui64 version, const TString& cluster, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::Console::AddVolatileConfigRequest>(settings);
        request.set_config(config);
        request.set_id(id);
        request.set_version(version);
        request.set_cluster(cluster);

        return RunSimple<Ydb::Console::V1::ConsoleService, Ydb::Console::AddVolatileConfigRequest, Ydb::Console::AddVolatileConfigResponse>(
            std::move(request),
            &Ydb::Console::V1::ConsoleService::Stub::AsyncAddVolatileConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus RemoveVolatileConfig(const TString& cluster, ui64 version, const TVector<ui64>& ids, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::Console::RemoveVolatileConfigRequest>(settings);

        request.set_cluster(cluster);
        request.set_version(version);

        for (auto& id: ids) {
            request.add_ids(id);
        }

        return RunSimple<Ydb::Console::V1::ConsoleService, Ydb::Console::RemoveVolatileConfigRequest, Ydb::Console::RemoveVolatileConfigResponse>(
            std::move(request),
            &Ydb::Console::V1::ConsoleService::Stub::AsyncRemoveVolatileConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncGetConfigResult GetConfig(const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::Console::GetConfigRequest>(settings);

        auto promise = NThreading::NewPromise<TGetConfigResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
                TString clusterName = "<unknown>";
                ui64 version = 0;
                TString config;
                TMap<ui64, TString> volatileConfigs;
                if (Ydb::Console::GetConfigResponse result; any && any->UnpackTo(&result)) {
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

        Connections_->RunDeferred<Ydb::Console::V1::ConsoleService, Ydb::Console::GetConfigRequest, Ydb::Console::GetConfigResponse>(
            std::move(request),
            extractor,
            &Ydb::Console::V1::ConsoleService::Stub::AsyncGetConfig,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncResolveConfigResult ResolveConfig(const TString& config, const TMap<ui64, TString>& volatileConfigs, const TMap<TString, TString>& labels, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::Console::ResolveConfigRequest>(settings);
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
                if (Ydb::Console::ResolveConfigResponse result; any && any->UnpackTo(&result)) {
                    config = result.config();
                }

                TResolveConfigResult val(TStatus(std::move(status)), std::move(config));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::Console::V1::ConsoleService, Ydb::Console::ResolveConfigRequest, Ydb::Console::ResolveConfigResponse>(
            std::move(request),
            extractor,
            &Ydb::Console::V1::ConsoleService::Stub::AsyncResolveConfig,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncResolveConfigResult ResolveConfig(const TString& config, const TMap<ui64, TString>& volatileConfigs, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::Console::ResolveAllConfigRequest>(settings);
        request.set_config(config);
        for (auto& [id, volatileConfig] : volatileConfigs) {
            auto* proto = request.add_volatile_configs();
            proto->set_id(id);
            proto->set_config(volatileConfig);
        }

        auto promise = NThreading::NewPromise<TResolveConfigResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
                TString config;
                if (Ydb::Console::ResolveAllConfigResponse result; any && any->UnpackTo(&result)) {
                    config = result.config();
                }

                TResolveConfigResult val(TStatus(std::move(status)), std::move(config));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::Console::V1::ConsoleService, Ydb::Console::ResolveAllConfigRequest, Ydb::Console::ResolveAllConfigResponse>(
            std::move(request),
            extractor,
            &Ydb::Console::V1::ConsoleService::Stub::AsyncResolveAllConfig,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }
};

TConsoleClient::TConsoleClient(const TDriver& driver)
    : Impl_(new TConsoleClient::TImpl(CreateInternalInterface(driver)))
{}

TAsyncStatus TConsoleClient::ApplyConfig(
    const TString& config,
    const TClusterConfigSettings& settings) {
    return Impl_->ApplyConfig(config, settings);
}

TAsyncStatus TConsoleClient::DropConfig(
    const TString& cluster,
    ui64 version,
    const TClusterConfigSettings& settings) {
    return Impl_->DropConfig(cluster, version, settings);
}

TAsyncStatus TConsoleClient::AddVolatileConfig(
    const TString& config,
    ui64 id,
    ui64 version,
    const TString& cluster,
    const TClusterConfigSettings& settings) {
    return Impl_->AddVolatileConfig(config, id, version, cluster, settings);
}

TAsyncStatus TConsoleClient::RemoveVolatileConfig(
    const TString& cluster,
    ui64 version,
    const TVector<ui64>& ids,
    const TClusterConfigSettings& settings) {
    return Impl_->RemoveVolatileConfig(cluster, version, ids, settings);
}

TAsyncGetConfigResult TConsoleClient::GetConfig(const TClusterConfigSettings& settings) {
    return Impl_->GetConfig(settings);
}

TAsyncResolveConfigResult TConsoleClient::ResolveConfig(
    const TString& config,
    const TMap<ui64, TString>& volatileConfigs,
    const TMap<TString, TString>& labels,
    const TClusterConfigSettings& settings) {
    return Impl_->ResolveConfig(config, volatileConfigs, labels, settings);
}

TAsyncResolveConfigResult TConsoleClient::ResolveConfig(
    const TString& config,
    const TMap<ui64, TString>& volatileConfigs,
    const TClusterConfigSettings& settings) {
    return Impl_->ResolveConfig(config, volatileConfigs, settings);
}

} // namespace NYdb::NConsole
