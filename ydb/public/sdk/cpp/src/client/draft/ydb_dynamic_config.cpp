#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_dynamic_config.h>

#include <ydb/public/api/grpc/draft/ydb_dynamic_config_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>
#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/make_request/make.h>

namespace NYdb::inline Dev::NDynamicConfig {

class TDynamicConfigClient::TImpl : public TClientImplCommon<TDynamicConfigClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections)
        : TClientImplCommon(std::move(connections), TDynamicConfigClientSettings{})
    {
    }

    TAsyncStatus SetConfig(const std::string& config, bool dryRun, bool allowUnknownFields, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::SetConfigRequest>(settings);
        request.set_config(TStringType{config});
        request.set_dry_run(dryRun);
        request.set_allow_unknown_fields(allowUnknownFields);

        return RunSimple<Ydb::DynamicConfig::V1::DynamicConfigService, Ydb::DynamicConfig::SetConfigRequest, Ydb::DynamicConfig::SetConfigResponse>(
            std::move(request),
            &Ydb::DynamicConfig::V1::DynamicConfigService::Stub::AsyncSetConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus ReplaceConfig(const std::string& config, bool dryRun, bool allowUnknownFields, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::ReplaceConfigRequest>(settings);
        request.set_config(TStringType{config});
        request.set_dry_run(dryRun);
        request.set_allow_unknown_fields(allowUnknownFields);

        return RunSimple<Ydb::DynamicConfig::V1::DynamicConfigService, Ydb::DynamicConfig::ReplaceConfigRequest, Ydb::DynamicConfig::ReplaceConfigResponse>(
            std::move(request),
            &Ydb::DynamicConfig::V1::DynamicConfigService::Stub::AsyncReplaceConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus DropConfig(const std::string& cluster, uint64_t version, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::DropConfigRequest>(settings);

        request.mutable_identity()->set_cluster(TStringType{cluster});
        request.mutable_identity()->set_version(version);

        return RunSimple<Ydb::DynamicConfig::V1::DynamicConfigService, Ydb::DynamicConfig::DropConfigRequest, Ydb::DynamicConfig::DropConfigResponse>(
            std::move(request),
            &Ydb::DynamicConfig::V1::DynamicConfigService::Stub::AsyncDropConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus AddVolatileConfig(const std::string& config, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::AddVolatileConfigRequest>(settings);
        request.set_config(TStringType{config});

        return RunSimple<Ydb::DynamicConfig::V1::DynamicConfigService, Ydb::DynamicConfig::AddVolatileConfigRequest, Ydb::DynamicConfig::AddVolatileConfigResponse>(
            std::move(request),
            &Ydb::DynamicConfig::V1::DynamicConfigService::Stub::AsyncAddVolatileConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus RemoveVolatileConfig(const std::string& cluster, uint64_t version, const std::vector<uint64_t>& ids, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::RemoveVolatileConfigRequest>(settings);

        request.mutable_identity()->set_cluster(TStringType{cluster});
        request.mutable_identity()->set_version(version);

        for (auto& id: ids) {
            request.mutable_ids()->add_ids(id);
        }

        return RunSimple<Ydb::DynamicConfig::V1::DynamicConfigService, Ydb::DynamicConfig::RemoveVolatileConfigRequest, Ydb::DynamicConfig::RemoveVolatileConfigResponse>(
            std::move(request),
            &Ydb::DynamicConfig::V1::DynamicConfigService::Stub::AsyncRemoveVolatileConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus RemoveAllVolatileConfigs(const std::string& cluster, uint64_t version, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::RemoveVolatileConfigRequest>(settings);

        request.mutable_identity()->set_cluster(TStringType{cluster});
        request.mutable_identity()->set_version(version);
        request.set_all(true);

        return RunSimple<Ydb::DynamicConfig::V1::DynamicConfigService, Ydb::DynamicConfig::RemoveVolatileConfigRequest, Ydb::DynamicConfig::RemoveVolatileConfigResponse>(
            std::move(request),
            &Ydb::DynamicConfig::V1::DynamicConfigService::Stub::AsyncRemoveVolatileConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus ForceRemoveVolatileConfig(const std::vector<uint64_t>& ids, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::RemoveVolatileConfigRequest>(settings);

        for (auto& id: ids) {
            request.mutable_ids()->add_ids(id);
        }

        request.set_force(true);

        return RunSimple<Ydb::DynamicConfig::V1::DynamicConfigService, Ydb::DynamicConfig::RemoveVolatileConfigRequest, Ydb::DynamicConfig::RemoveVolatileConfigResponse>(
            std::move(request),
            &Ydb::DynamicConfig::V1::DynamicConfigService::Stub::AsyncRemoveVolatileConfig,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus ForceRemoveAllVolatileConfigs(const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::RemoveVolatileConfigRequest>(settings);

        request.set_all(true);
        request.set_force(true);

        return RunSimple<Ydb::DynamicConfig::V1::DynamicConfigService, Ydb::DynamicConfig::RemoveVolatileConfigRequest, Ydb::DynamicConfig::RemoveVolatileConfigResponse>(
            std::move(request),
            &Ydb::DynamicConfig::V1::DynamicConfigService::Stub::AsyncRemoveVolatileConfig,
            TRpcRequestSettings::Make(settings));
    }


    TAsyncGetNodeLabelsResult GetNodeLabels(uint64_t nodeId, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::GetNodeLabelsRequest>(settings);
        request.set_node_id(nodeId);

        auto promise = NThreading::NewPromise<TGetNodeLabelsResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
                std::map<std::string, std::string> labels;
                if (Ydb::DynamicConfig::GetNodeLabelsResult result; any && any->UnpackTo(&result)) {
                    for (auto& label : result.labels()) {
                        labels[label.label()] = label.value();
                    }
                }

                TGetNodeLabelsResult val(TStatus(std::move(status)), std::move(labels));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::DynamicConfig::V1::DynamicConfigService, Ydb::DynamicConfig::GetNodeLabelsRequest, Ydb::DynamicConfig::GetNodeLabelsResponse>(
            std::move(request),
            extractor,
            &Ydb::DynamicConfig::V1::DynamicConfigService::Stub::AsyncGetNodeLabels,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncGetConfigResult GetConfig(const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::GetConfigRequest>(settings);

        auto promise = NThreading::NewPromise<TGetConfigResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
                std::string clusterName = "<unknown>";
                uint64_t version = 0;
                std::string config;
                std::map<uint64_t, std::string> volatileConfigs;
                if (Ydb::DynamicConfig::GetConfigResult result; any && any->UnpackTo(&result)) {
                    // only if they are present
                    if (result.identity_size() && result.config_size()) {
                        clusterName = result.identity(0).cluster();
                        version = result.identity(0).version();
                        config = result.config(0);
                    }
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

    TAsyncGetMetadataResult GetMetadata(const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::GetMetadataRequest>(settings);

        auto promise = NThreading::NewPromise<TGetMetadataResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
                std::string metadata;
                std::map<uint64_t, std::string> volatileConfigs;
                if (Ydb::DynamicConfig::GetMetadataResult result; any && any->UnpackTo(&result)) {
                    metadata = result.metadata();
                    for (const auto& config : result.volatile_configs()) {
                        volatileConfigs.emplace(config.id(), config.metadata());
                    }
                }

                TGetMetadataResult val(TStatus(std::move(status)),std::move(metadata), std::move(volatileConfigs));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::DynamicConfig::V1::DynamicConfigService, Ydb::DynamicConfig::GetMetadataRequest, Ydb::DynamicConfig::GetMetadataResponse>(
            std::move(request),
            extractor,
            &Ydb::DynamicConfig::V1::DynamicConfigService::Stub::AsyncGetMetadata,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncResolveConfigResult ResolveConfig(const std::string& config, const std::map<uint64_t, std::string>& volatileConfigs, const std::map<std::string, std::string>& labels, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::ResolveConfigRequest>(settings);
        request.set_config(TStringType{config});
        for (auto& [id, volatileConfig] : volatileConfigs) {
            auto* proto = request.add_volatile_configs();
            proto->set_id(id);
            proto->set_config(TStringType{volatileConfig});
        }
        for (auto& [name, value] : labels) {
            auto* proto = request.add_labels();
            proto->set_label(TStringType{name});
            proto->set_value(TStringType{value});
        }

        auto promise = NThreading::NewPromise<TResolveConfigResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
                std::string config;
                if (Ydb::DynamicConfig::ResolveConfigResult result; any && any->UnpackTo(&result)) {
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

    TAsyncResolveConfigResult ResolveConfig(const std::string& config, const std::map<uint64_t, std::string>& volatileConfigs, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::ResolveAllConfigRequest>(settings);
        request.set_config(TStringType{config});
        for (auto& [id, volatileConfig] : volatileConfigs) {
            auto* proto = request.add_volatile_configs();
            proto->set_id(id);
            proto->set_config(TStringType{volatileConfig});
        }

        auto promise = NThreading::NewPromise<TResolveConfigResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
                std::string config;
                if (Ydb::DynamicConfig::ResolveAllConfigResult result; any && any->UnpackTo(&result)) {
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

    TAsyncVerboseResolveConfigResult VerboseResolveConfig(const std::string& config, const std::map<uint64_t, std::string>& volatileConfigs, const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::ResolveAllConfigRequest>(settings);
        request.set_config(TStringType{config});
        for (auto& [id, volatileConfig] : volatileConfigs) {
            auto* proto = request.add_volatile_configs();
            proto->set_id(id);
            proto->set_config(TStringType{volatileConfig});
        }
        request.set_verbose_response(true);

        auto promise = NThreading::NewPromise<TVerboseResolveConfigResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
            auto convert = [] (const Ydb::DynamicConfig::YamlLabelExt::LabelType& label) -> TVerboseResolveConfigResult::TLabel::EType {
                switch(label) {
                case Ydb::DynamicConfig::YamlLabelExt::NOT_SET:
                    return TVerboseResolveConfigResult::TLabel::EType::Negative;
                case Ydb::DynamicConfig::YamlLabelExt::COMMON:
                    return TVerboseResolveConfigResult::TLabel::EType::Common;
                case Ydb::DynamicConfig::YamlLabelExt::EMPTY:
                    return TVerboseResolveConfigResult::TLabel::EType::Empty;
                default:
                    Y_ABORT("unexpected enum value");
                }
            };

            std::set<std::string> labels;
            TVerboseResolveConfigResult::ConfigByLabelSet configs;

            if (Ydb::DynamicConfig::ResolveAllConfigResult result; any && any->UnpackTo(&result)) {
                for (auto& config : result.configs()) {
                    std::set<std::vector<TVerboseResolveConfigResult::TLabel>> labelSets;
                    for (auto& labelSet : config.label_sets()) {
                        std::vector<TVerboseResolveConfigResult::TLabel> set;
                        for (auto& label : labelSet.labels()) {
                            labels.insert(label.label());
                            set.push_back(TVerboseResolveConfigResult::TLabel{convert(label.type()), label.value()});
                        }
                        labelSets.insert(set);
                    }
                    configs[labelSets] = config.config();
                }
            }

            TVerboseResolveConfigResult val(TStatus(std::move(status)), std::move(labels), std::move(configs));
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

    TAsyncFetchStartupConfigResult FetchStartupConfig(const TClusterConfigSettings& settings = {}) {
        auto request = MakeOperationRequest<Ydb::DynamicConfig::FetchStartupConfigRequest>(settings);

        auto promise = NThreading::NewPromise<TFetchStartupConfigResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
                std::string config;
                if (Ydb::DynamicConfig::FetchStartupConfigResult result; any && any->UnpackTo(&result)) {
                    config = result.config();
                }

                TFetchStartupConfigResult val(TStatus(std::move(status)), std::move(config));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::DynamicConfig::V1::DynamicConfigService, Ydb::DynamicConfig::FetchStartupConfigRequest, Ydb::DynamicConfig::FetchStartupConfigResponse>(
            std::move(request),
            extractor,
            &Ydb::DynamicConfig::V1::DynamicConfigService::Stub::AsyncFetchStartupConfig,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }
};

TDynamicConfigClient::TDynamicConfigClient(const TDriver& driver)
    : Impl_(new TDynamicConfigClient::TImpl(CreateInternalInterface(driver)))
{}

TAsyncStatus TDynamicConfigClient::SetConfig(
    const std::string& config,
    bool dryRun,
    bool allowUnknownFields,
    const TClusterConfigSettings& settings) {
    return Impl_->SetConfig(config, dryRun, allowUnknownFields, settings);
}

TAsyncStatus TDynamicConfigClient::ReplaceConfig(
    const std::string& config,
    bool dryRun,
    bool allowUnknownFields,
    const TClusterConfigSettings& settings) {
    return Impl_->ReplaceConfig(config, dryRun, allowUnknownFields, settings);
}

TAsyncStatus TDynamicConfigClient::DropConfig(
    const std::string& cluster,
    uint64_t version,
    const TClusterConfigSettings& settings) {
    return Impl_->DropConfig(cluster, version, settings);
}

TAsyncStatus TDynamicConfigClient::AddVolatileConfig(
    const std::string& config,
    const TClusterConfigSettings& settings) {
    return Impl_->AddVolatileConfig(config, settings);
}

TAsyncStatus TDynamicConfigClient::RemoveVolatileConfig(
    const std::string& cluster,
    uint64_t version,
    const std::vector<uint64_t>& ids,
    const TClusterConfigSettings& settings) {
    return Impl_->RemoveVolatileConfig(cluster, version, ids, settings);
}

TAsyncStatus TDynamicConfigClient::RemoveAllVolatileConfigs(
    const std::string& cluster,
    uint64_t version,
    const TClusterConfigSettings& settings) {
    return Impl_->RemoveAllVolatileConfigs(cluster, version, settings);
}

TAsyncStatus TDynamicConfigClient::ForceRemoveVolatileConfig(
    const std::vector<uint64_t>& ids,
    const TClusterConfigSettings& settings) {
    return Impl_->ForceRemoveVolatileConfig(ids, settings);
}

TAsyncStatus TDynamicConfigClient::ForceRemoveAllVolatileConfigs(
    const TClusterConfigSettings& settings) {
    return Impl_->ForceRemoveAllVolatileConfigs(settings);
}

TAsyncGetMetadataResult TDynamicConfigClient::GetMetadata(const TClusterConfigSettings& settings) {
    return Impl_->GetMetadata(settings);
}

TAsyncGetConfigResult TDynamicConfigClient::GetConfig(const TClusterConfigSettings& settings) {
    return Impl_->GetConfig(settings);
}

TAsyncGetNodeLabelsResult TDynamicConfigClient::GetNodeLabels(uint64_t nodeId, const TClusterConfigSettings& settings) {
    return Impl_->GetNodeLabels(nodeId, settings);
}

TAsyncResolveConfigResult TDynamicConfigClient::ResolveConfig(
    const std::string& config,
    const std::map<uint64_t, std::string>& volatileConfigs,
    const std::map<std::string, std::string>& labels,
    const TClusterConfigSettings& settings) {
    return Impl_->ResolveConfig(config, volatileConfigs, labels, settings);
}

TAsyncResolveConfigResult TDynamicConfigClient::ResolveConfig(
    const std::string& config,
    const std::map<uint64_t, std::string>& volatileConfigs,
    const TClusterConfigSettings& settings) {
    return Impl_->ResolveConfig(config, volatileConfigs, settings);
}

TAsyncVerboseResolveConfigResult TDynamicConfigClient::VerboseResolveConfig(
    const std::string& config,
    const std::map<uint64_t, std::string>& volatileConfigs,
    const TClusterConfigSettings& settings) {
    return Impl_->VerboseResolveConfig(config, volatileConfigs, settings);
}

TAsyncFetchStartupConfigResult TDynamicConfigClient::FetchStartupConfig(const TClusterConfigSettings& settings) {
    return Impl_->FetchStartupConfig(settings);
}

} // namespace NYdb::NDynamicConfig
