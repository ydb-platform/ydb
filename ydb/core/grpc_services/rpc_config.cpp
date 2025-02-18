#include "service_keyvalue.h"
#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <ydb/library/yaml_config/tools/util/defaults.h>
#include <ydb/library/yaml_config/public/yaml_config.h>
#include "rpc_config_base.h"

#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/mind/local.h>
#include <ydb/core/protos/local.pb.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>

namespace NKikimr::NGRpcService {

using TEvReplaceStorageConfigRequest =
    TGrpcRequestOperationCall<Ydb::Config::ReplaceConfigRequest,
        Ydb::Config::ReplaceConfigResponse>;
using TEvFetchStorageConfigRequest =
    TGrpcRequestOperationCall<Ydb::Config::FetchConfigRequest,
        Ydb::Config::FetchConfigResponse>;
using TEvBootstrapClusterRequest =
    TGrpcRequestOperationCall<Ydb::Config::BootstrapClusterRequest,
        Ydb::Config::BootstrapClusterResponse>;

using namespace NActors;
using namespace Ydb;

struct BSConfigApiShim {
    std::optional<bool> SwitchDedicatedStorageSection;
    std::optional<TString> MainConfig;
    std::optional<TString> StorageConfig;
    bool DedicatedConfigMode = false;
};

BSConfigApiShim ConvertConfigReplaceRequest(const auto& request) {
    BSConfigApiShim result;

    auto fillConfigs = [&](const auto& configBundle) {
        if (configBundle.has_main_config()) {
            result.MainConfig = configBundle.main_config();
        }

        if (configBundle.has_storage_config()) {
            result.StorageConfig = configBundle.storage_config();
        }
    };

    switch (request.action_case()) {
        case Ydb::Config::ReplaceConfigRequest::ActionCase::kReplaceEnableDedicatedStorageSection:
            result.SwitchDedicatedStorageSection = true;
            result.DedicatedConfigMode = true;
            fillConfigs(request.replace_enable_dedicated_storage_section());
            break;
        case Ydb::Config::ReplaceConfigRequest::ActionCase::kReplaceDisableDedicatedStorageSection:
            result.SwitchDedicatedStorageSection = false;
            result.MainConfig = request.replace_disable_dedicated_storage_section();
            break;
        case Ydb::Config::ReplaceConfigRequest::ActionCase::kReplaceWithDedicatedStorageSection:
            result.DedicatedConfigMode = true;
            fillConfigs(request.replace_with_dedicated_storage_section());
            break;
        case Ydb::Config::ReplaceConfigRequest::ActionCase::kReplace:
            result.MainConfig = request.replace();
            break;
        case Ydb::Config::ReplaceConfigRequest::ActionCase::ACTION_NOT_SET:
            break; // TODO: handle as error?
    }

    return result;
}

bool CopyToConfigRequest(const Ydb::Config::ReplaceConfigRequest &from, NKikimrBlobStorage::TConfigRequest *to) {
    auto shim = ConvertConfigReplaceRequest(from);

    to->CopyFrom(NKikimr::NYaml::BuildInitDistributedStorageCommand(shim.MainConfig.value_or(TString{})));
    return true;
}

void CopyFromConfigResponse(const NKikimrBlobStorage::TConfigResponse &/*from*/, Ydb::Config::ReplaceConfigResult* /*to*/) {
}

bool CopyToConfigRequest(const Ydb::Config::FetchConfigRequest &/*from*/, NKikimrBlobStorage::TConfigRequest *to) {
    to->AddCommand()->MutableReadHostConfig();
    to->AddCommand()->MutableReadBox();
    return true;
}

void CopyFromConfigResponse(const NKikimrBlobStorage::TConfigResponse &from, Ydb::Config::FetchConfigResult *to) {
    auto hostConfigStatus = from.GetStatus()[0];
    auto boxStatus = from.GetStatus()[1];
    NKikimrConfig::StorageConfig storageConfig;
    int itemConfigGeneration = 0;
    for (const auto& hostConfig: hostConfigStatus.GetHostConfig()) {
        itemConfigGeneration = std::max(itemConfigGeneration, static_cast<int>(hostConfig.GetItemConfigGeneration()));
        auto *newHostConfig = storageConfig.add_host_config();
        newHostConfig->set_host_config_id(hostConfig.GetHostConfigId());
        for (const auto& drive : hostConfig.GetDrive()) {
            auto *newDrive = newHostConfig->add_drive();
            newDrive->set_path(drive.GetPath());
            newDrive->set_type(GetDiskType(drive.GetType()));
            newDrive->set_shared_with_os(drive.GetSharedWithOs());
            newDrive->set_read_centric(drive.GetReadCentric());
            newDrive->set_kind(drive.GetKind());
            newDrive->set_expected_slot_count(hostConfig.GetDefaultHostPDiskConfig().GetExpectedSlotCount());
        }
    }
    auto boxes = boxStatus.GetBox();
    if (!boxes.empty()) {
        auto box = boxes[0];
        itemConfigGeneration = std::max(itemConfigGeneration, static_cast<int>(box.GetItemConfigGeneration()));
        for (const auto& host : box.GetHost()) {
            auto *newHost = storageConfig.add_host();
            newHost->set_host_config_id(host.GetHostConfigId());
            auto *newHostKey = newHost->mutable_key();
            const auto& hostKey = host.GetKey();
            if (hostKey.GetNodeId()) {
                newHostKey->set_node_id(hostKey.GetNodeId());
            }
            else {
                auto *endpoint = newHostKey->mutable_endpoint();
                endpoint->set_fqdn(hostKey.GetFqdn());
                endpoint->set_ic_port(hostKey.GetIcPort());
            }
        }
    }
    storageConfig.set_item_config_generation(itemConfigGeneration);
    auto& config = *to->add_config();
    auto& identity = *config.mutable_identity();
    identity.set_version(itemConfigGeneration);
    identity.set_cluster(AppData()->ClusterName);
    identity.mutable_storage();
    // TODO: !imp fill metadata ?
    // are we sure that it is storage config?
    config.set_config(NYaml::ParseProtoToYaml(storageConfig));
}

class TReplaceStorageConfigRequest : public TBSConfigRequestGrpc<TReplaceStorageConfigRequest, TEvReplaceStorageConfigRequest,
    Ydb::Config::ReplaceConfigResult> {
public:
    using TBase = TBSConfigRequestGrpc<TReplaceStorageConfigRequest, TEvReplaceStorageConfigRequest, Ydb::Config::ReplaceConfigResult>;
    using TBase::TBase;

    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/, NYql::TIssues& /*issues*/) override {
        return true;
    }
    NACLib::EAccessRights GetRequiredAccessRights() const {
        return NACLib::GenericManage;
    }

    void FillDistconfQuery(NStorage::TEvNodeConfigInvokeOnRoot& ev) {
        auto *cmd = ev.Record.MutableReplaceStorageConfig();

        auto shim = ConvertConfigReplaceRequest(*GetProtoRequest());

        if (shim.MainConfig) {
            cmd->SetYAML(*shim.MainConfig);
        }
        if (shim.StorageConfig) {
            cmd->SetStorageYAML(*shim.StorageConfig);
        }
        if (shim.SwitchDedicatedStorageSection) {
            cmd->SetSwitchDedicatedStorageSection(*shim.SwitchDedicatedStorageSection);
        }
        cmd->SetDedicatedStorageSectionConfigMode(shim.DedicatedConfigMode);
    }

    void FillDistconfResult(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult& /*record*/,
            Ydb::Config::ReplaceConfigResult& /*result*/)
    {}

    bool IsDistconfEnableQuery() const {
        NKikimrConfig::TAppConfig newConfig;
        try {
            auto shim = ConvertConfigReplaceRequest(*GetProtoRequest());
            newConfig = NYaml::Parse(shim.MainConfig.value_or(TString{})); // TODO: !imp check allow unknown fields
        } catch (const std::exception&) {
            return false; // assuming no distconf enabled in this config
        }
        return newConfig.GetSelfManagementConfig().GetEnabled();
    }

    std::unique_ptr<IEventBase> ProcessControllerQuery() override {
        auto *request = GetProtoRequest();

        auto shim = ConvertConfigReplaceRequest(*request);

        return std::make_unique<TEvBlobStorage::TEvControllerReplaceConfigRequest>(
            shim.MainConfig,
            shim.StorageConfig,
            shim.SwitchDedicatedStorageSection,
            shim.DedicatedConfigMode,
            request->allow_unknown_fields() || request->bypass_checks(),
            request->bypass_checks(),
            request->bypass_checks());
    }
};

class TFetchStorageConfigRequest : public TBSConfigRequestGrpc<TFetchStorageConfigRequest, TEvFetchStorageConfigRequest,
    Ydb::Config::FetchConfigResult> {
public:
    using TBase = TBSConfigRequestGrpc<TFetchStorageConfigRequest, TEvFetchStorageConfigRequest, Ydb::Config::FetchConfigResult>;
    using TBase::TBase;

    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/, NYql::TIssues& /*issues*/) override {
        return true;
    }
    NACLib::EAccessRights GetRequiredAccessRights() const {
        return NACLib::GenericManage;
    }

    void FillDistconfQuery(NStorage::TEvNodeConfigInvokeOnRoot& ev) const {
        ev.Record.MutableFetchStorageConfig();
    }

    void FillDistconfResult(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult& record,
            Ydb::Config::FetchConfigResult& result) {
        auto conf = record.GetFetchStorageConfig().GetYAML();
        auto metadata = NYamlConfig::GetMainMetadata(conf);
        // TODO: !imp error if empty
        auto& config = *result.add_config();
        auto& identity = *config.mutable_identity();
        identity.set_version(*metadata.Version);
        identity.set_cluster(AppData()->ClusterName);
        identity.mutable_main(); // TODO: is it really main ?
        config.set_config(conf);
    }

    bool IsDistconfEnableQuery() const {
        return false;
    }

    std::unique_ptr<IEventBase> ProcessControllerQuery() override {
        auto& request = *GetProtoRequest();
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerFetchConfigRequest>();
        auto& record = ev->Record;

        switch (request.mode_case()) {
            case Ydb::Config::FetchConfigRequest::ModeCase::kAll:
                if (request.all().config_transform_case() == Ydb::Config::FetchConfigRequest::FetchModeAll::ConfigTransformCase::kDetachStorageConfigSection) {
                    record.SetDedicatedStorageSection(true);
                    record.SetDedicatedClusterSection(true);
                }
                break;
            case Ydb::Config::FetchConfigRequest::ModeCase::kTarget:
                // TODO: !imp error
                break;
            case Ydb::Config::FetchConfigRequest::ModeCase::MODE_NOT_SET:
                break; // TODO: maybe error
        }

        return ev;
    }
};

void DoReplaceConfig(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TReplaceStorageConfigRequest(p.release()));
}

void DoFetchConfig(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFetchStorageConfigRequest(p.release()));
}

void DoBootstrapCluster(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    class TBootstrapClusterRequest : public TRpcOperationRequestActor<TBootstrapClusterRequest, TEvBootstrapClusterRequest> {
        using TBase = TRpcOperationRequestActor<TBootstrapClusterRequest, TEvBootstrapClusterRequest>;

    public:
        using TBase::TBase;

        void Bootstrap(const TActorContext& ctx) {
            TBase::Bootstrap(ctx);
            Become(&TBootstrapClusterRequest::StateFunc);

            const auto& request = *GetProtoRequest();

            auto ev = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            auto& record = ev->Record;
            auto *cmd = record.MutableBootstrapCluster();
            cmd->SetSelfAssemblyUUID(request.self_assembly_uuid());
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), ev.release());
        }

        void Handle(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr ev, const TActorContext& ctx) {
            auto& record = ev->Get()->Record;
            switch (record.GetStatus()) {
                case NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK:
                    Reply(Ydb::StatusIds::SUCCESS, ctx);
                    break;

                default:
                    Reply(Ydb::StatusIds::GENERIC_ERROR, record.GetErrorReason(), NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
                    break;
            }
        }

    protected:
        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                HFunc(NStorage::TEvNodeConfigInvokeOnRootResult, Handle);
                default:
                    return TBase::StateFuncBase(ev);
            }
        }
    };

    TActivationContext::Register(new TBootstrapClusterRequest(p.release()));
}

} // namespace NKikimr::NGRpcService
