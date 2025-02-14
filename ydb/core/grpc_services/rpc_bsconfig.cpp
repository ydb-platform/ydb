#include "service_keyvalue.h"
#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <ydb/library/yaml_config/tools/util/defaults.h>
#include "rpc_bsconfig_base.h"

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

bool CopyToConfigRequest(const Ydb::Config::ReplaceConfigRequest &from, NKikimrBlobStorage::TConfigRequest *to) {
    to->CopyFrom(NKikimr::NYaml::BuildInitDistributedStorageCommand(from.main_config()));
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
    to->set_main_config(NYaml::ParseProtoToYaml(storageConfig));
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
        cmd->SetYAML(GetProtoRequest()->main_config());
    }

    void FillDistconfResult(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult& /*record*/,
            Ydb::Config::ReplaceConfigResult& /*result*/)
    {}

    bool IsDistconfEnableQuery() const {
        NKikimrConfig::TAppConfig newConfig;
        try {
            newConfig = NYaml::Parse(GetProtoRequest()->main_config());
        } catch (const std::exception&) {
            return false; // assuming no distconf enabled in this config
        }
        return newConfig.GetSelfManagementConfig().GetEnabled();
    }

    std::unique_ptr<IEventBase> ProcessControllerQuery() override {
        auto *request = GetProtoRequest();
        auto opt = [&](auto&& has, auto&& get) {
            return std::invoke(has, request) ? std::make_optional(std::invoke(get, request)) : std::nullopt;
        };

        std::optional<bool> switch_dedicated_storage_section;
        bool dedicated_config_mode = false;
        switch (request->action_case()) {
            case Ydb::Config::ReplaceConfigRequest::ActionCase::kReplaceEnableDedicatedStorageSection:
                switch_dedicated_storage_section = true;
                dedicated_config_mode = true;
            case Ydb::Config::ReplaceConfigRequest::ActionCase::kReplaceDisableDedicatedStorageSection:
                switch_dedicated_storage_section = false;
            case Ydb::Config::ReplaceConfigRequest::ActionCase::kReplaceWithDedicatedStorageSection:
                dedicated_config_mode = true;
                [[fallthrough]];
            case Ydb::Config::ReplaceConfigRequest::ActionCase::kReplace:
                break;
            case Ydb::Config::ReplaceConfigRequest::ActionCase::ACTION_NOT_SET:
                break; // TODO: handle as error?
        }

        using T = std::decay_t<decltype(*request)>;
        return std::make_unique<TEvBlobStorage::TEvControllerReplaceConfigRequest>(
            opt(&T::has_main_config, &T::main_config),
            opt(&T::has_storage_config, &T::storage_config),
            switch_dedicated_storage_section,
            dedicated_config_mode,
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
        result.set_main_config(record.GetFetchStorageConfig().GetYAML());
    }

    bool IsDistconfEnableQuery() const {
        return false;
    }

    std::unique_ptr<IEventBase> ProcessControllerQuery() override {
        auto& request = *GetProtoRequest();
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerFetchConfigRequest>();
        auto& record = ev->Record;
        record.SetDedicatedStorageSection(request.dedicated_storage_section());
        record.SetDedicatedClusterSection(request.dedicated_cluster_section());
        return ev;
    }
};

void DoReplaceBSConfig(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TReplaceStorageConfigRequest(p.release()));
}

void DoFetchBSConfig(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
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
