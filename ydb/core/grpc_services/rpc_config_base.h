#pragma once
#include "defs.h"

#include "rpc_deferrable.h"

#include <ydb/public/api/protos/ydb_config.pb.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/base/blobstorage_console_events.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>
#include <ydb/core/protos/blobstorage_base3.pb.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>
#include <ydb/core/cms/console/console.h>

namespace NKikimr::NGRpcService {

struct TDriveDevice {
public:

    const TString GetPath() const {
        return Path;
    }

    constexpr NKikimrBlobStorage::EPDiskType GetType() const {
        return Type;
    }

    TDriveDevice(TString path, NKikimrBlobStorage::EPDiskType type) 
        : Path(path), Type(type) {}

    auto operator<=>(const TDriveDevice &) const = default;

private:
    TString Path;
    NKikimrBlobStorage::EPDiskType Type;
};

template <typename TResult>
Ydb::StatusIds::StatusCode PullStatus(const TResult& status) {
    if (!status.GetResponse().GetSuccess()) {
        return Ydb::StatusIds::INTERNAL_ERROR;
    }
    return Ydb::StatusIds::SUCCESS;
}

TString GetDiskType(NKikimrBlobStorage::EPDiskType type) {
    switch (type) {
        case NKikimrBlobStorage::EPDiskType::ROT:
            return "HDD";
        case NKikimrBlobStorage::EPDiskType::SSD:
            return "SSD";
        case NKikimrBlobStorage::EPDiskType::NVME:
            return "NVME";
        default:
            return "UNKNOWN";
    }
}

}

template <>
struct THash<NKikimr::NGRpcService::TDriveDevice> {
    std::size_t operator()(const NKikimr::NGRpcService::TDriveDevice &device) const {
        return THash<TString>()(device.GetPath()) ^ THash<NKikimrBlobStorage::EPDiskType>()(device.GetType());
    }
};

namespace NKikimr::NGRpcService {

class TDriveDeviceSet {
public:
    void AddDevice(const TDriveDevice& device) {
        if (Devices.insert(device).second) {
            Hash ^= THash<TDriveDevice>()(device);
        }
    }

    void RemoveDevice(const TDriveDevice& device) {
        if (Devices.erase(device)) {
            Hash ^= THash<TDriveDevice>()(device);
        }
    }

    std::size_t GetHash() const {
        return Hash;
    }

    const THashSet<TDriveDevice>& GetDevices() const {
        return Devices;
    }

    bool operator==(const TDriveDeviceSet& other) const {
        return Devices == other.Devices;
    }

private:
    THashSet<TDriveDevice> Devices;
    std::size_t Hash = 0;
};


bool CopyToConfigRequest(const Ydb::Config::ReplaceConfigRequest &from, NKikimrBlobStorage::TConfigRequest *to);
bool CopyToConfigRequest(const Ydb::Config::FetchConfigRequest &from, NKikimrBlobStorage::TConfigRequest *to);
void CopyFromConfigResponse(const NKikimrBlobStorage::TConfigResponse &/*from*/, Ydb::Config::ReplaceConfigResult* /*to*/);
void CopyFromConfigResponse(const NKikimrBlobStorage::TConfigResponse &from, Ydb::Config::FetchConfigResult *to);

template <typename TDerived>
class TBaseBSConfigRequest {
protected:
    void OnBootstrap() {
        auto self = static_cast<TDerived*>(this);
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        NYql::TIssues issues;
        if (!self->ValidateRequest(status, issues)) {
            self->Reply(status, issues, self->ActorContext());
            return;
        }
        if (const auto& userToken = self->Request_->GetSerializedToken()) {
            UserToken = new NACLib::TUserToken(userToken);
        }
    }

    bool CheckAccess(const TString& path, TIntrusivePtr<TSecurityObject> securityObject, ui32 access) {
        auto self = static_cast<TDerived*>(this);
        if (!UserToken || !securityObject) {
            return true;
        }
        if (securityObject->CheckAccess(access, *UserToken)) {
            return true;
        }
        self->Reply(Ydb::StatusIds::UNAUTHORIZED,
            TStringBuilder() << "Access denied"
                << ": for# " << UserToken->GetUserSID()
                << ", path# " << path
                << ", access# " << NACLib::AccessRightsToString(access),
            NKikimrIssues::TIssuesIds::ACCESS_DENIED,
            self->ActorContext());
        return false;
    }

private:
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
};

template <typename TDerived, typename TRequest, typename TResultRecord>
class TBSConfigRequestGrpc : public TRpcOperationRequestActor<TDerived, TRequest>
                           , public TBaseBSConfigRequest<TBSConfigRequestGrpc<TDerived, TRequest, TResultRecord>> {
    using TBase = TRpcOperationRequestActor<TDerived, TRequest>;

    friend class TBaseBSConfigRequest<TBSConfigRequestGrpc<TDerived, TRequest, TResultRecord>>;
public:
    TBSConfigRequestGrpc(IRequestOpCtx* request)
        : TBase(request) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);
        auto *self = Self();
        self->OnBootstrap();
        self->Become(&TBSConfigRequestGrpc::StateFunc);
        // ask Node Warden for configuration to see whether we are using distconf
        self->Send(MakeBlobStorageNodeWardenID(ctx.SelfID.NodeId()), new TEvNodeWardenQueryStorageConfig(false));
    }

protected:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);
            hFunc(TEvBlobStorage::TEvControllerFetchConfigResponse, Handle);
            hFunc(TEvBlobStorage::TEvControllerReplaceConfigResponse, Handle);
            hFunc(TEvNodeWardenStorageConfig, Handle);
            hFunc(NStorage::TEvNodeConfigInvokeOnRootResult, Handle);
        default:
            return TBase::StateFuncBase(ev);
        }
    }

    void Handle(TEvNodeWardenStorageConfig::TPtr ev) {
        auto *self = Self();
        if (ev->Get()->SelfManagementEnabled || self->IsDistconfEnableQuery()) { // distconf (will be) enabled
            auto ev = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            self->FillDistconfQuery(*ev);
            self->Send(MakeBlobStorageNodeWardenID(self->SelfId().NodeId()), ev.release());
        } else { // classic BSC
            CreatePipe();
        }
    }

    void Handle(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr ev) {
        auto *self = Self();
        auto& record = ev->Get()->Record;
        if (auto status = record.GetStatus(); status == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::CONTINUE_BSC) {
            // continue with BSC
            CreatePipe();
        } else if (status != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK) {
            self->Reply(Ydb::StatusIds::INTERNAL_ERROR, record.GetErrorReason(), NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                self->ActorContext());
        } else {
            TResultRecord result;
            self->FillDistconfResult(record, result);
            self->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, self->ActorContext());
        }
    }

    NTabletPipe::TClientConfig GetPipeConfig() {
        NTabletPipe::TClientConfig cfg;
        cfg.RetryPolicy = {
            .RetryLimitCount = 3u
        };
        return cfg;
    }

    void CreatePipe() {
        auto *self = Self();
        BSCPipeClient = self->Register(NTabletPipe::CreateClient(self->SelfId(), MakeBSControllerID(), GetPipeConfig()));

        auto req = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        auto& record = req->Record;
        auto *request = record.MutableRequest();
        request->AddCommand()->MutableGetInterfaceVersion();
        NTabletPipe::SendData(self->SelfId(), BSCPipeClient, req.release(), 0, TBase::Span_.GetTraceId());
        State = EState::GET_INTERFACE_VERSION;
    }

    void Handle(typename TEvBlobStorage::TEvControllerConfigResponse::TPtr &ev) {
        auto *self = Self();
        auto& record = ev->Get()->Record;
        switch (State) {
            case EState::GET_INTERFACE_VERSION:
                if (!record.HasResponse()) {
                    // strange, but ok
                } else if (const auto& response = record.GetResponse(); !response.GetSuccess()) {
                    // probably unsupported command (old version of BSC)
                } else if (response.StatusSize() != 1) {
                    // unexpected number of response statuses (?)
                } else {
                    InterfaceVersion = response.GetStatus(0).GetInterfaceVersion();
                }
                if (InterfaceVersion >= BSC_INTERFACE_REPLACE_CONFIG) {
                    // ask to replace/fetch config if needed
                    auto ev = self->ProcessControllerQuery();
                    NTabletPipe::SendData(self->SelfId(), BSCPipeClient, ev.release(), 0, TBase::Span_.GetTraceId());
                } else {
                    // no support for 'replace config' feature, fall back to generic mode
                    auto req = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
                    auto& rec = *self->GetProtoRequest();
                    if (!CopyToConfigRequest(rec, req->Record.MutableRequest())) {
                        return self->Reply(Ydb::StatusIds::BAD_REQUEST, self->ActorContext());
                    }
                    NTabletPipe::SendData(self->SelfId(), BSCPipeClient, req.release(), 0, TBase::Span_.GetTraceId());
                    State = EState::GENERIC_OPERATION;
                }
                return;

            case EState::GENERIC_OPERATION: {
                auto status = PullStatus(record);
                auto ctx = self->ActorContext();
                if (status != Ydb::StatusIds::SUCCESS) {
                    return self->Reply(status, ev->Get()->Record.GetResponse().GetErrorDescription(), NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
                }
                TResultRecord result;
                CopyFromConfigResponse(ev->Get()->Record.GetResponse(), &result);
                self->ReplyWithResult(status, result, self->ActorContext());
                return;
            }

            case EState::UNKNOWN:
                break;
        }

        Y_DEBUG_ABORT("unexpected state");
        self->Reply(Ydb::StatusIds::INTERNAL_ERROR, "unexpected state", NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
            self->ActorContext());
    }

    void Handle(TEvBlobStorage::TEvControllerFetchConfigResponse::TPtr ev) {
        auto *self = Self();
        if constexpr (std::is_same_v<TResultRecord, Ydb::Config::FetchConfigResult>) {
            TResultRecord result;
            const auto& record = ev->Get()->Record;
            if (record.HasClusterYaml()) {
                auto conf = ev->Get()->Record.GetClusterYaml();
                auto metadata = NYamlConfig::GetMainMetadata(conf);
                if (metadata.Version && metadata.Cluster) {
                    auto& config = *result.add_config();
                    auto& identity = *config.mutable_identity();
                    identity.set_version(*metadata.Version);
                    identity.set_cluster(*metadata.Cluster);
                    identity.mutable_main();
                    config.set_config(conf);
                } else {
                    // impossible
                }
            }
            if (record.HasStorageYaml()) {
                auto conf = ev->Get()->Record.GetStorageYaml();
                auto metadata = NYamlConfig::GetStorageMetadata(conf);
                if (metadata.Version && metadata.Cluster) {
                    auto& config = *result.add_config();
                    auto& identity = *config.mutable_identity();
                    identity.set_version(*metadata.Version);
                    identity.set_cluster(*metadata.Cluster);
                    identity.mutable_storage();
                    config.set_config(conf);
                } else {
                    // impossible
                }
            }
            self->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, self->ActorContext());
        } else {
            self->Reply(Ydb::StatusIds::INTERNAL_ERROR, "unexpected event", NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                self->ActorContext());
        }
    }

    void Handle(TEvBlobStorage::TEvControllerReplaceConfigResponse::TPtr ev) {
        auto *self = Self();
        if constexpr (std::is_same_v<TResultRecord, Ydb::Config::ReplaceConfigResult>) {
            const auto& record = ev->Get()->Record;
            if (record.GetStatus() == NKikimrBlobStorage::TEvControllerReplaceConfigResponse::Success) {
                TResultRecord result;
                self->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, self->ActorContext());
            } else {
                self->Reply(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "failed to replace configuration: "
                    << NKikimrBlobStorage::TEvControllerReplaceConfigResponse::EStatus_Name(record.GetStatus())
                    << ": " << record.GetErrorReason(), NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
            }
        } else {
            self->Reply(Ydb::StatusIds::INTERNAL_ERROR, "unexpected event", NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                self->ActorContext());
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            auto *self = Self();
            self->Reply(Ydb::StatusIds::UNAVAILABLE, "Failed to connect to BSC tablet",
                NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, self->ActorContext());
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        auto *self = Self();
        self->Reply(Ydb::StatusIds::UNAVAILABLE, "Connection to BSC tablet was lost",
            NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, self->ActorContext());
    }

    TActorId ConsolePipe;

    STFUNC(StateConsoleFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NConsole::TEvConsole::TEvGenericError, HandleConsole);
            hFunc(TEvTabletPipe::TEvClientDestroyed, HandleConsole);
            hFunc(TEvTabletPipe::TEvClientConnected, HandleConsole);
            default:
                return TBase::StateFuncBase(ev);
        }
    }

    void HandleConsole(NConsole::TEvConsole::TEvGenericError::TPtr& ev) {
        auto *self = Self();
        self->Reply(ev->Get()->Record.GetYdbStatus(), ev->Get()->Record.GetIssues(), self->ActorContext());
    }
    
    void HandleConsole(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        auto *self = Self();
        self->Reply(Ydb::StatusIds::UNAVAILABLE, "Connection to Console was lost",
                   NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, self->ActorContext());
    }

    void HandleConsole(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            auto *self = Self();
            self->Reply(Ydb::StatusIds::UNAVAILABLE, "Failed to connect to Console",
                       NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, self->ActorContext());
        }
    }

    virtual bool ValidateRequest(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) = 0;
    virtual std::unique_ptr<IEventBase> ProcessControllerQuery() = 0;

    const TDerived *Self() const { return static_cast<const TDerived*>(this); }
    TDerived *Self() { return static_cast<TDerived*>(this); }

private:
    enum class EState {
        UNKNOWN,
        GET_INTERFACE_VERSION,
        GENERIC_OPERATION,
    };

    EState State = EState::UNKNOWN;
    TActorId BSCPipeClient;
    ui32 InterfaceVersion = 0;
};

} // namespace NKikimr::NGRpcService

template <>
struct THash<NKikimr::NGRpcService::TDriveDeviceSet> {
    std::size_t operator()(const NKikimr::NGRpcService::TDriveDeviceSet &deviceSet) const {
        return deviceSet.GetHash();
    }
};
