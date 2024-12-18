#pragma once
#include "defs.h"

#include "rpc_deferrable.h"

#include <ydb/public/api/protos/ydb_bsconfig.pb.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>
#include <ydb/core/protos/blobstorage_base3.pb.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/public/lib/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>

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


bool CopyToConfigRequest(const Ydb::BSConfig::ReplaceStorageConfigRequest &from, NKikimrBlobStorage::TConfigRequest *to);
bool CopyToConfigRequest(const Ydb::BSConfig::FetchStorageConfigRequest &from, NKikimrBlobStorage::TConfigRequest *to);
void CopyFromConfigResponse(const NKikimrBlobStorage::TConfigResponse &/*from*/, Ydb::BSConfig::ReplaceStorageConfigResult* /*to*/);
void CopyFromConfigResponse(const NKikimrBlobStorage::TConfigResponse &from, Ydb::BSConfig::FetchStorageConfigResult *to);

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
            hFunc(TEvNodeWardenStorageConfig, Handle);
            hFunc(NStorage::TEvNodeConfigInvokeOnRootResult, Handle);
        default:
            return TBase::StateFuncBase(ev);
        }
    }

    void Handle(TEvNodeWardenStorageConfig::TPtr ev) {
        auto *self = Self();
        if (ev->Get()->Config->GetGeneration() || self->IsDistconfEnableQuery()) { // distconf (will be) enabled
            auto ev = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            self->FillDistconfQuery(*ev);
            self->Send(MakeBlobStorageNodeWardenID(self->SelfId().NodeId()), ev.release());
        } else { // classic BSC
            BSCTabletId = MakeBSControllerID();
            CreatePipe();
            SendRequest();
        }
    }

    void Handle(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr ev) {
        auto *self = Self();
        auto& record = ev->Get()->Record;
        if (record.GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK) {
            self->Reply(Ydb::StatusIds::INTERNAL_ERROR, record.GetErrorReason(), NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                TActivationContext::AsActorContext());
        } else {
            TResultRecord result;
            self->FillDistconfResult(record, result);
            self->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, TActivationContext::AsActorContext());
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
        BSCPipeClient = this->Register(NTabletPipe::CreateClient(this->SelfId(), BSCTabletId, GetPipeConfig()));
    }

    void SendRequest() {
        auto self = static_cast<TDerived*>(this);
        std::unique_ptr<TEvBlobStorage::TEvControllerConfigRequest> req = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        auto &rec = *this->GetProtoRequest();
        if (!CopyToConfigRequest(rec, req->Record.MutableRequest())) {
            return this->Reply(Ydb::StatusIds::BAD_REQUEST, self->ActorContext());
        }
        NTabletPipe::SendData(this->SelfId(), BSCPipeClient, req.release(), 0, TBase::Span_.GetTraceId());
    }

    void Handle(typename TEvBlobStorage::TEvControllerConfigResponse::TPtr &ev) {
        auto self = static_cast<TDerived*>(this);
        auto status = PullStatus(ev->Get()->Record);
        auto ctx = self->ActorContext();
        if (status != Ydb::StatusIds::SUCCESS) {
            this->Reply(status, ev->Get()->Record.GetResponse().GetErrorDescription(), NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            return;
        }
        TResultRecord result;
        CopyFromConfigResponse(ev->Get()->Record.GetResponse(), &result);
        this->ReplyWithResult(status, result, TActivationContext::AsActorContext());
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            this->Reply(Ydb::StatusIds::UNAVAILABLE, "Failed to connect to coordination node.", NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, this->ActorContext());
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        this->Reply(Ydb::StatusIds::UNAVAILABLE, "Connection to coordination node was lost.", NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, this->ActorContext());
    }

    virtual bool ValidateRequest(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) = 0;

    const TDerived *Self() const { return static_cast<const TDerived*>(this); }
    TDerived *Self() { return static_cast<TDerived*>(this); }

private:
    ui64 BSCTabletId = 0;
    TActorId BSCPipeClient;
};

} // namespace NKikimr::NGRpcService

template <>
struct THash<NKikimr::NGRpcService::TDriveDeviceSet> {
    std::size_t operator()(const NKikimr::NGRpcService::TDriveDeviceSet &deviceSet) const {
        return deviceSet.GetHash();
    }
};
