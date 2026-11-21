#include "rpc_bridge_base.h"
#include "service_bridge.h"
#include "rpc_common/rpc_common.h"

#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

namespace NKikimr::NGRpcService {

using TEvSetDatabaseBridgeModeRequest = TGrpcRequestOperationCall<Ydb::Bridge::SetDatabaseBridgeModeRequest,
                                                                  Ydb::Bridge::SetDatabaseBridgeModeResponse>;

class TSetDatabaseBridgeModeRequest : public TBridgeRequestGrpc<TSetDatabaseBridgeModeRequest,
                                                                TEvSetDatabaseBridgeModeRequest,
                                                                Ydb::Bridge::SetDatabaseBridgeModeResult> {
    using TBase = TBridgeRequestGrpc<TSetDatabaseBridgeModeRequest, TEvSetDatabaseBridgeModeRequest, Ydb::Bridge::SetDatabaseBridgeModeResult>;

    TString Database;
    THashSet<TString> PoolNames;
    NActors::TActorId BSCPipe;
    bool Changing = false;

public:
    using TBase::TBase;

    bool ValidateRequest(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) {
        if (!CheckAdministrator(status, issues)) {
            return false;
        }
        const auto& request = *GetProtoRequest();
        Database = request.database_path();
        if (!Database.StartsWith('/') || NKikimr::SplitPath(Database).empty()) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("database_path must be an absolute database path");
        } else if (!request.has_bridge_mode()) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("bridge_mode must be specified");
        } else if (request.bridge_mode() && !request.retain_pile_name().empty()) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("retain_pile_name requires ordinary storage (bridge_mode=false)");
        } else {
            Database = NKikimr::CanonizePath(Database);
            return true;
        }
        return false;
    }

    void SendRequest(const TActorContext& ctx) {
        auto request = std::make_unique<TEvTxUserProxy::TEvNavigate>();
        SetAuthToken(request, *Request_);
        request->Record.SetDatabaseName(Database);
        request->Record.MutableDescribePath()->SetPath(Database);
        ctx.Send(MakeTxProxyID(), request.release());
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            default: return TBase::StateFuncBase(ev);
        }
    }

private:
    void PassAway() override {
        if (BSCPipe) {
            NTabletPipe::CloseClient(SelfId(), BSCPipe);
        }
        TBase::PassAway();
    }

    void Fail(Ydb::StatusIds::StatusCode status, const TString& message) {
        Reply(status, message, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ActorContext());
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        const auto& record = ev->Get()->GetRecord();
        switch (record.GetStatus()) {
            case NKikimrScheme::StatusSuccess:
                break;
            case NKikimrScheme::StatusAccessDenied:
                return Fail(Ydb::StatusIds::UNAUTHORIZED, record.GetReason());
            case NKikimrScheme::StatusNotAvailable:
                return Fail(Ydb::StatusIds::UNAVAILABLE, record.GetReason());
            default:
                return Fail(Ydb::StatusIds::SCHEME_ERROR, record.GetReason());
        }

        const auto& description = record.GetPathDescription();
        const auto type = description.GetSelf().GetPathType();
        const bool rootDatabase = type == NKikimrSchemeOp::EPathTypeDir && NKikimr::SplitPath(Database).size() == 1;
        if (!description.HasDomainDescription() ||
            (!rootDatabase && type != NKikimrSchemeOp::EPathTypeSubDomain && type != NKikimrSchemeOp::EPathTypeExtSubDomain)) {
            return Fail(Ydb::StatusIds::BAD_REQUEST, "The selected path must be a database root");
        }
        const auto& domain = description.GetDomainDescription();
        if (domain.HasResourcesDomainKey() &&
            TPathId::FromDomainKey(domain.GetResourcesDomainKey()) != TPathId::FromDomainKey(domain.GetDomainKey())) {
            return Fail(Ydb::StatusIds::PRECONDITION_FAILED,
                        "Serverless databases do not own storage pools; change bridge mode of the shared database instead");
        }
        for (const auto& pool : domain.GetStoragePools()) {
            PoolNames.insert(pool.GetName());
        }
        if (PoolNames.empty() || PoolNames.contains("")) {
            return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Database has no usable storage pools");
        }

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {.RetryLimitCount = 3u};
        BSCPipe = Register(NTabletPipe::CreateClient(SelfId(), MakeBSControllerID(), pipeConfig));
        auto request = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        auto* read = request->Record.MutableRequest()->AddCommand()->MutableReadStoragePool();
        read->SetBoxId(Max<ui64>());
        for (const auto& name : PoolNames) {
            read->AddName(name);
        }
        NTabletPipe::SendData(SelfId(), BSCPipe, request.release());
    }

    void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev) {
        const auto& response = ev->Get()->Record.GetResponse();
        for (const auto& status : response.GetStatus()) {
            if (!status.GetSuccess()) {
                const auto code = status.GetFailReason() == NKikimrBlobStorage::TConfigResponse::TStatus::kItemConfigGenerationMismatch
                                  ? Ydb::StatusIds::ABORTED
                                  : Ydb::StatusIds::PRECONDITION_FAILED;
                return Fail(code,
                            status.GetErrorDescription().empty()
                            ? response.GetErrorDescription()
                            : status.GetErrorDescription());
            }
        }
        if (!response.GetSuccess()) {
            return Fail(Ydb::StatusIds::PRECONDITION_FAILED, response.GetErrorDescription());
        }
        const size_t expected = Changing ? PoolNames.size() : 1;
        if (response.StatusSize() != expected) {
            return Fail(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected number of storage controller command statuses");
        }
        if (Changing) {
            return ReplyWithResult(Ydb::StatusIds::SUCCESS, Ydb::Bridge::SetDatabaseBridgeModeResult(), ActorContext());
        }

        const auto& pools = response.GetStatus(0).GetStoragePool();
        THashSet<TString> found;
        auto request = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        for (const auto& pool : pools) {
            if (!PoolNames.contains(pool.GetName()) || !found.insert(pool.GetName()).second) {
                return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Storage pool names must uniquely identify the database's pools");
            }
            auto* command = request->Record.MutableRequest()->AddCommand()->MutableChangeStoragePoolBridgeMode();
            command->SetBoxId(pool.GetBoxId());
            command->SetStoragePoolId(pool.GetStoragePoolId());
            command->SetItemConfigGeneration(pool.GetItemConfigGeneration());
            command->SetBridgeMode(GetProtoRequest()->bridge_mode());
            command->SetRetainPileName(GetProtoRequest()->retain_pile_name());
        }
        if (found.size() != PoolNames.size()) {
            return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Some database storage pools were not found in the storage controller");
        }
        // A single BSC transaction validates every pool and commits all changes together, or none on failure.
        Changing = true;
        NTabletPipe::SendData(SelfId(), BSCPipe, request.release());
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            Fail(Ydb::StatusIds::UNAVAILABLE, "Failed to connect to the storage controller");
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        Fail(Ydb::StatusIds::UNAVAILABLE, "Connection to the storage controller was lost");
    }
};

void DoSetDatabaseBridgeMode(std::unique_ptr<IRequestOpCtx> request, const IFacilityProvider&) {
    TActivationContext::Register(new TSetDatabaseBridgeModeRequest(request.release()));
}

} // namespace NKikimr::NGRpcService
