#include "service_import.h"
#include "rpc_deferrable.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard_import.h>
#include <ydb/public/api/protos/ydb_import.pb.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_PROXY

namespace NKikimr::NGRpcService {

using TEvListObjectsInS3ExportRequest = TGrpcRequestOperationCall<Ydb::Import::ListObjectsInS3ExportRequest,
    Ydb::Import::ListObjectsInS3ExportResponse>;

class TListObjectsInS3ExportRPC: public TRpcOperationRequestActor<TListObjectsInS3ExportRPC, TEvListObjectsInS3ExportRequest> {
public:
    using TBase = TRpcOperationRequestActor<TListObjectsInS3ExportRPC, TEvListObjectsInS3ExportRequest>;
    using TRpcOperationRequestActor<TListObjectsInS3ExportRPC, TEvListObjectsInS3ExportRequest>::TRpcOperationRequestActor;

    explicit TListObjectsInS3ExportRPC(IRequestOpCtx* request)
        : TBase(request)
        , UserToken(CreateUserToken(request))
    {
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKikimr::NSchemeShard::TEvImport::TEvListObjectsInS3ExportResponse, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);

            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        default:
            return StateFuncBase(ev);
        }
    }

    void Bootstrap() {
        if (!Request_ || !Request_->GetDatabaseName()) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "Database name is not specified", NKikimrIssues::TIssuesIds::YDB_API_VALIDATION_ERROR, NActors::TActivationContext::AsActorContext());
        }

        ResolveDatabase();

        Become(&TListObjectsInS3ExportRPC::StateFunc);
    }

    void ResolveDatabase() {
        YDB_LOG_DEBUG("[ListObjectsInS3Export] Resolve database",
            {"SelfId", SelfId()},
            {"name", Request_->GetDatabaseName()});

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = *Request_->GetDatabaseName();

        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.Path = NKikimr::SplitPath(*Request_->GetDatabaseName());

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& request = ev->Get()->Request;

        YDB_LOG_DEBUG("[ListObjectsInS3Export] Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult",
            {"SelfId", SelfId()},
            {"request", (request ? request->ToString(*AppData()->TypeRegistry) : "nullptr")});

        if (request->ResultSet.empty()) {
            return Reply(Ydb::StatusIds::SCHEME_ERROR, "Scheme error", NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, NActors::TActivationContext::AsActorContext());
        }

        const auto& entry = request->ResultSet.front();

        if (request->ErrorCount > 0) {
            switch (entry.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
                return Reply(Ydb::StatusIds::UNAUTHORIZED, "Access denied", NKikimrIssues::TIssuesIds::ACCESS_DENIED, NActors::TActivationContext::AsActorContext());
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                return Reply(Ydb::StatusIds::SCHEME_ERROR, "Unknown database", NKikimrIssues::TIssuesIds::PATH_NOT_EXIST, NActors::TActivationContext::AsActorContext());
            case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                return Reply(Ydb::StatusIds::UNAVAILABLE, "Database lookup error", NKikimrIssues::TIssuesIds::RESOLVE_LOOKUP_ERROR, NActors::TActivationContext::AsActorContext());
            default:
                return Reply(Ydb::StatusIds::SCHEME_ERROR, "Scheme error", NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, NActors::TActivationContext::AsActorContext());
            }
        }

        if (!this->CheckDatabaseAccess(CanonizePath(entry.Path), entry.SecurityObject)) {
            return;
        }

        auto domainInfo = entry.DomainInfo;
        if (!domainInfo) {
            YDB_LOG_ERROR("[ListObjectsInS3Export] Got empty domain info",
                {"SelfId", SelfId()});
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, "Internal error", NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, NActors::TActivationContext::AsActorContext());
        }

        SchemeShardId = domainInfo->ExtractSchemeShard();
        SendRequestToSchemeShard();
    }

    bool CheckDatabaseAccess(const TString& path, TIntrusivePtr<TSecurityObject> securityObject) {
        const ui32 access = NACLib::DescribeSchema;

        if (!UserToken || !securityObject) {
            return true;
        }

        if (securityObject->CheckAccess(access, *UserToken)) {
            return true;
        }

        Reply(Ydb::StatusIds::UNAUTHORIZED,
            TStringBuilder() << "Access denied"
                << ": for# " << UserToken->GetUserSID()
                << ", path# " << path
                << ", access# " << NACLib::AccessRightsToString(access),
            NKikimrIssues::TIssuesIds::ACCESS_DENIED,
            NActors::TActivationContext::AsActorContext());
        return false;
    }

    void SendRequestToSchemeShard() {
        YDB_LOG_DEBUG("[ListObjectsInS3Export] Send request:",
            {"SelfId", SelfId()},
            {"schemeShardId", SchemeShardId});

        if (!PipeClient) {
            NTabletPipe::TClientConfig config;
            config.RetryPolicy = {.RetryLimitCount = 3};
            PipeClient = this->RegisterWithSameMailbox(NTabletPipe::CreateClient(this->SelfId(), SchemeShardId, config));
        }

        auto request = MakeHolder<NSchemeShard::TEvImport::TEvListObjectsInS3ExportRequest>();

        *request->Record.MutableOperationParams() = GetProtoRequest()->operation_params();
        *request->Record.MutableSettings() = GetProtoRequest()->settings();
        request->Record.SetPageSize(GetProtoRequest()->page_size());
        request->Record.SetPageToken(GetProtoRequest()->page_token());

        NTabletPipe::SendData(this->SelfId(), PipeClient, std::move(request), 0, Span_.GetTraceId());
    }

    void Handle(NKikimr::NSchemeShard::TEvImport::TEvListObjectsInS3ExportResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        YDB_LOG_DEBUG("[ListObjectsInS3Export] Handle TListObjectsInS3ExportRPC::TEvListObjectsInS3ExportResponse",
            {"SelfId", SelfId()},
            {"record", record.ShortDebugString()});

        if (record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            return Reply(record.GetStatus(), record.GetIssues(), NActors::TActivationContext::AsActorContext());
        } else {
            return ReplyWithResult(record.GetStatus(), record.GetIssues(), record.GetResult(), NActors::TActivationContext::AsActorContext());
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            DeliveryProblem();
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        DeliveryProblem();
    }

    void DeliveryProblem() {
        YDB_LOG_WARN("[ListObjectsInS3Export] Delivery problem",
            {"SelfId", SelfId()});
        Reply(Ydb::StatusIds::UNAVAILABLE, "Delivery problem", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, NActors::TActivationContext::AsActorContext());
    }

    void PassAway() override {
        NTabletPipe::CloseClient(this->SelfId(), PipeClient);
        TBase::PassAway();
    }

    static THolder<const NACLib::TUserToken> CreateUserToken(IRequestOpCtx* request) {
        if (const auto& userToken = request->GetSerializedToken()) {
            return MakeHolder<NACLib::TUserToken>(userToken);
        } else {
            return {};
        }
    }

private:
    ui64 SchemeShardId = 0;
    TActorId PipeClient;
    const THolder<const NACLib::TUserToken> UserToken;
};

void DoListObjectsInS3ExportRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TListObjectsInS3ExportRPC(p.release()));
}

} // namespace NKikimr::NGRpcService
