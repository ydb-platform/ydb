#include "rpc_scheme_base.h"
#include "service_secret.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/protos/ydb_secret.pb.h>

namespace NKikimr::NGRpcService {

using TEvReadSecretRequest = TGrpcRequestOperationCall<::Ydb::Secret::ReadSecretRequest,
    ::Ydb::Secret::ReadSecretResponse>;

class TReadSecretRPC : public TRpcSchemeRequestActor<TReadSecretRPC, TEvReadSecretRequest> {
    using TBase = TRpcSchemeRequestActor<TReadSecretRPC, TEvReadSecretRequest>;

public:
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        // TODO(vlad-serikov): After rebasing onto the EnableReadSecrets change, return gRPC
        // FAILED_PRECONDITION here when the flag is disabled, before checking the token.

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = CanonizePath(Request_->GetDatabaseName().GetOrElse(""));
        auto& root = request->ResultSet.emplace_back();
        root.Path = NKikimr::SplitPath(request->DatabaseName);
        if (root.Path.empty()) {
            Request_->RaiseIssue(NYql::TIssue("Database name is required"));
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }
        root.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        root.SyncVersion = true;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));

        Become(&TReadSecretRPC::StateResolveDatabase);
    }

private:
    STATEFN(StateResolveDatabase) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleDatabase);
        default:
            return TBase::StateWork(ev);
        }
    }

    void HandleDatabase(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        const auto& entries = ev->Get()->Request->ResultSet;
        if (entries.size() != 1) {
            Request_->RaiseIssue(NYql::TIssue("Unexpected database lookup response"));
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, ctx);
        }

        const auto& root = entries.front();
        switch (root.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
                Request_->RaiseIssue(NYql::TIssue("Access denied on resolving database"));
                return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);
            case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
                Request_->RaiseIssue(NYql::TIssue("Database lookup is temporarily unavailable"));
                return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);
            default:
                Request_->RaiseIssue(NYql::TIssue("Cannot resolve database"));
                return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
        }

        if (!root.DomainInfo || !root.DomainDescription) {
            Request_->RaiseIssue(NYql::TIssue("Database name must refer to a database root"));
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }

        const auto& token = Request_->GetInternalToken();
        if (!token || !root.SecurityObject || !root.SecurityObject->CheckAccess(NACLib::GenericUse, *token)) {
            Request_->RaiseIssue(NYql::TIssue("ReadSecret requires USE on the database root"));
            return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);
        }

        auto navigateRequest = std::make_unique<TEvTxUserProxy::TEvNavigate>();
        SetAuthToken(navigateRequest, *Request_);
        SetDatabase(navigateRequest.get(), *Request_);
        auto* describePath = navigateRequest->Record.MutableDescribePath();
        describePath->SetPath(Request_->NormalizePath(GetProtoRequest()->path()));
        describePath->MutableOptions()->SetReturnSecretValue(true);
        Send(MakeTxProxyID(), navigateRequest.release());

        Become(&TReadSecretRPC::StateDescribeScheme);
    }

    STATEFN(StateDescribeScheme) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
        default:
            return TBase::StateWork(ev);
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->GetRecord();
        const auto& pathDescription = record.GetPathDescription();

        if (record.HasReason()) {
            Request_->RaiseIssue(NYql::TIssue(record.GetReason()));
        }

        switch (record.GetStatus()) {
            case NKikimrScheme::StatusSuccess: {
                if (pathDescription.GetSelf().GetPathType() != NKikimrSchemeOp::EPathTypeSecret) {
                    Request_->RaiseIssue(NYql::TIssue(
                        TStringBuilder() << "Unexpected path type: " << pathDescription.GetSelf().GetPathType()));
                    return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
                }
                if (!pathDescription.HasSecretDescription()) {
                    Request_->RaiseIssue(NYql::TIssue("Secret description is missing"));
                    return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
                }
                const auto& secret = pathDescription.GetSecretDescription();
                if (!secret.HasValue()) {
                    Request_->RaiseIssue(NYql::TIssue("Secret value is missing from the description"));
                    return Reply(Ydb::StatusIds::INTERNAL_ERROR, ctx);
                }

                ::Ydb::Secret::ReadSecretResult result;
                result.set_value(secret.GetValue());
                return ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
            }
            case NKikimrScheme::StatusPathDoesNotExist:
            case NKikimrScheme::StatusSchemeError:
                return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
            case NKikimrScheme::StatusAccessDenied:
                return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);
            case NKikimrScheme::StatusNotAvailable:
                return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);
            default:
                return Reply(Ydb::StatusIds::GENERIC_ERROR, ctx);
        }
    }
};

void DoReadSecretRequest(std::unique_ptr<IRequestOpCtx> request, const IFacilityProvider& facility) {
    facility.RegisterActor(new TReadSecretRPC(request.release()));
}

}
