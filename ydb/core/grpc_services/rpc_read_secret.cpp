#include "rpc_scheme_base.h"
#include "service_secret.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/protos/ydb_secret.pb.h>

namespace NKikimr::NGRpcService {

using TEvReadSecretRequest = TGrpcRequestOperationCall<::Ydb::Secret::ReadSecretRequest,
    ::Ydb::Secret::ReadSecretResponse>;

class TReadSecretRPC : public TRpcSchemeRequestActor<TReadSecretRPC, TEvReadSecretRequest> {
    using TBase = TRpcSchemeRequestActor<TReadSecretRPC, TEvReadSecretRequest>;

    TPathId AuthorizedDomainKey;
    bool CanManageDatabase = false;

    TString DatabaseName;
    TString SecretPath;

public:
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        SecretPath = Request_->NormalizePath(GetProtoRequest()->path());
        DatabaseName = CanonizePath(Request_->GetDatabaseName().GetOrElse(""));

        // TODO(vlad-serikov): After rebasing onto the EnableReadSecrets change, return gRPC
        // FAILED_PRECONDITION here when the flag is disabled, before checking the token.

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = DatabaseName;

        auto& root = request->ResultSet.emplace_back();
        root.Path = NKikimr::SplitPath(DatabaseName);
        root.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        root.SyncVersion = true;

        auto& secret = request->ResultSet.emplace_back();
        secret.Path = NKikimr::SplitPath(SecretPath);
        secret.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        secret.SyncVersion = true;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));

        Become(&TReadSecretRPC::StateResolveDatabase);
    }

private:
    void DenyRead(const TActorContext& ctx) {
        Request_->RaiseIssue(NYql::TIssue(
            "ReadSecret requires both ydb.database.create and ydb.database.drop on the database root or ownership of the secret"));
        Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);
    }

    bool IsSecretOwner(const TString& owner) const {
        return !owner.empty() && Request_->GetInternalToken()->IsExist(owner);
    }

    STATEFN(StateResolveDatabase) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleDatabase);
        default:
            return TBase::StateWork(ev);
        }
    }

    void HandleDatabase(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        const auto& entries = ev->Get()->Request->ResultSet;
        if (entries.size() != 2) {
            Request_->RaiseIssue(NYql::TIssue("Unexpected scheme lookup response"));
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, ctx);
        }

        const auto& root = entries[0];
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
        if (!token) {
            Request_->RaiseIssue(NYql::TIssue("Cannot get user token from auth token"));
            return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);
        }
        AuthorizedDomainKey = root.DomainInfo->DomainKey;
        CanManageDatabase = root.SecurityObject && root.SecurityObject->CheckAccess(NACLib::GenericManage, *token);
        if (!CanManageDatabase) {
            const auto& secret = entries[1];
            switch (secret.Status) {
                case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
                    Request_->RaiseIssue(NYql::TIssue("Secret lookup is temporarily unavailable"));
                    return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);
                case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                    break;
                default:
                    Request_->RaiseIssue(NYql::TIssue("Cannot resolve secret"));
                    return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);
            }
            if (secret.Kind != NSchemeCache::TSchemeCacheNavigate::KindSecret
                || !secret.DomainInfo || secret.DomainInfo->DomainKey != AuthorizedDomainKey
                || !secret.SecurityObject || !IsSecretOwner(secret.SecurityObject->GetOwnerSID()))
            {
                Request_->RaiseIssue(NYql::TIssue(
                    "ReadSecret requires both ydb.database.create and ydb.database.drop on the database root or ownership of the secret"));
                return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);
            }
        }

        auto navigateRequest = std::make_unique<TEvTxUserProxy::TEvNavigate>();
        SetDatabase(navigateRequest.get(), *Request_);
        auto* describePath = navigateRequest->Record.MutableDescribePath();
        describePath->SetPath(SecretPath);
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
                const auto& domain = pathDescription.GetDomainDescription();
                if (!domain.HasDomainKey() || TPathId::FromDomainKey(domain.GetDomainKey()) != AuthorizedDomainKey) {
                    Request_->RaiseIssue(NYql::TIssue("Secret path does not belong to the authorized database"));
                    return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
                }
                if (!CanManageDatabase && !IsSecretOwner(pathDescription.GetSelf().GetOwner())) {
                    return DenyRead(ctx);
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
