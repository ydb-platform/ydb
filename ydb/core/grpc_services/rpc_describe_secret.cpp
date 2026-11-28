#include "service_secret.h"
#include "rpc_scheme_base.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/public/api/protos/ydb_secret.pb.h>

namespace NKikimr::NGRpcService {

using namespace NActors;

using TEvDescribeSecretRequest = TGrpcRequestOperationCall<::Ydb::Secret::DescribeSecretRequest,
    ::Ydb::Secret::DescribeSecretResponse>;

class TDescribeSecretRPC : public TRpcSchemeRequestActor<TDescribeSecretRPC, TEvDescribeSecretRequest> {
    using TBase = TRpcSchemeRequestActor<TDescribeSecretRPC, TEvDescribeSecretRequest>;

    TString SecretPath;
    TMaybe<TPathId> AuthorizedDatabase;

public:
    using TBase::TBase;

    void Bootstrap() {
        SecretPath = Request_->NormalizePath(GetProtoRequest()->path());

        if (!AppData()->FeatureFlags.GetEnableReadSecretRpcHandler()) {
            return DescribeSecret();
        }

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = CanonizePath(Request_->GetDatabaseName().GetOrElse(""));
        for (const auto& path : {request->DatabaseName, SecretPath}) {
            auto& entry = request->ResultSet.emplace_back();
            entry.Path = NKikimr::SplitPath(path);
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            entry.SyncVersion = true;
        }
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));

        Become(&TDescribeSecretRPC::StateCheckAccess);
    }

private:
    void DescribeSecret() {
        auto navigateRequest = std::make_unique<TEvTxUserProxy::TEvNavigate>();
        if (!AuthorizedDatabase.Defined()) {
            SetAuthToken(navigateRequest, *Request_);
        }
        SetDatabase(navigateRequest.get(), *Request_);
        navigateRequest->Record.MutableDescribePath()->SetPath(SecretPath);
        Send(MakeTxProxyID(), navigateRequest.release());

        Become(&TDescribeSecretRPC::StateDescribeScheme);
    }

    STATEFN(StateCheckAccess) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleAccess);
        default:
            return TBase::StateWork(ev);
        }
    }

    void HandleAccess(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        const auto& entries = ev->Get()->Request->ResultSet;
        if (entries.size() != 2) {
            Request_->RaiseIssue(NYql::TIssue("Unexpected database and secret lookup response"));
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, ctx);
        }

        const auto& secret = entries[1];
        switch (secret.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
                Request_->RaiseIssue(NYql::TIssue("Secret lookup is temporarily unavailable"));
                return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);
            default:
                return DescribeSecret();
        }

        const auto& token = Request_->GetInternalToken();
        if (token == nullptr) {
            Request_->RaiseIssue(NYql::TIssue("DescribeSecret requires an authenticated user"));
            return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);
        }
        if (secret.Kind != NSchemeCache::TSchemeCacheNavigate::KindSecret
            || (secret.SecurityObject != nullptr && secret.SecurityObject->CheckAccess(NACLib::DescribeSchema, *token))) {
            return DescribeSecret();
        }

        const auto& root = entries[0];
        switch (root.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
                Request_->RaiseIssue(NYql::TIssue("Access denied while resolving the database root"));
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
        if (root.DomainDescription == nullptr || root.DomainInfo == nullptr) {
            Request_->RaiseIssue(NYql::TIssue("Database name must refer to a database root"));
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }

        if (!secret.DomainInfo || secret.DomainInfo->DomainKey != root.DomainInfo->DomainKey) {
            Request_->RaiseIssue(NYql::TIssue("Secret path does not belong to the requested database"));
            return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
        }
        if (root.SecurityObject == nullptr || !root.SecurityObject->CheckAccess(NACLib::GenericManage, *token)) {
            Request_->RaiseIssue(NYql::TIssue("DescribeSecret requires DescribeSchema on the secret or both "
                "ydb.database.create and ydb.database.drop on the database root"));
            return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);
        }

        AuthorizedDatabase = root.DomainInfo->DomainKey;
        DescribeSecret();
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
                if (AuthorizedDatabase.Defined()) {
                    const auto& domain = pathDescription.GetDomainDescription();
                    if (!domain.HasDomainKey() || (TPathId::FromDomainKey(domain.GetDomainKey()) != *AuthorizedDatabase)) {
                        Request_->RaiseIssue(NYql::TIssue("Secret path does not belong to the requested database"));
                        return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
                    }
                }
                if (pathDescription.GetSelf().GetPathType() != NKikimrSchemeOp::EPathTypeSecret) {
                    Request_->RaiseIssue(NYql::TIssue(
                        TStringBuilder() << "Unexpected path type: " << pathDescription.GetSelf().GetPathType()));
                    return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
                }
                if (!pathDescription.HasSecretDescription()) {
                    Request_->RaiseIssue(NYql::TIssue("Secret description is missing"));
                    return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
                }

                ::Ydb::Secret::DescribeSecretResult result;
                ConvertDirectoryEntry(pathDescription.GetSelf(), result.mutable_self(), true);
                result.set_version(pathDescription.GetSecretDescription().GetVersion());
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

void DoDescribeSecretRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDescribeSecretRPC(p.release()));
}

}
