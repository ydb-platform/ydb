#include "rpc_scheme_base.h"
#include "service_secret.h"

#include <ydb/core/base/auth.h>
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

        const auto& token = Request_->GetInternalToken();
        if (token == nullptr || !IsAdministrator(AppData(ctx), token.Get())) {
            Request_->RaiseIssue(NYql::TIssue("ReadSecret requires a cluster administrator"));
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

private:
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
