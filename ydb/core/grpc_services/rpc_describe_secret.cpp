#include "service_secret.h"
#include "rpc_scheme_base.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/public/api/protos/ydb_secret.pb.h>

namespace NKikimr::NGRpcService {

using namespace NActors;

using TEvDescribeSecretRequest = TGrpcRequestOperationCall<::Ydb::Secret::DescribeSecretRequest,
    ::Ydb::Secret::DescribeSecretResponse>;

class TDescribeSecretRPC : public TRpcSchemeRequestActor<TDescribeSecretRPC, TEvDescribeSecretRequest> {
    using TBase = TRpcSchemeRequestActor<TDescribeSecretRPC, TEvDescribeSecretRequest>;

public:
    using TBase::TBase;

    void Bootstrap() {
        auto navigateRequest = std::make_unique<TEvTxUserProxy::TEvNavigate>();
        SetAuthToken(navigateRequest, *Request_);
        SetDatabase(navigateRequest.get(), *Request_);
        navigateRequest->Record.MutableDescribePath()->SetPath(
            Request_->NormalizePath(GetProtoRequest()->path()));

        Send(MakeTxProxyID(), navigateRequest.release());
        Become(&TDescribeSecretRPC::StateDescribeScheme);
    }

private:
    STATEFN(StateDescribeScheme) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
        default:
            return TBase::StateWork(ev);
        }
    }

    static void Convert(const NKikimrSchemeOp::TIamDelegation& from, ::Ydb::Secret::IamDelegation& to) {
        to.set_service_account_id(from.GetServiceAccountId());
        to.set_cloud_id(from.GetCloudId());
        to.set_referrer_id(from.GetReferrerId());
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
                ::Ydb::Secret::DescribeSecretResult result;
                ConvertDirectoryEntry(pathDescription.GetSelf(), result.mutable_self(), true);
                result.set_version(secret.GetVersion());
                if (secret.HasIamDelegation()) {
                    Convert(secret.GetIamDelegation(), *result.mutable_iam_delegation());
                }
                if (secret.HasPendingIamDelegation()) {
                    Convert(secret.GetPendingIamDelegation(), *result.mutable_pending_iam_delegation());
                }
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
