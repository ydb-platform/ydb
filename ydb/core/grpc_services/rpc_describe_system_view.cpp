#include "rpc_scheme_base.h"
#include "service_table.h"

#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/ydb_convert/table_description.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

using TEvDescribeSystemViewRequest = TGrpcRequestOperationCall<Ydb::Table::DescribeSystemViewRequest,
    Ydb::Table::DescribeSystemViewResponse>;

class TDescribeSystemViewRPC : public TRpcSchemeRequestActor<TDescribeSystemViewRPC, TEvDescribeSystemViewRequest> {
    using TBase = TRpcSchemeRequestActor<TDescribeSystemViewRPC, TEvDescribeSystemViewRequest>;

public:
    TDescribeSystemViewRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        const auto& path = GetProtoRequest()->path();
        const auto paths = NKikimr::SplitPath(path);
        if (paths.empty()) {
            Request_->RaiseIssue(NYql::TIssue("Invalid path"));
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }

        SendProposeRequest(path, ctx);
        Become(&TDescribeSystemViewRPC::StateWork);
    }

private:
    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            default:
                TBase::StateWork(ev);
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->GetRecord();
        const auto status = record.GetStatus();

        if (record.HasReason()) {
            auto issue = NYql::TIssue(record.GetReason());
            Request_->RaiseIssue(issue);
        }

        switch (status) {
            case NKikimrScheme::StatusSuccess: {
                Ydb::Table::DescribeSystemViewResult describeSysViewResult;
                Ydb::StatusIds_StatusCode status;
                TString error;

                const auto& pathDescription = record.GetPathDescription();
                if (!FillSysViewDescription(describeSysViewResult, pathDescription, status, error)) {
                    switch (status) {
                    case Ydb::StatusIds::INTERNAL_ERROR:
                        LOG_ERROR(ctx, NKikimrServices::GRPC_SERVER, error);
                        [[fallthrough]];
                    case Ydb::StatusIds::SCHEME_ERROR:
                        [[fallthrough]];
                    default:
                        Request_->RaiseIssue(NYql::TIssue(error));
                        return Reply(status, ctx);
                    }
                }

                return ReplyWithResult(Ydb::StatusIds::SUCCESS, describeSysViewResult, ctx);
            }

            case NKikimrScheme::StatusPathDoesNotExist:
            case NKikimrScheme::StatusSchemeError: {
                return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
            }

            case NKikimrScheme::StatusAccessDenied: {
                return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);
            }

            case NKikimrScheme::StatusNotAvailable: {
                return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);
            }

            default: {
                return Reply(Ydb::StatusIds::GENERIC_ERROR, ctx);
            }
        }
    }

    void SendProposeRequest(const TString& path, const TActorContext& ctx) {
        std::unique_ptr<TEvTxUserProxy::TEvNavigate> navigateRequest(new TEvTxUserProxy::TEvNavigate());
        SetAuthToken(navigateRequest, *Request_);
        SetDatabase(navigateRequest.get(), *Request_);
        NKikimrSchemeOp::TDescribePath* record = navigateRequest->Record.MutableDescribePath();
        record->SetPath(path);

        ctx.Send(MakeTxProxyID(), navigateRequest.release());
    }

    void ReplyOnException(const std::exception& ex, const char* logPrefix) noexcept {
        auto& ctx = TlsActivationContext->AsActorContext();
        LOG_ERROR(ctx, NKikimrServices::GRPC_SERVER, "%s: %s", logPrefix, ex.what());
        Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
        return Reply(Ydb::StatusIds::INTERNAL_ERROR, ctx);
    }
};

void DoDescribeSystemViewRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDescribeSystemViewRPC(p.release()));
}

} // namespace NKikimr
} // namespace NGRpcService
