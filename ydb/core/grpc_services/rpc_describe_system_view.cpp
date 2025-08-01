#include "rpc_scheme_base.h"
#include "service_table.h"

#include <library/cpp/protobuf/json/util.h>

#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/protos/sys_view_types.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

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
        Ydb::Table::DescribeSystemViewResult describeSysViewResult;
        switch (status) {
            case NKikimrScheme::StatusSuccess: {
                const auto& pathDescription = record.GetPathDescription();
                if (pathDescription.GetSelf().GetPathType() != NKikimrSchemeOp::EPathTypeSysView) {
                    Request_->RaiseIssue(NYql::TIssue(
                        TStringBuilder() << "Unexpected path type: " << pathDescription.GetSelf().GetPathType()
                    ));
                    return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
                }

                Ydb::Scheme::Entry* selfEntry = describeSysViewResult.mutable_self();
                ConvertDirectoryEntry(pathDescription.GetSelf(), selfEntry, true);

                const auto sysViewType = pathDescription.GetSysViewDescription().GetType();
                describeSysViewResult.set_sys_view_id(sysViewType);
                TString sysViewTypeName = NKikimrSysView::ESysViewType_Name(sysViewType).substr(1);
                NProtobufJson::ToSnakeCase(&sysViewTypeName);
                describeSysViewResult.set_sys_view_name(std::move(sysViewTypeName));

                const auto& tableDescription = pathDescription.GetTable();
                try {
                    FillColumnDescription(describeSysViewResult, tableDescription);
                } catch (const std::exception& ex) {
                    return ReplyOnException(ex, "Unable to fill column description");
                }

                describeSysViewResult.mutable_primary_key()->CopyFrom(tableDescription.GetKeyColumnNames());
                FillAttributes(describeSysViewResult, pathDescription);

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
