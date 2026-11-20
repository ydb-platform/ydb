#include "rpc_scheme_base.h"
#include "service_table.h"

#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/ydb_convert/external_table_description.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NGRpcService {

using namespace NActors;
using namespace NJson;
using namespace NKikimrSchemeOp;
using namespace NYql;

using TEvDescribeExternalTableRequest = TGrpcRequestOperationCall<
    Ydb::Table::DescribeExternalTableRequest,
    Ydb::Table::DescribeExternalTableResponse
>;

class TDescribeExternalTableRPC : public TRpcSchemeRequestActor<TDescribeExternalTableRPC, TEvDescribeExternalTableRequest> {
    using TBase = TRpcSchemeRequestActor<TDescribeExternalTableRPC, TEvDescribeExternalTableRequest>;

public:

    using TBase::TBase;

    void Bootstrap() {
        DescribeScheme();
    }

private:

    void DescribeScheme() {
        auto ev = std::make_unique<TEvTxUserProxy::TEvNavigate>();
        SetAuthToken(ev, *Request_);
        SetDatabase(ev.get(), *Request_);
        ev->Record.MutableDescribePath()->SetPath(
            Request_->NormalizePath(GetProtoRequest()->path()));

        Send(MakeTxProxyID(), ev.release());
        Become(&TDescribeExternalTableRPC::StateDescribeScheme);
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
            Request_->RaiseIssue(TIssue(record.GetReason()));
        }

        switch (record.GetStatus()) {
            case NKikimrScheme::StatusSuccess: {
                if (pathDescription.GetSelf().GetPathType() != NKikimrSchemeOp::EPathTypeExternalTable) {
                    Request_->RaiseIssue(TIssue(
                        TStringBuilder() << "Unexpected path type: " << pathDescription.GetSelf().GetPathType()
                    ));
                    return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
                }

                TString error;
                Ydb::StatusIds_StatusCode status;
                Ydb::Table::DescribeExternalTableResult describeResult;

                if (!FillExternalTableDescription(describeResult, pathDescription.GetExternalTableDescription(), pathDescription.GetSelf(), status, error)) {
                    TIssues issues;
                    issues.AddIssue(error);
                    Reply(status, issues, ctx);
                    return;
                }

                const auto header = Request_->GetPeerMetaValues(NYdb::YDB_DATABASE_HEADER);
                const auto database = Request_->GetDatabaseName();
                if (header && database) {
                    const TString rawDatabase = CGIUnescapeRet(*header);
                    const TString logicalDatabase = NKikimr::CanonizePath(rawDatabase);
                    const TString physicalPrefix = *database == "/" ? "/" : *database + "/";
                    const TString logicalPrefix = logicalDatabase == "/" ? "/" : logicalDatabase + "/";
                    const TStringBuf dataSourcePath(describeResult.data_source_path());
                    if (rawDatabase.StartsWith("/") && logicalDatabase != *database
                        && Request_->NormalizePath(logicalDatabase) == *database
                        && dataSourcePath.StartsWith(physicalPrefix)) {
                        describeResult.set_data_source_path(TStringBuilder()
                            << logicalPrefix << dataSourcePath.SubStr(physicalPrefix.size()));
                    }
                }

                return ReplyWithResult(
                    Ydb::StatusIds::SUCCESS,
                    describeResult,
                    ctx
                );
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

void DoDescribeExternalTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDescribeExternalTableRPC(p.release()));
}

}
