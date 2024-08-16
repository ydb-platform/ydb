#include "service_table.h"
#include <ydb/core/grpc_services/base/base.h>

#include "rpc_calls.h"
#include "rpc_scheme_base.h"

#include "service_table.h"
#include "rpc_common/rpc_common.h"
#include <ydb/core/base/table_index.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

using TEvDescribeTableRequest = TGrpcRequestOperationCall<Ydb::Table::DescribeTableRequest,
    Ydb::Table::DescribeTableResponse>;

class TDescribeTableRPC : public TRpcSchemeRequestActor<TDescribeTableRPC, TEvDescribeTableRequest> {
    using TBase = TRpcSchemeRequestActor<TDescribeTableRPC, TEvDescribeTableRequest>;

public:
    TDescribeTableRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        SendProposeRequest(ctx);
        Become(&TDescribeTableRPC::StateWork);
    }

private:
    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            default: TBase::StateWork(ev);
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->GetRecord();
        const auto status = record.GetStatus();
        if (record.HasReason()) {
            auto issue = NYql::TIssue(record.GetReason());
            Request_->RaiseIssue(issue);
        }
        Ydb::Table::DescribeTableResult describeTableResult;
        switch (status) {
            case NKikimrScheme::StatusSuccess: {
                const auto& pathDescription = record.GetPathDescription();
                Ydb::Scheme::Entry* selfEntry = describeTableResult.mutable_self();
                selfEntry->set_name(pathDescription.GetSelf().GetName());
                selfEntry->set_type(static_cast<Ydb::Scheme::Entry::Type>(pathDescription.GetSelf().GetPathType()));
                ConvertDirectoryEntry(pathDescription.GetSelf(), selfEntry, true);

                if (pathDescription.HasColumnTableDescription()) {
                    const auto& tableDescription = pathDescription.GetColumnTableDescription();
                    FillColumnDescription(describeTableResult, tableDescription);

                    if (GetProtoRequest()->include_table_stats()) {
                        FillTableStats(describeTableResult, pathDescription, false);

                        describeTableResult.mutable_table_stats()->set_partitions(
                            tableDescription.GetColumnShardCount());
                    }

                    return ReplyWithResult(Ydb::StatusIds::SUCCESS, describeTableResult, ctx);
                }

                const auto& tableDescription = pathDescription.GetTable();
                NKikimrMiniKQL::TType splitKeyType;

                try {
                    FillColumnDescription(describeTableResult, splitKeyType, tableDescription);
                } catch (const std::exception& ex) {
                    LOG_ERROR(ctx, NKikimrServices::GRPC_SERVER, "Unable to fill column description: %s", ex.what());
                    Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
                    return Reply(Ydb::StatusIds::INTERNAL_ERROR, ctx);
                }

                describeTableResult.mutable_primary_key()->CopyFrom(tableDescription.GetKeyColumnNames());

                try {
                    FillTableBoundary(describeTableResult, tableDescription, splitKeyType);
                } catch (const std::exception& ex) {
                    LOG_ERROR(ctx, NKikimrServices::GRPC_SERVER, "Unable to fill table boundary: %s", ex.what());
                    Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
                    return Reply(Ydb::StatusIds::INTERNAL_ERROR, ctx);
                }

                try {
                    FillIndexDescription(describeTableResult, tableDescription);
                } catch (const std::exception& ex) {
                    LOG_ERROR(ctx, NKikimrServices::GRPC_SERVER, "Unable to fill index description: %s", ex.what());
                    Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
                    return Reply(Ydb::StatusIds::INTERNAL_ERROR, ctx);
                }

                FillChangefeedDescription(describeTableResult, tableDescription);

                if (GetProtoRequest()->include_table_stats()) {
                    FillTableStats(describeTableResult, pathDescription,
                        GetProtoRequest()->include_partition_stats());
                }

                FillStorageSettings(describeTableResult, tableDescription);
                FillColumnFamilies(describeTableResult, tableDescription);
                FillAttributes(describeTableResult, pathDescription);
                FillPartitioningSettings(describeTableResult, tableDescription);
                FillKeyBloomFilter(describeTableResult, tableDescription);
                FillReadReplicasSettings(describeTableResult, tableDescription);

                return ReplyWithResult(Ydb::StatusIds::SUCCESS, describeTableResult, ctx);
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

    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = GetProtoRequest();
        const TString path = req->path();

        std::unique_ptr<TEvTxUserProxy::TEvNavigate> navigateRequest(new TEvTxUserProxy::TEvNavigate());
        SetAuthToken(navigateRequest, *Request_);
        SetDatabase(navigateRequest.get(), *Request_);
        NKikimrSchemeOp::TDescribePath* record = navigateRequest->Record.MutableDescribePath();
        record->SetPath(path);
        if (req->include_shard_key_bounds()) {
            record->MutableOptions()->SetReturnBoundaries(true);
        }

        if (req->include_partition_stats() && req->include_table_stats()) {
            record->MutableOptions()->SetReturnPartitionStats(true);
        }

        if (AppData(ctx)->AllowPrivateTableDescribeForTest || path.EndsWith(TStringBuilder() << "/" << NTableIndex::ImplTable)) {
            record->MutableOptions()->SetShowPrivateTable(true);
        }

        ctx.Send(MakeTxProxyID(), navigateRequest.release());
    }
};

void DoDescribeTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDescribeTableRPC(p.release()));
}

} // namespace NKikimr
} // namespace NGRpcService
