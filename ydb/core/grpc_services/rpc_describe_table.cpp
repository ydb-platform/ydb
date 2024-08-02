#include "rpc_calls.h"
#include "rpc_scheme_base.h"
#include "service_table.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
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

    TString OverrideName;

    static bool ShowPrivatePath(const TString& path) {
        if (AppData()->AllowPrivateTableDescribeForTest) {
           return true;
        }

        auto pathElements = ::NKikimr::SplitPath(path);
        if (pathElements.size() != 0 && NTableIndex::IsImplTable(pathElements.back())) {
            return true;
        }

        return false;
    }

public:
    TDescribeTableRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        const auto& path = GetProtoRequest()->path();
        const auto paths = NKikimr::SplitPath(path);
        if (paths.empty()) {
            Request_->RaiseIssue(NYql::TIssue("Invalid path"));
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }

        auto navigate = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        navigate->DatabaseName = CanonizePath(Request_->GetDatabaseName().GetOrElse(""));
        auto& entry = navigate->ResultSet.emplace_back();
        entry.Path = paths;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
        entry.SyncVersion = true;
        entry.ShowPrivatePath = ShowPrivatePath(path);

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate));
        Become(&TDescribeTableRPC::StateWork);
    }

private:
    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            default: TBase::StateWork(ev);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        auto* navigate = ev->Get()->Request.Get();

        Y_ABORT_UNLESS(navigate->ResultSet.size() == 1);
        const auto& entry = navigate->ResultSet.front();

        if (navigate->ErrorCount > 0) {
            switch (entry.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
                return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
            default:
                return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);
            }
        }

        if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindIndex) {
            auto list = entry.ListNodeEntry;
            if (!list || list->Children.size() != 1) {
                return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
            }

            OverrideName = entry.Path.back();
            SendProposeRequest(CanonizePath(ChildPath(entry.Path, list->Children.at(0).Name)), ctx);
        } else {
            SendProposeRequest(GetProtoRequest()->path(), ctx);
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
                ConvertDirectoryEntry(pathDescription.GetSelf(), selfEntry, true);
                if (OverrideName) {
                    selfEntry->set_name(OverrideName);
                }

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

    void SendProposeRequest(const TString& path, const TActorContext& ctx) {
        const auto req = GetProtoRequest();

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

        record->MutableOptions()->SetShowPrivateTable(ShowPrivatePath(path));

        ctx.Send(MakeTxProxyID(), navigateRequest.release());
    }
};

void DoDescribeTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDescribeTableRPC(p.release()));
}

} // namespace NKikimr
} // namespace NGRpcService
