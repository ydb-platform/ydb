#include "operation_helpers.h"
#include "rpc_calls.h"

#include "rpc_export_base.h"
#include "rpc_import_base.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/kikimr_issue.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <ydb/core/tx/schemeshard/schemeshard_build_index.h>
#include <ydb/core/tx/schemeshard/schemeshard_export.h>
#include <ydb/core/tx/schemeshard/schemeshard_import.h>

#include <ydb/core/protos/index_builder.pb.h>

#include <ydb/public/lib/operation_id/protos/operation_id.pb.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKikimr {
namespace NGRpcService {

using std::shared_ptr;

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)

IEventBase* CreateNavigateForPath(const TString& path) {
    auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();

    request->DatabaseName = path;

    auto& entry = request->ResultSet.emplace_back();
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
    entry.Path = ::NKikimr::SplitPath(path);
    entry.RedirectRequired = false;

    return new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release());
}

TActorId CreatePipeClient(ui64 id, const TActorContext& ctx) {
    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {.RetryLimitCount = 3};
    return ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, id, clientConfig));
}

Ydb::TOperationId ToOperationId(const NKikimrIndexBuilder::TIndexBuild& build) {
    Ydb::TOperationId operationId;
    operationId.SetKind(Ydb::TOperationId::BUILD_INDEX);
    NOperationId::AddOptionalValue(operationId, "id", ToString(build.GetId()));

    return operationId;
}

void ToOperation(const NKikimrIndexBuilder::TIndexBuild& build, Ydb::Operations::Operation* operation) {
    operation->set_id(NOperationId::ProtoToString(ToOperationId(build)));
    operation->mutable_issues()->CopyFrom(build.GetIssues());

    switch (build.GetState()) {
        case Ydb::Table::IndexBuildState::STATE_DONE:
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::SUCCESS);
        break;
        case Ydb::Table::IndexBuildState::STATE_CANCELLED:
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::CANCELLED);
        break;
        case Ydb::Table::IndexBuildState::STATE_REJECTED:
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::ABORTED);
        break;
        default:
            operation->set_ready(false);
    }

    Ydb::Table::IndexBuildMetadata metadata;
    metadata.set_state(build.GetState());
    metadata.set_progress(build.GetProgress());
    auto desc = metadata.mutable_description();
    desc->set_path(build.GetSettings().source_path());
    desc->mutable_index()->CopyFrom(build.GetSettings().index());

    auto data = operation->mutable_metadata();
    data->PackFrom(metadata);
}

bool TryGetId(const NOperationId::TOperationId& operationId, ui64& id) {
    const auto& ids = operationId.GetValue("id");

    if (ids.size() != 1) {
        return false;
    }

    if (!TryFromString(*ids[0], id)) {
        return false;
    }

    return id;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TSSOpSubscriber : public TActorBootstrapped<TSSOpSubscriber> {
public:
    TSSOpSubscriber(ui64 schemeshardId, ui64 txId, TString dbName, TOpType opType, shared_ptr<IRequestOpCtx>&& op)
        : SchemeshardId(schemeshardId)
        , TxId(txId)
        , DatabaseName(dbName)
        , OpType(opType)
        , Req(std::move(op))
    {
        StateFunc = &TSSOpSubscriber::DoSubscribe;
    }

    void Bootstrap(const TActorContext &ctx) {
        SSPipeClient = CreatePipeClient(SchemeshardId, ctx);

        LogPrefix = TStringBuilder() << "[SSOpSubscriber " << SelfId() << "] ";

        (this->*StateFunc)(ctx);

        Become(&TSSOpSubscriber::AwaitState);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr&, const TActorContext&) {}

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&, const TActorContext& ctx) {
        if (SSPipeClient) {
            NTabletPipe::CloseClient(ctx, SSPipeClient);
            SSPipeClient = TActorId();
        }

        LOG_E("Handle TEvTabletPipe::TEvClientDestroyed");
        if (AttemptsCounter > 3u) {
            TString error = "Too many attempts to create pipe to SS.";
            LOG_E(error);
            Req->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::YDB_DB_NOT_READY, error));
            Req->ReplyWithYdbStatus(Ydb::StatusIds::OVERLOADED);
            Die(ctx);
            return;
        }

        NTabletPipe::TClientConfig config;
        config.RetryPolicy = {
            .RetryLimitCount = 5,
            .MinRetryTime = TDuration::MilliSeconds(50),
            .MaxRetryTime = TDuration::Seconds(10),
            .DoFirstRetryInstantly = false
        };
        SSPipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, SchemeshardId, config));

        AttemptsCounter++;
        (this->*StateFunc)(ctx);
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr&, const TActorContext&) {}

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr&, const TActorContext& ctx) {
        //TODO: Change SS API to get operation status just from TEvNotifyTxCompletionResult
        switch (OpType) {
            case TOpType::Common:
                NTabletPipe::CloseClient(ctx, SSPipeClient);
                Req->ReplyWithYdbStatus(Ydb::StatusIds::SUCCESS);
                Die(ctx);
                return;
            case TOpType::BuildIndex:
                StateFunc = &TSSOpSubscriber::GetBuildIndexStatus;
                break;
            case TOpType::Export:
                StateFunc = &TSSOpSubscriber::GetExportStatus;
                break;
            case TOpType::Import:
                StateFunc = &TSSOpSubscriber::GetImportStatus;
                break;
            default:
                NTabletPipe::CloseClient(ctx, SSPipeClient);
                Req->ReplyWithYdbStatus(Ydb::StatusIds::INTERNAL_ERROR);
                Die(ctx);
                return;
        }
        (this->*StateFunc)(ctx);
    }

    STFUNC(AwaitState) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            HFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(NSchemeShard::TEvExport::TEvGetExportResponse, Handle);
            HFunc(NSchemeShard::TEvImport::TEvGetImportResponse, Handle);
            HFunc(NSchemeShard::TEvIndexBuilder::TEvGetResponse, Handle);
        default:
            {
                 Req->ReplyWithYdbStatus(Ydb::StatusIds::INTERNAL_ERROR);
                 Die(ctx);
            }
        }
    }

private:
    void PassAway() override {
        if (SSPipeClient) {
            NTabletPipe::CloseClient(SelfId(), SSPipeClient);
            SSPipeClient = TActorId();
        }
        IActor::PassAway();
    }

    void DoSubscribe(const TActorContext& ctx) {
        auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
        request->Record.SetTxId(TxId);
        NTabletPipe::SendData(ctx, SSPipeClient, request.Release());
    }

    void GetBuildIndexStatus(const TActorContext& ctx) {
        auto request = new NSchemeShard::TEvIndexBuilder::TEvGetRequest(DatabaseName, TxId);
        NTabletPipe::SendData(ctx, SSPipeClient, request);
    }

    void GetExportStatus(const TActorContext& ctx) {
        auto request = new NSchemeShard::TEvExport::TEvGetExportRequest(DatabaseName, TxId);
        NTabletPipe::SendData(ctx, SSPipeClient, request);
    }

    void GetImportStatus(const TActorContext& ctx) {
        auto request = new NSchemeShard::TEvImport::TEvGetImportRequest(DatabaseName, TxId);
        NTabletPipe::SendData(ctx, SSPipeClient, request);
    }

    void Handle(NSchemeShard::TEvExport::TEvGetExportResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record.GetResponse();

        LOG_D("Handle TEvExport::TEvGetExportResponse"
            << ": record# " << record.ShortDebugString());

        auto op = TExportConv::ToOperation(record.GetEntry());
        Req->SendOperation(op);
        Die(ctx);
    }

    void Handle(NSchemeShard::TEvImport::TEvGetImportResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record.GetResponse();

        LOG_D("Handle TEvImport::TEvGetImportResponse"
            << ": record# " << record.ShortDebugString());

        auto op = TImportConv::ToOperation(record.GetEntry());
        Req->SendOperation(op);
        Die(ctx);
    }

    void Handle(NSchemeShard::TEvIndexBuilder::TEvGetResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;

        LOG_D("Handle TEvIndexBuilder::TEvGetResponse"
            << ": record# " << record.ShortDebugString());

        if (record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            Req->ReplyWithYdbStatus(record.GetStatus());
        } else {
            Ydb::Operations::Operation op;
            ::NKikimr::NGRpcService::ToOperation(record.GetIndexBuild(), &op);
            Req->SendOperation(op);
        }
        Die(ctx);
    }

private:
    const ui64 SchemeshardId;
    const ui64 TxId;
    const TString DatabaseName;
    const TOpType OpType;
    shared_ptr<IRequestOpCtx> Req;
    using TStateFunc = void (TSSOpSubscriber::*)(const TActorContext& ctx);

    TActorId SSPipeClient;
    ui64 AttemptsCounter = 0;
    TStateFunc StateFunc;
    TString LogPrefix;
};

void CreateSSOpSubscriber(ui64 schemeshardId, ui64 txId, const TString& dbName, TOpType opType, shared_ptr<IRequestOpCtx>&& op, const TActorContext& ctx) {
    ctx.Register(new TSSOpSubscriber(schemeshardId, txId, dbName, opType, std::move(op)));
}

} // namespace NGRpcService
} // namespace NKikimr
