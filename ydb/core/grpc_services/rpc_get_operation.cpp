#include "service_operation.h"

#include "operation_helpers.h"
#include "rpc_export_base.h"
#include "rpc_import_base.h"
#include "rpc_operation_request_base.h"

#include <ydb/core/grpc_services/base/base.h>
#include <google/protobuf/text_format.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_build_index.h>
#include <ydb/core/tx/schemeshard/schemeshard_export.h>
#include <ydb/core/tx/schemeshard/schemeshard_import.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/public/lib/operation_id/operation_id.h>

#include <ydb/library/actors/core/hfunc.h>

#include <util/string/cast.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace NOperationId;
using namespace Ydb;

using TEvGetOperationRequest = TGrpcRequestOperationCall<Ydb::Operations::GetOperationRequest,
    Ydb::Operations::GetOperationResponse>;

class TGetOperationRPC : public TRpcOperationRequestActor<TGetOperationRPC, TEvGetOperationRequest, true>,
                         public TExportConv {

    TStringBuf GetLogPrefix() const override {
        switch (OperationId_.GetKind()) {
        case TOperationId::EXPORT:
            return "[GetExport]";
        case TOperationId::IMPORT:
            return "[GetImport]";
        case TOperationId::BUILD_INDEX:
            return "[GetIndexBuild]";
        case TOperationId::SCRIPT_EXECUTION:
            return "[GetScriptExecution]";
        default:
            return "[Untagged]";
        }
    }

    IEventBase* MakeRequest() override {
        switch (OperationId_.GetKind()) {
        case TOperationId::EXPORT:
            return new NSchemeShard::TEvExport::TEvGetExportRequest(GetDatabaseName(), RawOperationId_);
        case TOperationId::IMPORT:
            return new NSchemeShard::TEvImport::TEvGetImportRequest(GetDatabaseName(), RawOperationId_);
        case TOperationId::BUILD_INDEX:
            return new NSchemeShard::TEvIndexBuilder::TEvGetRequest(GetDatabaseName(), RawOperationId_);
        default:
            Y_ABORT("unreachable");
        }
    }

    void PassAway() override {
        if (PipeActorId_) {
            NTabletPipe::CloseClient(SelfId(), PipeActorId_);
            PipeActorId_ = TActorId();
        }

        TRpcOperationRequestActor::PassAway();
    }

public:
    using TRpcOperationRequestActor::TRpcOperationRequestActor;

    void Bootstrap(const TActorContext &ctx) {
        const auto req = GetProtoRequest();

        try {
            OperationId_ = TOperationId(req->id());

            switch (OperationId_.GetKind()) {
            case TOperationId::CMS_REQUEST:
                SendCheckCmsOperation(ctx);
                break;
            case TOperationId::EXPORT:
            case TOperationId::IMPORT:
            case TOperationId::BUILD_INDEX:
                if (!TryGetId(OperationId_, RawOperationId_)) {
                    return ReplyWithStatus(StatusIds::BAD_REQUEST);
                }
                ResolveDatabase();
                break;
            case TOperationId::SCRIPT_EXECUTION:
                SendGetScriptExecutionOperation();
                break;
            default:
                SendNotifyTxCompletion(ctx);
                break;
            }
        } catch (const yexception& ex) {
            return ReplyWithStatus(StatusIds::BAD_REQUEST);
        }

        Become(&TGetOperationRPC::AwaitState);
    }
    STFUNC(AwaitState) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, HandleResponse);
            HFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            HFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
            HFunc(NConsole::TEvConsole::TEvGetOperationResponse, Handle);
            HFunc(NSchemeShard::TEvExport::TEvGetExportResponse, Handle);
            HFunc(NSchemeShard::TEvImport::TEvGetImportResponse, Handle);
            HFunc(NSchemeShard::TEvIndexBuilder::TEvGetResponse, Handle);
            HFunc(NKqp::TEvGetScriptExecutionOperationResponse, Handle);

        default:
            return StateBase(ev);
        }
    }
private:
    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr&, const TActorContext& ctx) {
        ReplyGetOperationResponse(false, ctx);
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr&, const TActorContext& ctx) {
        ReplyGetOperationResponse(true, ctx);
    }

    void Handle(NConsole::TEvConsole::TEvGetOperationResponse::TPtr &ev, const TActorContext& ctx) {
        auto &rec = ev->Get()->Record.GetResponse();
        if (rec.operation().ready())
            ReplyWithError(rec.operation().status(), rec.operation().issues(), ctx);
        else
            ReplyGetOperationResponse(false, ctx);
    }

    void HandleResponse(typename TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        Y_UNUSED(ctx);
    }

    void SendCheckCmsOperation(const TActorContext& ctx) {
        ui64 tid;

        try {
            const auto& cmsIds = OperationId_.GetValue("cmstid");
            if (cmsIds.size() != 1) {
                return ReplyWithStatus(StatusIds::BAD_REQUEST);
            }
            if (!TryFromString(*cmsIds[0], tid)) {
                return ReplyWithStatus(StatusIds::BAD_REQUEST);
            }
        } catch (const yexception& ex) {
            Request->RaiseIssue(NYql::ExceptionToIssue(ex));
            return ReplyWithStatus(StatusIds::BAD_REQUEST);
        }

        IActor* pipeActor = NTabletPipe::CreateClient(ctx.SelfID, tid);
        Y_ABORT_UNLESS(pipeActor);
        PipeActorId_ = ctx.ExecutorThread.RegisterActor(pipeActor);

        auto request = MakeHolder<NConsole::TEvConsole::TEvGetOperationRequest>();
        request->Record.MutableRequest()->set_id(GetProtoRequest()->id());
        request->Record.SetUserToken(Request->GetSerializedToken());
        NTabletPipe::SendData(ctx, PipeActorId_, request.Release());
    }

    void SendNotifyTxCompletion(const TActorContext& ctx) {
        ui64 txId;
        ui64 schemeShardTabletId;
        try {
            const auto& txIds = OperationId_.GetValue("txid");
            const auto& sstIds = OperationId_.GetValue("sstid");
            if (txIds.size() != 1 || sstIds.size() != 1) {
                return ReplyWithStatus(StatusIds::BAD_REQUEST);
            }

            if (!TryFromString(*txIds[0], txId) || !TryFromString(*sstIds[0], schemeShardTabletId)) {
                return ReplyWithStatus(StatusIds::BAD_REQUEST);
            }
        } catch (const yexception& ex) {
            return ReplyWithStatus(StatusIds::BAD_REQUEST);
        }

        IActor* pipeActor = NTabletPipe::CreateClient(ctx.SelfID, schemeShardTabletId);
        Y_ABORT_UNLESS(pipeActor);
        PipeActorId_ = ctx.ExecutorThread.RegisterActor(pipeActor);

        auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
        request->Record.SetTxId(txId);
        NTabletPipe::SendData(ctx, PipeActorId_, request.Release());
    }

    void SendGetScriptExecutionOperation() {
        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), new NKqp::TEvGetScriptExecutionOperation(GetDatabaseName(), OperationId_));
    }

    void Handle(NSchemeShard::TEvExport::TEvGetExportResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record.GetResponse();

        LOG_D("Handle TEvExport::TEvGetExportResponse"
            << ": record# " << record.ShortDebugString());

        TEvGetOperationRequest::TResponse resp;
        *resp.mutable_operation() = TExportConv::ToOperation(record.GetEntry());
        Reply(resp, ctx);
    }

    void Handle(NSchemeShard::TEvImport::TEvGetImportResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record.GetResponse();

        LOG_D("Handle TEvImport::TEvGetImportResponse"
            << ": record# " << record.ShortDebugString());

        TEvGetOperationRequest::TResponse resp;
        *resp.mutable_operation() = TImportConv::ToOperation(record.GetEntry());
        Reply(resp, ctx);
    }

    void Handle(NSchemeShard::TEvIndexBuilder::TEvGetResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;

        LOG_D("Handle TEvIndexBuilder::TEvGetResponse"
            << ": record# " << record.ShortDebugString());

        if (record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            ReplyGetOperationResponse(true, ctx, record.GetStatus());
        } else {
            TEvGetOperationRequest::TResponse resp;

            ::NKikimr::NGRpcService::ToOperation(record.GetIndexBuild(), resp.mutable_operation());
            Reply(resp, ctx);
        }
    }

    void Handle(NKqp::TEvGetScriptExecutionOperationResponse::TPtr& ev, const TActorContext& ctx) {
        TEvGetOperationRequest::TResponse resp;
        auto deferred = resp.mutable_operation();
        if (ev->Get()->Status != Ydb::StatusIds::NOT_FOUND) {
            deferred->set_id(GetProtoRequest()->id());
        }
        deferred->set_ready(ev->Get()->Ready);
        deferred->set_status(ev->Get()->Status);
        if (ev->Get()->Issues) {
            for (const NYql::TIssue& issue : ev->Get()->Issues) {
                NYql::IssueToMessage(issue, deferred->add_issues());
            }
        }
        if (ev->Get()->Metadata) {
            deferred->mutable_metadata()->Swap(ev->Get()->Metadata.Get());
        }
        Reply(resp, ctx);
    }

    void ReplyWithError(const StatusIds::StatusCode status,
                        const google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> &issues,
                        const TActorContext &ctx) {
        TEvGetOperationRequest::TResponse resp;
        auto deferred = resp.mutable_operation();
        deferred->set_id(GetProtoRequest()->id());
        deferred->set_ready(true);
        deferred->set_status(status);
        if (issues.size())
            deferred->mutable_issues()->CopyFrom(issues);
        Reply(resp, ctx);
    }

    void ReplyGetOperationResponse(bool ready, const TActorContext& ctx, StatusIds::StatusCode status = StatusIds::SUCCESS) {
        TEvGetOperationRequest::TResponse resp;
        auto deferred = resp.mutable_operation();
        deferred->set_id(GetProtoRequest()->id());
        deferred->set_ready(ready);
        if (ready) {
            deferred->set_status(status);
        }
        Reply(resp, ctx);
    }

    template<typename TMessage>
    void ReplyGetOperationResponse(bool ready, const TActorContext& ctx,
        const TMessage& metadata, StatusIds::StatusCode status = StatusIds::SUCCESS)
    {
        TEvGetOperationRequest::TResponse resp;
        auto deferred = resp.mutable_operation();
        deferred->set_id(GetProtoRequest()->id());
        deferred->set_ready(ready);
        if (ready) {
            deferred->set_status(status);
        }
        auto data = deferred->mutable_metadata();
        data->PackFrom(metadata);
        Reply(resp, ctx);
    }


    void Reply(const TEvGetOperationRequest::TResponse& response, const TActorContext& ctx) {
        TProtoResponseHelper::SendProtoResponse(response, response.operation().status(), Request);
        Die(ctx);
    }

    void ReplyWithStatus(StatusIds::StatusCode status) {
        Request->ReplyWithYdbStatus(status);
        PassAway();
    }

    TOperationId OperationId_;
    ui64 RawOperationId_ = 0;
    TActorId PipeActorId_;
};

void DoGetOperationRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TGetOperationRPC(p.release()));
}

template<>
IActor* TEvGetOperationRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TGetOperationRPC(msg);
}

} // namespace NGRpcService
} // namespace NKikimr
