#include "service_yql_scripting.h"
#include "rpc_kqp_base.h"
#include "audit_dml_operations.h"

#include <ydb/public/api/protos/ydb_scripting.pb.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/grpc_services/grpc_integrity_trails.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>

#include <ydb/library/services/services.pb.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/core/protos/stream.pb.h>

namespace NKikimr {
namespace NGRpcService {

using TEvStreamExecuteYqlScriptRequest =
    TGrpcRequestNoOperationCall<Ydb::Scripting::ExecuteYqlRequest, Ydb::Scripting::ExecuteYqlPartialResponse>;

namespace {

using namespace NActors;
using namespace Ydb;

namespace {
    struct TParseRequestError {
        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;

        TParseRequestError()
            : Status(Ydb::StatusIds::INTERNAL_ERROR)
            , Issues({ MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                "Unexpected error while parsing request.") }) {}

        TParseRequestError(const Ydb::StatusIds::StatusCode& status, const NYql::TIssues& issues)
            : Status(status)
            , Issues(issues) {}
    };

    // Stores ResultSets from data query response until they are all sent to client one by one
    struct TDataQueryStreamContext {
        TDataQueryStreamContext(NKqp::TEvKqp::TEvDataQueryStreamPart::TPtr& handle)
            : Handle(handle.Release())
            , ResultIterator(Handle->Get()->Record.GetResults().begin())
        {}

        NKqp::TEvKqp::TEvDataQueryStreamPart::TPtr Handle;
        google::protobuf::RepeatedPtrField<NKikimrMiniKQL::TResult>::const_iterator ResultIterator;
    };

    enum EStreamRpcWakeupTag : ui64 {
        ClientLostTag = 1,
        ClientTimeoutTag = 2
    };
}

class TStreamExecuteYqlScriptRPC
    : public TRpcRequestWithOperationParamsActor<TStreamExecuteYqlScriptRPC, TEvStreamExecuteYqlScriptRequest, false> {

private:
    typedef TRpcRequestWithOperationParamsActor<TStreamExecuteYqlScriptRPC, TEvStreamExecuteYqlScriptRequest, false> TBase;

    static std::function<TEvStreamExecuteYqlScriptRequest::TFinishWrapper(std::function<void()>&&)>
    GetFinishWrapper(std::shared_ptr<std::atomic_bool> flag) {
        return [flag](std::function<void()>&& cb) {
            return [cb = std::move(cb), flag](const NYdbGrpc::IRequestContextBase::TAsyncFinishResult& future) mutable {
                Y_ASSERT(future.HasValue());
                if (future.GetValue() == NYdbGrpc::IRequestContextBase::EFinishStatus::CANCEL || flag->load()) {
                    cb();
                }
            };
        };
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_STREAM_REQ;
    }

    TStreamExecuteYqlScriptRPC(IRequestNoOpCtx* request, ui64 rpcBufferSize)
        : TBase(request)
        , RpcBufferSize_(rpcBufferSize)
        , CancelationFlag(std::make_shared<std::atomic_bool>(false))
    {
        // StreamExecuteYqlScript allows write in to table.
        // But CanselAfter should not trigger cancelation if we chage table
        // This logic is broken - just disable CancelAfter_ for a while
        CancelAfter_ = TDuration();

        auto call = dynamic_cast<TEvStreamExecuteYqlScriptRequest*>(request);
        Y_ABORT_UNLESS(call);
        call->SetCustomFinishWrapper(GetFinishWrapper(CancelationFlag));
    }

    using TBase::Request_;

    void Bootstrap(const TActorContext &ctx) {
        this->Become(&TStreamExecuteYqlScriptRPC::StateWork);
        TBase::Bootstrap(ctx);

        const auto& cfg = AppData(ctx)->StreamingConfig.GetOutputStreamConfig();

        InactiveClientTimeout_ = TDuration::FromValue(cfg.GetInactiveClientTimeout());
        if (InactiveClientTimeout_) {
            SetClientTimeoutTimer(InactiveClientTimeout_, ctx);
        }

        LastDataStreamTimestamp_ = TAppData::TimeProvider->Now();

        auto selfId = this->SelfId();
        auto as = TActivationContext::ActorSystem();

        RequestPtr()->SetFinishAction([selfId, as]() {
            as->Send(selfId, new TEvents::TEvWakeup(EStreamRpcWakeupTag::ClientLostTag));
        });

        RequestPtr()->SetStreamingNotify([selfId, as](size_t left) {
            as->Send(selfId, new TRpcServices::TEvGrpcNextReply(left));
        });

        Proceed(ctx);
    }

private:
    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvWakeup, Handle);
            HFunc(NKqp::TEvKqp::TEvDataQueryStreamPart, Handle);
            HFunc(TRpcServices::TEvGrpcNextReply, Handle);
            HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
            HFunc(NKqp::TEvKqpExecuter::TEvStreamData, Handle);
            // Overide default forget action which terminate this actor on client disconnect
            hFunc(TRpcServices::TEvForgetOperation, HandleForget);
            default: {
                return ReplyFinishStream(TStringBuilder()
                    << "Unexpected event received in TStreamExecuteYqlScriptRPC::StateWork: " << ev->GetTypeRewrite());
            }
        }
    }

    void HandleForget(TRpcServices::TEvForgetOperation::TPtr &ev) {
        Y_UNUSED(ev);
    }

    void Proceed(const TActorContext &ctx) {
        const auto& featureFlags = AppData(ctx)->FeatureFlags;
        if (!featureFlags.GetAllowStreamExecuteYqlScript()) {
            return ReplyFinishStream("StreamExecuteYqlScript request is not supported");
        }

        auto req = GetProtoRequest();
        const auto traceId = Request_->GetTraceId();

        AuditContextAppend(Request_.get(), *req);
        NDataIntegrity::LogIntegrityTrails(traceId, *GetProtoRequest(), ctx);

        auto script = req->script();

        NYql::TIssues issues;
        if (!CheckQuery(script, issues)) {
            return ReplyFinishStream(Ydb::StatusIds::BAD_REQUEST, issues);
        }

        ::Ydb::Operations::OperationParams operationParams;

        auto settings = NKqp::NPrivateEvents::TQueryRequestSettings()
            .SetKeepSession(false)
            .SetUseCancelAfter(false)
            .SetSyntax(req->syntax());

        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>(
            NKikimrKqp::QUERY_ACTION_EXECUTE,
            NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING,
            SelfId(),
            Request_,
            TString(), //sessionId
            std::move(script),
            TString(), //queryId
            nullptr, //tx_control
            &req->parameters(),
            req->collect_stats(),
            nullptr, // query_cache_policy
            req->has_operation_params() ? &req->operation_params() : nullptr,
            settings
        );

        if (!ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release())) {
            return ReplyFinishStream("Couldn't send request to KqpProxy");
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        switch ((EStreamRpcWakeupTag) ev->Get()->Tag) {
            case EStreamRpcWakeupTag::ClientLostTag:
                return HandleClientLost(ctx);
            case EStreamRpcWakeupTag::ClientTimeoutTag:
                return HandleClientTimeout(ctx);
            default:
                break;
        }
        switch ((TBase::EWakeupTag)ev->Get()->Tag) {
            case TBase::WakeupTagTimeout:
                return HandleOperationTimeout(ctx);
            default:
                break;
        }
    }

    void SendDataQueryResultPart(const TActorContext&) {
        ++ResultsReceived_;
        const auto& kqpResult = *DataQueryStreamContext->ResultIterator;

        Ydb::Scripting::ExecuteYqlPartialResponse response;
        response.set_status(StatusIds::SUCCESS);
        auto result = response.mutable_result();

        try {
            NKqp::ConvertKqpQueryResultToDbResult(kqpResult, result->mutable_result_set());
        } catch (std::exception ex) {
            ReplyFinishStream(ex.what());
        }

        result->set_result_set_index(ResultsReceived_ - 1);

        TString out;
        Y_PROTOBUF_SUPPRESS_NODISCARD response.SerializeToString(&out);

        GRpcResponsesSizeQueue_.push(out.size());
        GRpcResponsesSize_ += out.size();

        RequestPtr()->SendSerializedResult(std::move(out), StatusIds::SUCCESS);
    }

    // From TKqpStreamRequestHandler
    void Handle(NKqp::TEvKqp::TEvDataQueryStreamPart::TPtr& ev, const TActorContext& ctx) {
        GatewayRequestHandlerActorId_ = ActorIdFromProto(ev->Get()->Record.GetGatewayActorId());

        if (!ev->Get()->Record.GetResults().size()) {
            return ReplyFinishStream("Received TEvDataQueryStreamPart with no results");
        }
        if (DataQueryStreamContext) {
            return ReplyFinishStream("Received TEvDataQueryStreamPart event while previous data query is in progress");
        }

        DataQueryStreamContext = MakeHolder<TDataQueryStreamContext>(ev);

        SendDataQueryResultPart(ctx);
    }

    // From TKqpScanQueryStreamRequestHandler
    void Handle(NKqp::TEvKqpExecuter::TEvStreamData::TPtr& ev, const TActorContext& ctx) {
        if (GatewayRequestHandlerActorId_ != ev->Sender) {
            ++ResultsReceived_;
            GatewayRequestHandlerActorId_ = ev->Sender;
        }

        Ydb::Scripting::ExecuteYqlPartialResponse response;
        response.set_status(StatusIds::SUCCESS);
        auto result = response.mutable_result();
        result->mutable_result_set()->Swap(ev->Get()->Record.MutableResultSet());
        result->set_result_set_index(ResultsReceived_ - 1);

        TString out;
        Y_PROTOBUF_SUPPRESS_NODISCARD response.SerializeToString(&out);

        GRpcResponsesSizeQueue_.push(out.size());
        GRpcResponsesSize_ += out.size();

        RequestPtr()->SendSerializedResult(std::move(out), StatusIds::SUCCESS);

        ui64 freeSpace = GRpcResponsesSize_ < RpcBufferSize_
            ? RpcBufferSize_ - GRpcResponsesSize_
            : 0;

        if (freeSpace == 0) {
            WaitOnSeqNo_ = ev->Get()->Record.GetSeqNo();
        }

        LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << " Send stream data ack"
            << ", seqNo: " << ev->Get()->Record.GetSeqNo()
            << ", freeSpace: " << freeSpace
            << ", to: " << ev->Sender
            << ", queue: " << GRpcResponsesSizeQueue_.size());

        auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
        resp->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
        resp->Record.SetFreeSpace(freeSpace);

        ctx.Send(ev->Sender, resp.Release());
    }

    void Handle(TRpcServices::TEvGrpcNextReply::TPtr& ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << " NextReply"
            << ", left: " << ev->Get()->LeftInQueue
            << ", queue: " << GRpcResponsesSizeQueue_.size()
            << ", used memory: " << GRpcResponsesSize_
            << ", buffer size: " << RpcBufferSize_);
        LastDataStreamTimestamp_ = TAppData::TimeProvider->Now();

        if (DataQueryStreamContext) {
            //DataQuery in progress
            if (++DataQueryStreamContext->ResultIterator != DataQueryStreamContext->Handle->Get()->Record.GetResults().end()) {
                // Send next ResultSet to client
                return SendDataQueryResultPart(ctx);
            } else {
                // Send ack to gateway request handler actor
                auto resp = MakeHolder<NKqp::TEvKqp::TEvDataQueryStreamPartAck>();
                ctx.Send(GatewayRequestHandlerActorId_, resp.Release());
                DataQueryStreamContext.Reset();
                return;
            }

        } else {
            //ScanQuery in progress
            while (GRpcResponsesSizeQueue_.size() > ev->Get()->LeftInQueue) {
                GRpcResponsesSize_ -= GRpcResponsesSizeQueue_.front();
                GRpcResponsesSizeQueue_.pop();
            }
            Y_DEBUG_ABORT_UNLESS(GRpcResponsesSizeQueue_.empty() == (GRpcResponsesSize_ == 0));

            if (WaitOnSeqNo_ && RpcBufferSize_ > GRpcResponsesSize_) {
                ui64 freeSpace = RpcBufferSize_ - GRpcResponsesSize_;

                LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << " Send stream data ack"
                    << ", seqNo: " << *WaitOnSeqNo_
                    << ", freeSpace: " << freeSpace
                    << ", to: " << GatewayRequestHandlerActorId_);

                auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
                resp->Record.SetSeqNo(*WaitOnSeqNo_);
                resp->Record.SetFreeSpace(freeSpace);

                ctx.Send(GatewayRequestHandlerActorId_, resp.Release());

                WaitOnSeqNo_.Clear();
            }
        }
    }

    // Final response
    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        NDataIntegrity::LogIntegrityTrails(Request_->GetTraceId(), *GetProtoRequest(), ev, ctx);

        auto& record = ev->Get()->Record.GetRef();

        NYql::TIssues issues;
        const auto& issueMessage = record.GetResponse().GetQueryIssues();
        NYql::IssuesFromMessage(issueMessage, issues);

        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            Request_->SetRuHeader(record.GetConsumedRu());

            Ydb::Scripting::ExecuteYqlPartialResponse response;
            TString out;
            auto& kqpResponse = record.GetResponse();
            response.set_status(Ydb::StatusIds::SUCCESS);

            if (kqpResponse.HasQueryStats()) {
                FillQueryStats(*response.mutable_result()->mutable_query_stats(), kqpResponse);
            } else if (kqpResponse.HasQueryPlan()) {
                response.mutable_result()->mutable_query_stats()->set_query_plan(kqpResponse.GetQueryPlan());
            }

            AuditContextAppend(Request_.get(), *GetProtoRequest(), response);

            Y_PROTOBUF_SUPPRESS_NODISCARD response.SerializeToString(&out);
            RequestPtr()->SendSerializedResult(std::move(out), record.GetYdbStatus());
        }

        ReplyFinishStream(record.GetYdbStatus(), issues);
    }

private:
    void SetClientTimeoutTimer(TDuration timeout, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << " Set stream timeout timer for " << timeout);

        auto *ev = new IEventHandle(this->SelfId(), this->SelfId(), new TEvents::TEvWakeup(EStreamRpcWakeupTag::ClientTimeoutTag));
        ClientTimeoutTimerCookieHolder_.Reset(ISchedulerCookie::Make2Way());
        CreateLongTimer(ctx, timeout, ev, 0, ClientTimeoutTimerCookieHolder_.Get());
    }

    void HandleClientLost(const TActorContext& ctx) {
        LOG_WARN_S(ctx, NKikimrServices::RPC_REQUEST, "Client lost, ActorId: " << SelfId());

        // We must try to finish stream otherwise grpc will not free allocated memory
        // If stream already scheduled to be finished (ReplyFinishStream already called)
        // this call do nothing but Die will be called after reply to grpc
        ReplyFinishStream("Client should not see this message, if so... may the force be with you");
    }

    void HandleClientTimeout(const TActorContext& ctx) {
        TInstant now = TAppData::TimeProvider->Now();
        TDuration timeout;

        if (InactiveClientTimeout_ && GRpcResponsesSizeQueue_.size() > 0) {
            TDuration processTime = now - LastDataStreamTimestamp_;
            if (processTime >= InactiveClientTimeout_) {
                auto message = TStringBuilder() << this->SelfId() << " Client cannot process data in " << processTime
                   << " which exceeds client timeout " << InactiveClientTimeout_;
                LOG_WARN_S(ctx, NKikimrServices::RPC_REQUEST, message);

                CancelationFlag->store(true);
                auto issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, message);
                return ReplyFinishStream(StatusIds::TIMEOUT, issue);
            }
            TDuration remain = InactiveClientTimeout_ - processTime;
            timeout = timeout ? Min(timeout, remain) : remain;
        }

        if (timeout) {
            SetClientTimeoutTimer(timeout, ctx);
        }
    }

    void HandleOperationTimeout(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::RPC_REQUEST, TStringBuilder() << this->SelfId() << " Operation timeout.");

        CancelationFlag->store(true);
        auto issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Operation timeout");
        return ReplyFinishStream(StatusIds::TIMEOUT, issue);
    }

    void ReplyFinishStream(const TString& message) {
        NYql::TIssues issues;
        issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, message));
        ReplyFinishStream(Ydb::StatusIds::INTERNAL_ERROR, issues);
    }

    void ReplyFinishStream(Ydb::StatusIds::StatusCode status, const NYql::TIssue& issue) {
        google::protobuf::RepeatedPtrField<TYdbIssueMessageType> issuesMessage;
        NYql::IssueToMessage(issue, issuesMessage.Add());

        ReplyFinishStream(status, issuesMessage);
    }

    void ReplyFinishStream(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) {
        google::protobuf::RepeatedPtrField<TYdbIssueMessageType> issuesMessage;
        for (auto& issue : issues) {
            auto item = issuesMessage.Add();
            NYql::IssueToMessage(issue, item);
        }

        ReplyFinishStream(status, issuesMessage);
    }

    void ReplyFinishStream(Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<TYdbIssueMessageType>& message)
    {
        ALOG_INFO(NKikimrServices::RPC_REQUEST, "Finish grpc stream, status: "
            << Ydb::StatusIds::StatusCode_Name(status));

        // Skip sending empty result in case of success status - simplify client logic
        if (status != Ydb::StatusIds::SUCCESS) {
            TString out = NullSerializeResponse(message, status);
            RequestPtr()->SendSerializedResult(std::move(out), status);
        }

        RequestPtr()->FinishStream(status);
        this->PassAway();
    }

    static TString NullSerializeResponse(const google::protobuf::RepeatedPtrField<TYdbIssueMessageType>& message,
        Ydb::StatusIds::StatusCode status)
    {
        TString out;
        Ydb::Scripting::ExecuteYqlPartialResponse response;
        response.set_status(status);
        response.mutable_issues()->CopyFrom(message);
        Y_PROTOBUF_SUPPRESS_NODISCARD response.SerializeToString(&out);
        return out;
    }

private:
    const ui64 RpcBufferSize_;

    TDuration InactiveClientTimeout_;
    TQueue<ui64> GRpcResponsesSizeQueue_;
    ui64 GRpcResponsesSize_ = 0;
    TInstant LastDataStreamTimestamp_;
    TMaybe<ui64> WaitOnSeqNo_;

    TSchedulerCookieHolder ClientTimeoutTimerCookieHolder_;

    TActorId GatewayRequestHandlerActorId_;
    ui64 ResultsReceived_ = 0;
    // DataQuery
    THolder<TDataQueryStreamContext> DataQueryStreamContext;
    std::shared_ptr<std::atomic_bool> CancelationFlag;
};

} // namespace

void DoStreamExecuteYqlScript(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    ui64 rpcBufferSize = f.GetChannelBufferSize();
    f.RegisterActor(new TStreamExecuteYqlScriptRPC(p.release(), rpcBufferSize));
}

} // namespace NGRpcService
} // namespace NKikimr
