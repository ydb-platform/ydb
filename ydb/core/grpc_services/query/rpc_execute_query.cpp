#include "service_query.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/kikimr_issue.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_kqp_base.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/public/api/protos/draft/ydb_query.pb.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKikimr::NGRpcService {

namespace {

using namespace NActors;

using TEvExecuteQueryRequest = TGrpcRequestNoOperationCall<Ydb::Query::ExecuteQueryRequest,
    Ydb::Query::ExecuteQueryResponsePart>;

std::tuple<Ydb::StatusIds::StatusCode, NYql::TIssues> FillKqpRequest(
    const Ydb::Query::ExecuteQueryRequest& req, NKikimrKqp::TEvQueryRequest& kqpRequest)
{
    kqpRequest.MutableRequest()->MutableYdbParameters()->insert(req.parameters().begin(), req.parameters().end());
    switch (req.exec_mode()) {
        case Ydb::Query::EXEC_MODE_EXECUTE:
            kqpRequest.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            break;
        default: {
            NYql::TIssues issues;
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Unexpected query mode"));
            return {Ydb::StatusIds::BAD_REQUEST, issues};
        }
    }

    kqpRequest.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_QUERY);
    kqpRequest.MutableRequest()->SetKeepSession(false);

    // TODO: Use tx control from request.
    kqpRequest.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
    kqpRequest.MutableRequest()->MutableTxControl()->set_commit_tx(true);

    switch (req.query_case()) {
        case Ydb::Query::ExecuteQueryRequest::kQueryContent: {
            NYql::TIssues issues;
            if (!CheckQuery(req.query_content().text(), issues)) {
                return {Ydb::StatusIds::BAD_REQUEST, issues};
            }

            kqpRequest.MutableRequest()->SetQuery(req.query_content().text());
            break;
        }

        default: {
            NYql::TIssues issues;
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Unexpected query option"));
            return {Ydb::StatusIds::BAD_REQUEST, issues};
        }
    }

    return {Ydb::StatusIds::SUCCESS, {}};
}

class RpcFlowControlState {
public:
    RpcFlowControlState(ui64 inflightLimitBytes)
        : InflightLimitBytes_(inflightLimitBytes) {}

    void PushResponse(ui64 responseSizeBytes) {
        ResponseSizeQueue_.push(responseSizeBytes);
        TotalResponsesSize_ += responseSizeBytes;
    }

    void PopResponse() {
        Y_ENSURE(!ResponseSizeQueue_.empty());
        TotalResponsesSize_ -= ResponseSizeQueue_.front();
        ResponseSizeQueue_.pop();
    }

    size_t QueueSize() const {
        return ResponseSizeQueue_.size();
    }

    ui64 FreeSpaceBytes() const {
        return TotalResponsesSize_ < InflightLimitBytes_
            ? InflightLimitBytes_ - TotalResponsesSize_
            : 0;
    }

    ui64 InflightBytes() const {
        return TotalResponsesSize_;
    }

    ui64 InflightLimitBytes() const {
        return InflightLimitBytes_;
    }

private:
    const ui64 InflightLimitBytes_;

    TQueue<ui64> ResponseSizeQueue_;
    ui64 TotalResponsesSize_ = 0;
};

class TExecuteQueryRPC : public TActorBootstrapped<TExecuteQueryRPC> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_STREAM_REQ;
    }

    TExecuteQueryRPC(TEvExecuteQueryRequest* request, ui64 inflightLimitBytes)
        : Request_(request)
        , FlowControl_(inflightLimitBytes) {}

    void Bootstrap(const TActorContext &ctx) {
        this->Become(&TExecuteQueryRPC::StateWork);

        auto selfId = this->SelfId();
        auto as = TActivationContext::ActorSystem();

        Request_->SetClientLostAction([selfId, as]() {
            as->Send(selfId, new TEvents::TEvWakeup(EWakeupTag::ClientLostTag));
        });

        Request_->SetStreamingNotify([selfId, as](size_t left) {
            as->Send(selfId, new TRpcServices::TEvGrpcNextReply(left));
        });

        Proceed(ctx);
    }

private:
    void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
        try {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvents::TEvWakeup, Handle);
                HFunc(TRpcServices::TEvGrpcNextReply, Handle);
                HFunc(NKqp::TEvKqpExecuter::TEvExecuterProgress, Handle);
                HFunc(NKqp::TEvKqpExecuter::TEvStreamData, Handle);
                HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
                default:
                    UnexpectedEvent(__func__, ev, ctx);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what(), ctx);
        }
    }

    void Proceed(const TActorContext &ctx) {
        const auto req = Request_->GetProtoRequest();
        const auto traceId = Request_->GetTraceId();

        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        SetAuthToken(ev, *Request_);
        SetDatabase(ev, *Request_);
        SetRlPath(ev, *Request_);

        if (traceId) {
            ev->Record.SetTraceId(traceId.GetRef());
        }

        ActorIdToProto(this->SelfId(), ev->Record.MutableRequestActorId());

        auto [fillStatus, fillIssues] = FillKqpRequest(*req, ev->Record);
        if (fillStatus != Ydb::StatusIds::SUCCESS) {
            return ReplyFinishStream(fillStatus, fillIssues, ctx);
        }

        if (!ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release())) {
            NYql::TIssues issues;
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Internal error"));
            ReplyFinishStream(Ydb::StatusIds::INTERNAL_ERROR, issues, ctx);
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        switch ((EWakeupTag) ev->Get()->Tag) {
            case EWakeupTag::ClientLostTag:
                return HandleClientLost(ctx);
        }
    }

    void Handle(TRpcServices::TEvGrpcNextReply::TPtr& ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << " NextReply"
            << ", left: " << ev->Get()->LeftInQueue
            << ", queue: " << FlowControl_.QueueSize()
            << ", inflight bytes: " << FlowControl_.InflightBytes()
            << ", limit bytes: " << FlowControl_.InflightLimitBytes());

        while (FlowControl_.QueueSize() > ev->Get()->LeftInQueue) {
            FlowControl_.PopResponse();
        }

        ui64 freeSpaceBytes = FlowControl_.FreeSpaceBytes();
        if (ResumeWithSeqNo_ && freeSpaceBytes > 0) {
            LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << "Resume execution, "
                << ", seqNo: " << *ResumeWithSeqNo_
                << ", freeSpace: " << freeSpaceBytes
                << ", executer: " << ExecuterActorId_);

            auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
            resp->Record.SetSeqNo(*ResumeWithSeqNo_);
            resp->Record.SetFreeSpace(freeSpaceBytes);

            ctx.Send(ExecuterActorId_, resp.Release());

            ResumeWithSeqNo_.Clear();
        }
    }

    void Handle(NKqp::TEvKqpExecuter::TEvExecuterProgress::TPtr& ev, const TActorContext& ctx) {
        ExecuterActorId_ = ActorIdFromProto(ev->Get()->Record.GetExecuterActorId());
        LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << "ExecuterActorId: " << ExecuterActorId_);
    }

    void Handle(NKqp::TEvKqpExecuter::TEvStreamData::TPtr& ev, const TActorContext& ctx) {
        Ydb::Query::ExecuteQueryResponsePart response;
        response.set_status(Ydb::StatusIds::SUCCESS);
        response.set_result_set_index(0);
        response.mutable_result_set()->Swap(ev->Get()->Record.MutableResultSet());

        TString out;
        Y_PROTOBUF_SUPPRESS_NODISCARD response.SerializeToString(&out);

        Request_->SendSerializedResult(std::move(out), Ydb::StatusIds::SUCCESS);

        auto freeSpaceBytes = FlowControl_.FreeSpaceBytes();
        if (freeSpaceBytes == 0) {
            ResumeWithSeqNo_ = ev->Get()->Record.GetSeqNo();
        }

        LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << "Send stream data ack"
            << ", seqNo: " << ev->Get()->Record.GetSeqNo()
            << ", freeSpace: " << freeSpaceBytes
            << ", to: " << ev->Sender
            << ", queue: " << FlowControl_.QueueSize());

        auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
        resp->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
        resp->Record.SetFreeSpace(freeSpaceBytes);

        ctx.Send(ev->Sender, resp.Release());
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record.GetRef();

        NYql::TIssues issues;
        const auto& issueMessage = record.GetResponse().GetQueryIssues();
        NYql::IssuesFromMessage(issueMessage, issues);

        ReplyFinishStream(record.GetYdbStatus(), issues, ctx);
    }

private:
    void HandleClientLost(const TActorContext& ctx) {
        // TODO: Abort query execution.
        Y_UNUSED(ctx);
    }

    void ReplyFinishStream(Ydb::StatusIds::StatusCode status, const NYql::TIssue& issue, const TActorContext& ctx) {
        google::protobuf::RepeatedPtrField<TYdbIssueMessageType> issuesMessage;
        NYql::IssueToMessage(issue, issuesMessage.Add());

        ReplyFinishStream(status, issuesMessage, ctx);
    }

    void ReplyFinishStream(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues, const TActorContext& ctx) {
        google::protobuf::RepeatedPtrField<TYdbIssueMessageType> issuesMessage;
        for (auto& issue : issues) {
            auto item = issuesMessage.Add();
            NYql::IssueToMessage(issue, item);
        }

        ReplyFinishStream(status, issuesMessage, ctx);
    }

    void ReplyFinishStream(Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<TYdbIssueMessageType>& message, const TActorContext& ctx)
    {
        LOG_INFO_S(ctx, NKikimrServices::RPC_REQUEST, "Finish grpc stream, status: "
            << Ydb::StatusIds::StatusCode_Name(status));

        // Skip sending empty result in case of success status - simplify client logic
        if (status != Ydb::StatusIds::SUCCESS) {
            TString out;
            Ydb::Query::ExecuteQueryResponsePart response;
            response.set_status(status);
            response.mutable_issues()->CopyFrom(message);
            Y_PROTOBUF_SUPPRESS_NODISCARD response.SerializeToString(&out);
            Request_->SendSerializedResult(std::move(out), status);
        }

        Request_->FinishStream();
        this->PassAway();
    }

    void InternalError(const TString& message, const TActorContext& ctx) {
        LOG_ERROR_S(ctx, NKikimrServices::RPC_REQUEST, "Internal error, message: " << message);

        auto issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, message);
        ReplyFinishStream(Ydb::StatusIds::INTERNAL_ERROR, issue, ctx);
    }

    void UnexpectedEvent(const TString& state, TAutoPtr<NActors::IEventHandle>& ev, const TActorContext& ctx) {
        InternalError(TStringBuilder() << "TExecuteQueryRPC in state " << state << " received unexpected event " <<
            TypeName(*ev.Get()->GetBase()) << Sprintf("(0x%08" PRIx32 ")", ev->GetTypeRewrite()), ctx);
    }

private:
    enum EWakeupTag : ui64 {
        ClientLostTag = 1,
    };

private:
    std::unique_ptr<TEvExecuteQueryRequest> Request_;

    RpcFlowControlState FlowControl_;
    TMaybe<ui64> ResumeWithSeqNo_;

    TActorId ExecuterActorId_;
};

} // namespace

void DoExecuteQueryRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    // Use default channel buffer size as inflight limit
    ui64 inflightLimitBytes = f.GetAppConfig().GetTableServiceConfig().GetResourceManager().GetChannelBufferSize();

    auto* req = dynamic_cast<TEvExecuteQueryRequest*>(p.release());
    Y_VERIFY(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    TActivationContext::AsActorContext().Register(new TExecuteQueryRPC(req, inflightLimitBytes));
}

} // namespace NKikimr::NGRpcService
