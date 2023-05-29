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

struct TProducerState {
    TMaybe<ui64> LastSeqNo;
    ui64 AckedFreeSpaceBytes = 0;
};

class TRpcFlowControlState {
public:
    TRpcFlowControlState(ui64 inflightLimitBytes)
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

bool FillQueryContent(const Ydb::Query::ExecuteQueryRequest& req, NKikimrKqp::TEvQueryRequest& kqpRequest,
    NYql::TIssues& issues)
{
    switch (req.query_case()) {
        case Ydb::Query::ExecuteQueryRequest::kQueryContent:
            if (!CheckQuery(req.query_content().text(), issues)) {
                return false;
            }

            kqpRequest.MutableRequest()->SetQuery(req.query_content().text());
            return true;

        default:
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Unexpected query option"));
            return false;
    }
}

bool FillTxSettings(const Ydb::Query::TransactionSettings& from, Ydb::Table::TransactionSettings& to,
    NYql::TIssues& issues)
{
    switch (from.tx_mode_case()) {
        case Ydb::Query::TransactionSettings::kSerializableReadWrite:
            to.mutable_serializable_read_write();
            break;
        case Ydb::Query::TransactionSettings::kOnlineReadOnly:
            to.mutable_online_read_only()->set_allow_inconsistent_reads(
                from.online_read_only().allow_inconsistent_reads());
            break;
        case Ydb::Query::TransactionSettings::kStaleReadOnly:
            to.mutable_stale_read_only();
            break;
        case Ydb::Query::TransactionSettings::kSnapshotReadOnly:
            to.mutable_snapshot_read_only();
            break;
        default:
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                "Invalid tx_settings"));
            return false;
    }

    return true;
}

bool FillTxControl(const Ydb::Query::TransactionControl& from, Ydb::Table::TransactionControl& to,
    NYql::TIssues& issues)
{
    switch (from.tx_selector_case()) {
        case Ydb::Query::TransactionControl::kTxId:
            to.set_tx_id(from.tx_id());
            break;
        case Ydb::Query::TransactionControl::kBeginTx:
            if (!FillTxSettings(from.begin_tx(), *to.mutable_begin_tx(), issues)) {
                return false;
            }
            break;
        default:
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                "Invalid tx_control settings"));
            return false;
    }

    to.set_commit_tx(from.commit_tx());
    return true;
}

bool NeedReportStats(const Ydb::Query::ExecuteQueryRequest& req) {
    switch (req.exec_mode()) {
        case Ydb::Query::EXEC_MODE_EXPLAIN:
            return true;

        default:
            return false;
    }
}

std::tuple<Ydb::StatusIds::StatusCode, NYql::TIssues> FillKqpRequest(
    const Ydb::Query::ExecuteQueryRequest& req, NKikimrKqp::TEvQueryRequest& kqpRequest)
{
    kqpRequest.MutableRequest()->MutableYdbParameters()->insert(req.parameters().begin(), req.parameters().end());
    switch (req.exec_mode()) {
        case Ydb::Query::EXEC_MODE_VALIDATE:
            kqpRequest.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_VALIDATE);
            break;
        case Ydb::Query::EXEC_MODE_EXPLAIN:
            kqpRequest.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXPLAIN);
            break;
        case Ydb::Query::EXEC_MODE_EXECUTE:
            kqpRequest.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            break;
        default: {
            NYql::TIssues issues;
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Unexpected query mode"));
            return {Ydb::StatusIds::BAD_REQUEST, std::move(issues)};
        }
    }

    kqpRequest.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
    kqpRequest.MutableRequest()->SetKeepSession(false);

    if (req.has_tx_control()) {
        NYql::TIssues issues;
        if (!FillTxControl(req.tx_control(), *kqpRequest.MutableRequest()->MutableTxControl(), issues)) {
            return {Ydb::StatusIds::BAD_REQUEST, std::move(issues)};
        }
    }

    NYql::TIssues issues;
    if (!FillQueryContent(req, kqpRequest, issues)) {
        return {Ydb::StatusIds::BAD_REQUEST, std::move(issues)};
    }

    return {Ydb::StatusIds::SUCCESS, {}};
}

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

        Request_->SetFinishAction([selfId, as]() {
            as->Send(selfId, new TEvents::TEvWakeup(EWakeupTag::ClientLostTag));
        });

        Request_->SetStreamingNotify([selfId, as](size_t left) {
            as->Send(selfId, new TRpcServices::TEvGrpcNextReply(left));
        });

        Proceed(ctx);
    }

private:
    void StateWork(TAutoPtr<IEventHandle>& ev) {
        try {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvents::TEvWakeup, Handle);
                HFunc(TRpcServices::TEvGrpcNextReply, Handle);
                HFunc(NKqp::TEvKqpExecuter::TEvStreamData, Handle);
                HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
                HFunc(NKqp::TEvKqp::TEvProcessResponse, Handle);
                default:
                    UnexpectedEvent(__func__, ev);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
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
            return ReplyFinishStream(fillStatus, std::move(fillIssues));
        }

        if (!ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release())) {
            NYql::TIssues issues;
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Internal error"));
            ReplyFinishStream(Ydb::StatusIds::INTERNAL_ERROR, std::move(issues));
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

        for (auto& pair : StreamProducers_) {
            const auto& producerId = pair.first;
            auto& producer = pair.second;

            if (freeSpaceBytes > 0 && producer.LastSeqNo && producer.AckedFreeSpaceBytes == 0) {
                LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << "Resume execution, "
                    << ", producer: " << producerId
                    << ", seqNo: " << producer.LastSeqNo
                    << ", freeSpace: " << freeSpaceBytes);

                auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
                resp->Record.SetSeqNo(*producer.LastSeqNo);
                resp->Record.SetFreeSpace(freeSpaceBytes);

                ctx.Send(producerId, resp.Release());

                producer.AckedFreeSpaceBytes = freeSpaceBytes;
            }
        }

    }

    void Handle(NKqp::TEvKqpExecuter::TEvStreamData::TPtr& ev, const TActorContext& ctx) {
        Ydb::Query::ExecuteQueryResponsePart response;
        response.set_status(Ydb::StatusIds::SUCCESS);
        response.set_result_set_index(ev->Get()->Record.GetQueryResultIndex());
        response.mutable_result_set()->Swap(ev->Get()->Record.MutableResultSet());

        TString out;
        Y_PROTOBUF_SUPPRESS_NODISCARD response.SerializeToString(&out);

        FlowControl_.PushResponse(out.size());
        auto freeSpaceBytes = FlowControl_.FreeSpaceBytes();

        Request_->SendSerializedResult(std::move(out), Ydb::StatusIds::SUCCESS);

        auto& producer = StreamProducers_[ev->Sender];
        producer.LastSeqNo = ev->Get()->Record.GetSeqNo();
        producer.AckedFreeSpaceBytes = freeSpaceBytes;

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

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext&) {
        auto& record = ev->Get()->Record.GetRef();

        NYql::TIssues issues;
        const auto& issueMessage = record.GetResponse().GetQueryIssues();
        NYql::IssuesFromMessage(issueMessage, issues);

        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS && NeedReportStats(*Request_->GetProtoRequest())) {
            Ydb::Query::ExecuteQueryResponsePart response;
            response.set_status(Ydb::StatusIds::SUCCESS);

            auto& kqpResponse = record.GetResponse();
            FillQueryStats(*response.mutable_exec_stats(), kqpResponse);

            TString out;
            Y_PROTOBUF_SUPPRESS_NODISCARD response.SerializeToString(&out);
            Request_->SendSerializedResult(std::move(out), record.GetYdbStatus());
        }

        ReplyFinishStream(record.GetYdbStatus(), issues);
    }

    void Handle(NKqp::TEvKqp::TEvProcessResponse::TPtr& ev, const TActorContext&) {
        auto& record = ev->Get()->Record;

        NYql::TIssues issues;
        if (record.HasError()) {
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, record.GetError()));
        }

        ReplyFinishStream(record.GetYdbStatus(), issues);
    }

private:
    void HandleClientLost(const TActorContext& ctx) {
        // TODO: Abort query execution.
        Y_UNUSED(ctx);
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

    void InternalError(const TString& message) {
        ALOG_ERROR(NKikimrServices::RPC_REQUEST, "Internal error, message: " << message);

        auto issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, message);
        ReplyFinishStream(Ydb::StatusIds::INTERNAL_ERROR, issue);
    }

    void UnexpectedEvent(const TString& state, TAutoPtr<NActors::IEventHandle>& ev) {
        InternalError(TStringBuilder() << "TExecuteQueryRPC in state " << state << " received unexpected event " <<
            ev->GetTypeName() << Sprintf("(0x%08" PRIx32 ")", ev->GetTypeRewrite()));
    }

private:
    enum EWakeupTag : ui64 {
        ClientLostTag = 1,
    };

private:
    std::unique_ptr<TEvExecuteQueryRequest> Request_;

    TRpcFlowControlState FlowControl_;
    TMap<TActorId, TProducerState> StreamProducers_;
};

} // namespace

void DoExecuteQueryRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    // Use default channel buffer size as inflight limit
    ui64 inflightLimitBytes = f.GetAppConfig()->GetTableServiceConfig().GetResourceManager().GetChannelBufferSize();

    auto* req = dynamic_cast<TEvExecuteQueryRequest*>(p.release());
    Y_VERIFY(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TExecuteQueryRPC(req, inflightLimitBytes));
}

} // namespace NKikimr::NGRpcService
