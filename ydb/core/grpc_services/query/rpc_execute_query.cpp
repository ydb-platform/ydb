#include "service_query.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/audit_dml_operations.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/cancelation/cancelation_event.h>
#include <ydb/core/grpc_services/grpc_integrity_trails.h>
#include <ydb/core/grpc_services/rpc_kqp_base.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/public/api/protos/ydb_query.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NGRpcService {

namespace {

using namespace NActors;

using TEvExecuteQueryRequest = TGrpcRequestNoOperationCall<Ydb::Query::ExecuteQueryRequest,
    Ydb::Query::ExecuteQueryResponsePart>;

struct TProducerState {
    TMaybe<ui64> LastSeqNo;
    ui64 AckedFreeSpaceBytes = 0;
    TActorId ActorId;
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

bool ParseQueryAction(const Ydb::Query::ExecuteQueryRequest& req, NKikimrKqp::EQueryAction& queryAction,
    NYql::TIssues& issues)
{
    switch (req.exec_mode()) {
        case Ydb::Query::EXEC_MODE_VALIDATE:
            queryAction = NKikimrKqp::QUERY_ACTION_VALIDATE;
            return true;

        case Ydb::Query::EXEC_MODE_EXPLAIN:
            queryAction = NKikimrKqp::QUERY_ACTION_EXPLAIN;
            return true;

        case Ydb::Query::EXEC_MODE_EXECUTE:
            queryAction = NKikimrKqp::QUERY_ACTION_EXECUTE;
            return true;

        default:
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Unexpected query mode"));
            return false;
    }
}

bool ParseQueryContent(const Ydb::Query::ExecuteQueryRequest& req, TString& query, Ydb::Query::Syntax& syntax,
    NYql::TIssues& issues)
{
    switch (req.query_case()) {
        case Ydb::Query::ExecuteQueryRequest::kQueryContent:
            if (!CheckQuery(req.query_content().text(), issues)) {
                return false;
            }

            query = req.query_content().text();
            syntax = req.query_content().syntax();
            return true;

        default:
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Unexpected query option"));
            return false;
    }
}

bool NeedReportStats(const Ydb::Query::ExecuteQueryRequest& req) {
    switch (req.exec_mode()) {
        case Ydb::Query::EXEC_MODE_EXPLAIN:
            return true;

        case Ydb::Query::EXEC_MODE_EXECUTE:
            switch (req.stats_mode()) {
                case Ydb::Query::StatsMode::STATS_MODE_BASIC:
                case Ydb::Query::StatsMode::STATS_MODE_FULL:
                case Ydb::Query::StatsMode::STATS_MODE_PROFILE:
                    return true;
                default:
                    return false;
            }

        default:
            return false;
    }
}

bool NeedReportAst(const Ydb::Query::ExecuteQueryRequest& req) {
    switch (req.exec_mode()) {
        case Ydb::Query::EXEC_MODE_EXPLAIN:
            return true;

        case Ydb::Query::EXEC_MODE_EXECUTE:
            switch (req.stats_mode()) {
                case Ydb::Query::StatsMode::STATS_MODE_FULL:
                case Ydb::Query::StatsMode::STATS_MODE_PROFILE:
                    return true;
                default:
                    return false;
            }

        default:
            return false;
    }
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
                hFunc(NKikimr::NGRpcService::TEvSubscribeGrpcCancel, Handle);
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

        NYql::TIssues issues;
        if (!ParseQueryAction(*req, QueryAction, issues)) {
            return ReplyFinishStream(Ydb::StatusIds::BAD_REQUEST, std::move(issues));
        }

        TString query;
        Ydb::Query::Syntax syntax;
        if (!ParseQueryContent(*req, query, syntax, issues)) {
            return ReplyFinishStream(Ydb::StatusIds::BAD_REQUEST, std::move(issues));
        }

        Ydb::Table::TransactionControl* txControl = nullptr;
        if (req->has_tx_control()) {
            txControl = google::protobuf::Arena::CreateMessage<Ydb::Table::TransactionControl>(Request_->GetArena());
            if (!FillTxControl(req->tx_control(), *txControl, issues)) {
                return ReplyFinishStream(Ydb::StatusIds::BAD_REQUEST, std::move(issues));
            }
        }

        AuditContextAppend(Request_.get(), *req);
        NDataIntegrity::LogIntegrityTrails(traceId, *req, ctx);

        auto queryType = req->concurrent_result_sets()
            ? NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY
            : NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY;


        auto cachePolicy = google::protobuf::Arena::CreateMessage<Ydb::Table::QueryCachePolicy>(Request_->GetArena());
        cachePolicy->set_keep_in_cache(true);

        auto settings = NKqp::NPrivateEvents::TQueryRequestSettings()
            .SetKeepSession(false)
            .SetUseCancelAfter(false)
            .SetSyntax(syntax)
            .SetSupportStreamTrailingResult(true)
            .SetOutputChunkMaxSize(req->response_part_limit_bytes());

        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>(
            QueryAction,
            queryType,
            SelfId(),
            Request_,
            req->session_id(),
            std::move(query),
            "", // queryId
            txControl,
            &req->parameters(),
            GetCollectStatsMode(req->stats_mode()),
            cachePolicy,
            nullptr, // operationParams
            settings,
            req->pool_id());

        if (!ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release())) {
            NYql::TIssues issues;
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Internal error"));
            ReplyFinishStream(Ydb::StatusIds::INTERNAL_ERROR, std::move(issues));
        }
    }

    void Handle(NKikimr::NGRpcService::TEvSubscribeGrpcCancel::TPtr& ev) {
        auto as = TActivationContext::ActorSystem();
        PassSubscription(ev->Get(), Request_.get(), as);
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

        for (auto& pair : StreamChannels_) {
            const auto& channelId = pair.first;
            auto& channel = pair.second;

            if (freeSpaceBytes > 0 && channel.LastSeqNo && channel.AckedFreeSpaceBytes == 0) {
                LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << "Resume execution, "
                    << ", channel: " << channelId
                    << ", seqNo: " << channel.LastSeqNo
                    << ", freeSpace: " << freeSpaceBytes);

                auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
                resp->Record.SetSeqNo(*channel.LastSeqNo);
                resp->Record.SetFreeSpace(freeSpaceBytes);
                resp->Record.SetChannelId(channelId);

                ctx.Send(channel.ActorId, resp.Release());

                channel.AckedFreeSpaceBytes = freeSpaceBytes;
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

        auto& channel = StreamChannels_[ev->Get()->Record.GetChannelId()];
        channel.ActorId = ev->Sender;
        channel.LastSeqNo = ev->Get()->Record.GetSeqNo();
        channel.AckedFreeSpaceBytes = freeSpaceBytes;

        LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << "Send stream data ack"
            << ", seqNo: " << ev->Get()->Record.GetSeqNo()
            << ", freeSpace: " << freeSpaceBytes
            << ", to: " << ev->Sender
            << ", queue: " << FlowControl_.QueueSize());

        auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
        resp->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
        resp->Record.SetFreeSpace(freeSpaceBytes);
        resp->Record.SetChannelId(ev->Get()->Record.GetChannelId());

        ctx.Send(channel.ActorId, resp.Release());
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        NDataIntegrity::LogIntegrityTrails(Request_->GetTraceId(), *Request_->GetProtoRequest(), ev, ctx);

        auto& record = ev->Get()->Record.GetRef();

        const auto& issueMessage = record.GetResponse().GetQueryIssues();

        bool hasTrailingMessage = false;

        auto& kqpResponse = record.GetResponse();
        if (kqpResponse.GetYdbResults().size() > 1 && QueryAction != NKikimrKqp::QUERY_ACTION_EXPLAIN) {
            auto issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                "Unexpected trailing message with multiple result sets.");
            ReplyFinishStream(Ydb::StatusIds::INTERNAL_ERROR, issue);
            return;
        }

        Ydb::Query::ExecuteQueryResponsePart response;

        if (NeedReportStats(*Request_->GetProtoRequest())) {
            hasTrailingMessage = true;
            FillQueryStats(*response.mutable_exec_stats(), kqpResponse);
            if (NeedReportAst(*Request_->GetProtoRequest())) {
                response.mutable_exec_stats()->set_query_ast(kqpResponse.GetQueryAst());
            }
        }

        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            Request_->SetRuHeader(record.GetConsumedRu());

            if (QueryAction == NKikimrKqp::QUERY_ACTION_EXECUTE) {
                for(int i = 0; i < kqpResponse.GetYdbResults().size(); i++) {
                    hasTrailingMessage = true;
                    response.set_result_set_index(i);
                    response.mutable_result_set()->Swap(record.MutableResponse()->MutableYdbResults(i));
                }
            }

            AuditContextAppend(Request_.get(), *Request_->GetProtoRequest(), response);

            if (kqpResponse.HasTxMeta()) {
                hasTrailingMessage = true;
                response.mutable_tx_meta()->set_id(kqpResponse.GetTxMeta().id());
            }
        }

        if (hasTrailingMessage) {
            response.set_status(record.GetYdbStatus());
            response.mutable_issues()->CopyFrom(issueMessage);
            TString out;
            Y_PROTOBUF_SUPPRESS_NODISCARD response.SerializeToString(&out);
            ReplySerializedAndFinishStream(record.GetYdbStatus(), std::move(out));
        } else {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(issueMessage, issues);
            ReplyFinishStream(record.GetYdbStatus(), issueMessage);
        }
    }

private:
    void HandleClientLost(const TActorContext& ctx) {
        LOG_WARN_S(ctx, NKikimrServices::RPC_REQUEST, "Client lost");

        // We must try to finish stream otherwise grpc will not free allocated memory
        // If stream already scheduled to be finished (ReplyFinishStream already called)
        // this call do nothing but Die will be called after reply to grpc
        auto issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
            "Client should not see this message, if so... may the force be with you");
        ReplyFinishStream(Ydb::StatusIds::INTERNAL_ERROR, issue);
    }

    void ReplySerializedAndFinishStream(Ydb::StatusIds::StatusCode status, TString&& buf) {
        const auto finishStreamFlag = NYdbGrpc::IRequestContextBase::EStreamCtrl::FINISH;
        Request_->SendSerializedResult(std::move(buf), status, finishStreamFlag);
        this->PassAway();
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
        if (status != Ydb::StatusIds::SUCCESS || message.size() > 0) {
            TString out;
            Ydb::Query::ExecuteQueryResponsePart response;
            response.set_status(status);
            response.mutable_issues()->CopyFrom(message);
            Y_PROTOBUF_SUPPRESS_NODISCARD response.SerializeToString(&out);
            const auto finishStreamFlag = NYdbGrpc::IRequestContextBase::EStreamCtrl::FINISH;
            Request_->SendSerializedResult(std::move(out), status, finishStreamFlag);
        } else {
            Request_->FinishStream(status);
        }
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
    std::shared_ptr<TEvExecuteQueryRequest> Request_;

    NKikimrKqp::EQueryAction QueryAction;
    TRpcFlowControlState FlowControl_;
    TMap<ui64, TProducerState> StreamChannels_;
};

} // namespace

namespace NQuery {

void DoExecuteQuery(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    // Use default channel buffer size as inflight limit
    ui64 inflightLimitBytes = f.GetChannelBufferSize();

    auto* req = dynamic_cast<TEvExecuteQueryRequest*>(p.release());
    Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TExecuteQueryRPC(req, inflightLimitBytes));
}

}

} // namespace NKikimr::NGRpcService
