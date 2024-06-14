#include "service_table.h"
#include <ydb/core/grpc_services/base/base.h>

#include "rpc_common/rpc_common.h"
#include "rpc_calls.h"
#include "rpc_kqp_base.h"
#include "local_rate_limiter.h"
#include "service_table.h"

#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/tx_proxy/read_table.h>
#include <ydb/core/tx/tx_processing.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/actorlib_impl/async_destroyer.h>

#include <ydb/core/protos/ydb_table_impl.pb.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <util/generic/size_literals.h>

#include <ydb/core/protos/stream.pb.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;
using namespace NKqp;

using TEvReadTableRequest = TGrpcRequestNoOperationCall<Ydb::Table::ReadTableRequest,
    Ydb::Table::ReadTableResponse>;

static const TDuration RlMaxDuration = TDuration::Minutes(1);

static ui64 CalcRuConsumption(const TString& data) {
    constexpr ui64 unitSize = 1_MB;
    constexpr ui64 unitSizeAdjust = unitSize - 1;

    auto mb = (data.size() + unitSizeAdjust) / unitSize;
    return mb * 128; // 128 ru for 1 MiB
}

static void NullSerializeReadTableResponse(const TString& input, Ydb::StatusIds::StatusCode status, ui64 planStep, ui64 txId, TString* output) {
    Ydb::Impl::ReadTableResponse readTableResponse;
    readTableResponse.set_status(status);

    if (planStep && txId) {
        auto* snapshot = readTableResponse.mutable_snapshot();
        snapshot->set_plan_step(planStep);
        snapshot->set_tx_id(txId);
    }

    readTableResponse.mutable_result()->set_result_set(input.Data(), input.Size());
    Y_PROTOBUF_SUPPRESS_NODISCARD readTableResponse.SerializeToString(output);
}

static void NullSerializeReadTableResponse(const google::protobuf::RepeatedPtrField<TYdbIssueMessageType>& message, Ydb::StatusIds::StatusCode status, TString* output) {
    Ydb::Impl::ReadTableResponse readTableResponse;
    readTableResponse.set_status(status);
    if (!message.empty()) {
        readTableResponse.mutable_issues()->CopyFrom(message);
    }
    Y_PROTOBUF_SUPPRESS_NODISCARD readTableResponse.SerializeToString(output);
}

static NKikimrMiniKQL::TParams ConvertKey(const Ydb::TypedValue& key) {
    NKikimrMiniKQL::TParams protobuf;
    ConvertYdbTypeToMiniKQLType(key.type(), *protobuf.MutableType());
    ConvertYdbValueToMiniKQLValue(key.type(), key.value(), *protobuf.MutableValue());
    return protobuf;
}

template<class TGetOutput>
static void ConvertKeyRange(const Ydb::Table::KeyRange& keyRange, const TGetOutput& getOutput) {
    switch (keyRange.from_bound_case()) {
        case Ydb::Table::KeyRange::kGreaterOrEqual: {
            auto* output = getOutput();
            output->SetFromInclusive(true);
            output->MutableFrom()->CopyFrom(ConvertKey(keyRange.greater_or_equal()));
            break;
        }
        case Ydb::Table::KeyRange::kGreater: {
            auto* output = getOutput();
            output->SetFromInclusive(false);
            output->MutableFrom()->CopyFrom(ConvertKey(keyRange.greater()));
            break;
        }
        default:
            break;
    }
    switch (keyRange.to_bound_case()) {
        case Ydb::Table::KeyRange::kLessOrEqual: {
            auto* output = getOutput();
            output->SetToInclusive(true);
            output->MutableTo()->CopyFrom(ConvertKey(keyRange.less_or_equal()));
            break;
        }
        case Ydb::Table::KeyRange::kLess: {
            auto* output = getOutput();
            output->SetToInclusive(false);
            output->MutableTo()->CopyFrom(ConvertKey(keyRange.less()));
            break;
        }
        default:
            break;
    }
}

class TReadTableRPC : public TActorBootstrapped<TReadTableRPC> {
    enum EWakeupTag : ui64 {
        RlSendAllowed = 1,
        RlNoResource = 2,
        ClientInactivity = 3,
        ServerInactivity = 4,
    };

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_STREAM_REQ;
    }

    TReadTableRPC(IRequestNoOpCtx* msg)
        : Request_(msg)
        , QuotaLimit_(10)
        , QuotaReserved_(0)
        , LeftInGRpcAdaptorQueue_(0)
    {}

    void Bootstrap(const TActorContext& ctx) {
        Become(&TReadTableRPC::StateWork);

        const auto& cfg = AppData(ctx)->StreamingConfig.GetOutputStreamConfig();
        QuotaLimit_ = Max((size_t)1, (size_t)(cfg.GetMessageBufferSize() / cfg.GetMessageSizeLimit()));

        LastStatusTimestamp_ = ctx.Now();
        LastDataStreamTimestamp_ = ctx.Now();

        InactiveClientTimeout_ = TDuration::MicroSeconds(cfg.GetInactiveClientTimeout());
        InactiveServerTimeout_ = TDuration::MicroSeconds(cfg.GetInactiveServerTimeout());

        if (InactiveServerTimeout_) {
            StartInactivityTimer(InactiveServerTimer_, InactiveServerTimeout_, EWakeupTag::ServerInactivity, ctx);
            InactiveServerTimerPending_ = true;
        }

        MessageSizeLimit = cfg.GetMessageSizeLimit();

        SendProposeRequest(ctx);

        auto actorId = SelfId();
        const TActorSystem* const as = ctx.ExecutorThread.ActorSystem;
        auto clientLostCb = [actorId, as]() {
            LOG_WARN(*as, NKikimrServices::READ_TABLE_API, "ForgetAction occurred, send TEvPoisonPill");
            as->Send(actorId, new TEvents::TEvPoisonPill());
        };
        Request_->SetFinishAction(std::move(clientLostCb));
    }

    void PassAway() override {
        if (ReadTableActor) {
            Send(ReadTableActor, new TEvents::TEvPoison);
            ReadTableActor = { };
        }
        if (InactiveClientTimer_) {
            Send(InactiveClientTimer_, new TEvents::TEvPoison);
            InactiveClientTimer_ = { };
        }
        if (InactiveServerTimer_) {
            Send(InactiveServerTimer_, new TEvents::TEvPoison);
            InactiveServerTimer_ = { };
        }
        TActorBootstrapped::PassAway();
    }

private:
    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, HandleResponseData);
            HFunc(TRpcServices::TEvGrpcNextReply, Handle);
            HFunc(TEvTxProcessing::TEvStreamQuotaRequest, Handle);
            HFunc(TEvTxProcessing::TEvStreamQuotaRelease, Handle);
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvents::TEvSubscribe, Handle);
            HFunc(TEvents::TEvWakeup, Handle);
            HFunc(TEvDataShard::TEvGetReadTableStreamStateRequest, Handle);
            default:
                Y_ABORT("TRequestHandler: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

    void HandleResponseData(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
        const TEvTxUserProxy::TEvProposeTransactionStatus* msg = ev->Get();
        LastStatusTimestamp_ = ctx.Now();

        if (msg->Record.GetStep() && !PlanStep) {
            PlanStep = msg->Record.GetStep();
            TxId = msg->Record.GetTxId();
        }

        const auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(msg->Record.GetStatus());
        auto issueMessage = msg->Record.GetIssues();
        switch (status) {
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecResponseData: {
                // This check need to protect wrong response in case of new grpc -> old tx proxy request.
                if (msg->Record.HasReadTableResponseVersion() &&
                        msg->Record.GetReadTableResponseVersion() == NKikimrTxUserProxy::TReadTableTransaction::YDB_V1) {
                    auto result = msg->Record.GetSerializedReadTableResponse();
                    return TryToSendStreamResult(result, msg->Record.GetDataShardTabletId(),
                        msg->Record.GetTxId(), ctx);
                } else {
                    TStringStream str;
                    str << "Response version missmatched";
                    if (msg->Record.HasReadTableResponseVersion()) {
                        str << " , got: " << msg->Record.GetReadTableResponseVersion();
                    }
                    LOG_ERROR(ctx, NKikimrServices::READ_TABLE_API,
                              "%s", str.Str().data());
                    const NYql::TIssue& issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, str.Str());
                    auto tmp = issueMessage.Add();
                    NYql::IssueToMessage(issue, tmp);
                    return ReplyFinishStream(StatusIds::INTERNAL_ERROR, issueMessage, ctx);
                }
            }
            break;
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete: {
                if (msg->Record.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusSuccess ||
                    msg->Record.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusAlreadyExists)
                {
                    // We can't close stream if has not empty send buffer
                    if (SendBuffer_) {
                        HasPendingSuccess = true;
                    } else {
                        return ReplyFinishStream(Ydb::StatusIds::SUCCESS, issueMessage, ctx);
                    }
                }
                break;
            }
            case TEvTxUserProxy::TResultStatus::AccessDenied: {
                const NYql::TIssue& issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Got AccessDenied response from TxProxy");
                auto tmp = issueMessage.Add();
                NYql::IssueToMessage(issue, tmp);
                return ReplyFinishStream(Ydb::StatusIds::UNAUTHORIZED, issueMessage, ctx);
            }
            case TEvTxUserProxy::TResultStatus::ResolveError: {
                NYql::TIssue issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Got ResolveError response from TxProxy");
                auto tmp = issueMessage.Add();
                for (const auto& unresolved : msg->Record.GetUnresolvedKeys()) {
                    issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(unresolved));
                }
                NYql::IssueToMessage(issue, tmp);
                return ReplyFinishStream(Ydb::StatusIds::SCHEME_ERROR, issueMessage, ctx);
            }
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardOverloaded: {
                const auto req = TEvReadTableRequest::GetProtoRequest(Request_.get());

                auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::KIKIMR_OVERLOADED, TStringBuilder()
                    << "Table " << req->path() << " is overloaded");
                auto tmp = issueMessage.Add();
                NYql::IssueToMessage(issue, tmp);
                return ReplyFinishStream(Ydb::StatusIds::OVERLOADED, issueMessage, ctx);
            }
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyNotReady:
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardTryLater:
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown:
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorDeclined:
            case TEvTxUserProxy::TResultStatus::ProxyShardNotAvailable: {
                TStringStream str;
                str << "Got " << status << " response from TxProxy";
                const NYql::TIssue& issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, str.Str());
                auto tmp = issueMessage.Add();
                NYql::IssueToMessage(issue, tmp);
                return ReplyFinishStream(Ydb::StatusIds::UNAVAILABLE, issueMessage, ctx);
            }
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorUnknown: {
                TString str = TStringBuilder()
                    << "Got " << status << " response from TxProxy, transaction state unknown";
                const NYql::TIssue& issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, str);
                auto tmp = issueMessage.Add();
                NYql::IssueToMessage(issue, tmp);
                return ReplyFinishStream(Ydb::StatusIds::UNDETERMINED, issueMessage, ctx);
            }
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecTimeout: {
                TString str = "Transaction timed out";
                const NYql::TIssue& issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, str);
                auto tmp = issueMessage.Add();
                NYql::IssueToMessage(issue, tmp);
                return ReplyFinishStream(Ydb::StatusIds::TIMEOUT, issueMessage, ctx);
            }
            default: {
                TStringStream str;
                str << "Got unknown TEvProposeTransactionStatus (" << status << ") response from TxProxy";
                const NYql::TIssue& issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, str.Str());
                auto tmp = issueMessage.Add();
                NYql::IssueToMessage(issue, tmp);
                return ReplyFinishStream(StatusIds::INTERNAL_ERROR, issueMessage, ctx);
            }
        }
    }

    void Handle(TEvTxProcessing::TEvStreamQuotaRequest::TPtr& ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::READ_TABLE_API,
                    SelfId() << " Adding quota request to queue ShardId: "
                    << ev->Get()->Record.GetShardId() << ", TxId: "
                    << ev->Get()->Record.GetTxId());

        QuotaRequestQueue_.push_back(ev);
        TryToAllocateQuota();
    }

    void Handle(TEvTxProcessing::TEvStreamQuotaRelease::TPtr& ev, const TActorContext& ctx) {
        ui64 shardId = ev->Get()->Record.GetShardId();
        ui64 txId = ev->Get()->Record.GetTxId();

        LOG_DEBUG_S(ctx, NKikimrServices::READ_TABLE_API,
                    SelfId() << " Release quota for ShardId: " << shardId
                    << ", TxId: " << txId);

        ReleasedShards_.insert(shardId);

        auto it = QuotaByShard_.find(shardId);
        if (it != QuotaByShard_.end()) {
            QuotaReserved_ -= it->second;
            QuotaByShard_.erase(it);
            TryToAllocateQuota();
        }
    }

    void Handle(TRpcServices::TEvGrpcNextReply::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        if (LeftInGRpcAdaptorQueue_ > 0)
            LeftInGRpcAdaptorQueue_--;
        LastDataStreamTimestamp_ = ctx.Now();
        TryToAllocateQuota();
    }

    void HandlePoison(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        google::protobuf::RepeatedPtrField<TYdbIssueMessageType> message;
        auto item = message.Add();
        const NYql::TIssue& issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
            "Client should not see this message, if so... may the force be with you");
        NYql::IssueToMessage(issue, item);
        // We must try to finish stream otherwise grpc will not free allocated memory
        // If stream already scheduled to be finished (ReplyFinishStream already called)
        // this call do nothing but Die will be called after reply to grpc
        ReplyFinishStream(StatusIds::INTERNAL_ERROR, message, ctx);
    }

    void Handle(TEvents::TEvSubscribe::TPtr& ev, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::READ_TABLE_API,
                    SelfId() << " Add stream subscriber " << ev->Sender);

        StreamSubscribers_.insert(ev->Sender);
    }

    void Handle(TEvents::TEvWakeup::TPtr &ev, const TActorContext &ctx) {
        switch ((EWakeupTag) ev->Get()->Tag) {
            case EWakeupTag::RlSendAllowed:
                return ProcessRlSendAllowed(ctx);
            case EWakeupTag::RlNoResource:
                return ProcessRlNoResource(ctx);
            case EWakeupTag::ClientInactivity:
                InactiveClientTimer_ = { };
                InactiveClientTimerPending_ = false;
                return ProcessClientInactivity(ctx);
            case EWakeupTag::ServerInactivity:
                InactiveServerTimer_ = { };
                InactiveServerTimerPending_ = false;
                return ProcessServerInactivity(ctx);
        }
    }

    void ProcessRlSendAllowed(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::READ_TABLE_API,
                    SelfId() << " Success rate limiter response, we got "
                    << SendBuffer_.size() << " pending messages,"
                    " pending success# " << HasPendingSuccess);
        TBuffEntry& entry = SendBuffer_.front();

        DoSendStreamResult(std::move(entry.Buf), entry.ShardId, entry.TxId, entry.StartTime, ctx);

        SendBuffer_.pop_front();

        if (SendBuffer_.empty() && HasPendingSuccess) {
            const static google::protobuf::RepeatedPtrField<TYdbIssueMessageType> empty;
            return ReplyFinishStream(Ydb::StatusIds::SUCCESS, empty, ctx);
        }
    }

    void ProcessRlNoResource(const TActorContext& ctx) {
        const NYql::TIssue& issue = MakeIssue(NKikimrIssues::TIssuesIds::YDB_RESOURCE_USAGE_LIMITED,
            "Throughput limit exceeded for read table request");
        LOG_NOTICE_S(ctx, NKikimrServices::READ_TABLE_API,
                    SelfId() << " Throughput limit exceeded, we got "
                    << SendBuffer_.size() << " pending messages,"
                    " pending success# " << HasPendingSuccess
                    << " stream will be terminated");

        google::protobuf::RepeatedPtrField<TYdbIssueMessageType> message;
        NYql::IssueToMessage(issue, message.Add());

        return ReplyFinishStream(Ydb::StatusIds::OVERLOADED, message, ctx);
    }

    void ProcessClientInactivity(const TActorContext &ctx) {
        // We don't consider client inactive when there's nothing for the client to read
        if (!LeftInGRpcAdaptorQueue_) {
            return;
        }

        TInstant now = ctx.Now();
        TDuration processTime = now - LastDataStreamTimestamp_;

        if (processTime >= InactiveClientTimeout_) {
            TStringStream ss;
            ss << SelfId() << " Client cannot process data in " << processTime
                << " which exceeds client timeout " << InactiveClientTimeout_;
            LOG_NOTICE_S(ctx, NKikimrServices::READ_TABLE_API, ss.Str());
            return HandleStreamTimeout(ctx, ss.Str());
        }

        TDuration timeout = InactiveClientTimeout_ - processTime;
        StartInactivityTimer(InactiveClientTimer_, timeout, EWakeupTag::ClientInactivity, ctx);
        InactiveClientTimerPending_ = true;
    }

    void ProcessServerInactivity(const TActorContext &ctx) {
        TInstant now = ctx.Now();
        TDuration timeout = InactiveServerTimeout_;
        TDuration processTime = now - LastStatusTimestamp_;
        // Ignore server timeout if response buffer is full.
        if (LeftInGRpcAdaptorQueue_ == QuotaLimit_) {
            // nothing
        } else if (processTime >= InactiveServerTimeout_) {
            TStringStream ss;
            ss << SelfId()
                << " Server doesn't provide data for " << processTime
                << " which exceeds server timeout " << InactiveServerTimeout_
                << " (QuotaRequestQueue: " << QuotaRequestQueue_.size()
                << " ResponseQueue: " << LeftInGRpcAdaptorQueue_
                << " QuotaLimit: " << QuotaLimit_
                << " QuotaReserved: " << QuotaReserved_
                << " QuotaByShard: " << QuotaByShard_.size() << ")";
            LOG_NOTICE_S(ctx, NKikimrServices::READ_TABLE_API, ss.Str());
            return HandleStreamTimeout(ctx, ss.Str());
        } else {
            timeout = InactiveServerTimeout_ - processTime;
        }

        StartInactivityTimer(InactiveServerTimer_, timeout, EWakeupTag::ServerInactivity, ctx);
        InactiveServerTimerPending_ = true;
    }

    void Handle(TEvDataShard::TEvGetReadTableStreamStateRequest::TPtr &ev, const TActorContext &ctx)
    {
        auto *response = new TEvDataShard::TEvGetReadTableStreamStateResponse;
        response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

        //response->Record.SetReady(IsStreamReady);
        //response->Record.SetFinished(IsStreamFinished);
        response->Record.SetResponseQueueSize(LeftInGRpcAdaptorQueue_);
        for (auto &ev : QuotaRequestQueue_)
            response->Record.AddQuotaRequests()->SetId(ev->Get()->Record.GetShardId());
        for (auto &pr : QuotaByShard_) {
            auto &quota = *response->Record.AddShardQuotas();
            quota.SetShardId(pr.first);
            quota.SetQuota(pr.second);
        }
        response->Record.SetQuotaLimit(QuotaLimit_);
        response->Record.SetQuotaReserved(QuotaReserved_);
        for (auto shard : ReleasedShards_)
            response->Record.AddReleasedShards()->SetId(shard);
        response->Record.SetInactiveClientTimeout(InactiveClientTimeout_.GetValue());
        response->Record.SetInactiveServerTimeout(InactiveServerTimeout_.GetValue());
        response->Record.SetLastDataStreamTimestamp(LastDataStreamTimestamp_.GetValue());
        response->Record.SetLastStatusTimestamp(LastStatusTimestamp_.GetValue());

        ctx.Send(ev->Sender, response);
    }

    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = TEvReadTableRequest::GetProtoRequest(Request_.get());
        auto actorId = SelfId();
        const TActorSystem* const as = ctx.ExecutorThread.ActorSystem;
        auto cb = [actorId, as](size_t left) {
            as->Send(actorId, new TRpcServices::TEvGrpcNextReply{left});
        };

        Request_->SetStreamingNotify(cb);

        if (req->path().empty()) {
            const NYql::TIssue& issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Got empty table path");
            google::protobuf::RepeatedPtrField<TYdbIssueMessageType> message;
            auto item = message.Add();
            NYql::IssueToMessage(issue, item);
            return ReplyFinishStream(StatusIds::BAD_REQUEST, message, ctx);
        }

        MessageSizeLimit = std::min(MessageSizeLimit, req->batch_limit_bytes() ? req->batch_limit_bytes() : Max<ui64>());
        MessageRowsLimit = req->batch_limit_rows();

        // Snapshots are always enabled and cannot be disabled
        switch (req->use_snapshot()) {
            case Ydb::FeatureFlag::STATUS_UNSPECIFIED:
            case Ydb::FeatureFlag::ENABLED:
                break;

            default: {
                const NYql::TIssue& issue = MakeIssue(
                    NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                    TStringBuilder() << "Unsupported use_snapshot feature flag " << int(req->use_snapshot()));
                google::protobuf::RepeatedPtrField<TYdbIssueMessageType> message;
                auto item = message.Add();
                NYql::IssueToMessage(issue, item);
                return ReplyFinishStream(StatusIds::BAD_REQUEST, message, ctx);
            }
        }

        NKikimr::NTxProxy::TReadTableSettings settings;

        if (Request_->GetSerializedToken()) {
            settings.UserToken = Request_->GetSerializedToken();
        }
        settings.DatabaseName = CanonizePath(Request_->GetDatabaseName().GetOrElse(""));

        settings.Owner = SelfId();
        settings.TablePath = req->path();
        settings.Ordered = req->ordered();
        settings.RequireResultSet = true;

        // Right now assume return_not_null_data_as_optional is true by default
        // Sometimes we well change this default
        switch (req->return_not_null_data_as_optional()) {
            case Ydb::FeatureFlag::DISABLED:
                settings.DataFormat = NTxProxy::EReadTableFormat::YdbResultSetWithNotNullSupport;
                break;
            case Ydb::FeatureFlag::STATUS_UNSPECIFIED:
            case Ydb::FeatureFlag::ENABLED:
            default:
                settings.DataFormat = NTxProxy::EReadTableFormat::YdbResultSet;
                break;
        }

        if (req->row_limit()) {
            settings.MaxRows = req->row_limit();
        }

        for (auto &col : req->columns()) {
            settings.Columns.push_back(col);
        }

        try {
            ConvertKeyRange(req->key_range(), [&]{ return &settings.KeyRange; });
        } catch (const std::exception& ex) {
            const NYql::TIssue& issue = NYql::ExceptionToIssue(ex);
            google::protobuf::RepeatedPtrField<TYdbIssueMessageType> message;
            auto item = message.Add();
            NYql::IssueToMessage(issue, item);
            return ReplyFinishStream(StatusIds::BAD_REQUEST, message, ctx);
        }

        ReadTableActor = ctx.RegisterWithSameMailbox(NKikimr::NTxProxy::CreateReadTableSnapshotWorker(settings));
    }

    void ReplyFinishStream(Ydb::StatusIds::StatusCode status,
                         const google::protobuf::RepeatedPtrField<TYdbIssueMessageType>& message,
                         const TActorContext& ctx) {
        // Skip sending empty result in case of success status - simplify client logic
        if (status != Ydb::StatusIds::SUCCESS) {
            TString out;
            NullSerializeReadTableResponse(message, status, &out);
            Request_->SendSerializedResult(std::move(out), status);
        }
        Request_->FinishStream(status);
        LOG_NOTICE_S(ctx, NKikimrServices::READ_TABLE_API,
            SelfId() << " Finish grpc stream, status: " << (int)status);

        // Answer all pending quota requests.
        while (!QuotaRequestQueue_.empty()) {
            auto request = QuotaRequestQueue_.front();
            const auto& rec = request->Get()->Record;
            QuotaRequestQueue_.pop_front();

            TAutoPtr<TEvTxProcessing::TEvStreamQuotaResponse> response
                = new TEvTxProcessing::TEvStreamQuotaResponse;
            response->Record.SetTxId(rec.GetTxId());
            response->Record.SetMessageSizeLimit(MessageSizeLimit);
            response->Record.SetMessageRowsLimit(MessageRowsLimit);
            response->Record.SetReservedMessages(0);

            LOG_DEBUG_S(ctx, NKikimrServices::READ_TABLE_API,
                        SelfId() << " Send zero quota to Shard " << rec.GetShardId()
                        << ", TxId " << rec.GetTxId());
            ctx.Send(request->Sender, response.Release(), 0, request->Cookie);
        }

        for (const auto& id : StreamSubscribers_) {
            LOG_DEBUG_S(ctx, NKikimrServices::READ_TABLE_API,
                        SelfId() << " Send dead stream notification to subscriber " << id);
            ctx.Send(id, new TEvTxProcessing::TEvStreamIsDead);
        }

        Die(ctx);
    }

    void StartInactivityTimer(TActorId& timer, TDuration timeout, EWakeupTag tag, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::READ_TABLE_API,
                    SelfId() << " Starting inactivity timer for " << timeout << " with tag " << int(tag));

        if (timer) {
            ctx.Send(timer, new TEvents::TEvPoison);
            timer = {};
        }

        auto *ev = new IEventHandle(SelfId(), SelfId(), new TEvents::TEvWakeup(tag));
        timer = CreateLongTimer(timeout, ev);
    }

    void HandleStreamTimeout(const TActorContext &ctx, const TString& msg) {
        google::protobuf::RepeatedPtrField<TYdbIssueMessageType> message;
        const NYql::TIssue& issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, msg);
        auto tmp = message.Add();
        NYql::IssueToMessage(issue, tmp);
        return ReplyFinishStream(StatusIds::TIMEOUT, message, ctx);
    }

    void TryToAllocateQuota() {
        if (QuotaRequestQueue_.empty())
            return;

        auto &cfg = AppData()->StreamingConfig.GetOutputStreamConfig();
        if (QuotaLimit_ <= QuotaReserved_ + LeftInGRpcAdaptorQueue_)
            return;
        ui32 freeQuota = QuotaLimit_ - QuotaReserved_ - LeftInGRpcAdaptorQueue_;

        // Allow to ignore MinQuotaSize if limit is smaller.
        if (freeQuota < cfg.GetMinQuotaSize() && freeQuota != QuotaLimit_)
            return;

        // Limit number of concurrently streaming shards.
        if (QuotaByShard_.size() >= cfg.GetMaxStreamingShards())
            return;

        // Limit numbers of pending chunks
        if (SendBuffer_.size() >= cfg.GetMaxStreamingShards())
            return;

        auto request = QuotaRequestQueue_.front();
        const auto& rec = request->Get()->Record;
        QuotaRequestQueue_.pop_front();

        if (ReleasedShards_.contains(rec.GetShardId())) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::READ_TABLE_API,
                        "Quota request from released shard " << rec.GetShardId());
            return TryToAllocateQuota();
        }

        ui32 quotaSize = Min(cfg.GetMaxQuotaSize(), freeQuota);

        QuotaReserved_ += quotaSize;
        QuotaByShard_[rec.GetShardId()] += quotaSize;

        TAutoPtr<TEvTxProcessing::TEvStreamQuotaResponse> response
            = new TEvTxProcessing::TEvStreamQuotaResponse;
        response->Record.SetTxId(rec.GetTxId());
        response->Record.SetMessageSizeLimit(MessageSizeLimit);
        response->Record.SetMessageRowsLimit(MessageRowsLimit);
        response->Record.SetReservedMessages(quotaSize);

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::READ_TABLE_API,
                    SelfId() << " Assign stream quota to Shard " << rec.GetShardId()
                    << ", Quota " << quotaSize << ", TxId "  << rec.GetTxId()
                    << " Reserved: " << QuotaReserved_ << " of " << QuotaLimit_
                    << ", Queued: " << LeftInGRpcAdaptorQueue_);

        Send(request->Sender, response.Release(), 0, request->Cookie);
    }

    void TryToSendStreamResult(const TString& data, ui64 shardId, ui64 txId, const TActorContext& ctx) {
        auto ru = CalcRuConsumption(data);

        auto getRlPAth = [this] {
            if (auto path = Request_->GetRlPath()) {
                return Sprintf("rate limiter found, coordination node: %s, resource: %s",
                    path->CoordinationNode.c_str(), path->ResourcePath.c_str());
            } else {
                return TString("rate limiter absent");
            }
        };
        LOG_DEBUG_S(ctx, NKikimrServices::READ_TABLE_API,
                    SelfId() << " got stream part, size: " << data.size()
                    << ", RU required: " << ru << " " << getRlPAth());

        TString out;
        NullSerializeReadTableResponse(data, StatusIds::SUCCESS, PlanStep, TxId, &out);
        TInstant startTime = ctx.Now();

        if (Request_->GetRlPath()) {
            auto selfId = this->SelfId();
            auto as = TActivationContext::ActorSystem();

            auto onSendAllowed = [selfId, as]() mutable {
                as->Send(selfId, new TEvents::TEvWakeup(EWakeupTag::RlSendAllowed));
            };

            auto onSendTimeout = [selfId, as] {
                as->Send(selfId, new TEvents::TEvWakeup(EWakeupTag::RlNoResource));
            };

            SendBuffer_.emplace_back(TBuffEntry{std::move(out), shardId, txId, startTime});
            auto rlActor = NRpcService::RateLimiterAcquireUseSameMailbox(
                *Request_.get(), ru, RlMaxDuration,
                std::move(onSendAllowed), std::move(onSendTimeout), ctx);

            LOG_DEBUG_S(ctx, NKikimrServices::READ_TABLE_API,
                        SelfId() << " Launch rate limiter actor: "
                        << rlActor);
        } else {
            DoSendStreamResult(std::move(out), shardId, txId, TInstant(), ctx);
        }
    }

    void DoSendStreamResult(TString&& out, ui64 shardId, ui64 txId, TInstant startTime, const TActorContext& ctx) {
        auto it = QuotaByShard_.find(shardId);
        if (it != QuotaByShard_.end()) {
            --it->second;
            if (!it->second)
                QuotaByShard_.erase(it);
            --QuotaReserved_;
        } else {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::READ_TABLE_API,
                        SelfId() << " Response out of quota from Shard " << shardId
                        << ", TxId " << txId);
        }

        if (startTime) {
            auto delay = ctx.Now() - startTime;
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::READ_TABLE_API,
                    SelfId() << " Data response has been delayed for " << delay << " seconds");
        }

        Request_->SendSerializedResult(std::move(out), StatusIds::SUCCESS);

        if (LeftInGRpcAdaptorQueue_ == 0) {
            LastDataStreamTimestamp_ = ctx.Now();
            if (!InactiveClientTimerPending_ && InactiveClientTimeout_) {
                StartInactivityTimer(InactiveClientTimer_, InactiveClientTimeout_, EWakeupTag::ClientInactivity, ctx);
                InactiveClientTimerPending_ = true;
            }
        }

        LeftInGRpcAdaptorQueue_++;
        if (LeftInGRpcAdaptorQueue_ > QuotaLimit_) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::READ_TABLE_API,
                        SelfId() << " TMessageBusServerRequest: response queue limit is exceeded.");
        }

        TryToAllocateQuota();
    }

    std::unique_ptr<IRequestNoOpCtx> Request_;

    TActorId ReadTableActor;

    ui64 PlanStep = 0;
    ui64 TxId = 0;

    TList<TEvTxProcessing::TEvStreamQuotaRequest::TPtr> QuotaRequestQueue_;

    size_t QuotaLimit_;
    size_t QuotaReserved_;
    THashMap<ui64, size_t> QuotaByShard_;
    THashSet<ui64> ReleasedShards_;

    size_t LeftInGRpcAdaptorQueue_;

    THashSet<TActorId> StreamSubscribers_;

    TDuration InactiveClientTimeout_;
    TDuration InactiveServerTimeout_;
    TInstant LastDataStreamTimestamp_;
    TInstant LastStatusTimestamp_;

    TActorId InactiveClientTimer_;
    TActorId InactiveServerTimer_;
    bool InactiveClientTimerPending_ = false;
    bool InactiveServerTimerPending_ = false;

    ui64 MessageSizeLimit = 0;
    ui64 MessageRowsLimit = 0;

    struct TBuffEntry
    {
        TString Buf;
        ui64 ShardId;
        ui64 TxId;
        TInstant StartTime;
    };
    TDeque<TBuffEntry> SendBuffer_;
    bool HasPendingSuccess = false;
};

void DoReadTableRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TReadTableRPC(p.release()));
}

} // namespace NGRpcService
} // namespace NKikimr
