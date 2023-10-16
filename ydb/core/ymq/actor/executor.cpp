#include "executor.h"
#include "log.h"
#include "cfg.h"

#include <ydb/core/protos/tx_proxy.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/public/lib/value/value.h>
#include <ydb/core/ymq/queues/common/db_queries_maker.h>

#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <util/generic/ptr.h>
#include <util/generic/utility.h>

namespace NKikimr::NSQS {

constexpr ui64 EXECUTE_RETRY_WAKEUP_TAG = 1;
constexpr ui64 COMPILE_RETRY_WAKEUP_TAG = 2;

static TString MiniKQLDataResponseToString(const TSqsEvents::TEvExecuted::TRecord& record) {
    using NKikimr::NClient::TValue;
    const TValue val(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
    return val.GetValueText<NClient::TFormatJSON>();
}

static TString MiniKQLParamsToString(const NKikimrMiniKQL::TParams& params) {
    if (!params.HasValue() && !params.HasType()) {
        return "{}";
    }
    using NKikimr::NClient::TValue;
    const TValue val(TValue::Create(params.GetValue(), params.GetType()));
    return val.GetValueText<NClient::TFormatJSON>();
}

TExecutorBuilder::TExecutorBuilder(TActorId parent, const TString& requestId)
    : Parent_(parent)
    , RequestId_(requestId)
    , ProposeTransactionRequest_(MakeHolder<TEvTxUserProxy::TEvProposeTransaction>())
{
}

void TExecutorBuilder::Start() {
    if (!CreateExecutorActor_ && HasQueryId() && QueueName_) {
        SendToQueueLeader();
    } else {
        StartExecutorActor();
    }
}

void TExecutorBuilder::StartExecutorActor() {
    TQueuePath path(Cfg().GetRoot(), UserName_, QueueName_, QueueVersion_);
    if (Request().Record.GetTransaction().HasMiniKQLTransaction()) {
        if (!Request().Record.GetTransaction().GetMiniKQLTransaction().HasMode()) {
            Request().Record.MutableTransaction()->MutableMiniKQLTransaction()->SetMode(NKikimrTxUserProxy::TMiniKQLTransaction::COMPILE_AND_EXEC);
        }
    }

    if (HasQueryId()) {
        auto* trans = Request().Record.MutableTransaction()->MutableMiniKQLTransaction();
        if (!trans->MutableProgram()->HasText() && !trans->MutableProgram()->HasBin()) {
            TDbQueriesMaker queryMaker(
                Cfg().GetRoot(),
                UserName_,
                QueueName_,
                QueueVersion_,
                IsFifoQueue_,
                Shard_,
                TablesFormat_,
                DlqName_,
                DlqShard_,
                DlqVersion_,
                DlqTablesFormat_
            );
            Text(queryMaker(QueryId_));
        }
    }

    if (HasQueryId()) {
        RLOG_SQS_DEBUG("Starting executor actor for query(idx=" << QueryId_ << "). Mode: " << NKikimrTxUserProxy::TMiniKQLTransaction::EMode_Name(Request().Record.GetTransaction().GetMiniKQLTransaction().GetMode()));
    } else {
        RLOG_SQS_DEBUG("Starting executor actor for text query. Mode: " << NKikimrTxUserProxy::TMiniKQLTransaction::EMode_Name(Request().Record.GetTransaction().GetMiniKQLTransaction().GetMode()));
    }

    THolder<TMiniKqlExecutionActor> actor =
        MakeHolder<TMiniKqlExecutionActor>(
            Parent_,
            RequestId_,
            std::move(ProposeTransactionRequest_),
            RetryOnTimeout_,
            path,
            TransactionCounters_,
            Callback_);

    if (HasQueryId()) { // query with id
        actor->SetQueryIdForLogging(QueryId_);
    }

    TActivationContext::Register(actor.Release());
}

void TExecutorBuilder::SendToQueueLeader() {
    Y_ABORT_UNLESS(QueueLeaderActor_);

    auto ev = MakeHolder<TSqsEvents::TEvExecute>(Parent_, RequestId_, TQueuePath(Cfg().GetRoot(), UserName_, QueueName_, QueueVersion_), QueryId_, Shard_);
    ev->RetryOnTimeout = RetryOnTimeout_;
    ev->Cb = std::move(Callback_);
    Params(); // create params if not yet exist
    ev->Params = std::move(*Request().Record.MutableTransaction()->MutableMiniKQLTransaction()->MutableParams()->MutableProto());

    RLOG_SQS_DEBUG("Sending execute request for query(idx=" << QueryId_ << ") to queue leader");

    TActivationContext::Send(new IEventHandle(QueueLeaderActor_, Parent_, ev.Release()));
}

TMiniKqlExecutionActor::TMiniKqlExecutionActor(
        const TActorId sender,
        TString requestId,
        THolder<TRequest> req,
        bool retryOnTimeout,
        const TQueuePath& path, // queue or user
        const TIntrusivePtr<TTransactionCounters>& counters,
        TSqsEvents::TExecutedCallback cb)
    : Sender_(sender)
    , RequestId_(std::move(requestId))
    , Cb_(cb)
    , Request_(std::move(req))
    , Counters_(counters)
    , QueuePath_(path)
    , RetryOnTimeout_(retryOnTimeout)
{}

void TMiniKqlExecutionActor::Bootstrap() {
    StartTs_ = TActivationContext::Now();

    auto& transaction = *Request_->Record.MutableTransaction();
    if (RequestId_) {
        transaction.SetUserRequestId(RequestId_);
    }

    // Set timeout
    if (transaction.HasMiniKQLTransaction()) {
        const auto& cfg = Cfg();
        Request_->Record.SetExecTimeoutPeriod(3 * cfg.GetTransactionTimeoutMs());
    }

    auto& mkqlTx = *transaction.MutableMiniKQLTransaction();

    if (mkqlTx.HasParams() && mkqlTx.GetParams().HasProto()) {
        try {
            RLOG_SQS_TRACE(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " Serializing params: " << MiniKQLParamsToString(mkqlTx.GetParams().GetProto()));
            auto counters = Counters_ && Counters_->AllocPoolCounters ? *Counters_->AllocPoolCounters : TAlignedPagePoolCounters();
            NMiniKQL::TScopedAlloc alloc(__LOCATION__, std::move(counters), AppData()->FunctionRegistry->SupportsSizedAllocators());
            NMiniKQL::TTypeEnvironment env(alloc);
            NMiniKQL::TRuntimeNode node = NMiniKQL::ImportValueFromProto(mkqlTx.GetParams().GetProto(), env);
            TString bin = NMiniKQL::SerializeRuntimeNode(node, env);
            ProtoParamsForDebug.Swap(mkqlTx.MutableParams()->MutableProto());
            mkqlTx.MutableParams()->ClearProto();
            mkqlTx.MutableParams()->SetBin(bin);
        } catch (const yexception& e) {
            RLOG_SQS_ERROR(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " Error while making mkql execution request params: " << CurrentExceptionMessage());
            // TODO Set error
            Send(Sender_, MakeHolder<TSqsEvents::TEvExecuted>(Cb_, ui64(0)));
            LogRequestDuration();
            PassAway();
            return;
        }
    }

    if (mkqlTx.HasProgram() && mkqlTx.GetProgram().HasText()) {
        MkqlProgramText_ = mkqlTx.GetProgram().GetText();
        const bool compileMode = mkqlTx.GetMode() == NKikimrTxUserProxy::TMiniKQLTransaction::COMPILE;
        Mode_ = compileMode ? EMode::Compile : EMode::CompileAndExec;
        CompileProgram(compileMode);
    } else {
        Mode_ = EMode::Exec;
        ProceedWithExecution();
    }

    Become(&TMiniKqlExecutionActor::AwaitState);
}

void TMiniKqlExecutionActor::CompileProgram(bool forceRefresh) {
    auto compileEv = MakeHolder<TMiniKQLCompileServiceEvents::TEvCompile>(MkqlProgramText_);
    compileEv->ForceRefresh = forceRefresh;
    if (!CompileResolveCookies_.empty()) {
        compileEv->CompileResolveCookies = std::move(CompileResolveCookies_);
    }
    RLOG_SQS_TRACE(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " Compile program: " << MkqlProgramText_);
    Send(MakeMiniKQLCompileServiceID(), compileEv.Release());
    CompilationPending_ = true;
    INC_COUNTER(Counters_, CompileQueryCount);
}

void TMiniKqlExecutionActor::ProceedWithExecution() {
    if (!CompilationPending_) {
        THolder<TRequest> ev = MakeHolder<TRequest>();
        ev->Record.CopyFrom(Request_->Record);

        RLOG_SQS_TRACE(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " Execute program: " << ev->Record << ". Params: " << MiniKQLParamsToString(ProtoParamsForDebug));
        StartExecutionTs_ = TActivationContext::Now();
        Send(MakeTxProxyID(), std::move(ev));
        ++AttemptNumber_;

        INC_COUNTER(Counters_, TransactionsInfly);
    }
}

void TMiniKqlExecutionActor::HandleCompile(TMiniKQLCompileServiceEvents::TEvCompileStatus::TPtr& ev) {
    if (Mode_ == EMode::CompileAndExec) {
        const TDuration duration = TActivationContext::Now() - StartTs_;
        RLOG_SQS_DEBUG(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " Compilation duration: " << duration.MilliSeconds() << "ms");
    }
    const auto& result = ev->Get()->Result;
    auto& mkqlTx = *Request_->Record.MutableTransaction()->MutableMiniKQLTransaction();

    if (!result.Errors.Empty()) {
        const TString errors = result.Errors.ToString();
        RLOG_SQS_ERROR(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " Errors while compiling program: " << errors << ", program text: " << MkqlProgramText_);

        bool retriableResolveError = false;
        if (CompilationRetries_ > 0) {
            for (const NYql::TIssue& issue : result.Errors) {
                if (issue.GetCode() == NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR) {
                    retriableResolveError = true;
                    break;
                }
            }
        }

        if (retriableResolveError) {
            --CompilationRetries_;
            ScheduleRetry(true);
        } else {
            NKikimrTxUserProxy::TEvProposeTransactionStatus resp;
            resp.SetStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError);
            IssuesToMessage(result.Errors, resp.MutableIssues());
            resp.SetMiniKQLErrors(errors);
            THolder<TSqsEvents::TEvExecuted> e(new TSqsEvents::TEvExecuted(resp, Cb_, ui64(0)));
            Send(Sender_, std::move(e));
            LogRequestDuration();
            PassAway();
        }
        return;
    } else {
        PrevAttemptWaitTime_ = TDuration::Zero();
    }

    if (mkqlTx.GetMode() == NKikimrTxUserProxy::TMiniKQLTransaction::COMPILE) {
        NKikimrTxUserProxy::TEvProposeTransactionStatus resp;
        resp.SetStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete);
        resp.MutableMiniKQLCompileResults()->SetCompiledProgram(result.CompiledProgram);
        RLOG_SQS_TRACE(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " Compile program response: " << resp);
        Send(Sender_, MakeHolder<TSqsEvents::TEvExecuted>(resp, Cb_, ui64(0)));
        LogRequestDuration();
        PassAway();
        return;
    }

    auto& pgm = *mkqlTx.MutableProgram();
    pgm.ClearText();
    pgm.SetBin(result.CompiledProgram);

    CompileResolveCookies_ = std::move(ev->Get()->CompileResolveCookies);

    CompilationPending_ = false;
    ProceedWithExecution();
}

template<typename TKikimrResultRecord>
bool TMiniKqlExecutionActor::ShouldRetryOnFail(const TKikimrResultRecord& record) const {
    const auto status = NKikimr::NTxProxy::TResultStatus::EStatus(record.GetStatus());
    return NTxProxy::TResultStatus::IsSoftErrorWithoutSideEffects(status) ||
           RetryOnTimeout_ && record.GetStatusCode() == NKikimrIssues::TStatusIds::TIMEOUT ||
           (record.HasSchemeShardStatus() && record.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusMultipleModifications); // very rare case in queue creation
}

void TMiniKqlExecutionActor::HandleResponse(TResponse::TPtr& ev) {
    const TDuration executionDuration = TActivationContext::Now() - StartExecutionTs_;
    auto& response = *ev->Get();
    auto& record = response.Record;
    const auto status = NKikimr::NTxProxy::TResultStatus::EStatus(record.GetStatus());
    RLOG_SQS_TRACE(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " HandleResponse " << record);
    RLOG_SQS_DEBUG(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " Attempt " << AttemptNumber_ << " execution duration: " << executionDuration.MilliSeconds() << "ms");
    if (Counters_) {
        DEC_COUNTER(Counters_, TransactionsInfly);
        INC_COUNTER(Counters_, TransactionsCount);
        if (QueryId_.Defined()) {
            Counters_->QueryTypeCounters[*QueryId_].TransactionsCount->Inc();
        }
    }

    const bool resolveError =
        status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError ||
        status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable;

    if (resolveError && CompilationRetries_ > 0 && !MkqlProgramText_.empty()) {
        RLOG_SQS_INFO(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " Resolve error. Retrying mkql request");
        --CompilationRetries_;
        ScheduleRetry(true);
        return;
    }

    bool retryableError = false;
    bool failed = false;
    if (status != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete
        && status != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress
        && status != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAlready) {
        failed = true;
        retryableError = ShouldRetryOnFail(record);
        if (retryableError) {
            RLOG_SQS_WARN(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " Retryable error in mkql execution result: " << response.Record);
        } else {
            RLOG_SQS_ERROR(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " Error in mkql execution result: " << response.Record);
        }
    } else {
        PrevAttemptWaitTime_ = TDuration::Zero();
        if (status != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress) {
            if (QueryId_ && Counters_) {
                Counters_->QueryTypeCounters[*QueryId_].TransactionDuration->Collect(executionDuration.MilliSeconds());
            }
            RLOG_SQS_TRACE(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " Sending mkql execution result: " << response.Record);
            if (response.Record.HasExecutionEngineEvaluatedResponse()) {
                RLOG_SQS_TRACE(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " Minikql data response: " << MiniKQLDataResponseToString(response.Record));
            }
        }
    }

    const bool timeout = failed && RetryTimeoutExpired();
    if (retryableError && timeout) {
        INC_COUNTER(Counters_, TransactionRetryTimeouts);
        RLOG_SQS_ERROR(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " Can't retry. Timeout.");
    }
    if (retryableError && !timeout) {
        ScheduleRetry(false);
    } else {
        if (failed && Counters_) {
            INC_COUNTER(Counters_, TransactionsFailed);
            if (QueryId_.Defined()) {
                Counters_->QueryTypeCounters[*QueryId_].TransactionsFailed->Inc();
            }
        }
        if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress) {
            ResponseEvent_ = std::move(ev);
            WaitForCompletion();
        } else {
            Send(Sender_, MakeHolder<TSqsEvents::TEvExecuted>(response.Record, Cb_, ui64(0)));
            LogRequestDuration();
            PassAway();
        }
    }
}

void TMiniKqlExecutionActor::HandleWakeup(TEvWakeup::TPtr& ev) {
    Y_ABORT_UNLESS(ev->Get()->Tag != 0);
    switch (ev->Get()->Tag) {
    case EXECUTE_RETRY_WAKEUP_TAG: {
        ProceedWithExecution();
        break;
    }
    case COMPILE_RETRY_WAKEUP_TAG: {
        CompileProgram(Mode_ == EMode::Compile);
        break;
    }
    default: {
        Y_ABORT();
    }
    }
}

TDuration TMiniKqlExecutionActor::NextAttemptWaitDuration() const {
    const auto& cfg = Cfg();
    const TDuration attemptWaitTime = ClampVal(PrevAttemptWaitTime_ * 2,
                                               TDuration::MilliSeconds(cfg.GetTransactionRetryWaitDurationMs()),
                                               TDuration::MilliSeconds(cfg.GetTransactionMaxRetryWaitDurationMs()));
    return attemptWaitTime;
}

bool TMiniKqlExecutionActor::RetryTimeoutExpired() {
    const auto& cfg = Cfg();
    return TActivationContext::Now() + NextAttemptWaitDuration() >= StartTs_ + TDuration::MilliSeconds(cfg.GetTransactionTimeoutMs());
}

void TMiniKqlExecutionActor::ScheduleRetry(bool compilation) {
    INC_COUNTER(Counters_, TransactionRetries);
    const auto& cfg = Cfg();
    const TDuration randomComponent = TDuration::MilliSeconds(RandomNumber<ui64>(cfg.GetTransactionRetryWaitDurationMs() / 2));
    const TDuration attemptWaitTime = NextAttemptWaitDuration();
    PrevAttemptWaitTime_ = attemptWaitTime;
    const TDuration waitTime = attemptWaitTime + randomComponent;
    RLOG_SQS_WARN(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " Retry" << (compilation ? " compilation" : " transaction") << " in " << waitTime.MilliSeconds() << "ms");
    this->Schedule(waitTime, new TEvWakeup(compilation ? COMPILE_RETRY_WAKEUP_TAG : EXECUTE_RETRY_WAKEUP_TAG));
}

void TMiniKqlExecutionActor::LogRequestDuration() {
    const TInstant endTime = TActivationContext::Now();
    const TDuration duration = endTime - StartTs_;
    RLOG_SQS_DEBUG(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " " << GetActionType() << " duration: " << duration.MilliSeconds() << "ms");
}

TString TMiniKqlExecutionActor::GetActionType() const {
    switch (Mode_) {
    case EMode::Compile:
        return "compilation";
    case EMode::Exec:
        return "execution";
    case EMode::CompileAndExec:
        return "compile & exec";
    }
    return TString();
}

TString TMiniKqlExecutionActor::GetRequestType() const {
    TStringBuilder ret;
    const auto& trans = Request_->Record.GetTransaction();
    if (QueryId_.Defined()) {
        ret << "Query(idx=" << *QueryId_ << ")";
    } else if (trans.HasModifyScheme() && trans.GetModifyScheme().HasOperationType()) {
        ret << "ModifyScheme(op=" << EOperationType_Name(trans.GetModifyScheme().GetOperationType()) << ")";
    } else if (trans.HasMiniKQLTransaction()) {
        ret << "Text query";
    } else {
        ret << "Query";
    }
    return std::move(ret);
}

void TMiniKqlExecutionActor::PassAway() {
    if (TabletPipeClient_) {
        NTabletPipe::CloseClient(SelfId(), TabletPipeClient_);
        TabletPipeClient_ = TActorId();
    }
    TActorBootstrapped<TMiniKqlExecutionActor>::PassAway();
}

void TMiniKqlExecutionActor::WaitForCompletion(bool retry) {
    const ui64 schemeShardId = ResponseEvent_->Get()->Record.GetSchemeShardTabletId();
    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {.RetryLimitCount = 5, .MinRetryTime = TDuration::MilliSeconds(100), .DoFirstRetryInstantly = !retry};
    TabletPipeClient_ = RegisterWithSameMailbox(NTabletPipe::CreateClient(SelfId(), schemeShardId, clientConfig));

    TAutoPtr<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion> request(new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion());
    request->Record.SetTxId(ResponseEvent_->Get()->Record.GetTxId());
    NTabletPipe::SendData(SelfId(), TabletPipeClient_, request.Release());

    RLOG_SQS_DEBUG(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " Waiting for transaction to complete. TxId: " << ResponseEvent_->Get()->Record.GetTxId() << ". Scheme shard id: " << schemeShardId);
}

void TMiniKqlExecutionActor::HandleResult(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
    Y_ABORT_UNLESS(ev->Get()->Record.GetTxId() == ResponseEvent_->Get()->Record.GetTxId());
    ResponseEvent_->Get()->Record.SetStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete);
    RLOG_SQS_TRACE(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " Sending mkql execution result: " << ResponseEvent_->Get()->Record);

    Send(Sender_, MakeHolder<TSqsEvents::TEvExecuted>(ResponseEvent_->Get()->Record, Cb_, ui64(0)));
    LogRequestDuration();
    PassAway();
}

void TMiniKqlExecutionActor::HandlePipeClientConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
    if (ev->Get()->Status != NKikimrProto::OK) {
        RLOG_SQS_WARN(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " Failed to connect to pipe: " << ev->Get()->Status << ". Reconnecting");
        WaitForCompletion(true);
    }
}

void TMiniKqlExecutionActor::HandlePipeClientDisconnected(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
    RLOG_SQS_WARN(GetRequestType() << " Queue " << TLogQueueName(QueuePath_) << " Pipe disconnected. Reconnecting");
    WaitForCompletion(true);
}

} // namespace NKikimr::NSQS
