#pragma once
#include "defs.h"
#include "events.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/ymq/actor/params.h>
#include <ydb/core/ymq/base/counters.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>

namespace NKikimr::NSQS {

// Builds transaction request and properly executes it
// Can either send TEvExecute to queue leader or create execution actor.
class TExecutorBuilder {
public:
    explicit TExecutorBuilder(TActorId parent, const TString& requestId);

    TExecutorBuilder& User(const TString& user) {
        UserName_ = user;
        return *this;
    }
    TExecutorBuilder& Queue(const TString& queue) {
        QueueName_ = queue;
        return *this;
    }
    TExecutorBuilder& Shard(ui64 shard) {
        Shard_ = shard;
        return *this;
    }
    TExecutorBuilder& QueueVersion(ui64 version) {
        QueueVersion_ = version;
        return *this;
    }
    TExecutorBuilder& DlqName(const TString& dlqName) {
        DlqName_ = dlqName;
        return *this;
    }
    TExecutorBuilder& DlqShard(ui64 dlqShard) {
        DlqShard_ = dlqShard;
        return *this;
    }
    TExecutorBuilder& DlqVersion(ui64 dlqVersion) {
        DlqVersion_ = dlqVersion;
        return *this;
    }
    TExecutorBuilder& DlqTablesFormat(ui32 dlqTablesFormat) {
        DlqTablesFormat_ = dlqTablesFormat;
        return *this;
    }
    TExecutorBuilder& QueueLeader(const TActorId& queueLeaderActor) {
        QueueLeaderActor_ = queueLeaderActor;
        return *this;
    }
    TExecutorBuilder& TablesFormat(ui32 tablesFormat) {
        TablesFormat_ = tablesFormat;
        return *this;
    }
    TExecutorBuilder& Text(const TString& text, bool miniKql = true) {
        auto* trans = Request().Record.MutableTransaction()->MutableMiniKQLTransaction();
        trans->MutableProgram()->SetText(text);
        trans->SetFlatMKQL(miniKql);
        return *this;
    }
    TExecutorBuilder& Bin(const TString& program, bool miniKql = true) {
        auto* trans = Request().Record.MutableTransaction()->MutableMiniKQLTransaction();
        trans->MutableProgram()->SetBin(program);
        trans->SetFlatMKQL(miniKql);
        return *this;
    }
    TExecutorBuilder& QueryId(EQueryId id) {
        QueryId_ = id;
        return *this;
    }
    TExecutorBuilder& Fifo(bool isFifo) {
        IsFifoQueue_ = isFifo;
        return *this;
    }
    TExecutorBuilder& Mode(NKikimrTxUserProxy::TMiniKQLTransaction::EMode mode) {
        Request().Record.MutableTransaction()->MutableMiniKQLTransaction()->SetMode(mode);
        return *this;
    }
    TExecutorBuilder& RetryOnTimeout(bool retry = true) {
        RetryOnTimeout_ = retry;
        return *this;
    }
    TExecutorBuilder& OnExecuted(TSqsEvents::TExecutedCallback cb) {
        Callback_ = std::move(cb);
        return *this;
    }
    TExecutorBuilder& Counters(TIntrusivePtr<TTransactionCounters> counters) {
        if (counters) {
            TransactionCounters_ = std::move(counters);
        }
        return *this;
    }
    TExecutorBuilder& Counters(const TIntrusivePtr<TQueueCounters>& queueCounters) {
        if (queueCounters) {
            TransactionCounters_ = queueCounters->GetTransactionCounters();
        }
        return *this;
    }
    TExecutorBuilder& Counters(const TIntrusivePtr<TUserCounters>& userCounters) {
        if (userCounters) {
            TransactionCounters_ = userCounters->GetTransactionCounters();
        }
        return *this;
    }
    TExecutorBuilder& CreateExecutorActor(bool create) {
        CreateExecutorActor_ = create;
        return *this;
    }

    TParameters& Params() {
        if (!Parameters_) {
            Parameters_.ConstructInPlace(
                Request().Record.MutableTransaction()->MutableMiniKQLTransaction()->MutableParams()->MutableProto(),
                this
            );
        }
        return *Parameters_;
    }

    NClient::TWriteValue ParamsValue() {
        auto* params = Request().Record.MutableTransaction()->MutableMiniKQLTransaction()->MutableParams()->MutableProto();
        return NClient::TWriteValue::Create(*params->MutableValue(), *params->MutableType());
    }

    TEvTxUserProxy::TEvProposeTransaction& Request() {
        return *ProposeTransactionRequest_;
    }

    // Start transaction
    // Invalidates all internal data
    void Start(); // choose execution way automatically // prefered
    void StartExecutorActor(); // explicilty choose a way to start actor

private:
    void SendToQueueLeader(); // make transaction throught leader to use cached compiled query

    bool HasQueryId() const {
        return QueryId_ != EQueryId::QUERY_VECTOR_SIZE;
    }

private:
    const TActorId Parent_;
    TString RequestId_;
    THolder<TEvTxUserProxy::TEvProposeTransaction> ProposeTransactionRequest_;
    TMaybe<TParameters> Parameters_;
    bool RetryOnTimeout_ = false;
    bool IsFifoQueue_ = false;
    TString UserName_;
    TString QueueName_;
    ui64 Shard_ = 0;
    ui64 QueueVersion_ = 0;
    TString DlqName_;
    ui64 DlqShard_ = 0;
    ui64 DlqVersion_ = 0;
    ui32 DlqTablesFormat_ = 0;
    TActorId QueueLeaderActor_;
    ui32 TablesFormat_ = 0;
    TSqsEvents::TExecutedCallback Callback_;
    EQueryId QueryId_ = EQueryId::QUERY_VECTOR_SIZE;
    TIntrusivePtr<TTransactionCounters> TransactionCounters_;
    bool CreateExecutorActor_ = false;
};

class TMiniKqlExecutionActor
    : public TActorBootstrapped<TMiniKqlExecutionActor>
{
    using TRequest  = TEvTxUserProxy::TEvProposeTransaction;
    using TResponse = TEvTxUserProxy::TEvProposeTransactionStatus;

public:
    TMiniKqlExecutionActor(
        const TActorId sender,
        TString requestId,
        THolder<TRequest> req,
        bool retryOnTimeout,
        const TQueuePath& path, // queue or user
        const TIntrusivePtr<TTransactionCounters>& counters,
        TSqsEvents::TExecutedCallback cb = TSqsEvents::TExecutedCallback());

    void Bootstrap();

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_EXECUTOR_ACTOR;
    }

    void SetQueryIdForLogging(EQueryId queryId) {
        QueryId_ = queryId;
    }

private:
    void CompileProgram(bool forceRefresh);

    void ProceedWithExecution();

    TString GetRequestType() const;
    TString GetActionType() const;
    void LogRequestDuration();

private:
    STATEFN(AwaitState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvProposeTransactionStatus,    HandleResponse);
            hFunc(TMiniKQLCompileServiceEvents::TEvCompileStatus, HandleCompile);
            hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, HandleResult);
            hFunc(TEvTabletPipe::TEvClientDestroyed,              HandlePipeClientDisconnected);
            hFunc(TEvTabletPipe::TEvClientConnected,              HandlePipeClientConnected);
            hFunc(TEvWakeup,                             HandleWakeup);
        }
    }

    void PassAway();

    void HandleCompile(TMiniKQLCompileServiceEvents::TEvCompileStatus::TPtr& ev);

    template<typename TKikimrResultRecord>
    bool ShouldRetryOnFail(const TKikimrResultRecord& record) const;

    void HandleResponse(TResponse::TPtr& ev);
    void HandleWakeup(TEvWakeup::TPtr& ev);
    void HandleResult(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev);
    void HandlePipeClientConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev);
    void HandlePipeClientDisconnected(TEvTabletPipe::TEvClientDestroyed::TPtr& ev);

    TDuration NextAttemptWaitDuration() const;
    void ScheduleRetry(bool compilation);
    bool RetryTimeoutExpired();

    void WaitForCompletion(bool retry = false);

private:
    enum class EMode {
        Compile,
        Exec,
        CompileAndExec,
    };

private:
    const TActorId Sender_;
    const TString RequestId_;
    const TSqsEvents::TExecutedCallback Cb_;
    THolder<TRequest> Request_;
    TString MkqlProgramText_;
    THashMap<TString, ui64> CompileResolveCookies_;
    bool CompilationPending_ = false;
    size_t CompilationRetries_ = 3;
    TMaybe<EQueryId> QueryId_; // information for logging
    TInstant StartTs_;
    EMode Mode_ = EMode::CompileAndExec;
    TInstant StartExecutionTs_ = TInstant::Zero();
    size_t AttemptNumber_ = 0;
    TDuration PrevAttemptWaitTime_ = TDuration::Zero();
    TIntrusivePtr<TTransactionCounters> Counters_;
    TQueuePath QueuePath_;
    bool RetryOnTimeout_;
    NKikimrMiniKQL::TParams ProtoParamsForDebug;

    // Waiting for transaction to complete
    TResponse::TPtr ResponseEvent_;
    TActorId TabletPipeClient_;
};

} // namespace NKikimr::NSQS
