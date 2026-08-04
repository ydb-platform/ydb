#pragma once

#include <library/cpp/retry/retry_policy.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/size_literals.h>

#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fluent_settings_helpers.h>


namespace NKikimr {

class TQueryBase : public NActors::TActorBootstrapped<TQueryBase> {
    using TBase = NActors::TActorBootstrapped<TQueryBase>;

protected:
    struct TTxControl {
        using TSelf = TTxControl;

        static TTxControl CommitTx();
        static TTxControl BeginTx(bool snapshotRead = false);
        static TTxControl BeginAndCommitTx(bool snapshotRead = false);
        static TTxControl ContinueTx();
        static TTxControl ContinueAndCommitTx();

        FLUENT_SETTING_DEFAULT(bool, Begin, false);
        FLUENT_SETTING_DEFAULT(bool, Commit, false);
        FLUENT_SETTING_DEFAULT(bool, Continue, false);
        FLUENT_SETTING_DEFAULT(bool, SnapshotRead, false);
    };

    using TQueryResultHandler = void (TQueryBase::*)();
    using TStreamResultHandler = void (TQueryBase::*)(NYdb::TResultSet&&);

private:
    struct TEvQueryBasePrivate {
        // Event ids
        enum EEv : ui32 {
            EvDataQueryResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvStreamQueryResultPart,

            EvCreateSessionResult,
            EvDeleteSessionResponse,

            EvRollbackTransactionResponse,
            EvCommitTransactionResponse,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

        // Events
        struct TEvDataQueryResult : public NActors::TEventLocal<TEvDataQueryResult, EvDataQueryResult> {
            TEvDataQueryResult(Ydb::Table::ExecuteDataQueryResponse&& response);

            const Ydb::StatusIds::StatusCode Status;
            NYql::TIssues Issues;
            Ydb::Table::ExecuteQueryResult Result;
        };

        struct TEvStreamQueryResultPart : public NActors::TEventLocal<TEvStreamQueryResultPart, EvStreamQueryResultPart> {
            TEvStreamQueryResultPart(Ydb::Table::ExecuteScanQueryPartialResponse&& response);

            const Ydb::StatusIds::StatusCode Status;
            NYql::TIssues Issues;
            Ydb::ResultSet ResultSet;
        };

        struct TEvCreateSessionResult : public NActors::TEventLocal<TEvCreateSessionResult, EvCreateSessionResult> {
            TEvCreateSessionResult(Ydb::Table::CreateSessionResponse&& response);

            const Ydb::StatusIds::StatusCode Status;
            NYql::TIssues Issues;
            TString SessionId;
        };

        struct TEvDeleteSessionResponse : public NActors::TEventLocal<TEvDeleteSessionResponse, EvDeleteSessionResponse> {
            TEvDeleteSessionResponse(Ydb::Table::DeleteSessionResponse&& response);

            const Ydb::StatusIds::StatusCode Status;
            NYql::TIssues Issues;
        };

        struct TEvRollbackTransactionResponse : public NActors::TEventLocal<TEvRollbackTransactionResponse, EvRollbackTransactionResponse> {
            TEvRollbackTransactionResponse(Ydb::Table::RollbackTransactionResponse&& response);

            const Ydb::StatusIds::StatusCode Status;
            NYql::TIssues Issues;
        };

        struct TEvCommitTransactionResponse : public NActors::TEventLocal<TEvCommitTransactionResponse, EvCommitTransactionResponse> {
            TEvCommitTransactionResponse(Ydb::Table::CommitTransactionResponse&& response);

            const Ydb::StatusIds::StatusCode Status;
            NYql::TIssues Issues;
        };
    };

public:
    static constexpr char ActorName[] = "SQL_QUERY";

    explicit TQueryBase(ui64 logComponent, TString sessionId = {}, TString database = {}, bool isSystemUser = false, bool isStreamingMode = false);

    void Bootstrap();

    static TString GetDefaultDatabase();

    struct TLogInfo {
        ui64 LogComponent;
        TString OperationName;
        TString TraceId;
    };

    TLogInfo GetLogInfo() const;

protected:
    // Methods for using in derived classes.
    void Finish(Ydb::StatusIds::StatusCode status, const TString& message, bool rollbackOnError = true);
    void Finish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, bool rollbackOnError = true);
    void Finish();

    void RunDataQuery(TString sql, NYdb::TParamsBuilder* params = nullptr, TTxControl txControl = TTxControl::BeginAndCommitTx());
    void RunStreamQuery(TString sql, NYdb::TParamsBuilder* params = nullptr, ui64 channelBufferSize = 60_MB);
    void CancelStreamQuery();
    void CommitTransaction();

    void SetOperationInfo(const TString& operationName, const TString& traceId, NMonitoring::TDynamicCounterPtr counters = nullptr);
    void ClearTimeInfo();
    TDuration GetAverageTime() const;

    template <class THandlerFunc>
    void SetQueryResultHandler(THandlerFunc handler, const TString& stateDescription = "") {
        QueryResultHandler = static_cast<TQueryResultHandler>(handler);
        StateDescription = stateDescription;
    }

    template <class THandlerFunc>
    void SetStreamResultHandler(THandlerFunc handler) {
        StreamResultHandler = static_cast<TStreamResultHandler>(handler);
    }

    TString LogPrefix() const;

    virtual STFUNC(StateFunc);

private:
    // Methods for implementing in derived classes.
    virtual void OnRunQuery() = 0;
    virtual void OnQueryResult() {} // Must either run next query or finish
    virtual void OnStreamResult(NYdb::TResultSet&&) {}
    virtual void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) = 0;

private:
    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override;

    template <class TProto, class TEvent>
    void Subscribe(NThreading::TFuture<TProto>&& f) const {
        f.Subscribe([callback = GetOperationCallback<TProto, TEvent>()](NThreading::TFuture<TProto> f) {
            callback(f.ExtractValue());
        });
    }

    template <class TProto, class TEvent>
    std::function<void(TProto&&)> GetOperationCallback() const {
        return [actorSystem = NActors::TActivationContext::ActorSystem(), selfId = SelfId()](TProto&& result) {
            actorSystem->Send(selfId, new TEvent(std::move(result)));
        };
    }

    void RunCreateSession() const;
    void Handle(TEvQueryBasePrivate::TEvCreateSessionResult::TPtr& ev);

    void RunDeleteSession() const;
    void Handle(TEvQueryBasePrivate::TEvDeleteSessionResponse::TPtr& ev);

    void RollbackTransaction() const;
    void Handle(TEvQueryBasePrivate::TEvRollbackTransactionResponse::TPtr& ev);
    void Handle(TEvQueryBasePrivate::TEvCommitTransactionResponse::TPtr& ev);

    void Handle(TEvQueryBasePrivate::TEvDataQueryResult::TPtr& ev);
    void Handle(TEvQueryBasePrivate::TEvStreamQueryResultPart::TPtr& ev);

    void RunQuery();
    void CallOnQueryResult();

    void ReadNextStreamPart();
    void FinishStreamRequest();
    void CallOnStreamResult(NYdb::TResultSet&& resultSet);

protected:
    const ui64 LogComponent;
    TString Database;
    TString SessionId;
    bool IsSystemUser = false;
    bool IsStreamingMode = false;
    TString TxId;
    bool DeleteSession = false;
    bool RunningQuery = false;
    bool RunningCommit = false;
    bool Finished = false;
    bool CommitRequested = false;

    NActors::TActorId Owner;
    std::vector<NYdb::TResultSet> ResultSets;

    TString OperationName;
    TString StateDescription;
    TString TraceId;

    TInstant RequestStartTime;
    TDuration AmountRequestsTime;
    ui32 NumberRequests = 0;

    // Opt-in: forward TEvStreamQueryResultPart issues to Finish() on SUCCESS.
    bool ForwardStreamIssuesOnSuccess = false;
    NYql::TIssues AccumulatedStreamIssues;

private:
    TQueryResultHandler QueryResultHandler = &TQueryBase::CallOnQueryResult;
    TStreamResultHandler StreamResultHandler = &TQueryBase::CallOnStreamResult;
    NRpcService::TStreamReadProcessorPtr<Ydb::Table::ExecuteScanQueryPartialResponse> StreamQueryProcessor;

    NMonitoring::TDynamicCounters::TCounterPtr FinishOk;
    NMonitoring::TDynamicCounters::TCounterPtr FinishError;
};

class TQueryRetryActorBase {
public:
    using IRetryPolicy = IRetryPolicy<Ydb::StatusIds::StatusCode>;

    static ERetryErrorClass Retryable(Ydb::StatusIds::StatusCode status);

protected:
    void UpdateLogInfo(const TQueryBase::TLogInfo& logInfo, const TActorId& ownerId, const TActorId& selfId);

    TString LogPrefix() const;

protected:
    ui64 LogComponent;
    TString OperationName;
    TString TraceId;
    TActorId OwnerId;
    TActorId SelfId;
};

template<typename TQueryActor, typename TResponse, typename ...TArgs>
class TQueryRetryActor : public NActors::TActorBootstrapped<TQueryRetryActor<TQueryActor, TResponse, TArgs...>>, public TQueryRetryActorBase {
    static_assert(std::is_base_of<TQueryBase, TQueryActor>::value, "Query actor must inherit from TQueryBase");
    static_assert(std::is_base_of<IEventBase, TResponse>::value, "Invalid response type");

    using TBase = NActors::TActorBootstrapped<TQueryRetryActor<TQueryActor, TResponse, TArgs...>>;

public:
    explicit TQueryRetryActor(const NActors::TActorId& replyActorId, TArgs... args)
        : ReplyActorId(replyActorId)
        , RetryPolicy(IRetryPolicy::GetExponentialBackoffPolicy(
            Retryable, TDuration::MilliSeconds(10),
            TDuration::MilliSeconds(200), TDuration::Seconds(1),
            std::numeric_limits<size_t>::max(), TDuration::Seconds(1)
        ))
        , CreateQueryActor([...args = std::forward<TArgs>(args)]() {
            return std::make_unique<TQueryActor>(args...);
        })
    {}

    TQueryRetryActor(const NActors::TActorId& replyActorId, IRetryPolicy::TPtr retryPolicy, TArgs... args)
        : ReplyActorId(replyActorId)
        , RetryPolicy(std::move(retryPolicy))
        , CreateQueryActor([...args = std::forward<TArgs>(args)]() {
            return std::make_unique<TQueryActor>(args...);
        })
        , RetryState(RetryPolicy->CreateRetryState())
    {}

    void StartQueryActor() {
        auto queryActor = CreateQueryActor();
        UpdateLogInfo(queryActor->GetLogInfo(), ReplyActorId, TBase::SelfId());

        RetryAttempts++;
        const auto& queryActorId = TBase::Register(queryActor.release());
        YDB_LOG_DEBUG_COMP(LogComponent, "Starting query",
            {"logPrefix", LogPrefix()},
            {"actor", RetryAttempts},
            {"queryActorId", queryActorId});
    }

    void Bootstrap() {
        TBase::Become(&TQueryRetryActor::StateFunc);
        StartQueryActor();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NActors::TEvents::TEvWakeup, Wakeup);
        hFunc(TResponse, Handle);
    )

    void Wakeup(NActors::TEvents::TEvWakeup::TPtr&) {
        StartQueryActor();
    }

    void Handle(const typename TResponse::TPtr& ev) {
        const Ydb::StatusIds::StatusCode status = ev->Get()->Status;
        YDB_LOG_DEBUG_COMP(LogComponent, "Got response",
            {"logPrefix", LogPrefix()},
            {"sender", ev->Sender},
            {"status", status});

        if (Retryable(status) == ERetryErrorClass::NoRetry) {
            Reply(ev);
            return;
        }

        if (RetryState == nullptr) {
            RetryState = RetryPolicy->CreateRetryState();
        }

        if (auto delay = RetryState->GetNextRetryDelay(status)) {
            YDB_LOG_NOTICE_COMP(LogComponent, "Retry status after",
                {"logPrefix", LogPrefix()},
                {"status", status},
                {"delay", *delay});
            TBase::Schedule(*delay, new NActors::TEvents::TEvWakeup());
        } else {
            Reply(ev);
        }
    }

    void Reply(const typename TResponse::TPtr& ev) {
        TBase::Send(ev->Forward(ReplyActorId));
        TBase::PassAway();
    }

private:
    const NActors::TActorId ReplyActorId;
    const IRetryPolicy::TPtr RetryPolicy;
    const std::function<std::unique_ptr<TQueryActor>()> CreateQueryActor;
    IRetryPolicy::IRetryState::TPtr RetryState = nullptr;
    ui64 RetryAttempts = 0;
};

template<typename TQueryActor, typename TResponse>
class TQueryRetryActorMixin {
    template<typename... TArgs>
    using TRetry = TQueryRetryActor<TQueryActor, TResponse, TArgs...>;

public:
    template<typename... TArgs>
    static auto MakeRetry(const NActors::TActorId& replyActorId, TArgs&&... args) {
        return new TRetry<std::decay_t<TArgs>...>(replyActorId, std::forward<TArgs>(args)...);
    }

    template<typename... TArgs>
    static auto MakeRetry(const NActors::TActorId& replyActorId, TQueryRetryActorBase::IRetryPolicy::TPtr retryPolicy, TArgs&&... args) {
        return new TRetry<std::decay_t<TArgs>...>(replyActorId, std::move(retryPolicy), std::forward<TArgs>(args)...);
    }
};

} // namespace NKikimr
