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
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_types/fluent_settings_helpers.h>


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

    explicit TQueryBase(ui64 logComponent, TString sessionId = {}, TString database = {}, bool isSystemUser = false);

    void Bootstrap();

    static TString GetDefaultDatabase();

protected:
    // Methods for using in derived classes.
    void Finish(Ydb::StatusIds::StatusCode status, const TString& message, bool rollbackOnError = true);
    void Finish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, bool rollbackOnError = true);
    void Finish();

    void RunDataQuery(const TString& sql, NYdb::TParamsBuilder* params = nullptr, TTxControl txControl = TTxControl::BeginAndCommitTx());
    void RunStreamQuery(const TString& sql, NYdb::TParamsBuilder* params = nullptr, ui64 channelBufferSize = 60_MB);
    void CancelStreamQuery();
    void CommitTransaction();

    void SetOperationInfo(const TString& operationName, const TString& traceId, NMonitoring::TDynamicCounterPtr counters = nullptr);
    void ClearTimeInfo();
    TDuration GetAverageTime() const;

    template <class THandlerFunc>
    void SetQueryResultHandler(THandlerFunc handler, const TString& stateDescrption = "") {
        QueryResultHandler = static_cast<TQueryResultHandler>(handler);
        StateDescription = stateDescrption;
    }

    template <class THandlerFunc>
    void SetStreamResultHandler(THandlerFunc handler) {
        StreamResultHandler = static_cast<TStreamResultHandler>(handler);
    }

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

    STFUNC(StateFunc);

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

    TString LogPrefix() const;

protected:
    const ui64 LogComponent;
    TString Database;
    TString SessionId;
    bool IsSystemUser = false;
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

private:
    TQueryResultHandler QueryResultHandler = &TQueryBase::CallOnQueryResult;
    TStreamResultHandler StreamResultHandler = &TQueryBase::CallOnStreamResult;
    NRpcService::TStreamReadProcessorPtr<Ydb::Table::ExecuteScanQueryPartialResponse> StreamQueryProcessor;

    NMonitoring::TDynamicCounters::TCounterPtr FinishOk;
    NMonitoring::TDynamicCounters::TCounterPtr FinishError;
};

template<typename TQueryActor, typename TResponse, typename ...TArgs>
class TQueryRetryActor : public NActors::TActorBootstrapped<TQueryRetryActor<TQueryActor, TResponse, TArgs...>> {
public:
    using TBase = NActors::TActorBootstrapped<TQueryRetryActor<TQueryActor, TResponse, TArgs...>>;
    using IRetryPolicy = IRetryPolicy<Ydb::StatusIds::StatusCode>;

    explicit TQueryRetryActor(const NActors::TActorId& replyActorId, const TArgs&... args)
        : ReplyActorId(replyActorId)
        , RetryPolicy(IRetryPolicy::GetExponentialBackoffPolicy(
            Retryable, TDuration::MilliSeconds(10), 
            TDuration::MilliSeconds(200), TDuration::Seconds(1),
            std::numeric_limits<size_t>::max(), TDuration::Seconds(1)
        ))
        , CreateQueryActor([=]() {
            return new TQueryActor(args...);
        })
    {}

    TQueryRetryActor(const NActors::TActorId& replyActorId, IRetryPolicy::TPtr retryPolicy, const TArgs&... args)
        : ReplyActorId(replyActorId)
        , RetryPolicy(retryPolicy)
        , CreateQueryActor([=]() {
            return new TQueryActor(args...);
        })
        , RetryState(RetryPolicy->CreateRetryState())
    {}

    void StartQueryActor() const {
        TBase::Register(CreateQueryActor());
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
        if (Retryable(status) == ERetryErrorClass::NoRetry) {
            Reply(ev);
            return;
        }

        if (RetryState == nullptr) {
            RetryState = RetryPolicy->CreateRetryState();
        }

        if (auto delay = RetryState->GetNextRetryDelay(status)) {
            TBase::Schedule(*delay, new NActors::TEvents::TEvWakeup());
        } else {
            Reply(ev);
        }
    }

    void Reply(const typename TResponse::TPtr& ev) {
        TBase::Send(ev->Forward(ReplyActorId));
        TBase::PassAway();
    }

    static ERetryErrorClass Retryable(Ydb::StatusIds::StatusCode status) {
        if (status == Ydb::StatusIds::SUCCESS) {
            return ERetryErrorClass::NoRetry;
        }

        if (status == Ydb::StatusIds::INTERNAL_ERROR
            || status == Ydb::StatusIds::UNAVAILABLE
            || status == Ydb::StatusIds::BAD_SESSION
            || status == Ydb::StatusIds::SESSION_EXPIRED
            || status == Ydb::StatusIds::SESSION_BUSY
            || status == Ydb::StatusIds::ABORTED) {
            return ERetryErrorClass::ShortRetry;
        }

        if (status == Ydb::StatusIds::OVERLOADED
            || status == Ydb::StatusIds::UNDETERMINED) {
            return ERetryErrorClass::LongRetry;
        }

        return ERetryErrorClass::NoRetry;
    }

private:
    const NActors::TActorId ReplyActorId;
    const IRetryPolicy::TPtr RetryPolicy;
    const std::function<TQueryActor*()> CreateQueryActor;
    IRetryPolicy::IRetryState::TPtr RetryState = nullptr;
};

} // namespace NKikimr
