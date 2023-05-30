#pragma once
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actorid.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/threading/future/future.h>

namespace NKikimr {

// TODO: add retry logic
class TQueryBase : public NActors::TActorBootstrapped<TQueryBase> {
protected:
    struct TTxControl {
        static TTxControl CommitTx();
        static TTxControl BeginTx();
        static TTxControl BeginAndCommitTx();
        static TTxControl ContinueTx();
        static TTxControl ContinueAndCommitTx();

        bool Begin = false;
        bool Commit = false;
        bool Continue = false;
    };

private:
    struct TEvQueryBasePrivate {
        // Event ids
        enum EEv : ui32 {
            EvDataQueryResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvCreateSessionResult,
            EvDeleteSessionResult,
            EvRollbackTransactionResponse,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

        // Events
        static NYql::TIssues IssuesFromOperation(const Ydb::Operations::Operation& operation);

        struct TEvDataQueryResult : public NActors::TEventLocal<TEvDataQueryResult, EvDataQueryResult> {
            TEvDataQueryResult(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues);
            TEvDataQueryResult(const Ydb::Table::ExecuteDataQueryResponse& resp);

            Ydb::StatusIds::StatusCode Status;
            NYql::TIssues Issues;
            Ydb::Table::ExecuteQueryResult Result;
        };

        struct TEvCreateSessionResult : public NActors::TEventLocal<TEvCreateSessionResult, EvCreateSessionResult> {
            TEvCreateSessionResult(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues);
            TEvCreateSessionResult(const Ydb::Table::CreateSessionResponse& resp);

            Ydb::StatusIds::StatusCode Status;
            NYql::TIssues Issues;
            TString SessionId;
        };

        struct TEvDeleteSessionResult : public NActors::TEventLocal<TEvDeleteSessionResult, EvDeleteSessionResult> {
            TEvDeleteSessionResult(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues);
            TEvDeleteSessionResult(const Ydb::Table::DeleteSessionResponse& resp);

            Ydb::StatusIds::StatusCode Status;
            NYql::TIssues Issues;
        };

        struct TEvRollbackTransactionResponse : public NActors::TEventLocal<TEvRollbackTransactionResponse, EvRollbackTransactionResponse> {
            TEvRollbackTransactionResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues);
            TEvRollbackTransactionResponse(const Ydb::Table::RollbackTransactionResponse& resp);

            Ydb::StatusIds::StatusCode Status;
            NYql::TIssues Issues;
        };
    };

public:
    static constexpr char ActorName[] = "SQL_QUERY";

    explicit TQueryBase(ui64 logComponent, TString sessionId = {}, TString database = GetDefaultDatabase());

    void Bootstrap();

    static TString GetDefaultDatabase();

protected:
    // Methods for using in derived classes.
    void Finish(Ydb::StatusIds::StatusCode status, const TString& message, bool rollbackOnError = true);
    void Finish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, bool rollbackOnError = true);
    void Finish();

    void RunDataQuery(const TString& sql, NYdb::TParamsBuilder* params = nullptr, TTxControl txControl = TTxControl::BeginAndCommitTx());

private:
    // Methods for implementing in derived classes.
    virtual void OnRunQuery() = 0;
    virtual void OnQueryResult() = 0; // Must either run next query or finish
    virtual void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) = 0;

private:
    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override;

    template <class TProto, class TEvent>
    void Subscribe(NThreading::TFuture<TProto>&& f) {
        f.Subscribe(
            [as = NActors::TActivationContext::ActorSystem(), selfId = SelfId()](const NThreading::TFuture<TProto>& res)
            {
                as->Send(selfId, new TEvent(res.GetValue()));
            }
        );
    }

    STFUNC(StateFunc);
    void Handle(TEvQueryBasePrivate::TEvCreateSessionResult::TPtr& ev);
    void Handle(TEvQueryBasePrivate::TEvDeleteSessionResult::TPtr& ev);
    void Handle(TEvQueryBasePrivate::TEvDataQueryResult::TPtr& ev);
    void Handle(TEvQueryBasePrivate::TEvRollbackTransactionResponse::TPtr& ev);

    void RunQuery();
    void RunCreateSession();
    void RunDeleteSession();
    void RollbackTransaction();

protected:
    const ui64 LogComponent;
    const TString Database;
    TString SessionId;
    TString TxId;
    bool DeleteSession = false;
    bool RunningQuery = false;
    bool Finished = false;
    bool CommitRequested = false;

    NActors::TActorId Owner;

    std::vector<NYdb::TResultSet> ResultSets;
};

} // namespace NKikimr
