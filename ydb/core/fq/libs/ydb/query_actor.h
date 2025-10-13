#pragma once

#include <memory>
#include <ydb/library/actors/core/actor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <library/cpp/threading/future/core/future.h>
#include <ydb/library/query_actor/query_actor.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/public/sdk/cpp/adapters/issue/issue.h>

namespace NFq {

struct TEvQueryActor {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvExecuteDataQuery = EvBegin + 20,
        EvFinish,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");
    struct TEvExecuteDataQuery : NActors::TEventLocal<TEvExecuteDataQuery, EvExecuteDataQuery> {

        TEvExecuteDataQuery(
            const TString& sql,
            std::shared_ptr<NYdb::TParamsBuilder> params,
            NYdb::NTable::TTxControl txControl,
            NYdb::NTable::TExecDataQuerySettings execDataQuerySettings,
            NThreading::TPromise<NYdb::NTable::TDataQueryResult> promise)
            : Sql(sql)
            , Params(params)
            , TxControl(txControl)
            , ExecDataQuerySettings(execDataQuerySettings)
            , Promise(promise)
        {}
        const TString Sql;
        std::shared_ptr<NYdb::TParamsBuilder> Params;
        NYdb::NTable::TTxControl TxControl;
        NYdb::NTable::TExecDataQuerySettings ExecDataQuerySettings;
        NThreading::TPromise<NYdb::NTable::TDataQueryResult> Promise;
    };
    struct TEvFinish : public NActors::TEventLocal<TEvFinish, EvFinish> {
        TEvFinish(bool needRollback)
        : NeedRollback(needRollback) {
        }
        bool NeedRollback = false;
    };
};

class TQueryActor final : public NKikimr::TQueryBase {
public:
    struct TDataQuery{
        TString Sql;
        std::shared_ptr<NYdb::TParamsBuilder> Params;
        NYdb::NTable::TTxControl TxControl;
        NThreading::TPromise<NYdb::NTable::TDataQueryResult> Promise;
    };

    TQueryActor()
        : NKikimr::TQueryBase(NKikimrServices::STREAMS_STORAGE_SERVICE) {
        LOG_STREAMS_STORAGE_SERVICE_INFO("TQueryActor()");

        //SetOperationInfo(operationName, queryPath);
    }

    void OnRunQuery() final {
        LOG_STREAMS_STORAGE_SERVICE_INFO("TQueryActor::OnRunQuery()");

        ReadyToExecute = true;
        ProcessQueries();
      //  RunDataQuery(Sql, Params.get()/*TxContro NKikimr::TQueryBase::TTxControl::BeginAndCommitTx*/);
    }

    void OnQueryResult() final {
        Y_ABORT_UNLESS(IsExecuting);
        LOG_STREAMS_STORAGE_SERVICE_INFO("TQueryActor::OnQueryResult()");

        auto promise = DataQuery->Promise;
        DataQuery = std::nullopt;
        auto status = NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues());
        
        IsExecuting = false;
        
        if (IsFinishing) {
            if (NeedRollback) {
                LOG_STREAMS_STORAGE_SERVICE_INFO("TQueryActor::Call Rollback");
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            } else {
                LOG_STREAMS_STORAGE_SERVICE_INFO("TQueryActor::Call finish");
                Finish();
            }

        }
        LOG_STREAMS_STORAGE_SERVICE_INFO("TQueryActor::SetValue begin...");


        promise.SetValue(NYdb::NTable::TDataQueryResult(std::move(status), std::move(ResultSets), std::nullopt, std::nullopt, false, std::nullopt));
        LOG_STREAMS_STORAGE_SERVICE_INFO("TQueryActor::SetValue end");
    }

    void OnFinish(Ydb::StatusIds::StatusCode statusCode, NYql::TIssues&& issues) final {
        LOG_STREAMS_STORAGE_SERVICE_INFO("TQueryActor::OnFinish()");

        if (DataQuery) {          
            NYdb::TStatus status(static_cast<NYdb::EStatus>(statusCode), NYdb::NAdapters::ToSdkIssues(issues));
            DataQuery->Promise.SetValue(NYdb::NTable::TDataQueryResult(std::move(status), std::move(ResultSets), std::nullopt, std::nullopt, false, std::nullopt));
        }
        DataQuery = std::nullopt;
    }

    void ExecuteDataQuery(
        const TString& sql,
        std::shared_ptr<NYdb::TParamsBuilder> params,
        NYdb::NTable::TTxControl txControl,
        NThreading::TPromise<NYdb::NTable::TDataQueryResult> promise) {
        Y_ABORT_UNLESS(!IsExecuting);

        LOG_STREAMS_STORAGE_SERVICE_INFO("TQueryActor::ExecuteDataQuery()");

        TDataQuery q{sql, params, txControl, promise};
        DataQuery = std::move(q);
        ProcessQueries();
    }


     void Finish222(bool needRollback) {
        LOG_STREAMS_STORAGE_SERVICE_INFO("TQueryActor::Finish222()");

        IsFinishing = true;
        if (needRollback) {
            NeedRollback = true;
        }
        if (IsExecuting || DataQuery) {
            LOG_STREAMS_STORAGE_SERVICE_INFO("TQueryActor::Ignore finish");
            return;
        }
        if (needRollback) {
            LOG_STREAMS_STORAGE_SERVICE_INFO("TQueryActor::call rollback");

            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
        } else {
            LOG_STREAMS_STORAGE_SERVICE_INFO("TQueryActor::call finish");

            Finish();
        }
     }

    void ProcessQueries() {

        if (IsExecuting || !ReadyToExecute || !DataQuery) {
            return;
        }
       // auto& next = DataQueries.front();
        IsExecuting = true;
        LOG_STREAMS_STORAGE_SERVICE_INFO("TQueryActor::RunDataQuery " << DataQuery->Sql);

        RunDataQuery(DataQuery->Sql, DataQuery->Params.get()/*TxContro NKikimr::TQueryBase::TTxControl::BeginAndCommitTx*/);
    }

private:

    std::optional<TDataQuery> DataQuery;
    bool IsExecuting = false;
    bool ReadyToExecute = false;
    bool IsFinishing = false;
    bool NeedRollback = false;
};


struct TQuerySession : public NActors::TActorBootstrapped<TQuerySession> {

    TQuerySession() {
    }

    void Bootstrap(const NActors::TActorContext &ctx) {
        LOG_STREAMS_STORAGE_SERVICE_INFO("TQuerySession::Bootstrap ");
        Become(&TThis::StateFunc);
        QueryActor = new TQueryActor();
        QueryActorId = ctx.RegisterWithSameMailbox(QueryActor);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvQueryActor::TEvExecuteDataQuery, Handle);
            hFunc(TEvQueryActor::TEvFinish, Handle);
            sFunc(NActors::TEvents::TEvPoison, PassAway);
        }
    }

    void PassAway() {
        LOG_STREAMS_STORAGE_SERVICE_INFO("TQuerySession::PassAway ");
        if (!FinishSent) {
            LOG_STREAMS_STORAGE_SERVICE_INFO("TQuerySession send Finish ");
            QueryActor->Finish222(false);
        }
        NActors::TActor<TQuerySession>::PassAway();
    }

    void Handle(TEvQueryActor::TEvExecuteDataQuery::TPtr& ev) {
        LOG_STREAMS_STORAGE_SERVICE_INFO("TQuerySession::TEvExecuteDataQuery ");
        QueryActor->ExecuteDataQuery(ev->Get()->Sql, ev->Get()->Params, ev->Get()->TxControl, ev->Get()->Promise);
    }

    void Handle(TEvQueryActor::TEvFinish::TPtr& ev) {

        LOG_STREAMS_STORAGE_SERVICE_INFO("TQuerySession::TEvFinish ");
        QueryActor->Finish222(ev->Get()->NeedRollback);
        FinishSent = true;
    }

private: 
    TQueryActor* QueryActor = nullptr;
    NActors::TActorId QueryActorId;
    bool FinishSent = false;
};

std::unique_ptr<TQuerySession> MakeQueryActor();

} // namespace NFq
