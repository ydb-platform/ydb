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
        LOG_STREAMS_STORAGE_SERVICE_TRACE("TQueryActor contructed");
        //SetOperationInfo(operationName, queryPath);
    }

    void OnRunQuery() final {
        Cerr << "TQueryActor::OnRunQuery" << Endl;
        LOG_STREAMS_STORAGE_SERVICE_TRACE("TQueryActor::OnRunQuery");
        ReadyToExecute = true;
        ProcessQueries();
      //  RunDataQuery(Sql, Params.get()/*TxContro NKikimr::TQueryBase::TTxControl::BeginAndCommitTx*/);
    }

    void OnQueryResult() final {
        Y_ABORT_UNLESS(IsExecuting);

        Cerr << "OnQueryResult" << Endl;
        auto promise = DataQuery->Promise;
        DataQuery = std::nullopt;
        auto status = NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues());
        
        IsExecuting = false;
        
        if (IsFinishing) {
            if (NeedRollback) {
                Cerr << "Call Rollback" << Endl;
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            } else {
                Cerr << "Call finish" << Endl;
                Finish();
            }

        }
        Cerr << "TQueryActor::SetValue begin... "  << Endl;

        promise.SetValue(NYdb::NTable::TDataQueryResult(std::move(status), std::move(ResultSets), std::nullopt, std::nullopt, false, std::nullopt));
        Cerr << "TQueryActor::SetValue end"  << Endl;
    }

    void OnFinish(Ydb::StatusIds::StatusCode statusCode, NYql::TIssues&& issues) final {
        Cerr << "TQueryActor::OnFinish " << statusCode << Endl;
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

        Cerr << "TQueryActor::ExecuteDataQuery "  << Endl; 
        TDataQuery q{sql, params, txControl, promise};
        DataQuery = std::move(q);
        ProcessQueries();
    }


     void Finish222(bool needRollback) {
        Cerr << "Finish222 "  << Endl;
        IsFinishing = true;
        if (needRollback) {
            NeedRollback = true;
        }
        if (IsExecuting || DataQuery) {
            Cerr << "Ignore finish"  << Endl;
            return;
        }
        if (needRollback) {
            Cerr << "Call Rollback" << Endl;
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
        } else {
            Cerr << "Call finish" << Endl;
            Finish();
        }
     }

    void ProcessQueries() {
        Cerr << "TQueryActor::ProcessQueries "  << Endl;

        if (IsExecuting || !ReadyToExecute || !DataQuery) {
            return;
        }
       // auto& next = DataQueries.front();
        Cerr << "TQueryActor::RunDataQuery "  << Endl;
        IsExecuting = true;
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
        Cerr << "TQuerySession::Bootstrap "  << Endl;
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
        Cerr << "TQuerySession::PassAway "  << Endl;
        //Send(QueryActorId, new NActors::TEvents::TEvPoison());
        NActors::TActor<TQuerySession>::PassAway();
    }

    void Handle(TEvQueryActor::TEvExecuteDataQuery::TPtr& ev) {
        Cerr << "TQuerySession::TEvExecuteDataQuery "  << Endl;
        QueryActor->ExecuteDataQuery(ev->Get()->Sql, ev->Get()->Params, ev->Get()->TxControl, ev->Get()->Promise);
    }

    void Handle(TEvQueryActor::TEvFinish::TPtr& ev) {
        Cerr << "TQuerySession::TEvFinish "  << Endl;
        QueryActor->Finish222(ev->Get()->NeedRollback);

    }

private: 
    TQueryActor* QueryActor = nullptr;
    NActors::TActorId QueryActorId;
};

std::unique_ptr<TQuerySession> MakeQueryActor();

} // namespace NFq
