#pragma once

#include <memory>
#include <ydb/library/actors/core/actor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <library/cpp/threading/future/core/future.h>
#include <ydb/library/query_actor/query_actor.h>

namespace NFq {


class TQueryActor final : public NKikimr::TQueryBase {
public:
    //using TRetry = NKikimr::TQueryRetryActor<TExecuteDataQuery, TEvPrivate::TEvExecuteDataQueryResult, TString, TString, TString, std::shared_ptr<NYdb::TParamsBuilder>, NYdb::NTable::TTxControl>;

    //using NKikimr::TQueryBase::Finish; 

    struct TDataQuery{
        TString Sql;
        std::shared_ptr<NYdb::TParamsBuilder> Params;
        NYdb::NTable::TTxControl TxControl;
        NThreading::TPromise<NYdb::NTable::TDataQueryResult> Promise;
    };

    TQueryActor()
        : NKikimr::TQueryBase(NKikimrServices::STREAMS_STORAGE_SERVICE) {
        Cerr << "TExecuteDataQuery111" << Endl;
        //SetOperationInfo(operationName, queryPath);
    }

    void OnRunQuery() final {
        Cerr << "TQueryActor::OnRunQuery" << Endl;
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
            Cerr << "Call finish" << Endl;
            Finish();
        }
        promise.SetValue(NYdb::NTable::TDataQueryResult(std::move(status), std::move(ResultSets), std::nullopt, std::nullopt, false, std::nullopt));
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& /*issues*/) final {
        Cerr << "TQueryActor::OnFinish " << status << Endl;
      //  Send(Owner, new TEvPrivate::TEvExecuteDataQueryResult(status, std::move(issues)));
    }



    NThreading::TFuture<NYdb::NTable::TDataQueryResult> ExecuteDataQuery(
        const TString& sql,
        std::shared_ptr<NYdb::TParamsBuilder> params,
        NYdb::NTable::TTxControl txControl) {
        Y_ABORT_UNLESS(!IsExecuting);

        Cerr << "TQueryActor::ExecuteDataQuery "  << Endl; 
        TDataQuery q{sql, params, txControl, NThreading::NewPromise<NYdb::NTable::TDataQueryResult>()};
        DataQuery = std::move(q);
        ProcessQueries();
        auto p = NThreading::NewPromise<NYdb::NTable::TDataQueryResult>();
        return DataQuery->Promise.GetFuture();
       // return p.GetFuture();
    }


     void Finish222() {
        Cerr << "Finish222 "  << Endl;
        IsFinishing = true;
        if (IsExecuting) {
            Cerr << "Finish222 IsExecuting"  << Endl;
            return;
        }
        Finish();
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
};

// std::unique_ptr<NActors::IActor> NewDataQuery(
//     NThreading::TPromise<NYdb::NTable::TDataQueryResult> promise,
//     const TString& sql,
//     std::shared_ptr<NYdb::TParamsBuilder> params,
//     NYdb::NTable::TTxControl txControl);

std::unique_ptr<TQueryActor> MakeQueryActor();

} // namespace NFq
