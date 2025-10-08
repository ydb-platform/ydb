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

    struct TDataQuery{
        const TString Sql;
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
        Cerr << "OnQueryResult" << Endl;
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& /*issues*/) final {
        Cerr << "TQueryActor::OnFinish " << status << Endl;
      //  Send(Owner, new TEvPrivate::TEvExecuteDataQueryResult(status, std::move(issues)));
    }


    NThreading::TFuture<NYdb::NTable::TDataQueryResult> ExecuteDataQuery(
        const TString& sql,
        std::shared_ptr<NYdb::TParamsBuilder> params,
        NYdb::NTable::TTxControl txControl) {

            Cerr << "TQueryActor::ExecuteDataQuery "  << Endl;
        
        TDataQuery query{sql, params, txControl, NThreading::NewPromise<NYdb::NTable::TDataQueryResult>()};
        DataQueries.push_back(std::move(query));
        ProcessQueries();
        return DataQueries.back().Promise.GetFuture();
    }

    void ProcessQueries() {
        Cerr << "TQueryActor::ProcessQueries "  << Endl;

        if (IsExecuting || !ReadyToExecute || DataQueries.empty()) {
            return;
        }
        auto& next = DataQueries.front();
        Cerr << "TQueryActor::RunDataQuery "  << Endl;
        RunDataQuery(next.Sql, next.Params.get()/*TxContro NKikimr::TQueryBase::TTxControl::BeginAndCommitTx*/);
    }

private:

    std::list<TDataQuery> DataQueries;
    bool IsExecuting = false;
    bool ReadyToExecute = false;
};

// std::unique_ptr<NActors::IActor> NewDataQuery(
//     NThreading::TPromise<NYdb::NTable::TDataQueryResult> promise,
//     const TString& sql,
//     std::shared_ptr<NYdb::TParamsBuilder> params,
//     NYdb::NTable::TTxControl txControl);

std::unique_ptr<TQueryActor> MakeQueryActor();

} // namespace NFq
