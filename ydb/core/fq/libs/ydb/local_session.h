#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <library/cpp/threading/future/core/future.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/core/fq/libs/ydb/session.h>
#include <ydb/core/fq/libs/ydb/query_actor.h>
namespace NFq {

struct TLocalSession : public ISession { 

    TLocalSession() {
      //  QuerySession = MakeQueryActor().release();
        QuerySessionId = NActors::TActivationContext::AsActorContext().RegisterWithSameMailbox(MakeQueryActor().release());
    }

    NThreading::TFuture<NYdb::NTable::TDataQueryResult> ExecuteDataQuery(
        const TString& sql,
        std::shared_ptr<NYdb::TParamsBuilder> params,
        NYdb::NTable::TTxControl txControl,
        NYdb::NTable::TExecDataQuerySettings execDataQuerySettings = NYdb::NTable::TExecDataQuerySettings()) override {
        Cerr << "TLocalSession::ExecuteDataQuery" << Endl;
        //return QueryActor->ExecuteDataQuery(sql, params,txControl);

        auto p = NThreading::NewPromise<NYdb::NTable::TDataQueryResult>();

        NActors::TActivationContext::AsActorContext().Send(QuerySessionId, new TEvQueryActor::TEvExecuteDataQuery(sql, params, txControl, execDataQuerySettings, p));
        return p.GetFuture();
    }

    void Finish(bool needRollback) override {
        // Cerr << "TLocalSession::Finish" << Endl;
        // QueryActor->Finish222(needRollback);
        NActors::TActivationContext::AsActorContext().Send(QuerySessionId, new TEvQueryActor::TEvFinish(needRollback));
    }

    ~TLocalSession() {
        // if (QueryActor) {
        //     Cerr << "TLocalSession::call finish" << Endl;
        //     Finish(false);
        // }
                Cerr << "~TLocalSession" << Endl;

        NActors::TActivationContext::AsActorContext().Send(QuerySessionId, new NActors::TEvents::TEvPoison());
    }

private: 
  //  TQuerySession* QuerySession = nullptr;
    NActors::TActorId QuerySessionId;
};

} // namespace NFq
