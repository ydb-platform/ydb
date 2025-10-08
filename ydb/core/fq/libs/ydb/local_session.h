#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <library/cpp/threading/future/core/future.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/core/fq/libs/ydb/session.h>
#include <ydb/core/fq/libs/ydb/query_actor.h>
namespace NFq {

struct TLocalSession : public ISession { 

    TLocalSession() {
        QueryActor = MakeQueryActor().release();
        NActors::TActivationContext::AsActorContext().RegisterWithSameMailbox(QueryActor);
    }

    NThreading::TFuture<NYdb::NTable::TDataQueryResult> ExecuteDataQuery(
        const TString& sql,
        std::shared_ptr<NYdb::TParamsBuilder> params,
        NYdb::NTable::TTxControl txControl,
        NYdb::NTable::TExecDataQuerySettings /*execDataQuerySettings*/ = NYdb::NTable::TExecDataQuerySettings()) override {
        return QueryActor->ExecuteDataQuery(sql, params,txControl);
    }

private: 
    TQueryActor* QueryActor;
};

} // namespace NFq
