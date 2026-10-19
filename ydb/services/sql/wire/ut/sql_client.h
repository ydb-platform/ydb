#pragma once

#include <ydb/core/testlib/test_client.h>
#include <ydb/services/sql/session/events.h>

namespace NKikimr::NSql {

// Runs one statement on the session actor. The PostgreSQL socket is not opened.
class TSqlClient {
public:
    TSqlClient(Tests::TServer& server, TString database, ui32 internalErrorRetries = 3)
        : Runtime(*server.GetRuntime())
        , Edge(Runtime.AllocateEdgeActor())
        , Session(Runtime.Register(CreateSqlSession(std::move(database), internalErrorRetries, RecursionLimit(server))))
    {
    }

    struct TResult {
        bool Ok = false;
        TString SqlState;
        TString Message;
        TString CommandTag;
        char TxStatus = 'I';
        TVector<TQueryColumn> Columns;
        TVector<TVector<TQueryCell>> Rows;
        TString GucName;
        TString GucValue;
        TString AccessPath;
    };

    TResult Exec(const TString& sql) {
        auto request = MakeHolder<TEvExecute>();
        request->Sql = sql;
        Runtime.Send(new NActors::IEventHandle(Session, Edge, request.Release()), 0, true);
        const auto ev = Runtime.GrabEdgeEventRethrow<TEvExecuteResult>(Edge);
        const TEvExecuteResult& result = *ev->Get();
        return {result.Ok, result.SqlState, result.Message, result.CommandTag, result.TxStatus, result.Columns, result.Rows, result.GucName, result.GucValue, result.AccessPath};
    }

private:
    static ui32 RecursionLimit(Tests::TServer& server) {
        const auto& config = server.GetSettings().AppConfig;
        if (!config || config->GetSqlConfig().GetRecursionLimit() == 0) {
            return 100;
        }
        return config->GetSqlConfig().GetRecursionLimit();
    }

    TTestActorRuntime& Runtime;
    NActors::TActorId Edge;
    NActors::TActorId Session;
};

} // namespace NKikimr::NSql
