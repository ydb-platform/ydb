#include "events.h"

#include <ydb/services/sql/ddl/events.h>
#include <ydb/services/sql/ischema/columns.h>
#include <ydb/services/sql/ischema/schemata.h>
#include <ydb/services/sql/ischema/tables.h>
#include <ydb/services/sql/parse/parser.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/string/cast.h>

namespace NKikimr::NSql {

namespace {

struct TRequest {
    NActors::TActorId Client;
    TString Sql;
};

class TSqlSession : public NActors::TActorBootstrapped<TSqlSession> {
public:
    TSqlSession(TString database, ui32 internalErrorRetries)
        : Database(std::move(database))
        , InternalErrorRetries(internalErrorRetries)
    {
    }

    void Bootstrap() {
        Become(&TSqlSession::StateWork);
    }

private:
    void Reply(TEvExecuteResult* result) {
        Send(Client, result);
        Busy = false;
        Client = {};
        Pump();
    }

    void Fail(TString sqlState, TString message) {
        auto result = MakeHolder<TEvExecuteResult>();
        result->SqlState = std::move(sqlState);
        result->Message = std::move(message);
        Reply(result.Release());
    }

    void Succeed(TString tag, TVector<TQueryColumn> columns, TVector<TVector<TQueryCell>> rows) {
        auto result = MakeHolder<TEvExecuteResult>();
        result->Ok = true;
        result->CommandTag = std::move(tag);
        result->Columns = std::move(columns);
        result->Rows = std::move(rows);
        Reply(result.Release());
    }

    void Start(const TParseResult& parsed, const TString& sql) {
        Busy = true;
        if (parsed.CreateTable) {
            Register(CreateTableActor(SelfId(), Database, *parsed.CreateTable));
            return;
        }
        if (parsed.AlterTable) {
            Register(CreateAlterTableActor(SelfId(), Database, *parsed.AlterTable));
            return;
        }
        if (parsed.CreateSequence) {
            Register(CreateSequenceActor(SelfId(), Database, *parsed.CreateSequence));
            return;
        }
        if (parsed.CreateIndex) {
            Register(CreateIndexActor(SelfId(), Database, *parsed.CreateIndex));
            return;
        }
        if (parsed.CreateSchema) {
            Register(CreateSchemaActor(SelfId(), Database, *parsed.CreateSchema));
            return;
        }
        if (parsed.Drop) {
            if (parsed.Drop->Kind == EDropKind::Index) {
                Register(CreateDropIndexActor(SelfId(), Database, *parsed.Drop));
            } else {
                Register(CreateDropActor(SelfId(), Database, *parsed.Drop));
            }
            return;
        }
        if (parsed.AlterSequence) {
            Register(CreateAlterSequenceActor(SelfId(), Database, *parsed.AlterSequence));
            return;
        }
        if (parsed.Select) {
            if (parsed.Select->Schema && parsed.Select->Schema->Name == "information_schema" && parsed.Select->From.Name == "schemata") {
                Register(CreateSchemataActor(SelfId(), Database, *parsed.Select));
            } else if (parsed.Select->Schema && parsed.Select->Schema->Name == "information_schema" && parsed.Select->From.Name == "tables") {
                Register(CreateTablesActor(SelfId(), Database, *parsed.Select));
            } else if (parsed.Select->Schema && parsed.Select->Schema->Name == "information_schema" && parsed.Select->From.Name == "columns") {
                Register(CreateColumnsActor(SelfId(), Database, *parsed.Select));
            } else {
                Register(CreateSelectActor(SelfId(), Database, sql, *parsed.Select, InternalErrorRetries));
            }
            return;
        }
        Busy = false;
        Fail("0A000", "not supported");
    }

    void Pump() {
        if (Busy || Queue.empty()) {
            return;
        }
        const TRequest request = std::move(Queue.front());
        Queue.erase(Queue.begin());
        Client = request.Client;
        const TParseResult parsed = Parse(request.Sql);
        Start(parsed, request.Sql);
    }

    void HandleExecute(TEvExecute::TPtr& ev) {
        Queue.push_back({ev->Sender, std::move(ev->Get()->Sql)});
        Pump();
    }

    void HandleQuery(TEvQueryResult::TPtr& ev) {
        const TEvQueryResult& result = *ev->Get();
        if (!result.Ok) {
            Fail(result.SqlState.empty() ? TString("0A000") : result.SqlState,
                result.Message.empty() ? TString("not supported") : result.Message);
            return;
        }
        Succeed("SELECT " + ToString(result.Rows.size()), result.Columns, result.Rows);
    }

    void HandleDdl(TEvDdlResult::TPtr& ev) {
        const TEvDdlResult& result = *ev->Get();
        if (!result.Ok) {
            Fail(result.SqlState.empty() ? TString("0A000") : result.SqlState,
                result.Message.empty() ? TString("not supported") : result.Message);
            return;
        }
        Succeed(result.CommandTag, {}, {});
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExecute, HandleExecute);
            hFunc(TEvQueryResult, HandleQuery);
            hFunc(TEvDdlResult, HandleDdl);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    const TString Database;
    const ui32 InternalErrorRetries;
    TVector<TRequest> Queue;
    NActors::TActorId Client;
    bool Busy = false;
};

} // namespace

NActors::IActor* CreateSqlSession(TString database, ui32 internalErrorRetries) {
    return new TSqlSession(std::move(database), internalErrorRetries);
}

} // namespace NKikimr::NSql
