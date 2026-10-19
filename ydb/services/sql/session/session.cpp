#include "events.h"

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/services/sql/ddl/events.h>
#include <ydb/services/sql/ischema/columns.h>
#include <ydb/services/sql/ischema/constraints.h>
#include <ydb/services/sql/ischema/sequences.h>
#include <ydb/services/sql/ischema/schemata.h>
#include <ydb/services/sql/ischema/tables.h>
#include <ydb/services/sql/parse/parser.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/containers/absl/flat_hash_map.h>

#include <util/generic/mapfindptr.h>
#include <util/string/cast.h>

namespace NKikimr::NSql {

namespace {

struct TRequest {
    NActors::TActorId Client;
    TString Sql;
    TVector<TBoundParam> Params;
};

bool FillParam(TLiteral& literal, const TVector<TBoundParam>& params, TString& sqlState, TString& message) {
    if (literal.Kind != TLiteral::EKind::Parameter) {
        return true;
    }
    ui32 index = 0;
    if (!TryFromString(literal.Text, index) || index == 0 || index > params.size()) {
        sqlState = "22023";
        message = "parameter $" + literal.Text;
        return false;
    }
    const TBoundParam& bound = params[index - 1];
    if (bound.Null) {
        literal.Kind = TLiteral::EKind::Null;
        literal.Text.clear();
    } else if (bound.Number) {
        literal.Kind = TLiteral::EKind::Integer;
        literal.Text = bound.Text;
    } else {
        literal.Kind = TLiteral::EKind::String;
        literal.Text = bound.Text;
    }
    return true;
}

bool ApplyParams(TParseResult& parsed, const TVector<TBoundParam>& params, TString& sqlState, TString& message) {
    auto where = [&](TVector<TComparison>& comparisons) {
        for (TComparison& comparison : comparisons) {
            if (!FillParam(comparison.Value, params, sqlState, message)) {
                return false;
            }
        }
        return true;
    };
    if (parsed.Select && !where(parsed.Select->Where)) {
        return false;
    }
    if (!parsed.Insert) {
        return true;
    }
    for (TLiteral& literal : parsed.Insert->Values) {
        if (!FillParam(literal, params, sqlState, message)) {
            return false;
        }
    }
    for (TUpdateAssign& assign : parsed.Insert->Update) {
        if (!assign.Excluded && !FillParam(assign.Value, params, sqlState, message)) {
            return false;
        }
    }
    if (parsed.Insert->ConflictWhere && !FillParam(parsed.Insert->ConflictWhere->Value, params, sqlState, message)) {
        return false;
    }
    if (parsed.Insert->Source && !where(parsed.Insert->Source->Where)) {
        return false;
    }
    return true;
}

class TSqlSession : public NActors::TActorBootstrapped<TSqlSession> {
public:
    TSqlSession(TString database, ui32 internalErrorRetries)
        : Database(std::move(database))
        , InternalErrorRetries(internalErrorRetries)
    {
        Remember("DateStyle", "ISO, MDY");
        Remember("TimeZone", "UTC");
        Remember("standard_conforming_strings", "on");
        Remember("client_encoding", "UTF8");
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

    char StatusChar() const {
        if (Tx == ETx::Failed) {
            return 'E';
        }
        if (Tx == ETx::Open) {
            return 'T';
        }
        return 'I';
    }

    void Fail(TString sqlState, TString message, bool abortTx = true) {
        if (abortTx && Tx == ETx::Open) {
            Tx = ETx::Failed;
        }
        auto result = MakeHolder<TEvExecuteResult>();
        result->SqlState = std::move(sqlState);
        result->Message = std::move(message);
        result->TxStatus = StatusChar();
        Reply(result.Release());
    }

    void Succeed(TString tag, TVector<TQueryColumn> columns, TVector<TVector<TQueryCell>> rows, TString gucName = {}, TString gucValue = {}) {
        auto result = MakeHolder<TEvExecuteResult>();
        result->Ok = true;
        result->CommandTag = std::move(tag);
        result->Columns = std::move(columns);
        result->Rows = std::move(rows);
        result->TxStatus = StatusChar();
        result->GucName = std::move(gucName);
        result->GucValue = std::move(gucValue);
        Reply(result.Release());
    }

    static TString Lower(TString text) {
        for (char& ch : text) {
            if (ch >= 'A' && ch <= 'Z') {
                ch = static_cast<char>(ch - 'A' + 'a');
            }
        }
        return text;
    }

    void Remember(const TString& name, const TString& value) {
        Gucs[Lower(name)] = {name, value};
    }

    void RunSet(const TSetStmt& set) {
        const TString key = Lower(set.Name);
        TGuc* guc = MapFindPtr(Gucs, key);
        if (key == "search_path" || !guc) {
            Fail("0A000", "not supported");
            return;
        }
        if (key == "client_encoding") {
            if (Lower(set.Value) != "utf8") {
                Fail("0A000", "not supported");
                return;
            }
            Succeed("SET", {}, {}, guc->Name, guc->Value);
            return;
        }
        guc->Value = set.Value;
        Succeed("SET", {}, {}, guc->Name, guc->Value);
    }

    void RunShow(const TShowStmt& show) {
        const TGuc* guc = MapFindPtr(Gucs, Lower(show.Name));
        if (!guc) {
            Fail("0A000", "not supported");
            return;
        }
        Succeed("SHOW", {{guc->Name, 25, -1}}, {{{false, guc->Value}}});
    }

    bool IsDdl(const TParseResult& parsed) const {
        return parsed.CreateTable || parsed.AlterTable || parsed.CreateSequence || parsed.CreateIndex
            || parsed.CreateSchema || parsed.Drop || parsed.AlterSequence;
    }

    void RunTx(const TTxStmt& tx) {
        if (tx.Kind == ETxKind::Begin) {
            if (Tx != ETx::Idle) {
                Fail("25001", "transaction already open", false);
                return;
            }
            Allocating = true;
            Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId());
            return;
        }
        if (tx.Kind == ETxKind::Rollback) {
            if (!Locks.empty()) {
                Rolling = true;
                Register(CreateRollbackActor(SelfId(), TxId, Locks));
                return;
            }
            Pending.clear();
            Locks.clear();
            TxId = 0;
            Tx = ETx::Idle;
            Succeed("ROLLBACK", {}, {});
            return;
        }
        if (Tx == ETx::Idle) {
            Succeed("COMMIT", {}, {});
            return;
        }
        if (Pending.empty() && Locks.empty()) {
            Tx = ETx::Idle;
            TxId = 0;
            Succeed("COMMIT", {}, {});
            return;
        }
        Committing = true;
        Register(CreateCommitActor(SelfId(), TxId, Pending, Locks, InternalErrorRetries));
    }

    void HandleAllocate(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        if (!Allocating) {
            return;
        }
        Allocating = false;
        const ui64 txId = ev->Get()->TxId;
        if (!txId) {
            Fail("XX000", "internal error");
            return;
        }
        TxId = txId;
        Tx = ETx::Open;
        Succeed("BEGIN", {}, {});
    }

    void Start(const TParseResult& parsed, const TString& sql) {
        Busy = true;
        if (Tx == ETx::Failed) {
            if (parsed.Tx && parsed.Tx->Kind == ETxKind::Rollback) {
                RunTx(*parsed.Tx);
                return;
            }
            Fail("25P02", "current transaction is aborted");
            return;
        }
        if (parsed.Tx) {
            RunTx(*parsed.Tx);
            return;
        }
        if (parsed.Set) {
            RunSet(*parsed.Set);
            return;
        }
        if (parsed.Show) {
            RunShow(*parsed.Show);
            return;
        }
        if (Tx == ETx::Open && (IsDdl(parsed) || (parsed.Insert && parsed.Insert->Source))) {
            Fail("0A000", "not supported");
            return;
        }
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
        if (parsed.Select && parsed.Cte) {
            Register(CreateWithActor(SelfId(), Database, sql, *parsed.Select, parsed.SelectRight, parsed.Combine, *parsed.Cte, parsed.CteName, InternalErrorRetries, Tx == ETx::Open ? TxId : 0));
            return;
        }
        if (parsed.Select && (parsed.FromSubquery || !parsed.Scalars.empty())) {
            Register(CreateSubqueryActor(SelfId(), Database, sql, *parsed.Select, parsed.Scalars, parsed.FromSubquery, InternalErrorRetries, Tx == ETx::Open ? TxId : 0));
            return;
        }
        if (parsed.Select && parsed.Combine != ECombine::None && parsed.SelectRight) {
            Register(CreateSetOpActor(SelfId(), Database, sql, *parsed.Select, *parsed.SelectRight, parsed.Combine, InternalErrorRetries, Tx == ETx::Open ? TxId : 0));
            return;
        }
        if (parsed.Select) {
            const bool catalog = parsed.Select->Schema && parsed.Select->Schema->Name == "information_schema";
            if ((catalog || parsed.Select->Join)
                && (!parsed.Select->Order.empty() || !parsed.Select->Aggs.empty() || !parsed.Select->Group.empty()
                    || !parsed.Select->Having.empty() || parsed.Select->Distinct))
            {
                Fail("0A000", "not supported");
                return;
            }
            if (parsed.Select->Schema && parsed.Select->Schema->Name == "information_schema" && parsed.Select->From.Name == "schemata") {
                Register(CreateSchemataActor(SelfId(), Database, *parsed.Select));
            } else if (parsed.Select->Schema && parsed.Select->Schema->Name == "information_schema" && parsed.Select->From.Name == "tables") {
                Register(CreateTablesActor(SelfId(), Database, *parsed.Select));
            } else if (parsed.Select->Schema && parsed.Select->Schema->Name == "information_schema" && parsed.Select->From.Name == "columns") {
                Register(CreateColumnsActor(SelfId(), Database, *parsed.Select));
            } else if (parsed.Select->Schema && parsed.Select->Schema->Name == "information_schema"
                && (parsed.Select->From.Name == "table_constraints" || parsed.Select->From.Name == "key_column_usage"))
            {
                Register(CreateConstraintsActor(SelfId(), Database, *parsed.Select));
            } else if (parsed.Select->Schema && parsed.Select->Schema->Name == "information_schema" && parsed.Select->From.Name == "sequences") {
                Register(CreateSequencesActor(SelfId(), Database, *parsed.Select));
            } else if (parsed.Select->Join) {
                Register(CreateJoinActor(SelfId(), Database, *parsed.Select, InternalErrorRetries, Tx == ETx::Open ? TxId : 0));
            } else {
                Register(CreateSelectActor(SelfId(), Database, sql, *parsed.Select, InternalErrorRetries, std::nullopt, Tx == ETx::Open ? TxId : 0));
            }
            return;
        }
        if (parsed.Delete) {
            Register(CreateDeleteActor(SelfId(), Database, *parsed.Delete, InternalErrorRetries, Tx == ETx::Open));
            return;
        }
        if (parsed.Update) {
            Register(CreateUpdateActor(SelfId(), Database, *parsed.Update, InternalErrorRetries, Tx == ETx::Open));
            return;
        }
        if (parsed.Insert) {
            if (parsed.Insert->Source) {
                Register(CreateInsertSelectActor(SelfId(), Database, sql, *parsed.Insert, InternalErrorRetries));
            } else {
                Register(CreateInsertActor(SelfId(), Database, *parsed.Insert, InternalErrorRetries, Tx == ETx::Open));
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
        TParseResult parsed;
        if (const TParseResult* cached = MapFindPtr(Parsed, request.Sql)) {
            parsed = *cached;
        } else {
            parsed = Parse(request.Sql);
            if (!parsed.SqlState) {
                Parsed[request.Sql] = parsed;
            }
        }
        if (!parsed.SqlState) {
            TString sqlState;
            TString message;
            if (!ApplyParams(parsed, request.Params, sqlState, message)) {
                Fail(sqlState, message);
                return;
            }
        }
        Start(parsed, request.Sql);
    }

    void HandleExecute(TEvExecute::TPtr& ev) {
        Queue.push_back({ev->Sender, std::move(ev->Get()->Sql), std::move(ev->Get()->Params)});
        Pump();
    }

    void HandleQuery(TEvQueryResult::TPtr& ev) {
        const TEvQueryResult& result = *ev->Get();
        if (Rolling) {
            Rolling = false;
            Pending.clear();
            Locks.clear();
            TxId = 0;
            Tx = ETx::Idle;
            Succeed("ROLLBACK", {}, {});
            return;
        }
        if (Committing) {
            Committing = false;
            Pending.clear();
            Locks.clear();
            TxId = 0;
            if (!result.Ok) {
                Tx = ETx::Failed;
                Fail(result.SqlState.empty() ? TString("XX000") : result.SqlState,
                    result.Message.empty() ? TString("internal error") : result.Message, false);
                return;
            }
            Tx = ETx::Idle;
            Succeed("COMMIT", {}, {});
            return;
        }
        if (!result.Ok) {
            Fail(result.SqlState.empty() ? TString("0A000") : result.SqlState,
                result.Message.empty() ? TString("not supported") : result.Message);
            return;
        }
        if (Tx == ETx::Open) {
            for (const TDeferredWrite& write : result.Deferred) {
                Pending.push_back(write);
            }
        }
        if (Tx == ETx::Open) {
            for (const auto& lock : result.Locks) {
                Locks.push_back(lock);
            }
        }
        const TString tag = result.CommandTag.empty() ? "SELECT " + ToString(result.Rows.size()) : result.CommandTag;
        auto reply = MakeHolder<TEvExecuteResult>();
        reply->Ok = true;
        reply->CommandTag = tag;
        reply->Columns = result.Columns;
        reply->Rows = result.Rows;
        reply->TxStatus = StatusChar();
        reply->AccessPath = result.AccessPath;
        Reply(reply.Release());
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
            hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, HandleAllocate);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    const TString Database;
    const ui32 InternalErrorRetries;
    enum class ETx {
        Idle,
        Open,
        Failed,
    };

    TVector<TRequest> Queue;
    TVector<TDeferredWrite> Pending;
    TVector<NKikimrDataEvents::TLock> Locks;
    struct TGuc {
        TString Name;
        TString Value;
    };

    absl::flat_hash_map<TString, TParseResult> Parsed;
    absl::flat_hash_map<TString, TGuc> Gucs;
    NActors::TActorId Client;
    ETx Tx = ETx::Idle;
    ui64 TxId = 0;
    bool Busy = false;
    bool Committing = false;
    bool Rolling = false;
    bool Allocating = false;
};

} // namespace

NActors::IActor* CreateSqlSession(TString database, ui32 internalErrorRetries) {
    return new TSqlSession(std::move(database), internalErrorRetries);
}

} // namespace NKikimr::NSql
