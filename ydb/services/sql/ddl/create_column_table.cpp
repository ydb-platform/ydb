#include "events.h"
#include "types.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NSql {

namespace {

TString WorkingDir(const TString& database, const std::optional<TIdentifier>& schema) {
    TString path = database;
    if (path && path.back() == '/') {
        path.pop_back();
    }
    if (schema) {
        path += "/" + schema->Name;
    }
    return path;
}

template <class TDerived>
class TColumnDdlActor : public NActors::TActorBootstrapped<TDerived> {
public:
    TColumnDdlActor(const NActors::TActorId& replyTo, TString database, TString tag)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Tag(std::move(tag))
    {
    }

protected:
    void Reply(bool ok, TString sqlState, TString message) {
        auto result = MakeHolder<TEvDdlResult>();
        result->Ok = ok;
        result->SqlState = std::move(sqlState);
        result->Message = std::move(message);
        result->CommandTag = Tag;
        this->Send(ReplyTo, result.Release());
        if (Pipe) {
            NTabletPipe::CloseClient(this->SelfId(), Pipe);
            Pipe = {};
        }
        this->PassAway();
    }

    void SendPropose(NKikimrSchemeOp::TModifyScheme& modify) {
        auto event = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        event->Record.SetDatabaseName(Database);
        *event->Record.MutableTransaction()->MutableModifyScheme() = modify;
        this->Send(MakeTxProxyID(), event.Release());
        this->Become(&TDerived::StatePropose);
    }

    bool SchemeSucceeded(const TEvTxUserProxy::TEvProposeTransactionStatus& status, bool ifNotExists) const {
        const auto scheme = static_cast<NKikimrScheme::EStatus>(status.Record.GetSchemeShardStatus());
        if (scheme == NKikimrScheme::StatusSuccess) {
            return true;
        }
        return ifNotExists && scheme == NKikimrScheme::StatusAlreadyExists;
    }

    void FinishStatus(const TEvTxUserProxy::TEvProposeTransactionStatus& status, bool ifNotExists) {
        const auto code = status.Status();
        if (code == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress) {
            Pipe = this->Register(NTabletPipe::CreateClient(this->SelfId(), status.Record.GetSchemeShardTabletId()));
            auto notify = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(status.Record.GetTxId());
            NTabletPipe::SendData(this->SelfId(), Pipe, notify.Release());
            return;
        }
        if (code == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete && SchemeSucceeded(status, ifNotExists)) {
            Reply(true, {}, {});
            return;
        }
        TString message = status.Record.GetSchemeShardReason();
        if (!message) {
            message = "scheme request failed";
        }
        const auto scheme = static_cast<NKikimrScheme::EStatus>(status.Record.GetSchemeShardStatus());
        Reply(false, scheme == NKikimrScheme::StatusAlreadyExists ? "42P07" : "42P01", message);
    }

    void HandleNotify(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr&) {
        Reply(true, {}, {});
    }

    void HandleConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            Reply(false, "08006", "scheme pipe failed");
        }
    }

    void HandleDestroyed(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        Reply(false, "08006", "scheme pipe closed");
    }

    void IgnoreRegistered(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr&) {
    }

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TString Tag;
    NActors::TActorId Pipe;
};

class TCreateColumnTableActor : public TColumnDdlActor<TCreateColumnTableActor> {
    friend class TColumnDdlActor<TCreateColumnTableActor>;
public:
    TCreateColumnTableActor(const NActors::TActorId& replyTo, TString database, TCreateTableStmt statement)
        : TColumnDdlActor(replyTo, std::move(database), "CREATE TABLE")
        , Statement(std::move(statement))
    {
    }

    void Bootstrap() {
        if (!Statement.Unique.empty()) {
            Reply(false, "0A000", "not supported");
            return;
        }
        NKikimrSchemeOp::TModifyScheme modify;
        modify.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable);
        modify.SetWorkingDir(WorkingDir(Database, Statement.Schema));
        modify.SetFailOnExist(!Statement.IfNotExists);
        auto* table = modify.MutableCreateColumnTable();
        table->SetName(Statement.Name.Name);
        table->SetColumnShardCount(1);
        auto* schema = table->MutableSchema();
        auto* hash = table->MutableSharding()->MutableHashSharding();
        hash->SetFunction(NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_MODULO_N);
        ui32 nextId = 1;
        for (const TColumnDef& column : Statement.Columns) {
            if (column.Default) {
                Reply(false, "0A000", "not supported");
                return;
            }
            const TResolvedType resolved = ResolveColumnType(column.Type);
            if (!resolved.Ok()) {
                Reply(false, "0A000", resolved.Message ? resolved.Message : TString("not supported"));
                return;
            }
            bool key = false;
            for (const TIdentifier& name : Statement.PrimaryKey) {
                key = key || name.Name == column.Name.Name;
            }
            if (key && (resolved.Name == "Float" || resolved.Name == "Double")) {
                Reply(false, "0A000", "not supported");
                return;
            }
            auto* out = schema->AddColumns();
            out->SetId(nextId++);
            out->SetName(column.Name.Name);
            out->SetType(resolved.Name);
            if (column.NotNull || key) {
                out->SetNotNull(true);
            }
        }
        for (const TIdentifier& key : Statement.PrimaryKey) {
            schema->AddKeyColumnNames(key.Name);
            hash->AddColumns(key.Name);
        }
        schema->SetNextColumnId(nextId);
        if (schema->KeyColumnNamesSize() == 0) {
            Reply(false, "0A000", "not supported");
            return;
        }
        IfNotExists = Statement.IfNotExists;
        SendPropose(modify);
    }

private:
    void HandleStatus(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        FinishStatus(*ev->Get(), IfNotExists);
    }

    STATEFN(StatePropose) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, HandleStatus);
            hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, HandleNotify);
            hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered, IgnoreRegistered);
            hFunc(TEvTabletPipe::TEvClientConnected, HandleConnected);
            hFunc(TEvTabletPipe::TEvClientDestroyed, HandleDestroyed);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    const TCreateTableStmt Statement;
    bool IfNotExists = false;
};

class TAlterColumnTableActor : public TColumnDdlActor<TAlterColumnTableActor> {
    friend class TColumnDdlActor<TAlterColumnTableActor>;
public:
    TAlterColumnTableActor(const NActors::TActorId& replyTo, TString database, TAlterTableStmt statement)
        : TColumnDdlActor(replyTo, std::move(database), "ALTER TABLE")
        , Statement(std::move(statement))
    {
    }

    void Bootstrap() {
        NKikimrSchemeOp::TModifyScheme modify;
        modify.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterColumnTable);
        modify.SetWorkingDir(WorkingDir(Database, Statement.Schema));
        auto* alter = modify.MutableAlterColumnTable();
        alter->SetName(Statement.Name.Name);
        auto* schema = alter->MutableAlterSchema();
        if (Statement.Kind == EAlterTableKind::DropColumn) {
            schema->AddDropColumns()->SetName(Statement.Column.Name.Name);
        } else if (Statement.Kind == EAlterTableKind::AddColumn) {
            if (Statement.Column.Default) {
                Reply(false, "0A000", "not supported");
                return;
            }
            const TResolvedType resolved = ResolveColumnType(Statement.Column.Type);
            if (!resolved.Ok()) {
                Reply(false, "0A000", resolved.Message ? resolved.Message : TString("not supported"));
                return;
            }
            auto* column = schema->AddAddColumns();
            column->SetName(Statement.Column.Name.Name);
            column->SetType(resolved.Name);
            if (Statement.Column.NotNull) {
                column->SetNotNull(true);
            }
        } else {
            Reply(false, "0A000", "not supported");
            return;
        }
        SendPropose(modify);
    }

private:
    void HandleStatus(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        const auto& status = *ev->Get();
        if (Statement.Kind == EAlterTableKind::DropColumn && Statement.IfExists) {
            const auto scheme = static_cast<NKikimrScheme::EStatus>(status.Record.GetSchemeShardStatus());
            if (status.Status() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete
                && scheme != NKikimrScheme::StatusSuccess
                && status.Record.GetSchemeShardReason().Contains("unknown column"))
            {
                Reply(true, {}, {});
                return;
            }
        }
        FinishStatus(status, false);
    }

    STATEFN(StatePropose) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, HandleStatus);
            hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, HandleNotify);
            hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered, IgnoreRegistered);
            hFunc(TEvTabletPipe::TEvClientConnected, HandleConnected);
            hFunc(TEvTabletPipe::TEvClientDestroyed, HandleDestroyed);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    const TAlterTableStmt Statement;
};

} // namespace

NActors::IActor* CreateColumnTableActor(const NActors::TActorId& replyTo, TString database, TCreateTableStmt statement) {
    return new TCreateColumnTableActor(replyTo, std::move(database), std::move(statement));
}

NActors::IActor* CreateAlterColumnTableActor(const NActors::TActorId& replyTo, TString database, TAlterTableStmt statement) {
    return new TAlterColumnTableActor(replyTo, std::move(database), std::move(statement));
}

} // namespace NKikimr::NSql
