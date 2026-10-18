#include "events.h"
#include "types.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/scheme/scheme_type_info.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/storage_pool_kinds.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NSql {

namespace {

TString WorkingDir(const TString& database, const TCreateTableStmt& statement) {
    TString path = database;
    if (path && path.back() == '/') {
        path.pop_back();
    }
    if (statement.Schema) {
        path += "/" + statement.Schema->Name;
    }
    return path;
}

class TCreateTableActor : public NActors::TActorBootstrapped<TCreateTableActor> {
public:
    TCreateTableActor(const NActors::TActorId& replyTo, TString database, TCreateTableStmt statement)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
    {
    }

    void Bootstrap() {
        TString error;
        if (!BuildTable(error)) {
            Reply(false, "0A000", error ? error : TString("not supported"));
            return;
        }
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(Database);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        request->ResultSet.emplace_back(std::move(entry));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TCreateTableActor::StatePool);
    }

private:
    bool BuildTable(TString& error) {
        Table.SetName(Statement.Name.Name);
        for (const TIdentifier& key : Statement.PrimaryKey) {
            Table.AddKeyColumnNames(key.Name);
        }
        for (const TColumnDef& column : Statement.Columns) {
            const TResolvedType resolved = ResolveColumnType(column.Type);
            if (!resolved.Ok()) {
                error = resolved.Message;
                return false;
            }
            auto* out = Table.AddColumns();
            out->SetName(column.Name.Name);
            out->SetType(resolved.Name);
            bool notNull = column.NotNull;
            for (const TIdentifier& key : Statement.PrimaryKey) {
                if (key.Name == column.Name.Name) {
                    notNull = true;
                }
            }
            if (notNull) {
                out->SetNotNull(true);
            }
            if (column.Default) {
                TString message;
                if (!FillDefaultLiteral(*out->MutableDefaultFromLiteral(), resolved.Name, *column.Default, message)) {
                    error = message;
                    return false;
                }
            }
        }
        for (const TVector<TIdentifier>& columns : Statement.Unique) {
            TString indexName = Statement.Name.Name;
            auto* index = Table.AddTableIndexes();
            for (const TIdentifier& column : columns) {
                bool known = false;
                for (const TColumnDef& defined : Statement.Columns) {
                    if (defined.Name.Name == column.Name) {
                        known = true;
                    }
                }
                if (!known) {
                    error = column.Name;
                    return false;
                }
                indexName += "_" + column.Name;
                index->AddKeyColumnNames(column.Name);
            }
            index->SetName(indexName + "_key");
            index->SetType(NKikimrSchemeOp::EIndexTypeGlobalUnique);
            index->SetState(NKikimrSchemeOp::EIndexStateReady);
        }
        return true;
    }

    void ApplyPool(const TString& pool) {
        if (pool.empty()) {
            return;
        }
        auto* family = Table.MutablePartitionConfig()->AddColumnFamilies();
        family->SetId(0);
        family->MutableStorageConfig()->MutableData()->SetPreferredPoolKind(pool);
    }

    TString PoolFromDomain() const {
        if (!AppData() || !AppData()->DomainsInfo) {
            return {};
        }
        const auto& kinds = AppData()->DomainsInfo->GetDomain()->StoragePoolTypes;
        if (kinds.empty()) {
            return {};
        }
        return kinds.begin()->first;
    }

    void Reply(bool ok, TString sqlState, TString message) {
        auto result = MakeHolder<TEvDdlResult>();
        result->Ok = ok;
        result->SqlState = std::move(sqlState);
        result->Message = std::move(message);
        result->CommandTag = "CREATE TABLE";
        Send(ReplyTo, result.Release());
        if (Pipe) {
            NTabletPipe::CloseClient(SelfId(), Pipe);
            Pipe = {};
        }
        PassAway();
    }

    void Propose() {
        auto event = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        event->Record.SetDatabaseName(Database);
        auto* modify = event->Record.MutableTransaction()->MutableModifyScheme();
        modify->SetWorkingDir(WorkingDir(Database, Statement));
        modify->SetFailOnExist(!Statement.IfNotExists);
        if (Table.TableIndexesSize() == 0) {
            modify->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
            *modify->MutableCreateTable() = Table;
        } else {
            modify->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexedTable);
            auto* indexed = modify->MutableCreateIndexedTable();
            *indexed->MutableTableDescription() = Table;
            indexed->MutableTableDescription()->ClearTableIndexes();
            for (const auto& index : Table.GetTableIndexes()) {
                auto* created = indexed->AddIndexDescription();
                created->SetName(index.GetName());
                created->SetType(index.GetType());
                created->SetState(index.GetState());
                for (const auto& column : index.GetKeyColumnNames()) {
                    created->AddKeyColumnNames(column);
                }
            }
        }
        Send(MakeTxProxyID(), event.Release());
        Become(&TCreateTableActor::StatePropose);
    }

    void HandlePool(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        TString pool = PoolFromDomain();
        const auto* request = ev->Get()->Request.Get();
        if (request && request->ResultSet.size() == 1 && request->ResultSet[0].DomainDescription) {
            const auto& description = request->ResultSet[0].DomainDescription->Description;
            if (description.StoragePoolsSize() > 0) {
                pool = description.GetStoragePools(0).GetKind();
            }
        }
        ApplyPool(pool);
        Propose();
    }

    bool SchemeSucceeded(const TEvTxUserProxy::TEvProposeTransactionStatus& status) const {
        const auto scheme = static_cast<NKikimrScheme::EStatus>(status.Record.GetSchemeShardStatus());
        if (scheme == NKikimrScheme::StatusSuccess) {
            return true;
        }
        return Statement.IfNotExists && scheme == NKikimrScheme::StatusAlreadyExists;
    }

    void HandleStatus(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        const auto& status = *ev->Get();
        const auto code = status.Status();
        if (code == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress) {
            const ui64 tabletId = status.Record.GetSchemeShardTabletId();
            Pipe = Register(NTabletPipe::CreateClient(SelfId(), tabletId));
            auto notify = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(status.Record.GetTxId());
            NTabletPipe::SendData(SelfId(), Pipe, notify.Release());
            return;
        }
        if (code == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete && SchemeSucceeded(status)) {
            Reply(true, {}, {});
            return;
        }
        TString message = status.Record.GetSchemeShardReason();
        if (!message) {
            message = "scheme request failed";
        }
        const auto scheme = static_cast<NKikimrScheme::EStatus>(status.Record.GetSchemeShardStatus());
        const char* sqlState = scheme == NKikimrScheme::StatusAlreadyExists ? "42P07" : "42P01";
        Reply(false, sqlState, message);
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

    STATEFN(StatePool) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandlePool);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
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

    void IgnoreRegistered(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr&) {
    }

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TCreateTableStmt Statement;
    NKikimrSchemeOp::TTableDescription Table;
    NActors::TActorId Pipe;
};

class TAlterTableActor : public NActors::TActorBootstrapped<TAlterTableActor> {
public:
    TAlterTableActor(const NActors::TActorId& replyTo, TString database, TAlterTableStmt statement)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
    {
    }

    void Bootstrap() {
        if (Statement.Kind == EAlterTableKind::SetDefault) {
            auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            TString path = Database;
            if (path && path.back() == '/') {
                path.pop_back();
            }
            if (Statement.Schema) {
                path += "/" + Statement.Schema->Name;
            }
            path += "/" + Statement.Name.Name;
            entry.Path = SplitPath(path);
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
            request->ResultSet.emplace_back(std::move(entry));
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
            Become(&TAlterTableActor::StateColumn);
            return;
        }
        TString error;
        if (!Build(error)) {
            Reply(false, "0A000", error ? error : TString("not supported"));
            return;
        }
        Propose();
    }

    void Propose() {
        auto event = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        event->Record.SetDatabaseName(Database);
        auto* modify = event->Record.MutableTransaction()->MutableModifyScheme();
        modify->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterTable);
        TString dir = Database;
        if (dir && dir.back() == '/') {
            dir.pop_back();
        }
        if (Statement.Schema) {
            dir += "/" + Statement.Schema->Name;
        }
        modify->SetWorkingDir(dir);
        *modify->MutableAlterTable() = Alter;
        if (Statement.Kind == EAlterTableKind::SetNotNull) {
            // SchemeShard rejects NotNull=true unless the request is internal.
            modify->SetInternal(true);
        }
        Send(MakeTxProxyID(), event.Release());
        Become(&TAlterTableActor::StatePropose);
    }

private:
    bool Build(TString& error) {
        Alter.SetName(Statement.Name.Name);
        if (Statement.Kind == EAlterTableKind::DropColumn) {
            Alter.AddDropColumns()->SetName(Statement.Column.Name.Name);
            return true;
        }
        if (Statement.Kind == EAlterTableKind::SetNotNull || Statement.Kind == EAlterTableKind::DropNotNull) {
            auto* column = Alter.AddColumns();
            column->SetName(Statement.Column.Name.Name);
            column->SetNotNull(Statement.Kind == EAlterTableKind::SetNotNull);
            return true;
        }
        if (Statement.Kind == EAlterTableKind::DropDefault) {
            auto* column = Alter.AddColumns();
            column->SetName(Statement.Column.Name.Name);
            column->SetEmptyDefault(google::protobuf::NULL_VALUE);
            return true;
        }
        if (Statement.Kind == EAlterTableKind::SetDefault) {
            auto* column = Alter.AddColumns();
            column->SetName(Statement.Column.Name.Name);
            if (!Statement.Column.Default || DefaultType.empty()) {
                error = "default";
                return false;
            }
            return FillDefaultLiteral(*column->MutableDefaultFromLiteral(), DefaultType, *Statement.Column.Default, error);
        }
        const TResolvedType resolved = ResolveColumnType(Statement.Column.Type);
        if (!resolved.Ok()) {
            error = resolved.Message;
            return false;
        }
        auto* column = Alter.AddColumns();
        column->SetName(Statement.Column.Name.Name);
        column->SetType(resolved.Name);
        if (Statement.Column.NotNull) {
            column->SetNotNull(true);
        }
        if (Statement.Column.Default) {
            TString message;
            if (!FillDefaultLiteral(*column->MutableDefaultFromLiteral(), resolved.Name, *Statement.Column.Default, message)) {
                error = message;
                return false;
            }
        }
        return true;
    }

    void Reply(bool ok, TString sqlState, TString message) {
        auto result = MakeHolder<TEvDdlResult>();
        result->Ok = ok;
        result->SqlState = std::move(sqlState);
        result->Message = std::move(message);
        result->CommandTag = "ALTER TABLE";
        Send(ReplyTo, result.Release());
        if (Pipe) {
            NTabletPipe::CloseClient(SelfId(), Pipe);
            Pipe = {};
        }
        PassAway();
    }

    void HandleStatus(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        const auto& status = *ev->Get();
        const auto code = status.Status();
        if (code == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress) {
            Pipe = Register(NTabletPipe::CreateClient(SelfId(), status.Record.GetSchemeShardTabletId()));
            auto notify = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(status.Record.GetTxId());
            NTabletPipe::SendData(SelfId(), Pipe, notify.Release());
            return;
        }
        const auto scheme = static_cast<NKikimrScheme::EStatus>(status.Record.GetSchemeShardStatus());
        if (code == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete
            && scheme == NKikimrScheme::StatusSuccess)
        {
            Reply(true, {}, {});
            return;
        }
        TString message = status.Record.GetSchemeShardReason();
        if (!message) {
            message = "scheme request failed";
        }
        if (Statement.Kind == EAlterTableKind::DropColumn && Statement.IfExists
            && message.Contains("unknown column"))
        {
            Reply(true, {}, {});
            return;
        }
        Reply(false, scheme == NKikimrScheme::StatusAlreadyExists ? "42701" : "42P01", message);
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

    void HandleColumn(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        if (!request || request->ResultSet.size() != 1
            || request->ResultSet[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok)
        {
            Reply(false, "42P01", Statement.Name.Name);
            return;
        }
        for (const auto& [id, column] : request->ResultSet[0].Columns) {
            Y_UNUSED(id);
            if (column.Name == Statement.Column.Name.Name) {
                DefaultType = NScheme::TypeName(column.PType, column.PTypeMod);
                break;
            }
        }
        TString error;
        if (!Build(error)) {
            Reply(false, "0A000", error ? error : TString("not supported"));
            return;
        }
        Propose();
    }

    STATEFN(StateColumn) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleColumn);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
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

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TAlterTableStmt Statement;
    NKikimrSchemeOp::TTableDescription Alter;
    TString DefaultType;
    NActors::TActorId Pipe;
};

} // namespace

NActors::IActor* CreateTableActor(const NActors::TActorId& replyTo, TString database, TCreateTableStmt statement) {
    return new TCreateTableActor(replyTo, std::move(database), std::move(statement));
}

NActors::IActor* CreateAlterTableActor(const NActors::TActorId& replyTo, TString database, TAlterTableStmt statement) {
    return new TAlterTableActor(replyTo, std::move(database), std::move(statement));
}

class TCreateSequenceActor : public NActors::TActorBootstrapped<TCreateSequenceActor> {
public:
    TCreateSequenceActor(const NActors::TActorId& replyTo, TString database, TCreateSequenceStmt statement)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
    {
    }

    void Bootstrap() {
        auto event = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        event->Record.SetDatabaseName(Database);
        auto* modify = event->Record.MutableTransaction()->MutableModifyScheme();
        modify->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence);
        modify->SetFailOnExist(!Statement.IfNotExists);
        TString dir = Database;
        if (dir && dir.back() == '/') {
            dir.pop_back();
        }
        if (Statement.Schema) {
            dir += "/" + Statement.Schema->Name;
        }
        modify->SetWorkingDir(dir);
        auto* sequence = modify->MutableSequence();
        sequence->SetName(Statement.Name.Name);
        if (Statement.DataType) {
            sequence->SetDataType(Statement.DataType);
        }
        if (Statement.Increment) {
            sequence->SetIncrement(*Statement.Increment);
        }
        if (Statement.MinValue) {
            sequence->SetMinValue(*Statement.MinValue);
        }
        if (Statement.MaxValue) {
            sequence->SetMaxValue(*Statement.MaxValue);
        }
        if (Statement.StartValue) {
            sequence->SetStartValue(*Statement.StartValue);
        }
        if (Statement.Cache) {
            sequence->SetCache(*Statement.Cache);
        }
        sequence->SetCycle(Statement.Cycle);
        Send(MakeTxProxyID(), event.Release());
        Become(&TCreateSequenceActor::StatePropose);
    }

private:
    void Reply(bool ok, TString sqlState, TString message) {
        auto result = MakeHolder<TEvDdlResult>();
        result->Ok = ok;
        result->SqlState = std::move(sqlState);
        result->Message = std::move(message);
        result->CommandTag = "CREATE SEQUENCE";
        Send(ReplyTo, result.Release());
        if (Pipe) {
            NTabletPipe::CloseClient(SelfId(), Pipe);
            Pipe = {};
        }
        PassAway();
    }

    void HandleStatus(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        const auto& status = *ev->Get();
        const auto code = status.Status();
        if (code == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress) {
            Pipe = Register(NTabletPipe::CreateClient(SelfId(), status.Record.GetSchemeShardTabletId()));
            auto notify = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(status.Record.GetTxId());
            NTabletPipe::SendData(SelfId(), Pipe, notify.Release());
            return;
        }
        const auto scheme = static_cast<NKikimrScheme::EStatus>(status.Record.GetSchemeShardStatus());
        if (code == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete
            && (scheme == NKikimrScheme::StatusSuccess
                || (Statement.IfNotExists && scheme == NKikimrScheme::StatusAlreadyExists)))
        {
            Reply(true, {}, {});
            return;
        }
        TString message = status.Record.GetSchemeShardReason();
        if (!message) {
            message = "scheme request failed";
        }
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

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TCreateSequenceStmt Statement;
    NActors::TActorId Pipe;
};

NActors::IActor* CreateSequenceActor(const NActors::TActorId& replyTo, TString database, TCreateSequenceStmt statement) {
    return new TCreateSequenceActor(replyTo, std::move(database), std::move(statement));
}

class TAlterSequenceActor : public NActors::TActorBootstrapped<TAlterSequenceActor> {
public:
    TAlterSequenceActor(const NActors::TActorId& replyTo, TString database, TAlterSequenceStmt statement)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
    {
    }

    void Bootstrap() {
        auto event = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        event->Record.SetDatabaseName(Database);
        auto* modify = event->Record.MutableTransaction()->MutableModifyScheme();
        modify->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterSequence);
        TString dir = Database;
        if (dir && dir.back() == '/') {
            dir.pop_back();
        }
        if (Statement.Schema) {
            dir += "/" + Statement.Schema->Name;
        }
        modify->SetWorkingDir(dir);
        auto* sequence = modify->MutableSequence();
        sequence->SetName(Statement.Name.Name);
        if (Statement.DataType) {
            sequence->SetDataType(Statement.DataType);
        }
        if (Statement.Restart) {
            sequence->SetRestart(true);
            if (Statement.RestartWith) {
                sequence->MutableSetVal()->SetNextValue(*Statement.RestartWith);
                sequence->MutableSetVal()->SetNextUsed(false);
            }
        }
        Send(MakeTxProxyID(), event.Release());
        Become(&TAlterSequenceActor::StatePropose);
    }

private:
    void Reply(bool ok, TString sqlState, TString message) {
        auto result = MakeHolder<TEvDdlResult>();
        result->Ok = ok;
        result->SqlState = std::move(sqlState);
        result->Message = std::move(message);
        result->CommandTag = "ALTER SEQUENCE";
        Send(ReplyTo, result.Release());
        if (Pipe) {
            NTabletPipe::CloseClient(SelfId(), Pipe);
            Pipe = {};
        }
        PassAway();
    }

    void HandleStatus(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        const auto& status = *ev->Get();
        const auto code = status.Status();
        if (code == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress) {
            Pipe = Register(NTabletPipe::CreateClient(SelfId(), status.Record.GetSchemeShardTabletId()));
            auto notify = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(status.Record.GetTxId());
            NTabletPipe::SendData(SelfId(), Pipe, notify.Release());
            return;
        }
        const auto scheme = static_cast<NKikimrScheme::EStatus>(status.Record.GetSchemeShardStatus());
        if (code == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete
            && scheme == NKikimrScheme::StatusSuccess)
        {
            Reply(true, {}, {});
            return;
        }
        TString message = status.Record.GetSchemeShardReason();
        if (!message) {
            message = "scheme request failed";
        }
        Reply(false, "42P01", message);
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

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TAlterSequenceStmt Statement;
    NActors::TActorId Pipe;
};

NActors::IActor* CreateAlterSequenceActor(const NActors::TActorId& replyTo, TString database, TAlterSequenceStmt statement) {
    return new TAlterSequenceActor(replyTo, std::move(database), std::move(statement));
}

class TDropActor : public NActors::TActorBootstrapped<TDropActor> {
public:
    TDropActor(const NActors::TActorId& replyTo, TString database, TDropStmt statement)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
    {
    }

    void Bootstrap() {
        auto event = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        event->Record.SetDatabaseName(Database);
        auto* modify = event->Record.MutableTransaction()->MutableModifyScheme();
        modify->SetOperationType(Statement.Kind == EDropKind::Sequence
            ? NKikimrSchemeOp::EOperationType::ESchemeOpDropSequence
            : NKikimrSchemeOp::EOperationType::ESchemeOpDropTable);
        TString dir = Database;
        if (dir && dir.back() == '/') {
            dir.pop_back();
        }
        if (Statement.Schema) {
            dir += "/" + Statement.Schema->Name;
        }
        modify->SetWorkingDir(dir);
        modify->MutableDrop()->SetName(Statement.Name.Name);
        Send(MakeTxProxyID(), event.Release());
        Become(&TDropActor::StatePropose);
    }

private:
    void Reply(bool ok, TString sqlState, TString message) {
        auto result = MakeHolder<TEvDdlResult>();
        result->Ok = ok;
        result->SqlState = std::move(sqlState);
        result->Message = std::move(message);
        result->CommandTag = Statement.Kind == EDropKind::Sequence ? "DROP SEQUENCE" : "DROP TABLE";
        Send(ReplyTo, result.Release());
        if (Pipe) {
            NTabletPipe::CloseClient(SelfId(), Pipe);
            Pipe = {};
        }
        PassAway();
    }

    bool MissingIsSuccess(NKikimrScheme::EStatus scheme) const {
        return Statement.IfExists && scheme == NKikimrScheme::StatusPathDoesNotExist;
    }

    void HandleStatus(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        const auto& status = *ev->Get();
        const auto code = status.Status();
        const auto scheme = static_cast<NKikimrScheme::EStatus>(status.Record.GetSchemeShardStatus());
        if (code == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress) {
            Pipe = Register(NTabletPipe::CreateClient(SelfId(), status.Record.GetSchemeShardTabletId()));
            auto notify = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(status.Record.GetTxId());
            NTabletPipe::SendData(SelfId(), Pipe, notify.Release());
            return;
        }
        if (MissingIsSuccess(scheme)
            || (code == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete
                && scheme == NKikimrScheme::StatusSuccess))
        {
            Reply(true, {}, {});
            return;
        }
        TString message = status.Record.GetSchemeShardReason();
        if (!message) {
            message = "scheme request failed";
        }
        Reply(false, "42P01", message);
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

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TDropStmt Statement;
    NActors::TActorId Pipe;
};

NActors::IActor* CreateDropActor(const NActors::TActorId& replyTo, TString database, TDropStmt statement) {
    return new TDropActor(replyTo, std::move(database), std::move(statement));
}

class TCreateSchemaActor : public NActors::TActorBootstrapped<TCreateSchemaActor> {
public:
    TCreateSchemaActor(const NActors::TActorId& replyTo, TString database, TSchemaStmt statement)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
    {
    }

    void Bootstrap() {
        auto event = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        event->Record.SetDatabaseName(Database);
        auto* modify = event->Record.MutableTransaction()->MutableModifyScheme();
        modify->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMkDir);
        modify->SetFailOnExist(!Statement.IfNotExists);
        TString dir = Database;
        if (dir && dir.back() == '/') {
            dir.pop_back();
        }
        modify->SetWorkingDir(dir);
        modify->MutableMkDir()->SetName(Statement.Name.Name);
        Send(MakeTxProxyID(), event.Release());
        Become(&TCreateSchemaActor::StatePropose);
    }

private:
    void Reply(bool ok, TString sqlState, TString message) {
        auto result = MakeHolder<TEvDdlResult>();
        result->Ok = ok;
        result->SqlState = std::move(sqlState);
        result->Message = std::move(message);
        result->CommandTag = "CREATE SCHEMA";
        Send(ReplyTo, result.Release());
        if (Pipe) {
            NTabletPipe::CloseClient(SelfId(), Pipe);
            Pipe = {};
        }
        PassAway();
    }

    void HandleStatus(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        const auto& status = *ev->Get();
        const auto code = status.Status();
        const auto scheme = static_cast<NKikimrScheme::EStatus>(status.Record.GetSchemeShardStatus());
        if (code == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress) {
            Pipe = Register(NTabletPipe::CreateClient(SelfId(), status.Record.GetSchemeShardTabletId()));
            auto notify = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(status.Record.GetTxId());
            NTabletPipe::SendData(SelfId(), Pipe, notify.Release());
            return;
        }
        if (code == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete
            && (scheme == NKikimrScheme::StatusSuccess
                || (Statement.IfNotExists && scheme == NKikimrScheme::StatusAlreadyExists)))
        {
            Reply(true, {}, {});
            return;
        }
        TString message = status.Record.GetSchemeShardReason();
        if (!message) {
            message = "scheme request failed";
        }
        Reply(false, "42P06", message);
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

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TSchemaStmt Statement;
    NActors::TActorId Pipe;
};

NActors::IActor* CreateSchemaActor(const NActors::TActorId& replyTo, TString database, TSchemaStmt statement) {
    return new TCreateSchemaActor(replyTo, std::move(database), std::move(statement));
}

} // namespace NKikimr::NSql
