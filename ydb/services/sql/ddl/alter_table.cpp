#include "events.h"
#include "types.h"

#include <ydb/core/base/path.h>
#include <ydb/core/scheme/scheme_type_info.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NSql {

namespace {

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

NActors::IActor* CreateAlterTableActor(const NActors::TActorId& replyTo, TString database, TAlterTableStmt statement) {
    return new TAlterTableActor(replyTo, std::move(database), std::move(statement));
}


} // namespace NKikimr::NSql
