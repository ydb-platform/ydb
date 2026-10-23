#include "events.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NSql {

namespace {

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
        NKikimrSchemeOp::EOperationType operation = NKikimrSchemeOp::EOperationType::ESchemeOpDropTable;
        if (Statement.Kind == EDropKind::Sequence) {
            operation = NKikimrSchemeOp::EOperationType::ESchemeOpDropSequence;
        } else if (Statement.Kind == EDropKind::Schema) {
            operation = NKikimrSchemeOp::EOperationType::ESchemeOpRmDir;
        }
        modify->SetOperationType(operation);
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
        result->CommandTag = "DROP TABLE";
        if (Statement.Kind == EDropKind::Sequence) {
            result->CommandTag = "DROP SEQUENCE";
        } else if (Statement.Kind == EDropKind::Schema) {
            result->CommandTag = "DROP SCHEMA";
        }
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

} // namespace

NActors::IActor* CreateDropActor(const NActors::TActorId& replyTo, TString database, TDropStmt statement) {
    return new TDropActor(replyTo, std::move(database), std::move(statement));
}


} // namespace NKikimr::NSql
