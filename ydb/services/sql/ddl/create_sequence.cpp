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

} // namespace

NActors::IActor* CreateSequenceActor(const NActors::TActorId& replyTo, TString database, TCreateSequenceStmt statement) {
    return new TCreateSequenceActor(replyTo, std::move(database), std::move(statement));
}


} // namespace NKikimr::NSql
