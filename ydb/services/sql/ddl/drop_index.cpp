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

class TDropIndexActor : public NActors::TActorBootstrapped<TDropIndexActor> {
public:
    TDropIndexActor(const NActors::TActorId& replyTo, TString database, TDropStmt statement)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
    {
    }

    void Bootstrap() {
        SendList(Directory());
        Become(&TDropIndexActor::StateList);
    }

private:
    TString Directory() const {
        TString path = Database;
        if (path && path.back() == '/') {
            path.pop_back();
        }
        if (Statement.Schema) {
            path += "/" + Statement.Schema->Name;
        }
        return path;
    }

    void SendList(const TString& path) {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(path);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
        request->ResultSet.emplace_back(std::move(entry));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void Reply(bool ok, TString sqlState, TString message) {
        auto result = MakeHolder<TEvDdlResult>();
        result->Ok = ok;
        result->SqlState = std::move(sqlState);
        result->Message = std::move(message);
        result->CommandTag = "DROP INDEX";
        Send(ReplyTo, result.Release());
        if (Pipe) {
            NTabletPipe::CloseClient(SelfId(), Pipe);
            Pipe = {};
        }
        PassAway();
    }

    void Missing() {
        if (Statement.IfExists) {
            Reply(true, {}, {});
            return;
        }
        Reply(false, "42P01", "index does not exist");
    }

    void HandleList(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* navigate = ev->Get()->Request.Get();
        if (!navigate || navigate->ResultSet.size() != 1
            || navigate->ResultSet[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok
            || !navigate->ResultSet[0].ListNodeEntry)
        {
            Missing();
            return;
        }
        const TString dir = Directory();
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        for (const auto& child : navigate->ResultSet[0].ListNodeEntry->Children) {
            if (child.Kind != NSchemeCache::TSchemeCacheNavigate::KindTable) {
                continue;
            }
            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Path = SplitPath(dir + "/" + child.Name);
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
            request->ResultSet.emplace_back(std::move(entry));
            Tables.push_back(child.Name);
        }
        if (Tables.empty()) {
            Missing();
            return;
        }
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TDropIndexActor::StateTables);
    }

    void HandleTables(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* navigate = ev->Get()->Request.Get();
        if (!navigate) {
            Missing();
            return;
        }
        for (size_t i = 0; i < navigate->ResultSet.size() && i < Tables.size(); ++i) {
            const auto& entry = navigate->ResultSet[i];
            if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                continue;
            }
            for (const auto& index : entry.Indexes) {
                if (index.GetName() == Statement.Name.Name) {
                    TableName = Tables[i];
                    Propose();
                    return;
                }
            }
        }
        Missing();
    }

    void Propose() {
        auto event = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        event->Record.SetDatabaseName(Database);
        auto* modify = event->Record.MutableTransaction()->MutableModifyScheme();
        modify->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropIndex);
        modify->SetWorkingDir(Directory());
        auto* drop = modify->MutableDropIndex();
        drop->SetTableName(TableName);
        drop->SetIndexName(Statement.Name.Name);
        Send(MakeTxProxyID(), event.Release());
        Become(&TDropIndexActor::StatePropose);
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

    STATEFN(StateList) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleList);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateTables) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleTables);
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
    const TDropStmt Statement;
    TVector<TString> Tables;
    TString TableName;
    NActors::TActorId Pipe;
};

} // namespace

NActors::IActor* CreateDropIndexActor(const NActors::TActorId& replyTo, TString database, TDropStmt statement) {
    return new TDropIndexActor(replyTo, std::move(database), std::move(statement));
}

} // namespace NKikimr::NSql
