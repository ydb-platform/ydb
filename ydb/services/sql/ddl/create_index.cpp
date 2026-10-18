#include "events.h"

#include <ydb/core/base/path.h>
#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NSql {

namespace {

class TCreateIndexActor : public NActors::TActorBootstrapped<TCreateIndexActor> {
public:
    TCreateIndexActor(const NActors::TActorId& replyTo, TString database, TCreateIndexStmt statement)
        : ReplyTo(replyTo)
        , Database(std::move(database))
        , Statement(std::move(statement))
    {
    }

    void Bootstrap() {
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId());
        Become(&TCreateIndexActor::StateAllocate);
    }

private:
    TString TablePath() const {
        TString path = Database;
        if (path && path.back() == '/') {
            path.pop_back();
        }
        if (Statement.Schema) {
            path += "/" + Statement.Schema->Name;
        }
        path += "/" + Statement.Table.Name;
        return path;
    }

    void Reply(bool ok, TString sqlState, TString message) {
        auto result = MakeHolder<TEvDdlResult>();
        result->Ok = ok;
        result->SqlState = std::move(sqlState);
        result->Message = std::move(message);
        result->CommandTag = "CREATE INDEX";
        Send(ReplyTo, result.Release());
        if (Pipe) {
            NTabletPipe::CloseClient(SelfId(), Pipe);
            Pipe = {};
        }
        PassAway();
    }

    void HandleAllocate(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        TxId = ev->Get()->TxId;
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(TablePath());
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        request->ResultSet.emplace_back(std::move(entry));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TCreateIndexActor::StateNavigate);
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* navigate = ev->Get()->Request.Get();
        if (!navigate || navigate->ResultSet.size() != 1
            || navigate->ResultSet[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok
            || !navigate->ResultSet[0].DomainInfo)
        {
            Reply(false, "42P01", "table does not exist");
            return;
        }
        Pipe = Register(NTabletPipe::CreateClient(
            SelfId(), navigate->ResultSet[0].DomainInfo->ExtractSchemeShard()));
        Ydb::Table::TableIndex index;
        index.set_name(Statement.Name.Name);
        for (const TIdentifier& column : Statement.Keys) {
            index.add_index_columns(column.Name);
        }
        for (const TIdentifier& column : Statement.Include) {
            index.add_data_columns(column.Name);
        }
        if (Statement.Unique) {
            index.mutable_global_unique_index();
        } else {
            index.mutable_global_index();
        }
        NKikimrIndexBuilder::TIndexBuildSettings settings;
        settings.set_source_path(TablePath());
        *settings.mutable_index() = std::move(index);
        auto request = MakeHolder<NSchemeShard::TEvIndexBuilder::TEvCreateRequest>(
            TxId, Database, std::move(settings));
        NTabletPipe::SendData(SelfId(), Pipe, request.Release());
        Become(&TCreateIndexActor::StateBuild);
    }

    void HandleCreate(NSchemeShard::TEvIndexBuilder::TEvCreateResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            TString message = record.IssuesSize() ? record.GetIssues(0).message() : TString("scheme request failed");
            const char* sqlState = record.GetStatus() == Ydb::StatusIds::ALREADY_EXISTS ? "42P07" : "42P01";
            Reply(false, sqlState, message);
            return;
        }
        auto notify = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(TxId);
        NTabletPipe::SendData(SelfId(), Pipe, notify.Release());
    }

    void HandleNotify(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr&) {
        auto request = MakeHolder<NSchemeShard::TEvIndexBuilder::TEvGetRequest>(Database, TxId);
        NTabletPipe::SendData(SelfId(), Pipe, request.Release());
    }

    void HandleGet(NSchemeShard::TEvIndexBuilder::TEvGetResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetStatus() == Ydb::StatusIds::SUCCESS
            && record.GetIndexBuild().GetState() == Ydb::Table::IndexBuildState::STATE_DONE)
        {
            Reply(true, {}, {});
            return;
        }
        TString message;
        if (record.GetIndexBuild().IssuesSize()) {
            message = record.GetIndexBuild().GetIssues(0).message();
        } else if (record.IssuesSize()) {
            message = record.GetIssues(0).message();
        } else {
            message = "scheme request failed";
        }
        Reply(false, "42P01", message);
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

    STATEFN(StateAllocate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, HandleAllocate);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateNavigate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateBuild) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSchemeShard::TEvIndexBuilder::TEvCreateResponse, HandleCreate);
            hFunc(NSchemeShard::TEvIndexBuilder::TEvGetResponse, HandleGet);
            hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, HandleNotify);
            hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered, IgnoreRegistered);
            hFunc(TEvTabletPipe::TEvClientConnected, HandleConnected);
            hFunc(TEvTabletPipe::TEvClientDestroyed, HandleDestroyed);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    const NActors::TActorId ReplyTo;
    const TString Database;
    const TCreateIndexStmt Statement;
    ui64 TxId = 0;
    NActors::TActorId Pipe;
};

} // namespace

NActors::IActor* CreateIndexActor(const NActors::TActorId& replyTo, TString database, TCreateIndexStmt statement) {
    return new TCreateIndexActor(replyTo, std::move(database), std::move(statement));
}


} // namespace NKikimr::NSql
