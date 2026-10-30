#include "schemeshard_impl.h"
#include "index/index_build_info.h"

#include <ydb/core/base/tablet_pipe.h>

namespace NKikimr::NSchemeShard {

namespace {

using TStage = NKikimrTxDataShard::TDatabaseFreezeState;

// The persistent fence and participant set live in SchemeShard. The worker is
// disposable: after a reboot a retry starts it at FENCE_WRITES and DataShards
// acknowledge stages already completed without moving backwards.
class TDatabaseFreezeWorker : public TActorBootstrapped<TDatabaseFreezeWorker> {
    const TActorId Owner;
    const ui64 SchemeShardId;
    const TPathId PathId;
    const TPathId DomainKey;
    const ui64 Epoch;
    const TVector<ui64> Shards;
    TStage::EStage Stage;
    THashSet<ui64> Pending;
    THashMap<ui64, TActorId> Pipes;

    void SendPending() {
        for (const auto shard : Pending) {
            auto& pipe = Pipes[shard];
            if (!pipe) {
                NTabletPipe::TClientConfig config;
                config.RetryPolicy = {.RetryLimitCount = 3};
                pipe = Register(NTabletPipe::CreateClient(SelfId(), shard, config));
            }
            auto request = MakeHolder<TEvDataShard::TEvDatabaseFreeze>();
            request->Record.SetEpoch(Epoch);
            request->Record.SetSchemeShardId(SchemeShardId);
            request->Record.SetDomainOwnerId(DomainKey.OwnerId);
            request->Record.SetDomainLocalPathId(DomainKey.LocalPathId);
            request->Record.SetStage(Stage);
            NTabletPipe::SendData(SelfId(), pipe, request.Release());
        }
    }

    void Finish(ui32 status, const TString& issue = {}) {
        auto result = MakeHolder<TEvSchemeShard::TEvDatabaseFreezeResult>();
        result->Record.SetPathId(PathId.LocalPathId);
        result->Record.SetEpoch(Epoch);
        result->Record.SetReady(true);
        result->Record.SetStatus(status);
        result->Record.SetIssue(issue);
        Send(Owner, result.Release());
        PassAway();
    }

    void StartStage() {
        Pending.clear();
        Pending.insert(Shards.begin(), Shards.end());
        if (Pending.empty()) {
            return Finish(Ydb::StatusIds::SUCCESS);
        }
        SendPending();
    }

    void Handle(TEvDataShard::TEvDatabaseFreezeResult::TPtr& ev) {
        const auto& result = ev->Get()->Record;
        if (result.GetEpoch() != Epoch || result.GetStage() != Stage
            || !Pending.contains(result.GetTabletId()))
        {
            return;
        }
        if (result.GetStatus() != Ydb::StatusIds::SUCCESS) {
            return Finish(result.GetStatus(), result.GetIssue());
        }
        if (!result.GetReady()) {
            return;
        }
        Pending.erase(result.GetTabletId());
        if (Pending.empty()) {
            if (Stage == TStage::OPEN || Stage == TStage::DRAIN_CDC) {
                return Finish(Ydb::StatusIds::SUCCESS);
            }
            Stage = static_cast<TStage::EStage>(static_cast<int>(Stage) + 1);
            StartStage();
        }
    }

    void Reconnect(ui64 tabletId) {
        auto it = Pipes.find(tabletId);
        if (it != Pipes.end()) {
            NTabletPipe::CloseClient(SelfId(), it->second);
            Pipes.erase(it);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        auto it = Pipes.find(ev->Get()->TabletId);
        if (ev->Get()->Status != NKikimrProto::OK && it != Pipes.end() && it->second == ev->Get()->ClientId) {
            Reconnect(ev->Get()->TabletId);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        auto it = Pipes.find(ev->Get()->TabletId);
        if (it != Pipes.end() && it->second == ev->Get()->ClientId) {
            Reconnect(ev->Get()->TabletId);
        }
    }

    void Wakeup() {
        SendPending();
        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
    }

    void PassAway() override {
        for (const auto& [_, pipe] : Pipes) {
            NTabletPipe::CloseClient(SelfId(), pipe);
        }
        TActorBootstrapped::PassAway();
    }

public:
    TDatabaseFreezeWorker(TActorId owner, ui64 schemeShardId, TPathId pathId,
            TPathId domainKey, ui64 epoch, bool freeze, TVector<ui64> shards)
        : Owner(owner)
        , SchemeShardId(schemeShardId)
        , PathId(pathId)
        , DomainKey(domainKey)
        , Epoch(epoch)
        , Shards(std::move(shards))
        , Stage(freeze ? TStage::FENCE_WRITES : TStage::OPEN)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        if (Shards.empty()) {
            return Finish(Ydb::StatusIds::SUCCESS);
        }
        StartStage();
        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvDatabaseFreezeResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            cFunc(TEvents::TEvWakeup::EventType, Wakeup);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

} // anonymous namespace

bool TSchemeShard::IsDatabaseFreezeBlocking(const TPathId& pathId) const {
    if (!PathsById.contains(pathId)) {
        return false;
    }
    const auto domainId = ResolvePathIdForDomain(pathId);
    auto it = DatabaseFreezes.find(domainId);
    if (it == DatabaseFreezes.end()) {
        it = DatabaseFreezes.find(pathId);
    }
    return it != DatabaseFreezes.end() && (it->second.GetFreeze() || !it->second.GetReady()
        || it->second.GetStatus() != Ydb::StatusIds::SUCCESS);
}

bool TSchemeShard::HasDatabaseFreezeSchemaWork(const TPathId& domainId) const {
    for (const auto& [_, tx] : TxInFlight) {
        if ((PathsById.contains(tx.TargetPathId) && ResolvePathIdForDomain(tx.TargetPathId) == domainId)
            || (PathsById.contains(tx.SourcePathId) && ResolvePathIdForDomain(tx.SourcePathId) == domainId))
        {
            return true;
        }
    }
    for (const auto& [_, build] : IndexBuilds) {
        if (build->DomainPathId == domainId && !build->IsFinished()) {
            return true;
        }
    }
    for (const auto& [_, import] : Imports) {
        if (import->DomainPathId == domainId && !import->IsFinished()) {
            return true;
        }
    }
    for (const auto& [_, operation] : SetColumnConstraintOperations) {
        if (operation->DomainPathId == domainId && !operation->IsFinished()) {
            return true;
        }
    }
    for (const auto& [_, operation] : IncrementalBackups) {
        if (operation->DomainPathId == domainId && !operation->IsFinished()) {
            return true;
        }
    }
    for (const auto& [_, operation] : FullBackups) {
        if (operation->DomainPathId == domainId && !operation->IsFinished()) {
            return true;
        }
    }
    for (const auto& [_, operation] : IncrementalRestoreStates) {
        if (operation.State != TIncrementalRestoreState::EState::Completed
            && operation.State != TIncrementalRestoreState::EState::Failed
            && PathsById.contains(operation.BackupCollectionPathId)
            && ResolvePathIdForDomain(operation.BackupCollectionPathId) == domainId)
        {
            return true;
        }
    }
    return false;
}

struct TSchemeShard::TTxDatabaseFreeze : public TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvDatabaseFreeze::TPtr Request;
    THolder<TEvSchemeShard::TEvDatabaseFreezeResult> Response;
    TPathId PathId;
    bool RestartWorker = false;
    bool StartWorker = false;

    TTxDatabaseFreeze(TSelf* self, TEvSchemeShard::TEvDatabaseFreeze::TPtr ev)
        : TRwTxBase(self), Request(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_DATABASE_FREEZE; }

    void DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& request = Request->Get()->Record;
        PathId = TPathId(Self->TabletID(), request.GetPathId());
        Response = MakeHolder<TEvSchemeShard::TEvDatabaseFreezeResult>();
        auto& response = Response->Record;
        response.SetPathId(request.GetPathId());
        response.SetEpoch(request.GetEpoch());
        response.SetStatus(Ydb::StatusIds::SUCCESS);

        auto fail = [&](ui32 status, const TString& issue) {
            response.SetStatus(status);
            response.SetIssue(issue);
            response.SetReady(true);
        };
        if (!request.GetEpoch() || !Self->SubDomains.contains(PathId)
            || Self->PathsById.at(PathId)->Dropped())
        {
            return fail(Ydb::StatusIds::NOT_FOUND, "Database does not exist at this SchemeShard");
        }

        auto it = Self->DatabaseFreezes.find(PathId);
        if (it != Self->DatabaseFreezes.end() && request.GetEpoch() < it->second.GetEpoch()) {
            return fail(Ydb::StatusIds::ABORTED, "Stale database freeze epoch");
        }
        if (it != Self->DatabaseFreezes.end() && request.GetEpoch() == it->second.GetEpoch()
            && request.GetFreeze() != it->second.GetFreeze())
        {
            return fail(Ydb::StatusIds::BAD_REQUEST, "Conflicting database freeze request");
        }

        // Refuse unsupported participants before closing any write paths.
        for (const auto& tablePathId : Self->ColumnTables.GetAllPathIds()) {
            if (request.GetFreeze() && !Self->PathsById.at(tablePathId)->Dropped()
                && Self->ResolvePathIdForDomain(tablePathId) == PathId)
            {
                return fail(Ydb::StatusIds::UNSUPPORTED, "Database freeze supports row tables only");
            }
        }
        for (const auto& [_, shard] : Self->ShardInfos) {
            if (request.GetFreeze() && shard.TabletType == TTabletTypes::ColumnShard
                && Self->PathsById.contains(shard.PathId)
                && Self->ResolvePathIdForDomain(shard.PathId) == PathId)
            {
                return fail(Ydb::StatusIds::UNSUPPORTED, "Database freeze supports row tables only");
            }
        }

        NIceDb::TNiceDb db(txc.DB);
        auto& state = Self->DatabaseFreezes[PathId];
        bool changed = false;
        if (request.GetEpoch() > state.GetEpoch()) {
            state.Clear();
            state.SetEpoch(request.GetEpoch());
            state.SetFreeze(request.GetFreeze());
            RestartWorker = true;
            changed = true;
        }
        if (!state.GetParticipantsFixed() && !Self->HasDatabaseFreezeSchemaWork(PathId)) {
            THashSet<ui64> participants;
            // Retired split sources may still be present in ShardInfos. The
            // completed split protocol has drained their CDC and activated the
            // destinations before acknowledging partitioning changes.
            for (const auto& [tablePathId, table] : Self->Tables) {
                if (!Self->PathsById.at(tablePathId)->Dropped()
                    && Self->ResolvePathIdForDomain(tablePathId) == PathId)
                {
                    for (const auto* partition : table->GetPartitions()) {
                        participants.insert(ui64(Self->ShardInfos.at(partition->ShardIdx).TabletID));
                    }
                }
            }
            for (const auto tablet : participants) {
                state.AddShards(tablet);
            }
            state.SetParticipantsFixed(true);
            changed = true;
        }
        StartWorker = state.GetParticipantsFixed() && !state.GetReady()
            && (RestartWorker || !Self->DatabaseFreezeWorkers.contains(PathId));
        if (changed) {
            db.Table<Schema::DatabaseFreezes>().Key(PathId.LocalPathId)
                .Update<Schema::DatabaseFreezes::State>(state.SerializeAsString());
        }
        response.SetReady(state.GetReady());
        if (state.GetReady()) {
            response.SetStatus(state.GetStatus());
            response.SetIssue(state.GetIssue());
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        // Another request may execute before this transaction's Complete.
        // Never start a superseding epoch using this request's participant
        // readiness, or replace a worker already started by a duplicate poll.
        const auto state = Self->DatabaseFreezes.find(PathId);
        if (state != Self->DatabaseFreezes.end()
            && state->second.GetEpoch() == Request->Get()->Record.GetEpoch())
        {
            if (RestartWorker) {
                auto it = Self->DatabaseFreezeWorkers.find(PathId);
                if (it != Self->DatabaseFreezeWorkers.end()) {
                    ctx.Send(it->second, new TEvents::TEvPoisonPill);
                    Self->DatabaseFreezeWorkers.erase(it);
                }
            }
            if (StartWorker && state->second.GetParticipantsFixed() && !state->second.GetReady()
                && !Self->DatabaseFreezeWorkers.contains(PathId))
            {
                // DataShard stores the domain path in its owning SchemeShard,
                // including a tenant SchemeShard's local root path.
                const auto key = PathId;
                TVector<ui64> shards(state->second.GetShards().begin(), state->second.GetShards().end());
                Self->DatabaseFreezeWorkers[PathId] = ctx.RegisterWithSameMailbox(new TDatabaseFreezeWorker(
                    Self->SelfId(), Self->TabletID(), PathId, key, state->second.GetEpoch(),
                    state->second.GetFreeze(), std::move(shards)));
            }
        }
        ctx.Send(Request->Sender, Response.Release(), 0, Request->Cookie);
    }
};

struct TSchemeShard::TTxDatabaseFreezeDone : public TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvDatabaseFreezeResult::TPtr Event;
    TPathId PathId;
    NKikimrScheme::TDatabaseFreezeState Completed;

    TTxDatabaseFreezeDone(TSelf* self, TEvSchemeShard::TEvDatabaseFreezeResult::TPtr ev)
        : TRwTxBase(self), Event(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_DATABASE_FREEZE; }

    void DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& result = Event->Get()->Record;
        PathId = TPathId(Self->TabletID(), result.GetPathId());
        auto it = Self->DatabaseFreezes.find(PathId);
        auto worker = Self->DatabaseFreezeWorkers.find(PathId);
        if (it == Self->DatabaseFreezes.end() || worker == Self->DatabaseFreezeWorkers.end()
            || worker->second != Event->Sender || it->second.GetEpoch() != result.GetEpoch())
        {
            PathId = InvalidPathId;
            return;
        }
        Completed.CopyFrom(it->second);
        Completed.SetReady(true);
        Completed.SetStatus(result.GetStatus());
        Completed.SetIssue(result.GetIssue());
        NIceDb::TNiceDb(txc.DB).Table<Schema::DatabaseFreezes>().Key(PathId.LocalPathId)
            .Update<Schema::DatabaseFreezes::State>(Completed.SerializeAsString());
    }

    void DoComplete(const TActorContext&) override {
        if (PathId && Self->DatabaseFreezes.at(PathId).GetEpoch() == Completed.GetEpoch()) {
            Self->DatabaseFreezes.at(PathId).CopyFrom(Completed);
            Self->DatabaseFreezeWorkers.erase(PathId);
        }
    }
};

void TSchemeShard::Handle(TEvSchemeShard::TEvDatabaseFreeze::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDatabaseFreeze(this, ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvDatabaseFreezeResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDatabaseFreezeDone(this, ev), ctx);
}

} // namespace NKikimr::NSchemeShard
