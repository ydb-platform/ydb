#include "datashard_impl.h"
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NDataShard {

void TDataShard::OnLeaderUserAuxUpdate(TString update) {
    if (!IsFollower() || !AppData()->FeatureFlags.GetEnableHnswIndex()
            || !TStringBuf(update).StartsWith(HnswGraphAuxPrefix)) {
        return;
    }
    auto event = MakeHolder<TEvHnswCache::TEvGraphAnnouncement>();
    const auto payload = TStringBuf(update).SubStr(HnswGraphAuxPrefix.size());
    if (event->Record.ParseFromArray(payload.data(), payload.size())) {
        Send(SelfId(), event.Release());
    }
}

// Pipe notifications have a dedicated owner. Late notifications from a
// cancelled bootstrap cannot reach ordinary DataShard pipe management.
class THnswPipeEvents : public NActors::TActorBootstrapped<THnswPipeEvents> {
public:
    explicit THnswPipeEvents(TActorId owner) : Owner(owner) {}
    void Bootstrap() { Become(&TThis::StateWork); }
private:
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        const auto& event = *ev->Get();
        Send(Owner, new TDataShard::TEvPrivate::TEvHnswPipeStatus(
            event.ClientId, event.Status == NKikimrProto::OK && event.Leader, event.Generation));
    }
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        Send(Owner, new TDataShard::TEvPrivate::TEvHnswPipeStatus(ev->Get()->ClientId, false, 0));
    }
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }
    TActorId Owner;
};

void TDataShard::Handle(TEvPrivate::TEvHnswPipeStatus::TPtr& ev, const TActorContext&) {
    if (ev->Sender != HnswPipeEventsActor) {
        return;
    }
    for (auto& [localTid, entry] : HnswIndexCache) {
        if (entry.FollowerPipe != ev->Get()->ClientId) {
            continue;
        }
        if (!ev->Get()->Connected || !IsFollower()
                || !AppData()->FeatureFlags.GetEnableHnswIndex()) {
            InvalidateHnswIndex(localTid);
            DeferHnswIndexBuild(localTid, TDuration::Seconds(1));
        } else {
            entry.FollowerLeaderGeneration = ev->Get()->Generation;
            if (entry.FollowerSnapshot.HasLeaderGeneration()
                    && entry.FollowerSnapshot.GetLeaderGeneration() != entry.FollowerLeaderGeneration) {
                InvalidateHnswIndex(localTid);
                DeferHnswIndexBuild(localTid, TDuration::Seconds(1));
            } else {
                TryInstallFollowerHnswIndex(localTid);
            }
        }
        return;
    }
}

void TDataShard::Handle(TEvHnswCache::TEvGraphAnnouncement::TPtr& ev, const TActorContext&) {
    const auto& announcement = ev->Get()->Record;
    if (!IsFollower() || !AppData()->FeatureFlags.GetEnableHnswIndex()
            || announcement.GetTableOwnerId() != PathOwnerId
            || announcement.GetLeaderGeneration() != Executor()->Generation())
        return;
    const auto table = TableInfos.find(announcement.GetTablePathId());
    if (table == TableInfos.end() || !table->second->HnswSettings
            || table->second->GetTableSchemaVersion() != announcement.GetSchemaVersion()) {
        ScheduleHnswFollowerSync();
        return;
    }
    auto& entry = HnswIndexCache[table->second->LocalTid];
    entry.FollowerDesiredBase = Max(entry.FollowerDesiredBase,
        TRowVersion(announcement.GetBaseStep(), announcement.GetBaseTxId()));
    if (!entry.Index || entry.Index->GetBaseVersion() < entry.FollowerDesiredBase) {
        entry.FollowerNotRequired = false;
        ScheduleHnswFollowerSync();
    }
}

void TDataShard::ScheduleHnswFollowerSync() {
    if (!IsFollower() || IsStopping() || !AppData()->FeatureFlags.GetEnableHnswIndex() || HnswFollowerSyncScheduled)
        return;
    HnswFollowerSyncScheduled = true;
    Schedule(TDuration::Seconds(1), new TEvPrivate::TEvHnswFollowerSync(HnswFollowerSyncEpoch));
}

class TDataShard::TTxHnswFollowerSync : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    explicit TTxHnswFollowerSync(TDataShard* self) : TBase(self) {}
    TTxType GetTxType() const override { return TXTYPE_SYNC_SCHEME_ON_FOLLOWER; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        if (!Self->IsFollower() || !AppData()->FeatureFlags.GetEnableHnswIndex())
            return true;
        NKikimrTxDataShard::TError::EKind status;
        TString error;
        if (!Self->SyncSchemeOnFollower(txc, ctx, status, error))
            return false;
        if (status != NKikimrTxDataShard::TError::OK)
            return true;
        Self->SyncSchemeOnFollowerNeeded = false;
        for (const auto& [pathId, table] : Self->TableInfos) {
            if (!table->HnswSettings || !table->HnswVectorColumnTag
                    || table->HnswSettings->vector_type() != Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT)
                continue;
            Self->GetHnswCacheMemoryLimit();
            const auto localTid = table->LocalTid;
            Self->PrepareFollowerHnswIndex(localTid, txc.DB,
                Self->SnapshotManager.GetFollowerReadEdge().first);
            auto& entry = Self->HnswIndexCache[localTid];
            if ((entry.Index && entry.Index->GetBaseVersion() >= entry.FollowerDesiredBase)
                    || entry.FollowerNotRequired) {
                continue;
            }
            if (entry.Building && !entry.BuildObsolete && entry.Changes->Valid && ctx.Now() < entry.FollowerRequestAt + TDuration::Minutes(5)) {
                Self->TryInstallFollowerHnswIndex(localTid);
                continue;
            }
            if (ctx.Now() < entry.NextScanAttemptAt)
                continue;
            if (!entry.Index || !entry.FollowerJournalTracking || !entry.Changes->Valid) {
                Self->InvalidateHnswIndex(localTid);
                entry.Changes = std::make_shared<THnswIndexChanges>();
                entry.FollowerReadVersion = TRowVersion::Min();
            } else {
                // Keep serving the current generation while its replacement
                // downloads. The same private journal covers both bases.
                entry.FollowerGraph.reset();
                entry.FollowerJournal.reset();
                entry.FollowerSnapshot.Clear();
                entry.FollowerBootstrapChanges = {};
            }
            entry.Settings = *table->HnswSettings;
            entry.VectorColumnTag = table->HnswVectorColumnTag;
            entry.BuildVersion = TRowVersion::Min();
            entry.BuildToken = ++Self->NextHnswBuildToken;
            entry.Building = true;
            entry.BuildObsolete = false;
            entry.FollowerJournalTracking = true;
            entry.FollowerChangeCounter = txc.DB.Head(localTid);
            entry.FollowerRequestAt = ctx.Now();
            Self->TrackHnswOpenTransactions(localTid, txc.DB);
            if (!entry.Changes->Valid)
                continue;
            auto request = MakeHolder<TEvHnswCache::TEvSnapshotRequest>();
            request->Record.SetTableOwnerId(Self->PathOwnerId);
            request->Record.SetTablePathId(pathId);
            request->Record.SetSchemaVersion(table->GetTableSchemaVersion());
            request->Record.SetRequestId(entry.BuildToken);
            request->Record.SetFormatVersion(HnswSnapshotFormatVersion);
            if (!entry.FollowerPipe) {
                if (!Self->HnswPipeEventsActor) {
                    Self->HnswPipeEventsActor = Self->Register(new THnswPipeEvents(Self->SelfId()));
                    Self->Actors.insert(Self->HnswPipeEventsActor);
                }
                NTabletPipe::TClientConfig config;
                config.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
                entry.FollowerPipe = Self->Register(NTabletPipe::CreateClient(Self->HnswPipeEventsActor, Self->TabletID(), config));
            }
            Requests.emplace_back(entry.FollowerPipe, std::move(request));
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        for (auto& [pipe, request] : Requests)
            NTabletPipe::SendData(ctx, pipe, request.Release());
        bool pending = Self->SyncSchemeOnFollowerNeeded;
        for (const auto& [_, table] : Self->TableInfos) {
            if (table->HnswSettings
                    && table->HnswSettings->vector_type() == Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT) {
                const auto cache = Self->HnswIndexCache.find(table->LocalTid);
                pending |= cache == Self->HnswIndexCache.end()
                    || cache->second.Building
                    || (!cache->second.Index && !cache->second.FollowerNotRequired)
                    || (cache->second.Index && !cache->second.FollowerNotRequired
                        && cache->second.Index->GetBaseVersion() < cache->second.FollowerDesiredBase);
            }
        }
        if (pending)
            Self->ScheduleHnswFollowerSync();
    }

private:
    TVector<std::pair<TActorId, THolder<TEvHnswCache::TEvSnapshotRequest>>> Requests;
};

void TDataShard::Handle(TEvPrivate::TEvHnswFollowerSync::TPtr& ev, const TActorContext& ctx) {
    if (ev->Get()->Epoch != HnswFollowerSyncEpoch)
        return;
    HnswFollowerSyncScheduled = false;
    if (IsFollower() && AppData()->FeatureFlags.GetEnableHnswIndex())
        Execute(new TTxHnswFollowerSync(this), ctx);
}

class TDataShard::TTxHnswSnapshot : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHnswSnapshot(TDataShard* self, TEvHnswCache::TEvSnapshotRequest::TPtr request)
        : TBase(self), Request(std::move(request)) {}
    TTxType GetTxType() const override { return TXTYPE_READ; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Response = MakeHolder<TEvHnswCache::TEvSnapshotResponse>();
        auto& result = Response->Record;
        const auto& request = Request->Get()->Record;
        result.SetRequestId(request.GetRequestId());
        result.SetTablePathId(request.GetTablePathId());
        result.SetFormatVersion(HnswSnapshotFormatVersion);
        if (request.GetFormatVersion() != HnswSnapshotFormatVersion) {
            result.SetError("Incompatible HNSW snapshot format");
            return true;
        }
        const auto tableIt = Self->TableInfos.find(request.GetTablePathId());
        if (Self->IsFollower() || Self->IsStopping() || Self->State != TShardState::Ready
                || !AppData()->FeatureFlags.GetEnableHnswIndex()
                || Self->PathOwnerId != request.GetTableOwnerId() || tableIt == Self->TableInfos.end()
                || !tableIt->second->HnswSettings
                || tableIt->second->GetTableSchemaVersion() != request.GetSchemaVersion()) {
            result.SetError("HNSW table or leader incarnation changed");
            return true;
        }
        const auto& table = *tableIt->second;
        result.SetSchemaVersion(table.GetTableSchemaVersion());
        result.SetVectorColumnTag(table.HnswVectorColumnTag);
        result.MutableSettings()->CopyFrom(*table.HnswSettings);
        const auto it = Self->HnswIndexCache.find(table.LocalTid);
        if (it != Self->HnswIndexCache.end() && it->second.Index
                && (it->second.VectorColumnTag != table.HnswVectorColumnTag
                    || !AreHnswIndexSettingsCompatible(it->second.Settings, *table.HnswSettings)))
            Self->InvalidateHnswIndex(table.LocalTid);
        if (it == Self->HnswIndexCache.end() || !it->second.Changes->Valid
                || (!it->second.Index && !it->second.Building
                    && it->second.NextScanAttemptAt != TInstant::Max())) {
            auto& missing = Self->HnswIndexCache[table.LocalTid];
            missing.InitialBuildAttemptsLeft = Max<ui32>(missing.InitialBuildAttemptsLeft, 30);
            missing.NextScanAttemptAt = TInstant::Zero();
            Self->ScheduleHnswRebuild(table.LocalTid);
            result.SetError("Leader HNSW graph is not ready");
            return true;
        }
        const auto& entry = it->second;
        result.SetLeaderGeneration(Self->Executor()->Generation());
        result.SetCheckpointStep(Self->Executor()->GetFollowerSnapshotCheckpoint());
        result.SetSourceNodeId(Self->SelfId().NodeId());
        if (!entry.Index) {
            if (!entry.Building && entry.NextScanAttemptAt == TInstant::Max())
                result.SetNotRequired(true);
            else
                result.SetError("Leader HNSW graph is not ready");
            return true;
        }
        const auto base = entry.Index->GetBaseVersion();
        if (base >= Self->Pipeline.GetUnreadableEdge()
                || Self->VolatileTxManager.HasVolatileTxsAtSnapshot(base)) {
            result.SetError("Leader HNSW snapshot is waiting for a readable version");
            return true;
        }
        // An idle table's stale-read edge can precede an eager graph's base.
        // Protect that snapshot from future writes and persist its completion
        // so followers can advance to it without needing another user write.
        Self->PromoteImmediatePostExecuteEdges(base, EPromotePostExecuteEdges::RepeatableRead, txc);
        Self->SnapshotManager.PromoteCompleteEdge(base, txc);
        Self->PromoteFollowerReadEdge(txc);
        result.SetGraphKey(entry.GraphKey);
        result.SetBaseStep(base.Step);
        result.SetBaseTxId(base.TxId);
        result.SetRowCount(entry.Index->Size());
        result.SetGraphMemoryBytes(entry.Index->EstimatedMemoryBytes() + 4096);
        result.SetJournalKey(TStringBuilder() << entry.GraphKey << ":journal:"
            << result.GetCheckpointStep() << ':' << ++Self->NextHnswJournalId);
        const ui64 journalBytes = entry.Changes->GetEstimatedBytes() + 4096;
        result.SetJournalMemoryBytes(journalBytes);
        auto reservation = Self->TryReserveHnswCacheMemory(journalBytes);
        if (!reservation) {
            result.SetError("HNSW journal memory admission pending");
            return true;
        }
        auto journal = std::make_shared<THnswJournalSnapshot>();
        journal->Reservation = std::move(reservation);
        for (const auto& [key, history] : entry.Changes->Rows) {
            for (const auto& change : history) {
                if (change.Version > base)
                    journal->Changes.push_back({key, change.Version, change.Vector});
            }
        }
        Graph = MakeHolder<TEvHnswCache::TEvPublish>();
        Graph->Key = entry.GraphKey;
        Graph->Graph = entry.Index->CreateView(base, std::make_shared<THnswIndexChanges>());
        Graph->Settings = entry.Settings;
        Journal = MakeHolder<TEvHnswCache::TEvPublish>();
        Journal->Key = result.GetJournalKey();
        Journal->Journal = std::move(journal);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Graph)
            ctx.Send(MakeHnswCacheId(), Graph.Release());
        if (Journal)
            ctx.Send(MakeHnswCacheId(), Journal.Release());
        ctx.Send(Request->Sender, Response.Release(), 0, Request->Cookie);
    }

private:
    TEvHnswCache::TEvSnapshotRequest::TPtr Request;
    THolder<TEvHnswCache::TEvSnapshotResponse> Response;
    THolder<TEvHnswCache::TEvPublish> Graph;
    THolder<TEvHnswCache::TEvPublish> Journal;
};

void TDataShard::Handle(TEvHnswCache::TEvSnapshotRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxHnswSnapshot(this, ev), ctx);
}

void TDataShard::Handle(TEvHnswCache::TEvSnapshotResponse::TPtr& ev, const TActorContext&) {
    if (!IsFollower())
        return;
    const auto& snapshot = ev->Get()->Record;
    const auto table = TableInfos.find(snapshot.GetTablePathId());
    if (table == TableInfos.end() || !table->second->HnswSettings)
        return;
    auto& entry = HnswIndexCache[table->second->LocalTid];
    if (!IsHnswBuildCurrent(table->second->LocalTid, snapshot.GetRequestId()))
        return;
    if (!AppData()->FeatureFlags.GetEnableHnswIndex()) {
        InvalidateHnswIndex(table->second->LocalTid);
        return;
    }
    if (!snapshot.GetError().empty()
            || snapshot.GetSourceNodeId() != ev->Sender.NodeId()
            || !snapshot.GetLeaderGeneration()
            || (entry.FollowerLeaderGeneration && snapshot.GetLeaderGeneration() != entry.FollowerLeaderGeneration)
            || (!snapshot.GetNotRequired() && (snapshot.GetGraphKey().empty() || snapshot.GetJournalKey().empty()
                || !snapshot.GetGraphMemoryBytes() || !snapshot.GetJournalMemoryBytes()))
            || snapshot.GetFormatVersion() != HnswSnapshotFormatVersion
            || snapshot.GetSchemaVersion() != table->second->GetTableSchemaVersion()
            || snapshot.GetVectorColumnTag() != table->second->HnswVectorColumnTag
            || !AreHnswIndexSettingsCompatible(snapshot.GetSettings(), *table->second->HnswSettings)) {
        DeferHnswIndexBuild(table->second->LocalTid, TDuration::Seconds(1));
        return;
    }
    if (entry.FollowerSnapshot.HasRequestId())
        return;
    const TRowVersion base(snapshot.GetBaseStep(), snapshot.GetBaseTxId());
    const auto checkpoint = snapshot.GetCheckpointStep();
    if ((snapshot.GetNotRequired() && entry.FollowerBootstrapChanges.HasChangesAfter(checkpoint))
            || !entry.FollowerBootstrapChanges.SetSnapshot(base, checkpoint)) {
        InvalidateHnswIndex(table->second->LocalTid);
        DeferHnswIndexBuild(table->second->LocalTid, TDuration::Seconds(1));
        return;
    }
    entry.BuildVersion = base;
    entry.FollowerSnapshot = snapshot;
    if (snapshot.GetNotRequired()) {
        entry.FollowerNotRequired = true;
        entry.Building = false;
        return;
    }
    for (bool journal : {false, true}) {
        auto request = MakeHolder<TEvHnswCache::TEvAcquire>();
        request->Key = journal ? snapshot.GetJournalKey() : snapshot.GetGraphKey();
        request->SourceNodeId = snapshot.GetSourceNodeId();
        request->RequestId = snapshot.GetRequestId();
        request->MemoryBytes = journal ? snapshot.GetJournalMemoryBytes() : snapshot.GetGraphMemoryBytes();
        request->Journal = journal;
        request->Settings = snapshot.GetSettings();
        Send(MakeHnswCacheId(), request.Release());
    }
}

void TDataShard::Handle(TEvHnswCache::TEvAcquired::TPtr& ev, const TActorContext&) {
    if (!IsFollower())
        return;
    const auto& result = *ev->Get();
    for (auto& [localTid, entry] : HnswIndexCache) {
        if (!IsHnswBuildCurrent(localTid, result.RequestId))
            continue;
        if (!AppData()->FeatureFlags.GetEnableHnswIndex()) {
            InvalidateHnswIndex(localTid);
            return;
        }
        if (!result.Error.empty()) {
            DeferHnswIndexBuild(localTid, TDuration::Seconds(1));
            return;
        }
        if (result.Key == entry.FollowerSnapshot.GetGraphKey() && result.Graph && !result.Journal)
            entry.FollowerGraph = result.Graph;
        else if (result.Key == entry.FollowerSnapshot.GetJournalKey() && result.Journal && !result.Graph)
            entry.FollowerJournal = result.Journal;
        else
            return;
        TryInstallFollowerHnswIndex(localTid);
        return;
    }
}

void TDataShard::TryInstallFollowerHnswIndex(ui32 localTid) {
    if (!IsFollower() || IsStopping() || !AppData()->FeatureFlags.GetEnableHnswIndex()) {
        return;
    }
    auto& entry = HnswIndexCache.at(localTid);
    const auto& snapshot = entry.FollowerSnapshot;
    if (!entry.Building || !entry.FollowerLeaderGeneration
            || entry.FollowerLeaderGeneration != snapshot.GetLeaderGeneration()
            || !entry.FollowerGraph || !entry.FollowerJournal || !entry.Changes->Valid
            || !entry.FollowerJournalTracking || Executor()->Generation() != snapshot.GetLeaderGeneration()
            || Executor()->Step() < snapshot.GetCheckpointStep())
        return;
    const TRowVersion base(snapshot.GetBaseStep(), snapshot.GetBaseTxId());
    const auto table = TableInfos.find(snapshot.GetTablePathId());
    if (table == TableInfos.end() || table->second->LocalTid != localTid
            || table->second->GetTableSchemaVersion() != snapshot.GetSchemaVersion()
            || table->second->HnswVectorColumnTag != entry.VectorColumnTag
            || !table->second->HnswSettings
            || !AreHnswIndexSettingsCompatible(*table->second->HnswSettings, snapshot.GetSettings())) {
        InvalidateHnswIndex(localTid);
        DeferHnswIndexBuild(localTid, TDuration::Seconds(1));
        return;
    }
    for (const auto& change : entry.FollowerJournal->Changes) {
        if (change.Version <= base) {
            InvalidateHnswIndex(localTid);
            DeferHnswIndexBuild(localTid, TDuration::Seconds(1));
            return;
        }
        // Redo has already reached the checkpoint. An equal version in our
        // journal includes at least that state and may contain a later replay.
        const auto history = entry.Changes->Rows.find(change.Key);
        if (history != entry.Changes->Rows.end() && AnyOf(history->second,
                [&](const auto& known) { return known.Version == change.Version; })) {
            continue;
        }
        auto reservation = TryReserveHnswCacheMemory(
            THnswIndexChanges::EstimateBytes(change.Key, change.Vector ? change.Vector->size() : 0));
        if (!reservation) {
            InvalidateHnswIndex(localTid);
            DeferHnswIndexBuild(localTid, TDuration::Seconds(1));
            return;
        }
        entry.Changes->Set(change.Key, change.Version, change.Vector, std::move(reservation));
    }
    auto graph = entry.FollowerGraph->CreateView(base, entry.Changes, base);
    entry.FollowerGraph.reset();
    entry.FollowerJournal.reset();
    const auto token = entry.BuildToken;
    const auto key = snapshot.GetGraphKey();
    SetHnswIndex(localTid, std::move(graph), {}, snapshot.GetRowCount(),
        entry.VectorColumnTag, entry.Settings, base, token);
    if (entry.Index && entry.Index->GetBaseVersion() == base)
        entry.GraphKey = key;
}

NTable::IRedoObserver* TDataShard::GetFollowerRedoObserver() {
    // Keep existing journals complete across feature-flag changes, even when
    // no read observes the temporarily disabled feature.
    return !HnswIndexCache.empty() ? this : nullptr;
}

bool TDataShard::TrackFollowerHnswCommit(ui32 localTid, TRowVersion version) {
    auto& entry = HnswIndexCache.at(localTid);
    const auto step = Executor()->Step();
    std::shared_ptr<void> reservation;
    if (entry.FollowerBootstrapChanges.NeedsRecord(step)) {
        reservation = TryReserveHnswCacheMemory(THnswBootstrapChanges::RecordBytes);
        if (!reservation) {
            InvalidateHnswIndex(localTid);
            DeferHnswIndexBuild(localTid, TDuration::Seconds(1));
            return false;
        }
    }
    if (!entry.FollowerBootstrapChanges.Observe(step, version, std::move(reservation))) {
        InvalidateHnswIndex(localTid);
        DeferHnswIndexBuild(localTid, TDuration::Seconds(1));
        return false;
    }
    return !entry.FollowerBootstrapChanges.Covers(step);
}

void TDataShard::OnRedoUpdate(ui32 localTid, NTable::ERowOp op, TArrayRef<const TRawTypeValue> key,
        TArrayRef<const NTable::TUpdateOp> updates, TArrayRef<const NPageCollection::TMemGlob> annex,
        TRowVersion version, ui64 txId, ui32 savepoint) {
    if (localTid == Schema::UserTables::TableId)
        ScheduleHnswFollowerSync();
    auto it = HnswIndexCache.find(localTid);
    if (it == HnswIndexCache.end() || !it->second.FollowerJournalTracking || !it->second.Changes->Valid)
        return;
    auto& entry = it->second;
    if (!txId && entry.FollowerBootstrapChanges.Covers(Executor()->Step()))
        return;
    if (entry.FollowerNotRequired) {
        entry.FollowerNotRequired = false;
        ScheduleHnswFollowerSync();
    }
    if (savepoint || op == NTable::ERowOp::Reset) {
        InvalidateHnswIndex(localTid);
        return;
    }
    bool affected = op == NTable::ERowOp::Erase;
    std::optional<TString> vector;
    for (const auto& update : updates) {
        if (update.Tag != entry.VectorColumnTag || update.Op == NTable::ECellOp::Empty)
            continue;
        affected = true;
        if (update.Op == NTable::ECellOp::Reset) {
            InvalidateHnswIndex(localTid);
            return;
        }
        if (update.Op == NTable::ECellOp::Set) {
            auto cell = update.AsCell();
            if (update.Op == NTable::ELargeObj::Extern) {
                const auto index = cell.AsValue<ui32>();
                if (index >= annex.size() || !annex[index].Data) {
                    InvalidateHnswIndex(localTid);
                    return;
                }
                cell = TCell(annex[index].Data.data(), annex[index].Data.size());
            }
            if (!cell.IsNull() && THnswIndex::IsValidVector(cell.AsBuf(), entry.Settings.vector_dimension()))
                vector = TString(cell.AsBuf());
        }
    }
    if (txId && !entry.PendingReservations.contains(txId)) {
        auto reservation = TryReserveHnswCacheMemory(THnswIndexCacheEntry::PendingTransactionBytes);
        if (!reservation) {
            InvalidateHnswIndex(localTid);
            return;
        }
        entry.PendingReservations.emplace(txId, std::move(reservation));
        entry.Pending[txId];
    }
    if (!affected)
        return;
    TVector<TCell> cells;
    for (const auto& value : key)
        cells.emplace_back(static_cast<const char*>(value.Data()), value.Size());
    auto serialized = TSerializedCellVec::Serialize(cells);
    auto reservation = TryReserveHnswCacheMemory(
        THnswIndexChanges::EstimateBytes(serialized, vector ? vector->size() : 0));
    if (!reservation) {
        InvalidateHnswIndex(localTid);
        return;
    }
    THnswIndexChanges::TVersion change{version, std::move(vector), std::move(reservation)};
    if (txId)
        entry.Pending[txId][std::move(serialized)] = std::move(change);
    else if (TrackFollowerHnswCommit(localTid, version))
        ApplyHnswIndexChange(localTid, std::move(serialized), std::move(change));
}

void TDataShard::OnRedoCommit(ui32 localTid, ui64 txId, TRowVersion version) {
    auto it = HnswIndexCache.find(localTid);
    if (it == HnswIndexCache.end() || !it->second.FollowerJournalTracking)
        return;
    auto& entry = it->second;
    if (entry.FollowerBootstrapChanges.Covers(Executor()->Step())) {
        OnRedoRemove(localTid, txId);
        return;
    }
    auto pending = entry.Pending.find(txId);
    if (entry.UntrackedTransactions.erase(txId) || pending == entry.Pending.end()) {
        InvalidateHnswIndex(localTid);
        return;
    }
    auto changes = std::move(pending->second);
    entry.Pending.erase(pending);
    entry.PendingReservations.erase(txId);
    if (!changes.empty() && !TrackFollowerHnswCommit(localTid, version))
        return;
    for (auto& [key, change] : changes) {
        change.Version = version;
        ApplyHnswIndexChange(localTid, key, std::move(change));
    }
}

void TDataShard::OnRedoRemove(ui32 localTid, ui64 txId) {
    if (auto it = HnswIndexCache.find(localTid); it != HnswIndexCache.end()) {
        it->second.Pending.erase(txId);
        it->second.PendingReservations.erase(txId);
        it->second.UntrackedTransactions.erase(txId);
    }
}

} // namespace NKikimr::NDataShard
