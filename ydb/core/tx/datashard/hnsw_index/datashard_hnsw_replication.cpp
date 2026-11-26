#include <ydb/core/tx/datashard/datashard_impl.h>
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
    ForEachHnswIndexEntry([&](ui32 localTid, TPathId indexId, auto& entry) {
        if (entry.FollowerPipe != ev->Get()->ClientId) {
            return;
        }
        if (!ev->Get()->Connected || !IsFollower()
                || !AppData()->FeatureFlags.GetEnableHnswIndex()) {
            InvalidateHnswIndex(localTid, indexId);
            DeferHnswIndexBuild(localTid, TDuration::Seconds(1), indexId);
        } else {
            entry.FollowerLeaderGeneration = ev->Get()->Generation;
            if (entry.FollowerSnapshot.HasLeaderGeneration()
                    && entry.FollowerSnapshot.GetLeaderGeneration() != entry.FollowerLeaderGeneration) {
                InvalidateHnswIndex(localTid, indexId);
                DeferHnswIndexBuild(localTid, TDuration::Seconds(1), indexId);
            } else {
                TryInstallFollowerHnswIndex(localTid, indexId);
            }
        }
    });
}

void TDataShard::Handle(TEvHnswCache::TEvGraphAnnouncement::TPtr& ev, const TActorContext&) {
    const auto& announcement = ev->Get()->Record;
    if (!IsFollower() || !AppData()->FeatureFlags.GetEnableHnswIndex()
            || announcement.GetTableOwnerId() != PathOwnerId
            || announcement.GetLeaderGeneration() != Executor()->Generation())
        return;
    const auto table = TableInfos.find(announcement.GetTablePathId());
    const auto indexId = announcement.HasIndexOwnerId() && announcement.HasIndexLocalId()
        ? TPathId(announcement.GetIndexOwnerId(), announcement.GetIndexLocalId()) : TPathId{};
    if (table == TableInfos.end() || table->second->GetTableSchemaVersion() != announcement.GetSchemaVersion()) {
        ScheduleHnswFollowerSync();
        return;
    }
    auto* cached = GetOrCreateHnswIndexEntry(table->second->LocalTid, indexId);
    if (!cached) {
        return;
    }
    auto& entry = *cached;
    entry.FollowerDesiredBase = Max(entry.FollowerDesiredBase,
        TRowVersion(announcement.GetBaseStep(), announcement.GetBaseTxId()));
    if (!entry.Index || entry.Index->GetBaseVersion() < entry.FollowerDesiredBase) {
        entry.FollowerNotRequired = false;
        ScheduleHnswFollowerSync();
    }
}

void TDataShard::PrepareFollowerHnswIndex(ui32 localTid, const NTable::TDatabase& db,
        const TRowVersion& readVersion, TPathId indexId)
{
    if (readVersion.IsMax()) {
        return;
    }
    auto* entry = FindHnswIndexEntry(localTid, indexId);
    if (!entry || !entry->FollowerJournalTracking || !entry->Changes->Valid) {
        return;
    }
    entry->FollowerChangeCounter = db.Head(localTid);
    entry->FollowerReadVersion = Max(entry->FollowerReadVersion, readVersion);
    if (entry->Index && entry->Index->GetBaseVersion() <= readVersion) {
        entry->Index->SetSnapshot(entry->Index->GetBaseVersion(), entry->Changes, entry->FollowerReadVersion);
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
            for (const auto& [indexId, metadata] : table->HnswIndexes) {
                if (!metadata.VectorColumnTag
                        || metadata.Settings.vector_type() != Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT) {
                    continue;
                }
                Self->GetHnswCacheMemoryLimit();
                const auto localTid = table->LocalTid;
                Self->PrepareFollowerHnswIndex(localTid, txc.DB,
                    Self->SnapshotManager.GetFollowerReadEdge().first, indexId);
                auto& entry = *Self->GetOrCreateHnswIndexEntry(localTid, indexId);
                if ((entry.Index && entry.Index->GetBaseVersion() >= entry.FollowerDesiredBase)
                        || entry.FollowerNotRequired) {
                    continue;
                }
                if (entry.Building && entry.Changes->Valid) {
                    const ui32 step = Self->Executor()->Step();
                    if (entry.FollowerSnapshot.HasRequestId() && step > entry.FollowerReplayStep) {
                        entry.FollowerReplayStep = step;
                        entry.FollowerProgressAt = ctx.Now();
                    }
                    // The cache times downloads from the most recent chunk.
                    const bool downloading = entry.FollowerSnapshot.HasRequestId()
                        && (!entry.FollowerGraph || !entry.FollowerJournal);
                    if (downloading || ctx.Now() < entry.FollowerProgressAt + TDuration::Minutes(5)) {
                        Self->TryInstallFollowerHnswIndex(localTid, indexId);
                        continue;
                    }
                }
                if (ctx.Now() < entry.NextScanAttemptAt)
                    continue;
                // Covered redo may be omitted once the manifest arrives. Reads
                // use the exact path until the new graph and journal are installed.
                const auto desiredBase = entry.FollowerDesiredBase;
                Self->InvalidateHnswIndex(localTid, indexId);
                entry.FollowerDesiredBase = desiredBase;
                Self->ResetHnswIndexChanges(entry);
                entry.IndexSchemaVersion = metadata.SchemaVersion;
                entry.Settings = metadata.Settings;
                entry.VectorColumnTag = metadata.VectorColumnTag;
                entry.BuildVersion = TRowVersion::Min();
                entry.BuildToken = ++Self->NextHnswBuildToken;
                entry.Building = true;
                entry.FollowerJournalTracking = true;
                entry.FollowerChangeCounter = txc.DB.Head(localTid);
                entry.FollowerProgressAt = ctx.Now();
                entry.FollowerReplayStep = Self->Executor()->Step();
                Self->TrackHnswOpenTransactions(localTid, txc.DB, indexId);
                if (!entry.Changes->Valid)
                    continue;
                auto request = MakeHolder<TEvHnswCache::TEvSnapshotRequest>();
                request->Record.SetTableOwnerId(Self->PathOwnerId);
                request->Record.SetTablePathId(pathId);
                request->Record.SetSchemaVersion(table->GetTableSchemaVersion());
                request->Record.SetRequestId(entry.BuildToken);
                request->Record.SetIndexOwnerId(indexId.OwnerId);
                request->Record.SetIndexLocalId(indexId.LocalPathId);
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
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        for (auto& [pipe, request] : Requests)
            NTabletPipe::SendData(ctx, pipe, request.Release());
        bool pending = Self->SyncSchemeOnFollowerNeeded;
        for (const auto& [_, table] : Self->TableInfos) {
            for (const auto& [indexId, metadata] : table->HnswIndexes) {
                if (!metadata.VectorColumnTag
                        || metadata.Settings.vector_type() != Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT) {
                    continue;
                }
                const auto* entry = Self->FindHnswIndexEntry(table->LocalTid, indexId);
                pending |= !entry || entry->Building || (!entry->FollowerNotRequired
                    && (!entry->Index || entry->Index->GetBaseVersion() < entry->FollowerDesiredBase));
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
        const auto indexId = request.HasIndexOwnerId() && request.HasIndexLocalId()
            ? TPathId(request.GetIndexOwnerId(), request.GetIndexLocalId()) : TPathId{};
        if (request.GetFormatVersion() != HnswSnapshotFormatVersion) {
            result.SetError("Incompatible HNSW snapshot format");
            return true;
        }
        const auto tableIt = Self->TableInfos.find(request.GetTablePathId());
        if (Self->IsFollower() || Self->IsStopping() || Self->State != TShardState::Ready
                || !AppData()->FeatureFlags.GetEnableHnswIndex()
                || Self->PathOwnerId != request.GetTableOwnerId() || tableIt == Self->TableInfos.end()
                || tableIt->second->GetTableSchemaVersion() != request.GetSchemaVersion()) {
            result.SetError("HNSW table or leader incarnation changed");
            return true;
        }
        const auto& table = *tableIt->second;
        const auto* metadata = Self->FindHnswIndexInfo(table.LocalTid, indexId);
        if (!metadata) {
            result.SetError("HNSW index has changed");
            return true;
        }
        const auto resolvedIndexId = Self->ResolveHnswIndexId(table.LocalTid, indexId);
        result.SetIndexOwnerId(resolvedIndexId.OwnerId);
        result.SetIndexLocalId(resolvedIndexId.LocalPathId);
        result.SetSchemaVersion(table.GetTableSchemaVersion());
        result.SetVectorColumnTag(metadata->VectorColumnTag);
        result.MutableSettings()->CopyFrom(metadata->Settings);
        auto& entry = *Self->GetOrCreateHnswIndexEntry(table.LocalTid, indexId);
        if (entry.Index && (entry.VectorColumnTag != metadata->VectorColumnTag
                || !AreHnswIndexSettingsCompatible(entry.Settings, metadata->Settings))) {
            Self->InvalidateHnswIndex(table.LocalTid, indexId);
        }
        if (!entry.Changes->Valid || (!entry.Index && !entry.Building && entry.NextScanAttemptAt != TInstant::Max())) {
            Self->RequestHnswIndexBuild(table.LocalTid, metadata->VectorColumnTag, metadata->Settings, indexId);
            result.SetError("Leader HNSW graph is not ready");
            return true;
        }
        result.SetLeaderGeneration(Self->Executor()->Generation());
        result.SetCheckpointStep(Self->Executor()->CreateFollowerSnapshotCheckpoint());
        result.SetSourceNodeId(Self->SelfId().NodeId());
        if (!entry.Index) {
            if (!entry.Building && entry.BelowMinRows)
                result.SetNotRequired(true);
            else
                result.SetError("Leader HNSW graph is not ready");
            return true;
        }
        const auto base = entry.Index->GetBaseVersion();
        const auto lower = Max(base, entry.Changes->LowerReadVersion);
        if (lower >= Self->Pipeline.GetUnreadableEdge()
                || Self->VolatileTxManager.HasVolatileTxsAtSnapshot(lower)) {
            result.SetError("Leader HNSW snapshot is waiting for a readable version");
            return true;
        }
        // An idle follower read edge can precede the exported history.
        // Protect that snapshot from future writes and persist its completion
        // so followers can advance to it without needing another user write.
        Self->PromoteImmediatePostExecuteEdges(lower, EPromotePostExecuteEdges::RepeatableRead, txc);
        Self->SnapshotManager.PromoteCompleteEdge(lower, txc);
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
        journal->LowerReadVersion = lower;
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
    if (table == TableInfos.end()) {
        return;
    }
    const auto indexId = snapshot.HasIndexOwnerId() && snapshot.HasIndexLocalId()
        ? TPathId(snapshot.GetIndexOwnerId(), snapshot.GetIndexLocalId()) : TPathId{};
    const ui32 localTid = table->second->LocalTid;
    const auto* metadata = FindHnswIndexInfo(localTid, indexId);
    if (!metadata || !IsHnswBuildCurrent(localTid, snapshot.GetRequestId(), indexId)) {
        return;
    }
    auto& entry = *FindHnswIndexEntry(localTid, indexId);
    if (!AppData()->FeatureFlags.GetEnableHnswIndex()) {
        InvalidateHnswIndex(localTid, indexId);
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
            || snapshot.GetVectorColumnTag() != metadata->VectorColumnTag
            || !AreHnswIndexSettingsCompatible(snapshot.GetSettings(), metadata->Settings)) {
        DeferHnswIndexBuild(localTid, TDuration::Seconds(1), indexId);
        return;
    }
    if (entry.FollowerSnapshot.HasRequestId())
        return;
    const TRowVersion base(snapshot.GetBaseStep(), snapshot.GetBaseTxId());
    const auto checkpoint = snapshot.GetCheckpointStep();
    if ((snapshot.GetNotRequired() && entry.FollowerBootstrapChanges.HasChangesAfter(checkpoint))
            || !entry.FollowerBootstrapChanges.SetSnapshot(base, checkpoint)) {
        InvalidateHnswIndex(localTid, indexId);
        DeferHnswIndexBuild(localTid, TDuration::Seconds(1), indexId);
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

void TDataShard::Handle(TEvHnswCache::TEvAcquired::TPtr& ev, const TActorContext& ctx) {
    if (!IsFollower())
        return;
    const auto& result = *ev->Get();
    ForEachHnswIndexEntry([&](ui32 localTid, TPathId indexId, auto& entry) {
        if (!IsHnswBuildCurrent(localTid, result.RequestId, indexId))
            return;
        if (!AppData()->FeatureFlags.GetEnableHnswIndex()) {
            InvalidateHnswIndex(localTid, indexId);
            return;
        }
        if (!result.Error.empty()) {
            DeferHnswIndexBuild(localTid, TDuration::Seconds(1), indexId);
            return;
        }
        if (result.Key == entry.FollowerSnapshot.GetGraphKey() && result.Graph && !result.Journal)
            entry.FollowerGraph = result.Graph;
        else if (result.Key == entry.FollowerSnapshot.GetJournalKey() && result.Journal && !result.Graph)
            entry.FollowerJournal = result.Journal;
        else
            return;
        entry.FollowerProgressAt = ctx.Now();
        TryInstallFollowerHnswIndex(localTid, indexId);
    });
}

void TDataShard::TryInstallFollowerHnswIndex(ui32 localTid, TPathId indexId) {
    if (!IsFollower() || IsStopping() || !AppData()->FeatureFlags.GetEnableHnswIndex()) {
        return;
    }
    auto& entry = *FindHnswIndexEntry(localTid, indexId);
    const auto& snapshot = entry.FollowerSnapshot;
    if (!entry.Building || !entry.FollowerLeaderGeneration
            || entry.FollowerLeaderGeneration != snapshot.GetLeaderGeneration()
            || !entry.FollowerGraph || !entry.FollowerJournal || !entry.Changes->Valid
            || !entry.FollowerJournalTracking || Executor()->Generation() != snapshot.GetLeaderGeneration()
            || Executor()->Step() < snapshot.GetCheckpointStep())
        return;
    const TRowVersion base(snapshot.GetBaseStep(), snapshot.GetBaseTxId());
    const auto table = TableInfos.find(snapshot.GetTablePathId());
    const auto* metadata = FindHnswIndexInfo(localTid, indexId);
    if (!metadata || table == TableInfos.end() || table->second->LocalTid != localTid
            || table->second->GetTableSchemaVersion() != snapshot.GetSchemaVersion()
            || metadata->VectorColumnTag != entry.VectorColumnTag
            || !AreHnswIndexSettingsCompatible(metadata->Settings, snapshot.GetSettings())) {
        InvalidateHnswIndex(localTid, indexId);
        DeferHnswIndexBuild(localTid, TDuration::Seconds(1), indexId);
        return;
    }
    for (const auto& change : entry.FollowerJournal->Changes) {
        if (change.Version <= base) {
            InvalidateHnswIndex(localTid, indexId);
            DeferHnswIndexBuild(localTid, TDuration::Seconds(1), indexId);
            return;
        }
        // Multiple immediate writes can have the same MVCC version. Only
        // redo after the checkpoint supersedes a value exported by the leader.
        const auto history = entry.Changes->Rows.find(change.Key);
        if (history != entry.Changes->Rows.end() && AnyOf(history->second,
                [&](const auto& known) {
                    return known.Version == change.Version && known.RedoStep > snapshot.GetCheckpointStep();
                })) {
            continue;
        }
        auto reservation = TryReserveHnswCacheMemory(
            THnswIndexChanges::EstimateBytes(change.Key, change.Vector ? change.Vector->size() : 0), entry.MemoryOwner);
        if (!reservation) {
            InvalidateHnswIndex(localTid, indexId);
            DeferHnswIndexBuild(localTid, TDuration::Seconds(1), indexId);
            return;
        }
        entry.Changes->Set(change.Key, change.Version, change.Vector, std::move(reservation), snapshot.GetCheckpointStep());
    }
    // Redo since the request cannot reconstruct history dropped by the leader
    // before it exported this journal.
    entry.SnapshotReadPins.clear();
    entry.Changes->AdvanceRetentionFloor(entry.FollowerJournal->LowerReadVersion);
    auto graph = entry.FollowerGraph->CreateView(base, entry.Changes, base);
    entry.FollowerGraph.reset();
    entry.FollowerJournal.reset();
    const auto token = entry.BuildToken;
    const auto key = snapshot.GetGraphKey();
    SetHnswIndex(localTid, std::move(graph), {}, snapshot.GetRowCount(),
        entry.VectorColumnTag, entry.Settings, base, token, indexId);
    if (entry.Index && entry.Index->GetBaseVersion() == base)
        entry.GraphKey = key;
}

class TDataShard::TTxHnswReplicaStatus : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHnswReplicaStatus(TDataShard* self, TEvHnswCache::TEvStatusRequest::TPtr request)
        : TBase(self), Request(std::move(request)) {}
    TTxType GetTxType() const override { return TXTYPE_READ; }
    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Result = MakeHolder<TEvHnswCache::TEvStatusResponse>();
        auto& result = Result->Record;
        const auto& request = Request->Get()->Record;
        result.SetTabletId(Self->TabletID());
        result.SetFollowerId(Self->FollowerId());
        result.SetLeaderGeneration(Self->Executor()->Generation());
        if (!AppData()->FeatureFlags.GetEnableHnswIndex() || Self->IsStopping()
                || (!Self->IsFollower() && Self->State != TShardState::Ready)) {
            return true;
        }
        if (Self->IsFollower()) {
            Self->ScheduleHnswFollowerSync();
            NKikimrTxDataShard::TError::EKind status;
            TString error;
            if (!Self->SyncSchemeOnFollower(txc, ctx, status, error))
                return false;
            if (status != NKikimrTxDataShard::TError::OK)
                return true;
        }
        auto tableIt = Self->TableInfos.find(request.GetTablePathId());
        if (!AppData()->FeatureFlags.GetEnableHnswIndex() || Self->PathOwnerId != request.GetTableOwnerId()
                || tableIt == Self->TableInfos.end()
                || tableIt->second->GetTableSchemaVersion() != request.GetSchemaVersion())
            return true;
        const auto& table = *tableIt->second;
        const auto indexId = Self->ResolveHnswIndexId(table.LocalTid, {});
        const auto* metadata = Self->FindHnswIndexInfo(table.LocalTid, indexId);
        if (!metadata || metadata->Settings.vector_type() != Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT) {
            return true;
        }
        result.SetSchemaVersion(table.GetTableSchemaVersion());
        if (!Self->IsFollower()) {
            const auto* cached = Self->FindHnswIndexEntry(table.LocalTid, indexId);
            if (cached && cached->Index) {
                const auto lower = Max(cached->Index->GetBaseVersion(), cached->Changes->LowerReadVersion);
                if (lower < Self->Pipeline.GetUnreadableEdge()
                        && !Self->VolatileTxManager.HasVolatileTxsAtSnapshot(lower)) {
                    Self->PromoteImmediatePostExecuteEdges(lower, EPromotePostExecuteEdges::RepeatableRead, txc);
                    Self->SnapshotManager.PromoteCompleteEdge(lower, txc);
                }
            } else {
                Self->RequestHnswIndexBuild(table.LocalTid, metadata->VectorColumnTag, metadata->Settings, indexId);
            }
            Self->PromoteFollowerReadEdge(txc);
        }
        const auto [edge, repeatable] = Self->SnapshotManager.GetFollowerReadEdge();
        result.SetReadStep(edge.Step);
        result.SetReadTxId(edge.TxId);
        result.SetRepeatable(repeatable);
        if (!repeatable || edge < TRowVersion(request.GetTargetStep(), request.GetTargetTxId()))
            return true;
        if (Self->IsFollower())
            Self->PrepareFollowerHnswIndex(table.LocalTid, txc.DB, edge, indexId);
        const auto* cached = Self->FindHnswIndexEntry(table.LocalTid, indexId);
        if (!cached) {
            return true;
        }
        const auto& entry = *cached;
        const bool notRequired = Self->IsFollower()
            ? entry.FollowerNotRequired && entry.FollowerJournalTracking && entry.Changes->Valid
                && entry.FollowerLeaderGeneration == result.GetLeaderGeneration()
                && entry.FollowerSnapshot.GetLeaderGeneration() == result.GetLeaderGeneration()
                && Self->Executor()->Step() >= entry.FollowerSnapshot.GetCheckpointStep()
            : !entry.Index && !entry.Building && entry.BelowMinRows;
        result.SetNotRequired(notRequired);
        result.SetReady(notRequired);
        if (!notRequired) {
            if (auto graph = Self->GetHnswIndex(table.LocalTid, metadata->VectorColumnTag,
                    metadata->Settings, false, edge, indexId)) {
                // Loading a graph is insufficient if its first search cannot
                // obtain workspace under the current node-wide memory grant.
                result.SetReady(graph->CanAdmitSearch());
            }
        }
        result.SetGraphKey(entry.GraphKey);
        return true;
    }
    void Complete(const TActorContext& ctx) override {
        ctx.Send(Request->Sender, Result.Release(), 0, Request->Cookie);
    }
private:
    TEvHnswCache::TEvStatusRequest::TPtr Request;
    THolder<TEvHnswCache::TEvStatusResponse> Result;
};

void TDataShard::Handle(TEvHnswCache::TEvStatusRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxHnswReplicaStatus(this, ev), ctx);
}

NTable::IRedoObserver* TDataShard::GetFollowerRedoObserver() {
    // Keep existing journals complete across feature-flag changes, even when
    // no read observes the temporarily disabled feature.
    return !HnswIndexCache.empty() ? this : nullptr;
}

bool TDataShard::TrackFollowerHnswCommit(ui32 localTid, TPathId indexId, TRowVersion version) {
    auto& entry = *FindHnswIndexEntry(localTid, indexId);
    const auto step = Executor()->Step();
    std::shared_ptr<void> reservation;
    if (entry.FollowerBootstrapChanges.NeedsRecord(step)) {
        reservation = TryReserveHnswCacheMemory(THnswBootstrapChanges::RecordBytes, entry.MemoryOwner);
        if (!reservation) {
            InvalidateHnswIndex(localTid, indexId);
            DeferHnswIndexBuild(localTid, TDuration::Seconds(1), indexId);
            return false;
        }
    }
    if (!entry.FollowerBootstrapChanges.Observe(step, version, std::move(reservation))) {
        InvalidateHnswIndex(localTid, indexId);
        DeferHnswIndexBuild(localTid, TDuration::Seconds(1), indexId);
        return false;
    }
    return !entry.FollowerBootstrapChanges.Covers(step);
}

void TDataShard::OnRedoUpdate(ui32 localTid, NTable::ERowOp op, TArrayRef<const TRawTypeValue> key,
        TArrayRef<const NTable::TUpdateOp> updates, TArrayRef<const NPageCollection::TMemGlob> annex,
        TRowVersion version, ui64 txId, ui32 savepoint) {
    if (localTid == Schema::UserTables::TableId)
        ScheduleHnswFollowerSync();
    auto table = HnswIndexCache.find(localTid);
    if (table == HnswIndexCache.end()) {
        return;
    }
    const auto apply = [&](TPathId indexId, auto& entry) {
        if (!entry.FollowerJournalTracking || !entry.Changes->Valid) {
            return;
        }
        if (!txId && entry.FollowerBootstrapChanges.Covers(Executor()->Step()))
            return;
        if (entry.FollowerNotRequired) {
            entry.FollowerNotRequired = false;
            ScheduleHnswFollowerSync();
        }
        if (savepoint || op == NTable::ERowOp::Reset) {
            InvalidateHnswIndex(localTid, indexId);
            return;
        }
        bool affected = op == NTable::ERowOp::Erase;
        std::optional<TString> vector;
        for (const auto& update : updates) {
            if (update.Tag != entry.VectorColumnTag || update.Op == NTable::ECellOp::Empty)
                continue;
            affected = true;
            if (update.Op == NTable::ECellOp::Reset) {
                InvalidateHnswIndex(localTid, indexId);
                return;
            }
            if (update.Op == NTable::ECellOp::Set) {
                auto cell = update.AsCell();
                if (update.Op == NTable::ELargeObj::Extern) {
                    const auto index = cell.AsValue<ui32>();
                    if (index >= annex.size() || !annex[index].Data) {
                        InvalidateHnswIndex(localTid, indexId);
                        return;
                    }
                    cell = TCell(annex[index].Data.data(), annex[index].Data.size());
                }
                if (!cell.IsNull() && THnswIndex::IsValidVector(cell.AsBuf(), entry.Settings.vector_dimension()))
                    vector = TString(cell.AsBuf());
            }
        }
        if (txId && !entry.PendingReservations.contains(txId)) {
            auto reservation = TryReserveHnswCacheMemory(THnswIndexCacheEntry::PendingTransactionBytes, entry.MemoryOwner);
            if (!reservation) {
                InvalidateHnswIndex(localTid, indexId);
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
            THnswIndexChanges::EstimateBytes(serialized, vector ? vector->size() : 0), entry.MemoryOwner);
        if (!reservation) {
            InvalidateHnswIndex(localTid, indexId);
            return;
        }
        THnswIndexChanges::TVersion change{version, std::move(vector), std::move(reservation), Executor()->Step()};
        if (txId)
            entry.Pending[txId][std::move(serialized)] = std::move(change);
        else if (TrackFollowerHnswCommit(localTid, indexId, version))
            ApplyHnswIndexChange(localTid, std::move(serialized), std::move(change), indexId);
    };
    for (auto& [indexId, entry] : table->second) {
        apply(indexId, entry);
    }
}

void TDataShard::OnRedoCommit(ui32 localTid, ui64 txId, TRowVersion version) {
    auto table = HnswIndexCache.find(localTid);
    if (table == HnswIndexCache.end()) {
        return;
    }
    const auto apply = [&](TPathId indexId, auto& entry) {
        if (!entry.FollowerJournalTracking) {
            return;
        }
        if (entry.FollowerBootstrapChanges.Covers(Executor()->Step())) {
            entry.Pending.erase(txId);
            entry.PendingReservations.erase(txId);
            entry.UntrackedTransactions.erase(txId);
            return;
        }
        auto pending = entry.Pending.find(txId);
        if (entry.UntrackedTransactions.erase(txId)) {
            InvalidateHnswIndex(localTid, indexId);
            return;
        }
        if (pending == entry.Pending.end()) {
            return;
        }
        auto changes = std::move(pending->second);
        entry.Pending.erase(pending);
        entry.PendingReservations.erase(txId);
        if (!changes.empty() && !TrackFollowerHnswCommit(localTid, indexId, version))
            return;
        for (auto& [key, change] : changes) {
            change.Version = version;
            change.RedoStep = Executor()->Step();
            ApplyHnswIndexChange(localTid, key, std::move(change), indexId);
        }
    };
    for (auto& [indexId, entry] : table->second) {
        apply(indexId, entry);
    }
}

void TDataShard::OnRedoRemove(ui32 localTid, ui64 txId) {
    if (auto table = HnswIndexCache.find(localTid); table != HnswIndexCache.end()) {
        for (auto& [_, entry] : table->second) {
            entry.Pending.erase(txId);
            entry.PendingReservations.erase(txId);
            entry.UntrackedTransactions.erase(txId);
        }
    }
}

} // namespace NKikimr::NDataShard
