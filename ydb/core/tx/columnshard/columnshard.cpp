#include "columnshard_impl.h"
#include "columnshard_txs.h"

namespace NKikimr {

IActor* CreateColumnShard(const TActorId& tablet, TTabletStorageInfo* info) {
    return new NColumnShard::TColumnShard(info, tablet);
}

}

namespace NKikimr::NColumnShard {

IActor* CreateIndexingActor(ui64 tabletId, const TActorId& parent);
IActor* CreateCompactionActor(ui64 tabletId, const TActorId& parent);
IActor* CreateEvictionActor(ui64 tabletId, const TActorId& parent);
IActor* CreateWriteActor(ui64 tabletId, const NOlap::TIndexInfo& indexTable,
                         const TActorId& dstActor, TBlobBatch&& blobBatch, bool blobGrouppingEnabled,
                         TAutoPtr<TEvColumnShard::TEvWrite> ev, const TInstant& deadline = TInstant::Max());
IActor* CreateWriteActor(ui64 tabletId, const NOlap::TIndexInfo& indexTable,
                         const TActorId& dstActor, TBlobBatch&& blobBatch, bool blobGrouppingEnabled,
                         TAutoPtr<TEvPrivate::TEvWriteIndex> ev, const TInstant& deadline = TInstant::Max());
IActor* CreateColumnShardScan(const TActorId& scanComputeActor, ui32 scanId, ui64 txId);

void TColumnShard::BecomeBroken(const TActorContext& ctx)
{
    Become(&TThis::StateBroken);
    ctx.Send(Tablet(), new TEvents::TEvPoisonPill);
    ctx.Send(IndexingActor, new TEvents::TEvPoisonPill);
    ctx.Send(CompactionActor, new TEvents::TEvPoisonPill);
    ctx.Send(EvictionActor, new TEvents::TEvPoisonPill);
}

void TColumnShard::SwitchToWork(const TActorContext& ctx) {
    Become(&TThis::StateWork);
    LOG_S_INFO("Switched to work at " << TabletID() << " actor " << ctx.SelfID);
    IndexingActor = ctx.Register(CreateIndexingActor(TabletID(), ctx.SelfID));
    CompactionActor = ctx.Register(CreateCompactionActor(TabletID(), ctx.SelfID));
    EvictionActor = ctx.Register(CreateEvictionActor(TabletID(), ctx.SelfID));
    SignalTabletActive(ctx);
}

void TColumnShard::OnActivateExecutor(const TActorContext& ctx) {
    LOG_S_DEBUG("OnActivateExecutor at " << TabletID() << " actor " << ctx.SelfID);
    Executor()->RegisterExternalTabletCounters(TabletCountersPtr.release());
    BlobManager = std::make_unique<TBlobManager>(Info(), Executor()->Generation());

    auto& icb = *AppData(ctx)->Icb;
    BlobManager->RegisterControls(icb);
    Limits.RegisterControls(icb);
    CompactionLimits.RegisterControls(icb);
    Settings.RegisterControls(icb);

    Execute(CreateTxInitSchema(), ctx);
}

void TColumnShard::Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) {
    LOG_S_DEBUG("Handle TEvents::TEvPoisonPill");
    Y_UNUSED(ev);
    BecomeBroken(ctx);
}

void TColumnShard::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext&) {
    auto tabletId = ev->Get()->TabletId;

    if (PipeClientCache->OnConnect(ev)) {
        LOG_S_DEBUG("Connected to tablet at " << TabletID() << ", remote " << tabletId);
        return;
    }

    LOG_S_INFO("Failed to connect at " << TabletID() << ", remote " << tabletId);
}

void TColumnShard::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext&) {
    auto tabletId = ev->Get()->TabletId;
    LOG_S_DEBUG("Client pipe reset at " << TabletID() << ", remote " << tabletId);

    PipeClientCache->OnDisconnect(ev);
}

void TColumnShard::Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext&) {
    auto tabletId = ev->Get()->TabletId;
    LOG_S_DEBUG("Server pipe connected at tablet " << TabletID() << ", remote " << tabletId);
}

void TColumnShard::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext&) {
    auto tabletId = ev->Get()->TabletId;
    LOG_S_DEBUG("Server pipe reset at tablet " << TabletID() << ", remote " << tabletId);
}

void TColumnShard::Handle(TEvColumnShard::TEvProposeTransaction::TPtr& ev, const TActorContext& ctx) {
    auto& record = Proto(ev->Get());
    ui32 txKind = record.GetTxKind();
    ui64 txId = record.GetTxId();
    LOG_S_DEBUG("ProposeTransaction kind " << txKind << " txId " << txId << " at tablet " << TabletID());

    Execute(new TTxProposeTransaction(this, ev), ctx);
}

void TColumnShard::Handle(TEvTxProcessing::TEvPlanStep::TPtr& ev, const TActorContext& ctx) {
    ui64 step = ev->Get()->Record.GetStep();
    ui64 mediatorId = ev->Get()->Record.GetMediatorID();
    LOG_S_DEBUG("PlanStep " << step << " at tablet " << TabletID() << ", mediator " << mediatorId);

    Execute(new TTxPlanStep(this, ev), ctx);
}

// EvWrite -> WriteActor (attach BlobId without proto changes) -> EvWrite
void TColumnShard::Handle(TEvColumnShard::TEvWrite::TPtr& ev, const TActorContext& ctx) {
    OnYellowChannels(std::move(ev->Get()->YellowMoveChannels), std::move(ev->Get()->YellowStopChannels));

    auto& data = Proto(ev->Get()).GetData();
    const ui64 tableId = ev->Get()->Record.GetTableId();
    bool error = data.empty() || data.size() > TLimits::MAX_BLOB_SIZE || !PrimaryIndex || !IsTableWritable(tableId)
        || ev->Get()->PutStatus == NKikimrProto::ERROR;

    if (error) {
        LOG_S_WARN("Write (fail) " << data.size() << " bytes at tablet " << TabletID());

        ev->Get()->PutStatus = NKikimrProto::ERROR;
        Execute(new TTxWrite(this, ev), ctx);
    } else if (InsertTable->IsOverloaded(tableId)) {
        LOG_S_INFO("Write (overload) " << data.size() << " bytes for table " << tableId << " at tablet " << TabletID());

        ev->Get()->PutStatus = NKikimrProto::TRYLATER;
        Execute(new TTxWrite(this, ev), ctx);
    } else if (ev->Get()->BlobId.IsValid()) {
        LOG_S_DEBUG("Write (record) " << data.size() << " bytes at tablet " << TabletID());

        Execute(new TTxWrite(this, ev), ctx);
    } else {
        if (IsAnyChannelYellowStop()) {
            LOG_S_ERROR("Write (out of disk space) at tablet " << TabletID());

            IncCounter(COUNTER_OUT_OF_SPACE);
            ev->Get()->PutStatus = NKikimrProto::TRYLATER;
            Execute(new TTxWrite(this, ev), ctx);
        } else {
            LOG_S_DEBUG("Write (blob) " << data.size() << " bytes at tablet " << TabletID());

            ev->Get()->MaxSmallBlobSize = Settings.MaxSmallBlobSize;

            ctx.Register(CreateWriteActor(TabletID(), PrimaryIndex->GetIndexInfo(), ctx.SelfID,
                BlobManager->StartBlobBatch(), Settings.BlobWriteGrouppingEnabled, ev->Release()));
        }
    }
}

void TColumnShard::Handle(TEvColumnShard::TEvRead::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    TRowVersion readVersion(msg->Record.GetPlanStep(), msg->Record.GetTxId());
    TRowVersion maxReadVersion = GetMaxReadVersion();
    LOG_S_DEBUG("Read at tablet " << TabletID() << " version=" << readVersion << " readable=" << maxReadVersion);

    if (maxReadVersion < readVersion) {
        WaitingReads.emplace(readVersion, std::move(ev));
        WaitPlanStep(readVersion.Step);
        return;
    }

    Execute(new TTxRead(this, ev), ctx);
}

void TColumnShard::Handle(TEvPrivate::TEvWriteIndex::TPtr& ev, const TActorContext& ctx) {
    auto& blobs = ev->Get()->Blobs;
    bool isCompaction = ev->Get()->GranuleCompaction;
    if (isCompaction && blobs.empty()) {
        ev->Get()->PutStatus = NKikimrProto::OK;
    }

    if (ev->Get()->PutStatus == NKikimrProto::UNKNOWN) {
        if (IsAnyChannelYellowStop()) {
            LOG_S_ERROR("WriteIndex (out of disk space) at tablet " << TabletID());

            IncCounter(COUNTER_OUT_OF_SPACE);
            ev->Get()->PutStatus = NKikimrProto::TRYLATER;
            Execute(new TTxWriteIndex(this, ev), ctx);
        } else {
            LOG_S_DEBUG("WriteIndex (" << blobs.size() << " blobs) at tablet " << TabletID());

            Y_VERIFY(!blobs.empty());
            ctx.Register(CreateWriteActor(TabletID(), NOlap::TIndexInfo("dummy", 0), ctx.SelfID,
                BlobManager->StartBlobBatch(), Settings.BlobWriteGrouppingEnabled, ev->Release()));
        }
    } else {
        if (ev->Get()->PutStatus == NKikimrProto::OK) {
            LOG_S_DEBUG("WriteIndex (records) at tablet " << TabletID());
        } else {
            LOG_S_INFO("WriteIndex error at tablet " << TabletID());
        }

        OnYellowChannels(std::move(ev->Get()->YellowMoveChannels), std::move(ev->Get()->YellowStopChannels));
        Execute(new TTxWriteIndex(this, ev), ctx);
    }
}

void TColumnShard::Handle(TEvColumnShard::TEvScan::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    ui64 txId = msg->Record.GetTxId();
    const auto& snapshot = msg->Record.GetSnapshot();
    TRowVersion readVersion(snapshot.GetStep(), snapshot.GetTxId());
    TRowVersion maxReadVersion = GetMaxReadVersion();
    LOG_S_DEBUG("Scan at tablet " << TabletID() << " version=" << readVersion << " readable=" << maxReadVersion);

    if (maxReadVersion < readVersion) {
        WaitingScans.emplace(readVersion, std::move(ev));
        WaitPlanStep(readVersion.Step);
        return;
    }

    ScanTxInFlight.insert({txId, TAppData::TimeProvider->Now()});
    SetCounter(COUNTER_SCAN_IN_FLY, ScanTxInFlight.size());
    Execute(new TTxScan(this, ev), ctx);
}

void TColumnShard::Handle(TEvPrivate::TEvScanStats::TPtr& ev, const TActorContext &ctx) {
    Y_UNUSED(ctx);

    IncCounter(COUNTER_SCANNED_ROWS, ev->Get()->Rows);
    IncCounter(COUNTER_SCANNED_BYTES, ev->Get()->Bytes);
}

void TColumnShard::Handle(TEvPrivate::TEvReadFinished::TPtr& ev, const TActorContext &ctx) {
    Y_UNUSED(ctx);
    ui64 readCookie = ev->Get()->RequestCookie;
    LOG_S_DEBUG("Finished read cookie: " << readCookie << " at tablet " << TabletID());
    InFlightReadsTracker.RemoveInFlightRequest(ev->Get()->RequestCookie, *BlobManager);

    ui64 txId = ev->Get()->TxId;
    if (ScanTxInFlight.count(txId)) {
        TDuration duration = TAppData::TimeProvider->Now() - ScanTxInFlight[txId];
        IncCounter(COUNTER_SCAN_LATENCY, duration);
        ScanTxInFlight.erase(txId);
        SetCounter(COUNTER_SCAN_IN_FLY, ScanTxInFlight.size());
    }
}

void TColumnShard::Handle(TEvColumnShard::TEvReadBlobRanges::TPtr& ev, const TActorContext& ctx) {
    LOG_S_DEBUG("Read blob ranges at tablet " << TabletID() << ev->Get()->Record);
    Execute(new TTxReadBlobRanges(this, ev), ctx);
}

void TColumnShard::Handle(TEvPrivate::TEvPeriodicWakeup::TPtr& ev, const TActorContext& ctx) {
    if (ev->Get()->Manual) {
        EnqueueBackgroundActivities();
        return;
    }

    if (LastBackActivation < TInstant::Now() - ActivationPeriod) {
        SendWaitPlanStep(GetOutdatedStep());
    }

    ctx.Schedule(ActivationPeriod, new TEvPrivate::TEvPeriodicWakeup());
}

void TColumnShard::UpdateBlobMangerCounters() {
    const auto counters = BlobManager->GetCountersUpdate();
    IncCounter(COUNTER_BLOB_MANAGER_GC_REQUESTS, counters.GcRequestsSent);
    IncCounter(COUNTER_BLOB_MANAGER_KEEP_BLOBS, counters.BlobKeepEntries);
    IncCounter(COUNTER_BLOB_MANAGER_DONT_KEEP_BLOBS, counters.BlobDontKeepEntries);
    IncCounter(COUNTER_BLOB_MANAGER_SKIPPED_BLOBS, counters.BlobSkippedEntries);
    IncCounter(COUNTER_SMALL_BLOB_WRITE_COUNT, counters.SmallBlobsWritten);
    IncCounter(COUNTER_SMALL_BLOB_WRITE_BYTES, counters.SmallBlobsBytesWritten);
    IncCounter(COUNTER_SMALL_BLOB_DELETE_COUNT, counters.SmallBlobsDeleted);
    IncCounter(COUNTER_SMALL_BLOB_DELETE_BYTES, counters.SmallBlobsBytesDeleted);
}

void TColumnShard::UpdateInsertTableCounters() {
    auto& prepared = InsertTable->GetCountersPrepared();
    auto& committed = InsertTable->GetCountersCommitted();

    SetCounter(COUNTER_PREPARED_RECORDS, prepared.Rows);
    SetCounter(COUNTER_PREPARED_BYTES, prepared.Bytes);
    SetCounter(COUNTER_COMMITTED_RECORDS, committed.Rows);
    SetCounter(COUNTER_COMMITTED_BYTES, committed.Bytes);

    LOG_S_DEBUG("InsertTable. Prepared: " << prepared.Bytes << " in " << prepared.Rows
        << " records, committed: " << committed.Bytes << " in " << committed.Rows
        << " records at tablet " << TabletID());
}

void TColumnShard::UpdateIndexCounters() {
    if (!PrimaryIndex) {
        return;
    }

    auto& stats = PrimaryIndex->GetTotalStats();
    SetCounter(COUNTER_INDEX_TABLES, stats.Tables);
    SetCounter(COUNTER_INDEX_GRANULES, stats.Granules);
    SetCounter(COUNTER_INDEX_EMPTY_GRANULES, stats.EmptyGranules);
    SetCounter(COUNTER_INDEX_OVERLOADED_GRANULES, stats.OverloadedGranules);
    SetCounter(COUNTER_INDEX_COLUMN_RECORDS, stats.ColumnRecords);
    SetCounter(COUNTER_INDEX_COLUMN_METADATA_BYTES, stats.ColumnMetadataBytes);
    SetCounter(COUNTER_INSERTED_PORTIONS, stats.Inserted.Portions);
    SetCounter(COUNTER_INSERTED_BLOBS, stats.Inserted.Blobs);
    SetCounter(COUNTER_INSERTED_ROWS, stats.Inserted.Rows);
    SetCounter(COUNTER_INSERTED_BYTES, stats.Inserted.Bytes);
    SetCounter(COUNTER_INSERTED_RAW_BYTES, stats.Inserted.RawBytes);
    SetCounter(COUNTER_COMPACTED_PORTIONS, stats.Compacted.Portions);
    SetCounter(COUNTER_COMPACTED_BLOBS, stats.Compacted.Blobs);
    SetCounter(COUNTER_COMPACTED_ROWS, stats.Compacted.Rows);
    SetCounter(COUNTER_COMPACTED_BYTES, stats.Compacted.Bytes);
    SetCounter(COUNTER_COMPACTED_RAW_BYTES, stats.Compacted.RawBytes);
    SetCounter(COUNTER_SPLIT_COMPACTED_PORTIONS, stats.SplitCompacted.Portions);
    SetCounter(COUNTER_SPLIT_COMPACTED_BLOBS, stats.SplitCompacted.Blobs);
    SetCounter(COUNTER_SPLIT_COMPACTED_ROWS, stats.SplitCompacted.Rows);
    SetCounter(COUNTER_SPLIT_COMPACTED_BYTES, stats.SplitCompacted.Bytes);
    SetCounter(COUNTER_SPLIT_COMPACTED_RAW_BYTES, stats.SplitCompacted.RawBytes);
    SetCounter(COUNTER_INACTIVE_PORTIONS, stats.Inactive.Portions);
    SetCounter(COUNTER_INACTIVE_BLOBS, stats.Inactive.Blobs);
    SetCounter(COUNTER_INACTIVE_ROWS, stats.Inactive.Rows);
    SetCounter(COUNTER_INACTIVE_BYTES, stats.Inactive.Bytes);
    SetCounter(COUNTER_INACTIVE_RAW_BYTES, stats.Inactive.RawBytes);

    SetCounter(COUNTER_EVICTED_PORTIONS, stats.Evicted.Portions);
    SetCounter(COUNTER_EVICTED_BLOBS, stats.Evicted.Blobs);
    SetCounter(COUNTER_EVICTED_ROWS, stats.Evicted.Rows);
    SetCounter(COUNTER_EVICTED_BYTES, stats.Evicted.Bytes);
    SetCounter(COUNTER_EVICTED_RAW_BYTES, stats.Evicted.RawBytes);
}

void TColumnShard::UpdateResourceMetrics(const TUsage& usage) {
    auto * metrics = Executor()->GetResourceMetrics();
    if (!metrics) {
        return;
    }

    ui64 storageBytes =
        TabletCounters->Simple()[COUNTER_PREPARED_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_COMMITTED_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_INSERTED_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_COMPACTED_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_SPLIT_COMPACTED_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_INACTIVE_BYTES].Get();

    ui64 memory =
        Tables.size() * sizeof(TTableInfo) +
        PathsToDrop.size() * sizeof(ui64) +
        Ttl.PathsCount() * sizeof(TTtl::TDescription) +
        SchemaPresets.size() * sizeof(TSchemaPreset) +
        //TtlSettingsPresets.size() * sizeof(TTtlSettingsPreset) +
        AltersInFlight.size() * sizeof(TAlterMeta) +
        CommitsInFlight.size() * sizeof(TCommitMeta) +
        TabletCounters->Simple()[COUNTER_PREPARED_RECORDS].Get() * sizeof(NOlap::TInsertedData) +
        TabletCounters->Simple()[COUNTER_COMMITTED_RECORDS].Get() * sizeof(NOlap::TInsertedData);
    if (PrimaryIndex) {
        memory += PrimaryIndex->MemoryUsage();
    }

    const TActorContext& ctx = TlsActivationContext->AsActorContext();
    TInstant now = AppData(ctx)->TimeProvider->Now();
    metrics->CPU.Increment(usage.CPUExecTime, now);
    metrics->Network.Increment(usage.Network, now);
    //metrics->StorageSystem
    metrics->StorageUser.Set(storageBytes);
    metrics->Memory.Set(memory);
    //metrics->ReadThroughput
    //metrics->WriteThroughput

    metrics->TryUpdate(ctx);
}

}
