#include "columnshard_impl.h"
#include "columnshard_schema.h"
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>

namespace NKikimr::NColumnShard {

// NOTE: We really want to batch log records by default in columnshards!
// But in unittests we want to test both scenarios
bool gAllowLogBatchingDefaultValue = true;

namespace
{

NTabletPipe::TClientConfig GetPipeClientConfig() {
    NTabletPipe::TClientConfig config;
    config.RetryPolicy = {
        .MinRetryTime = TDuration::MilliSeconds(50),
        .MaxRetryTime = TDuration::Seconds(2),
    };
    return config;
}

}

TColumnShard::TColumnShard(TTabletStorageInfo* info, const TActorId& tablet)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, nullptr)
    , PipeClientCache(NTabletPipe::CreateBoundedClientCache(new NTabletPipe::TBoundedClientCacheConfig(), GetPipeClientConfig()))
    , InsertTable(std::make_unique<NOlap::TInsertTable>())
{
    TabletCountersPtr.reset(new TProtobufTabletCounters<
        ESimpleCounters_descriptor,
        ECumulativeCounters_descriptor,
        EPercentileCounters_descriptor,
        ETxTypes_descriptor
    >());
    TabletCounters = TabletCountersPtr.get();
}

void TColumnShard::OnDetach(const TActorContext& ctx) {
    Die(ctx);
}

void TColumnShard::OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ev);
    Die(ctx);
}

void TColumnShard::TryRegisterMediatorTimeCast() {
    if (MediatorTimeCastRegistered) {
        return; // already registered
    }

    if (!ProcessingParams) {
        return; // cannot register without processing params
    }

    Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvRegisterTablet(TabletID(), *ProcessingParams));
    MediatorTimeCastRegistered = true;
}

void TColumnShard::UnregisterMediatorTimeCast() {
    if (MediatorTimeCastRegistered) {
        Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvUnregisterTablet(TabletID()));
        MediatorTimeCastRegistered = false;
        MediatorTimeCastEntry = nullptr;
    }
}

bool TColumnShard::WaitPlanStep(ui64 step) {
    if (step <= LastPlannedStep) {
        return false;
    }
    if (MediatorTimeCastEntry) {
        ui64 mediatorStep = MediatorTimeCastEntry->Get(TabletID());
        if (step <= mediatorStep) {
            return false;
        }
    }
    if (MediatorTimeCastRegistered) {
        if (MediatorTimeCastWaitingSteps.empty() ||
            step < *MediatorTimeCastWaitingSteps.begin())
        {
            MediatorTimeCastWaitingSteps.insert(step);
            SendWaitPlanStep(step);
            LOG_S_DEBUG("Waiting for PlanStep# " << step << " from mediator time cast");
            return true;
        }
    }
    return false;
}

void TColumnShard::SendWaitPlanStep(ui64 step) {
    Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvWaitPlanStep(TabletID(), step));
}

void TColumnShard::RescheduleWaitingReads() {
    ui64 minWaitingStep = Max<ui64>();
    TRowVersion maxReadVersion = GetMaxReadVersion();
    for (auto it = WaitingReads.begin(); it != WaitingReads.end();) {
        if (maxReadVersion < it->first) {
            minWaitingStep = Min(minWaitingStep, it->first.Step);
            break;
        }
        TActivationContext::Send(it->second.Release());
        it = WaitingReads.erase(it);
    }
    for (auto it = WaitingScans.begin(); it != WaitingScans.end();) {
        if (maxReadVersion < it->first) {
            minWaitingStep = Min(minWaitingStep, it->first.Step);
            break;
        }
        TActivationContext::Send(it->second.Release());
        it = WaitingScans.erase(it);
    }
    if (minWaitingStep != Max<ui64>()) {
        WaitPlanStep(minWaitingStep);
    }
}

TRowVersion TColumnShard::GetMaxReadVersion() const {
    if (PlanQueue) {
        // We may only read just before the first transaction in the queue
        auto it = PlanQueue.begin();
        return TRowVersion(it->Step, it->TxId).Prev();
    }
    ui64 step = LastPlannedStep;
    if (MediatorTimeCastEntry) {
        ui64 mediatorStep = MediatorTimeCastEntry->Get(TabletID());
        step = Max(step, mediatorStep);
    }
    return TRowVersion(step, Max<ui64>());
}

ui64 TColumnShard::GetOutdatedStep() const {
    ui64 step = LastPlannedStep;
    if (MediatorTimeCastEntry) {
        step = Max(step, MediatorTimeCastEntry->Get(TabletID()));
    }
    return step;
}

ui64 TColumnShard::GetAllowedStep() const {
    return Max(GetOutdatedStep() + 1, TAppData::TimeProvider->Now().MilliSeconds());
}

ui64 TColumnShard::GetMinReadStep() const {
    ui64 delayMillisec = MaxReadStaleness.MilliSeconds();
    ui64 passedStep = GetOutdatedStep();
    ui64 minReadStep = (passedStep > delayMillisec ? passedStep - delayMillisec : 0);
    return minReadStep;
}

bool TColumnShard::HaveOutdatedTxs() const {
    if (!DeadlineQueue) {
        return false;
    }
    ui64 step = GetOutdatedStep();
    auto it = DeadlineQueue.begin();
    // Return true if the first transaction has no chance to be planned
    return it->MaxStep <= step;
}

TWriteId TColumnShard::GetLongTxWrite(NIceDb::TNiceDb& db, const NLongTxService::TLongTxId& longTxId) {
    auto it = LongTxWritesByUniqueId.find(longTxId.UniqueId);
    if (it != LongTxWritesByUniqueId.end()) {
        return (TWriteId)it->second->WriteId;
    }

    TWriteId writeId = ++LastWriteId;
    auto& lw = LongTxWrites[writeId];
    lw.WriteId = (ui64)writeId;
    lw.LongTxId = longTxId;
    LongTxWritesByUniqueId[longTxId.UniqueId] = &lw;

    Schema::SaveSpecialValue(db, Schema::EValueIds::LastWriteId, (ui64)writeId);
    Schema::SaveLongTxWrite(db, writeId, longTxId);

    return writeId;
}

void TColumnShard::AddLongTxWrite(TWriteId writeId, ui64 txId) {
    auto& lw = LongTxWrites.at(writeId);
    lw.PreparedTxId = txId;
}

void TColumnShard::LoadLongTxWrite(TWriteId writeId, const NLongTxService::TLongTxId& longTxId) {
    auto& lw = LongTxWrites[writeId];
    lw.WriteId = (ui64)writeId;
    lw.LongTxId = longTxId;
    LongTxWritesByUniqueId[longTxId.UniqueId] = &lw;
}

bool TColumnShard::RemoveLongTxWrite(NIceDb::TNiceDb& db, TWriteId writeId, ui64 txId) {
    if (auto* lw = LongTxWrites.FindPtr(writeId)) {
        ui64 prepared = lw->PreparedTxId;
        if (!prepared || txId == prepared) {
            Schema::EraseLongTxWrite(db, writeId);
            LongTxWritesByUniqueId.erase(lw->LongTxId.UniqueId);
            LongTxWrites.erase(writeId);
            return true;
        }
    }
    return false;
}

bool TColumnShard::RemoveTx(NTable::TDatabase& database, ui64 txId) {
    auto it = BasicTxInfo.find(txId);
    if (it == BasicTxInfo.end()) {
        return false;
    }

    NIceDb::TNiceDb db(database);

    switch (it->second.TxKind) {
        case NKikimrTxColumnShard::TX_KIND_SCHEMA: {
            AltersInFlight.erase(txId);
            break;
        }
        case NKikimrTxColumnShard::TX_KIND_COMMIT: {
            if (auto* meta = CommitsInFlight.FindPtr(txId)) {
                if (meta->MetaShard == 0) {
                    for (TWriteId writeId : meta->WriteIds) {
                        // TODO: we probably need to have more complex
                        // logic in the future, when there are multiple
                        // inflight commits for the same writeId.
                        RemoveLongTxWrite(db, writeId, txId);
                    }
                }
                TBlobGroupSelector dsGroupSelector(Info());
                NOlap::TDbWrapper dbTable(database, &dsGroupSelector);
                InsertTable->Abort(dbTable, meta->MetaShard, meta->WriteIds);

                CommitsInFlight.erase(txId);
            }
            break;
        }
        default: {
            Y_FAIL("Unsupported TxKind");
        }
    }

    if (it->second.PlanStep != 0) {
        PlanQueue.erase(TPlanQueueItem(it->second.PlanStep, txId));
        RescheduleWaitingReads();
    } else if (it->second.MaxStep != Max<ui64>()) {
        DeadlineQueue.erase(TDeadlineQueueItem(it->second.MaxStep, txId));
    }

    BasicTxInfo.erase(it);

    Schema::EraseTxInfo(db, txId);
    return true;
}

void TColumnShard::TryAbortWrites(NIceDb::TNiceDb& db, NOlap::TDbWrapper& dbTable, THashSet<TWriteId>&& writesToAbort) {
    std::vector<TWriteId> failedAborts;
    for (auto& writeId : writesToAbort) {
        if (!RemoveLongTxWrite(db, writeId)) {
            failedAborts.push_back(writeId);
        }
        BatchCache.EraseInserted(TWriteId(writeId));
    }
    for (auto& writeId : failedAborts) {
        writesToAbort.erase(writeId);
    }
    if (!writesToAbort.empty()) {
        InsertTable->Abort(dbTable, {}, writesToAbort);
    }
}

void TColumnShard::UpdateSchemaSeqNo(const TMessageSeqNo& seqNo, NTabletFlatExecutor::TTransactionContext& txc) {
    if (LastSchemaSeqNo < seqNo) {
        LastSchemaSeqNo = seqNo;

        NIceDb::TNiceDb db(txc.DB);
        Schema::SaveSpecialValue(db, Schema::EValueIds::LastSchemaSeqNoGeneration, LastSchemaSeqNo.Generation);
        Schema::SaveSpecialValue(db, Schema::EValueIds::LastSchemaSeqNoRound, LastSchemaSeqNo.Round);
    }
}

void TColumnShard::ProtectSchemaSeqNo(const NKikimrTxColumnShard::TSchemaSeqNo& seqNoProto,
                                      NTabletFlatExecutor::TTransactionContext& txc) {
    auto seqNo = SeqNoFromProto(seqNoProto);
    if (LastSchemaSeqNo <= seqNo) {
        UpdateSchemaSeqNo(++seqNo, txc);
    }
}

bool TColumnShard::IsTableWritable(ui64 tableId) const {
    auto it = Tables.find(tableId);
    if (it == Tables.end()) {
        return false;
    }
    return !it->second.IsDropped();
}

ui32 TColumnShard::EnsureSchemaPreset(NIceDb::TNiceDb& db, const NKikimrSchemeOp::TColumnTableSchemaPreset& presetProto,
                                      const TRowVersion& version) {
    if (!SchemaPresets.contains(presetProto.GetId())) {
        auto& preset = SchemaPresets[presetProto.GetId()];
        preset.Id = presetProto.GetId();
        preset.Name = presetProto.GetName();
        auto& info = preset.Versions[version];
        info.SetId(preset.Id);
        info.SetSinceStep(version.Step);
        info.SetSinceTxId(version.TxId);
        *info.MutableSchema() = presetProto.GetSchema();

        Y_VERIFY(preset.Name == "default", "Only schema preset named 'default' is supported");

        Schema::SaveSchemaPresetInfo(db, preset.Id, preset.Name);
        Schema::SaveSchemaPresetVersionInfo(db, preset.Id, version, info);
        SetCounter(COUNTER_TABLE_PRESETS, SchemaPresets.size());
    }

    return presetProto.GetId();
}

void TColumnShard::RunSchemaTx(const NKikimrTxColumnShard::TSchemaTxBody& body, const TRowVersion& version,
                               NTabletFlatExecutor::TTransactionContext& txc) {
    switch (body.TxBody_case()) {
        case NKikimrTxColumnShard::TSchemaTxBody::kInitShard: {
            RunInit(body.GetInitShard(), version, txc);
            return;
        }
        case NKikimrTxColumnShard::TSchemaTxBody::kEnsureTables: {
            for (const auto& tableProto : body.GetEnsureTables().GetTables()) {
                RunEnsureTable(tableProto, version, txc);
            }
            return;
        }
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterTable: {
            RunAlterTable(body.GetAlterTable(), version, txc);
            return;
        }
        case NKikimrTxColumnShard::TSchemaTxBody::kDropTable: {
            RunDropTable(body.GetDropTable(), version, txc);
            return;
        }
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterStore: {
            RunAlterStore(body.GetAlterStore(), version, txc);
            return;
        }
        case NKikimrTxColumnShard::TSchemaTxBody::TXBODY_NOT_SET: {
            break;
        }
    }

    Y_FAIL("Unsupported schema tx type");
}

void TColumnShard::RunInit(const NKikimrTxColumnShard::TInitShard& proto, const TRowVersion& version,
                           NTabletFlatExecutor::TTransactionContext& txc) {
    Y_UNUSED(version);

    NIceDb::TNiceDb db(txc.DB);

    if (proto.HasStorePathId()) {
        StorePathId = proto.GetStorePathId();
        Schema::SaveSpecialValue(db, Schema::EValueIds::StorePathId, StorePathId);
    }
}

void TColumnShard::RunEnsureTable(const NKikimrTxColumnShard::TCreateTable& tableProto, const TRowVersion& version,
                                  NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    const ui64 pathId = tableProto.GetPathId();
    if (!Tables.contains(pathId)) {
        auto& table = Tables[pathId];
        table.PathId = pathId;
        auto& tableVerProto = table.Versions[version];
        tableVerProto.SetPathId(pathId);

        Y_VERIFY(!tableProto.HasSchema(), "Tables with explicit schema are not supported");

        ui32 schemaPresetId = 0;
        if (tableProto.HasSchemaPreset()) {
            schemaPresetId = EnsureSchemaPreset(db, tableProto.GetSchemaPreset(), version);
            tableVerProto.SetSchemaPresetId(schemaPresetId);
        }

        if (tableProto.HasTtlSettings()) {
            *tableVerProto.MutableTtlSettings() = tableProto.GetTtlSettings();
            Ttl.SetPathTtl(pathId, TTtl::TDescription(tableProto.GetTtlSettings()));
            SetCounter(COUNTER_TABLE_TTLS, Ttl.PathsCount());
        }

        if (!PrimaryIndex && schemaPresetId) {
            auto& schemaPresetVerProto = SchemaPresets[schemaPresetId].Versions[version];
            TMap<NOlap::TSnapshot, NOlap::TIndexInfo> schemaPreset;
            schemaPreset.emplace(NOlap::TSnapshot{version.Step, version.TxId},
                                 ConvertSchema(schemaPresetVerProto.GetSchema()));

            SetPrimaryIndex(std::move(schemaPreset));
        }

        tableVerProto.SetSchemaPresetVersionAdj(tableProto.GetSchemaPresetVersionAdj());
        tableVerProto.SetTtlSettingsPresetVersionAdj(tableProto.GetTtlSettingsPresetVersionAdj());

        Schema::SaveTableInfo(db, table.PathId);
        Schema::SaveTableVersionInfo(db, table.PathId, version, tableVerProto);
        SetCounter(COUNTER_TABLES, Tables.size());
    }
}

void TColumnShard::RunAlterTable(const NKikimrTxColumnShard::TAlterTable& alterProto, const TRowVersion& version,
                                 NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    const ui64 pathId = alterProto.GetPathId();
    auto* tablePtr = Tables.FindPtr(pathId);
    Y_VERIFY(tablePtr && !tablePtr->IsDropped(), "AlterTable on a dropped or non-existent table");
    auto& table = *tablePtr;

    Y_VERIFY(!alterProto.HasSchema(), "Tables with explicit schema are not supported");

    auto& info = table.Versions[version];

    if (alterProto.HasSchemaPreset()) {
        info.SetSchemaPresetId(EnsureSchemaPreset(db, alterProto.GetSchemaPreset(), version));
    }

    if (alterProto.HasTtlSettings()) {
        *info.MutableTtlSettings() = alterProto.GetTtlSettings();
        Ttl.SetPathTtl(pathId, TTtl::TDescription(alterProto.GetTtlSettings()));
    } else {
        Ttl.DropPathTtl(pathId);
    }

    info.SetSchemaPresetVersionAdj(alterProto.GetSchemaPresetVersionAdj());

    Schema::SaveTableVersionInfo(db, table.PathId, version, info);
}

void TColumnShard::RunDropTable(const NKikimrTxColumnShard::TDropTable& dropProto, const TRowVersion& version,
                                NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    const ui64 pathId = dropProto.GetPathId();
    auto* table = Tables.FindPtr(pathId);
    Y_VERIFY_DEBUG(table && !table->IsDropped());
    if (table && !table->IsDropped()) {
        PathsToDrop.insert(pathId);
        Ttl.DropPathTtl(pathId);

        // TODO: Allow to read old snapshots after DROP
        TBlobGroupSelector dsGroupSelector(Info());
        NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);
        THashSet<TWriteId> writesToAbort = InsertTable->DropPath(dbTable, pathId);
        TryAbortWrites(db, dbTable, std::move(writesToAbort));

        table->DropVersion = version;
        Schema::SaveTableDropVersion(db, pathId, version.Step, version.TxId);
    }
}

void TColumnShard::RunAlterStore(const NKikimrTxColumnShard::TAlterStore& proto, const TRowVersion& version,
                                 NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    if (proto.HasStorePathId()) {
        StorePathId = proto.GetStorePathId();
        Schema::SaveSpecialValue(db, Schema::EValueIds::StorePathId, StorePathId);
    }

    TMap<NOlap::TSnapshot, NOlap::TIndexInfo> schemaPreset;

    for (ui32 id : proto.GetDroppedSchemaPresets()) {
        if (!SchemaPresets.contains(id)) {
            continue;
        }
        auto& preset = SchemaPresets.at(id);
        Y_VERIFY(preset.Name != "default", "Cannot drop the default preset");
        preset.DropVersion = version;
        Schema::SaveSchemaPresetDropVersion(db, id, version);
    }

    for (const auto& presetProto : proto.GetSchemaPresets()) {
        if (!SchemaPresets.contains(presetProto.GetId())) {
            continue; // we don't update presets that we don't use
        }

        auto& preset = SchemaPresets[presetProto.GetId()];
        auto& info = preset.Versions[version];
        info.SetId(preset.Id);
        info.SetSinceStep(version.Step);
        info.SetSinceTxId(version.TxId);
        *info.MutableSchema() = presetProto.GetSchema();

        if (preset.Name == "default") {
            schemaPreset.emplace(NOlap::TSnapshot{version.Step, version.TxId}, ConvertSchema(info.GetSchema()));
        }

        Schema::SaveSchemaPresetVersionInfo(db, preset.Id, version, info);
    }

    if (!schemaPreset.empty()) {
        SetPrimaryIndex(std::move(schemaPreset));
    }
}

void TColumnShard::SetPrimaryIndex(TMap<NOlap::TSnapshot, NOlap::TIndexInfo>&& schemaVersions) {
    for (auto& [snap, indexInfo] : schemaVersions) {
        for (auto& columnName : Ttl.TtlColumns()) {
            indexInfo.AddTtlColumn(columnName);
        }

        if (!PrimaryIndex) {
            PrimaryIndex = std::make_unique<NOlap::TColumnEngineForLogs>(std::move(indexInfo), TabletID());
            SetCounter(COUNTER_INDEXES, 1);
        } else {
            PrimaryIndex->UpdateDefaultSchema(snap, std::move(indexInfo));
        }
    }
}

void TColumnShard::EnqueueBackgroundActivities(bool periodic, bool insertOnly) {
    if (periodic && LastBackActivation > TInstant::Now() - ActivationPeriod) {
        return;
    }

    const TActorContext& ctx = TActivationContext::ActorContextFor(SelfId());
    SendPeriodicStats();

    if (insertOnly) {
        if (auto event = SetupIndexation()) {
            ctx.Send(IndexingActor, event.release());
        }
        return;
    }

    // Preventing conflicts between indexing and compaction leads to election between them.
    // Indexing vs compaction probability depends on index and insert table overload status.
    // Prefer compaction: 25% by default; 50% if IndexOverloaded(); 6.25% if InsertTableOverloaded().
    ui32 mask = IndexOverloaded() ? 0x1 : 0x3;
    if (InsertTableOverloaded()) {
        mask = 0x0F;
    }
    bool preferIndexing = (++BackgroundActivation) & mask;

    if (preferIndexing) {
        if (auto evIdx = SetupIndexation()) {
            ctx.Send(IndexingActor, evIdx.release());
        } else if (auto event = SetupCompaction()) {
            ctx.Send(CompactionActor, event.release());
        }
    } else {
        if (auto event = SetupCompaction()) {
            ctx.Send(CompactionActor, event.release());
        } else if (auto evIdx = SetupIndexation()) {
            ctx.Send(IndexingActor, evIdx.release());
        }
    }

    if (auto event = SetupCleanup()) {
        ctx.Send(SelfId(), event.release());
    }

    if (auto event = SetupTtl()) {
        if (event->NeedWrites()) {
            ctx.Send(EvictionActor, event.release());
        } else {
            ctx.Send(SelfId(), event->TxEvent.release());
        }
    }

    LastBackActivation = TInstant::Now();
}

std::unique_ptr<TEvPrivate::TEvIndexing> TColumnShard::SetupIndexation() {
    if (ActiveIndexingOrCompaction) {
        LOG_S_DEBUG("Indexing/compaction already in progress at tablet " << TabletID());
        return {};
    }
    if (!PrimaryIndex) {
        LOG_S_NOTICE("Indexing not started: no index at tablet " << TabletID());
        return {};
    }

    ui32 blobs = 0;
    ui32 ignored = 0;
    ui64 size = 0;
    ui64 bytesToIndex = 0;
    TVector<const NOlap::TInsertedData*> dataToIndex;
    dataToIndex.reserve(TLimits::MIN_SMALL_BLOBS_TO_INSERT);
    THashMap<ui64, ui64> overloadedPathGranules;
    for (auto& [pathId, committed] : InsertTable->GetCommitted()) {
        for (auto& data : committed) {
            ui32 dataSize = data.BlobSize();
            Y_VERIFY(dataSize);

            size += dataSize;
            if (bytesToIndex && (bytesToIndex + dataSize) > (ui64)Limits.MaxInsertBytes) {
                continue;
            }
            if (auto* pMap = PrimaryIndex->GetOverloadedGranules(data.PathId)) {
                overloadedPathGranules[pathId] = pMap->size();
                InsertTable->SetOverloaded(data.PathId, true);
                ++ignored;
                continue;
            } else {
                InsertTable->SetOverloaded(data.PathId, false);
            }
            ++blobs;
            bytesToIndex += dataSize;
            dataToIndex.push_back(&data);
        }
    }

    for (auto& [p, cnt] : overloadedPathGranules) {
        ui64 pathId(p);
        ui64 count(cnt);
        LOG_S_INFO("Overloaded granules (" << count << ") for pathId " << pathId << " at tablet " << TabletID());
    }

    if (bytesToIndex < (ui64)Limits.MinInsertBytes && blobs < TLimits::MIN_SMALL_BLOBS_TO_INSERT) {
        LOG_S_DEBUG("Few data for indexation (" << bytesToIndex << " bytes in " << blobs << " blobs, ignored "
            << ignored << ") at tablet " << TabletID());

        // Force small indexations simetimes to keep BatchCache smaller
        if (!bytesToIndex || SkippedIndexations < TSettings::MAX_INDEXATIONS_TO_SKIP) {
            ++SkippedIndexations;
            return {};
        }
    }
    SkippedIndexations = 0;

    LOG_S_DEBUG("Prepare indexing " << bytesToIndex << " bytes in " << dataToIndex.size() << " batches of committed "
        << size << " bytes in " << blobs << " blobs ignored " << ignored
        << " at tablet " << TabletID());

    TVector<NOlap::TInsertedData> data;
    THashMap<TUnifiedBlobId, std::shared_ptr<arrow::RecordBatch>> cachedBlobs;
    data.reserve(dataToIndex.size());
    for (auto& ptr : dataToIndex) {
        data.push_back(*ptr);
        if (auto inserted = BatchCache.GetInserted(TWriteId(ptr->WriteTxId)); inserted.second) {
            Y_VERIFY(ptr->BlobId == inserted.first);
            cachedBlobs.emplace(ptr->BlobId, inserted.second);
        }
    }

    Y_VERIFY(data.size());
    auto indexChanges = PrimaryIndex->StartInsert(std::move(data));
    if (!indexChanges) {
        LOG_S_NOTICE("Cannot prepare indexing at tablet " << TabletID());
        return {};
    }

    ActiveIndexingOrCompaction = true;
    auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(PrimaryIndex->GetIndexInfo(), indexChanges,
        Settings.CacheDataAfterIndexing, std::move(cachedBlobs));
    return std::make_unique<TEvPrivate::TEvIndexing>(std::move(ev));
}

std::unique_ptr<TEvPrivate::TEvCompaction> TColumnShard::SetupCompaction() {
    if (ActiveIndexingOrCompaction) {
        LOG_S_DEBUG("Compaction/indexing already in progress at tablet " << TabletID());
        return {};
    }
    if (!PrimaryIndex) {
        LOG_S_NOTICE("Compaction not started: no index at tablet " << TabletID());
        return {};
    }

    PrimaryIndex->UpdateCompactionLimits(CompactionLimits.Get());
    auto compactionInfo = PrimaryIndex->Compact();
    if (!compactionInfo || compactionInfo->Empty()) {
        LOG_S_DEBUG("Compaction not started: no portions to compact at tablet " << TabletID());
        return {};
    }

    // TODO: Compact granules in parallel

    // Rotate compaction granules: do not choose the same granule all the time.
    LastCompactedGranule = compactionInfo->ChooseOneGranule(LastCompactedGranule);
    Y_VERIFY(compactionInfo->Good());

    LOG_S_DEBUG("Prepare " << *compactionInfo << " at tablet " << TabletID());

    ui64 ourdatedStep = GetOutdatedStep();
    auto indexChanges = PrimaryIndex->StartCompaction(std::move(compactionInfo), {ourdatedStep, 0});
    if (!indexChanges) {
        LOG_S_DEBUG("Compaction not started: cannot prepare compaction at tablet " << TabletID());
        return {};
    }

    ActiveIndexingOrCompaction = true;
    auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(PrimaryIndex->GetIndexInfo(), indexChanges,
        Settings.CacheDataAfterCompaction);
    return std::make_unique<TEvPrivate::TEvCompaction>(std::move(ev), *BlobManager);
}

std::unique_ptr<TEvPrivate::TEvEviction> TColumnShard::SetupTtl(const THashMap<ui64, NOlap::TTiersInfo>& pathTtls,
                                                               bool force) {
    if (ActiveTtl) {
        LOG_S_DEBUG("TTL already in progress at tablet " << TabletID());
        return {};
    }
    if (!PrimaryIndex) {
        LOG_S_NOTICE("TTL not started. No index for TTL at tablet " << TabletID());
        return {};
    }

    THashMap<ui64, NOlap::TTiersInfo> regularTtls;
    if (pathTtls.empty()) {
        regularTtls = Ttl.MakeIndexTtlMap(TInstant::Now(), force);
    }

    if (pathTtls.empty() && regularTtls.empty()) {
        LOG_S_TRACE("TTL not started. No tables to activate it on (or delayed) at tablet " << TabletID());
        return {};
    }

    LOG_S_DEBUG("Prepare TTL at tablet " << TabletID());

    std::shared_ptr<NOlap::TColumnEngineChanges> indexChanges;
    if (pathTtls.empty()) {
        indexChanges = PrimaryIndex->StartTtl(regularTtls);
    } else {
        indexChanges = PrimaryIndex->StartTtl(pathTtls);
    }

    if (!indexChanges) {
        LOG_S_NOTICE("Cannot prepare TTL at tablet " << TabletID());
        return {};
    }
    if (indexChanges->NeedRepeat) {
        Ttl.Repeat();
    }

    bool needWrites = !indexChanges->PortionsToEvict.empty();

    ActiveTtl = true;
    auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(PrimaryIndex->GetIndexInfo(), indexChanges, false);
    return std::make_unique<TEvPrivate::TEvEviction>(std::move(ev), *BlobManager, needWrites);
}

std::unique_ptr<TEvPrivate::TEvWriteIndex> TColumnShard::SetupCleanup() {
    if (ActiveCleanup) {
        LOG_S_DEBUG("Cleanup already in progress at tablet " << TabletID());
        return {};
    }
    if (!PrimaryIndex) {
        LOG_S_NOTICE("Cleanup not started. No index for cleanup at tablet " << TabletID());
        return {};
    }

    NOlap::TSnapshot cleanupSnapshot{GetMinReadStep(), 0};

    auto changes = PrimaryIndex->StartCleanup(cleanupSnapshot, PathsToDrop);
    if (!changes) {
        LOG_S_NOTICE("Cannot prepare cleanup at tablet " << TabletID());
        return {};
    }

    Y_VERIFY(!changes->CompactionInfo);
    Y_VERIFY(changes->DataToIndex.empty());
    Y_VERIFY(changes->AppendedPortions.empty());

    // TODO: limit PortionsToDrop total size. Delete them in small portions.
    // Filter PortionsToDrop
    TVector<NOlap::TPortionInfo> portionsCanBedropped;
    THashSet<ui64> excludedPortions;
    for (const auto& portionInfo : changes->PortionsToDrop) {
        ui64 portionId = portionInfo.Records.front().Portion;
        // Exclude portions that are used by in-flght reads/scans
        if (!InFlightReadsTracker.IsPortionUsed(portionId)) {
            portionsCanBedropped.push_back(portionInfo);
        } else {
            excludedPortions.insert(portionId);
        }
    }
    changes->PortionsToDrop.swap(portionsCanBedropped);

    LOG_S_DEBUG("Prepare Cleanup snapshot: " << cleanupSnapshot
        << " portions to drop: " << changes->PortionsToDrop.size()
        << " in use by reads: " << excludedPortions.size()
        << " at tablet " << TabletID());

    if (changes->PortionsToDrop.empty()) {
        return {};
    }

    auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(PrimaryIndex->GetIndexInfo(), changes, false);
    ev->PutStatus = NKikimrProto::OK; // No new blobs to write

    ActiveCleanup = true;
    return ev;
}

static NOlap::TCompression ConvertCompression(const NKikimrSchemeOp::TCompressionOptions& compression) {
    NOlap::TCompression out;
    if (compression.HasCompressionCodec()) {
        switch (compression.GetCompressionCodec()) {
            case NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain:
                out.Codec = arrow::Compression::UNCOMPRESSED;
                break;
            case NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4:
                out.Codec = arrow::Compression::LZ4_FRAME;
                break;
            case NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD:
                out.Codec = arrow::Compression::ZSTD;
                break;
        }
    }

    if (compression.HasCompressionLevel()) {
        out.Level = compression.GetCompressionLevel();
    }
    return out;
}

NOlap::TIndexInfo TColumnShard::ConvertSchema(const NKikimrSchemeOp::TColumnTableSchema& schema) {
    Y_VERIFY(schema.GetEngine() == NKikimrSchemeOp::COLUMN_ENGINE_REPLACING_TIMESERIES);

    ui32 indexId = 0;
    NOlap::TIndexInfo indexInfo("", indexId);

    for (const auto& col : schema.GetColumns()) {
        const ui32 id = col.GetId();
        const TString& name = col.GetName();

        indexInfo.Columns[id] = NTable::TColumn(name, id, col.GetTypeId());
        indexInfo.ColumnNames[name] = id;
    }

    for (const auto& keyName : schema.GetKeyColumnNames()) {
        Y_VERIFY(indexInfo.ColumnNames.count(keyName));
        indexInfo.KeyColumns.push_back(indexInfo.ColumnNames[keyName]);
    }

    if (schema.HasDefaultCompression()) {
        NOlap::TCompression compression = ConvertCompression(schema.GetDefaultCompression());
        indexInfo.SetDefaultCompression(compression);
    }

    for (auto& tierConfig : schema.GetStorageTiers()) {
        auto& tierName = tierConfig.GetName();
        TierConfigs[tierName] = TTierConfig{tierConfig};

        NOlap::TStorageTier tier{ .Name = tierName };
        if (tierConfig.HasCompression()) {
            tier.Compression = ConvertCompression(tierConfig.GetCompression());
        }
        if (TierConfigs[tierName].NeedExport()) {
            S3Actors[tierName] = {}; // delayed actor creation
        }
        indexInfo.AddStorageTier(std::move(tier));
    }

    return indexInfo;
}

void TColumnShard::MapExternBlobs(const TActorContext& /*ctx*/, NOlap::TReadMetadata& metadata) {
    if (!metadata.SelectInfo) {
        return;
    }

    if (!BlobManager->HasExternBlobs()) {
        return;
    }

    THashSet<TUnifiedBlobId> uniqBlobs;
    for (auto& portion : metadata.SelectInfo->Portions) {
        for (auto& rec : portion.Records) {
            uniqBlobs.insert(rec.BlobRange.BlobId);
        }
    }

    THashMap<TUnifiedBlobId, TUnifiedBlobId> extMap;

    for (auto& blobId : uniqBlobs) {
        TEvictMetadata meta;
        auto evicted = BlobManager->GetEvicted(blobId, meta);
        if (evicted.ExternBlob.IsValid()) {
            extMap[blobId] = evicted.ExternBlob;
        }
    }

    if (!extMap.empty()) {
        metadata.ExternBlobs = std::make_shared<const THashMap<TUnifiedBlobId, TUnifiedBlobId>>(std::move(extMap));
    }
}

TActorId TColumnShard::GetS3ActorForTier(const TString& tierName, const TString& phase) {
    if (!S3Actors.count(tierName)) {
        LOG_S_ERROR("No S3 actor for tier '" << tierName << "' (on " << phase << ") at tablet " << TabletID());
        return {};
    }
    auto s3 = S3Actors[tierName];
    if (!s3) {
        LOG_S_ERROR("Not started S3 actor for tier '" << tierName << "' (on " << phase << ") at tablet " << TabletID());
        return {};
    }
    return s3;
}

void TColumnShard::ExportBlobs(const TActorContext& ctx, ui64 exportNo, const TString& tierName,
                               THashMap<TUnifiedBlobId, TString>&& blobsIds) {
    if (auto s3 = GetS3ActorForTier(tierName, "export")) {
        auto event = std::make_unique<TEvPrivate::TEvExport>(exportNo, tierName, s3, std::move(blobsIds));
        ctx.Register(CreateExportActor(TabletID(), ctx.SelfID, event.release()));
    }
}

void TColumnShard::ForgetBlobs(const TActorContext& ctx, const TString& tierName,
                               std::vector<NOlap::TEvictedBlob>&& blobs) {
    if (auto s3 = GetS3ActorForTier(tierName, "forget")) {
        auto forget = std::make_unique<TEvPrivate::TEvForget>();
        forget->Evicted = std::move(blobs);
        ctx.Send(s3, forget.release());
    }
}

bool TColumnShard::GetExportedBlob(const TActorContext& ctx, TActorId dst, ui64 cookie, const TString& tierName,
                                   NOlap::TEvictedBlob&& evicted, std::vector<NOlap::TBlobRange>&& ranges) {
    if (auto s3 = GetS3ActorForTier(tierName, "get exported")) {
        auto get = std::make_unique<TEvPrivate::TEvGetExported>();
        get->DstActor = dst;
        get->DstCookie = cookie;
        get->Evicted = std::move(evicted);
        get->BlobRanges = std::move(ranges);
        ctx.Send(s3, get.release());
        return true;
    }
    return false;
}

ui32 TColumnShard::InitS3Actors(const TActorContext& ctx, bool init) {
    ui32 count = 0;
#ifndef KIKIMR_DISABLE_S3_OPS
    for (auto& [tierName, actor] : S3Actors) {
        if (!init && actor) {
            continue;
        }

        Y_VERIFY(!actor);
        Y_VERIFY(TierConfigs.count(tierName));
        auto& tierConfig = TierConfigs[tierName];
        Y_VERIFY(tierConfig.NeedExport());

        actor = ctx.Register(CreateS3Actor(TabletID(), ctx.SelfID, tierName));
        ctx.Send(actor, new TEvPrivate::TEvS3Settings(tierConfig.S3Settings()));
        ++count;
    }
#else
    Y_UNUSED(ctx);
    Y_UNUSED(init);
#endif
    return count;
}

void TColumnShard::StopS3Actors(const TActorContext& ctx) {
#ifndef KIKIMR_DISABLE_S3_OPS
    for (auto& [_, actor] : S3Actors) {
        if (actor) {
            ctx.Send(actor, new TEvents::TEvPoisonPill);
            actor = {};
        }
    }
#else
    Y_UNUSED(ctx);
#endif
}

}
