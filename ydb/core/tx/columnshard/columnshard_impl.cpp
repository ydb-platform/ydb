#include "columnshard_impl.h"
#include "columnshard_schema.h"
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tx/tiering/external_data.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/services/metadata/service.h>
#include <ydb/core/tx/tiering/manager.h>

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

bool ValidateTableSchema(const NKikimrSchemeOp::TColumnTableSchema& schema) {
    namespace NTypeIds = NScheme::NTypeIds;

    static const THashSet<NScheme::TTypeId> supportedTypes = {
        NTypeIds::Timestamp,
        NTypeIds::Int8,
        NTypeIds::Int16,
        NTypeIds::Int32,
        NTypeIds::Int64,
        NTypeIds::Uint8,
        NTypeIds::Uint16,
        NTypeIds::Uint32,
        NTypeIds::Uint64,
        NTypeIds::Date,
        NTypeIds::Datetime,
        //NTypeIds::Interval,
        //NTypeIds::Float,
        //NTypeIds::Double,
        NTypeIds::String,
        NTypeIds::Utf8
    };

    if (!schema.HasEngine() ||
        schema.GetEngine() != NKikimrSchemeOp::EColumnTableEngine::COLUMN_ENGINE_REPLACING_TIMESERIES) {
        return false;
    }

    if (!schema.KeyColumnNamesSize()) {
        return false;
    }

    TString firstKeyColumn = schema.GetKeyColumnNames()[0];
    THashSet<TString> keyColumns(schema.GetKeyColumnNames().begin(), schema.GetKeyColumnNames().end());

    for (const NKikimrSchemeOp::TOlapColumnDescription& column : schema.GetColumns()) {
        TString name = column.GetName();
        keyColumns.erase(name);

        if (name == firstKeyColumn && !supportedTypes.contains(column.GetTypeId())) {
            return false;
        }
    }

    if (!keyColumns.empty()) {
        return false;
    }
    return true;
}

bool ValidateTablePreset(const NKikimrSchemeOp::TColumnTableSchemaPreset& preset) {
    if (preset.HasName() && preset.GetName() != "default") {
        return false;
    }
    return ValidateTableSchema(preset.GetSchema());
}

}

bool TColumnShard::TAlterMeta::Validate(const NOlap::ISnapshotSchema::TPtr& schema) const {
    switch (Body.TxBody_case()) {
        case NKikimrTxColumnShard::TSchemaTxBody::kInitShard:
            break;
        case NKikimrTxColumnShard::TSchemaTxBody::kEnsureTables:
            for (auto& table : Body.GetEnsureTables().GetTables()) {
                if (table.HasSchemaPreset() && !ValidateTablePreset(table.GetSchemaPreset())) {
                    return false;
                }
                if (table.HasSchema() && !ValidateTableSchema(table.GetSchema())) {
                    return false;
                }
                // TODO: validate TtlSettings
            }
            break;
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterTable:
            if (schema) {
                if (Body.HasAlterTable() && Body.GetAlterTable().HasSchema()) {
                    return schema->GetIndexInfo().CheckAlterScheme(Body.GetAlterTable().GetSchema());
                }
                if (Body.HasAlterTable() && Body.GetAlterTable().HasSchemaPreset() && Body.GetAlterTable().GetSchemaPreset().HasSchema()) {
                    return schema->GetIndexInfo().CheckAlterScheme(Body.GetAlterTable().GetSchemaPreset().GetSchema());
                }
            }
            return true;
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterStore:
            if (schema) {
                for (const auto& presetProto : Body.GetAlterStore().GetSchemaPresets()) {
                    if (!schema->GetIndexInfo().CheckAlterScheme(presetProto.GetSchema())) {
                        return false;
                    }
                }
                return true;
            }
        case NKikimrTxColumnShard::TSchemaTxBody::kDropTable:
        case NKikimrTxColumnShard::TSchemaTxBody::TXBODY_NOT_SET:
            break;
    }
    return true;
}


TColumnShard::TColumnShard(TTabletStorageInfo* info, const TActorId& tablet)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, nullptr)
    , PipeClientCache(NTabletPipe::CreateBoundedClientCache(new NTabletPipe::TBoundedClientCacheConfig(), GetPipeClientConfig()))
    , InsertTable(std::make_unique<NOlap::TInsertTable>())
    , ReadCounters("Read")
    , ScanCounters("Scan")
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
    if (MediatorTimeCastRegistered) {
        Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvWaitPlanStep(TabletID(), step));
    }
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
    if (!PlanQueue.empty()) {
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

TWriteId TColumnShard::HasLongTxWrite(const NLongTxService::TLongTxId& longTxId, const ui32 partId) {
    auto it = LongTxWritesByUniqueId.find(longTxId.UniqueId);
    if (it != LongTxWritesByUniqueId.end()) {
        auto itPart = it->second.find(partId);
        if (itPart != it->second.end()) {
            return (TWriteId)itPart->second->WriteId;
        }
    }
    return (TWriteId)0;
}

TWriteId TColumnShard::GetLongTxWrite(NIceDb::TNiceDb& db, const NLongTxService::TLongTxId& longTxId, const ui32 partId) {
    auto it = LongTxWritesByUniqueId.find(longTxId.UniqueId);
    if (it != LongTxWritesByUniqueId.end()) {
        auto itPart = it->second.find(partId);
        if (itPart != it->second.end()) {
            return (TWriteId)itPart->second->WriteId;
        }
    } else {
        it = LongTxWritesByUniqueId.emplace(longTxId.UniqueId, TPartsForLTXShard()).first;
    }

    TWriteId writeId = ++LastWriteId;
    auto& lw = LongTxWrites[writeId];
    lw.WriteId = (ui64)writeId;
    lw.WritePartId = partId;
    lw.LongTxId = longTxId;
    it->second[partId] = &lw;

    Schema::SaveSpecialValue(db, Schema::EValueIds::LastWriteId, (ui64)writeId);
    Schema::SaveLongTxWrite(db, writeId, partId, longTxId);

    return writeId;
}

void TColumnShard::AddLongTxWrite(TWriteId writeId, ui64 txId) {
    auto& lw = LongTxWrites.at(writeId);
    lw.PreparedTxId = txId;
}

void TColumnShard::LoadLongTxWrite(TWriteId writeId, const ui32 writePartId, const NLongTxService::TLongTxId& longTxId) {
    auto& lw = LongTxWrites[writeId];
    lw.WritePartId = writePartId;
    lw.WriteId = (ui64)writeId;
    lw.LongTxId = longTxId;
    LongTxWritesByUniqueId[longTxId.UniqueId][writePartId] = &lw;
}

bool TColumnShard::RemoveLongTxWrite(NIceDb::TNiceDb& db, TWriteId writeId, ui64 txId) {
    if (auto* lw = LongTxWrites.FindPtr(writeId)) {
        ui64 prepared = lw->PreparedTxId;
        if (!prepared || txId == prepared) {
            Schema::EraseLongTxWrite(db, writeId);
            auto& ltxParts = LongTxWritesByUniqueId[lw->LongTxId.UniqueId];
            ltxParts.erase(lw->WritePartId);
            if (ltxParts.empty()) {
                LongTxWritesByUniqueId.erase(lw->LongTxId.UniqueId);
            }
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

    if (proto.HasOwnerPathId()) {
        OwnerPathId = proto.GetOwnerPathId();
        Schema::SaveSpecialValue(db, Schema::EValueIds::OwnerPathId, OwnerPathId);
    }

    if (proto.HasOwnerPath()) {
        OwnerPath = proto.GetOwnerPath();
        Schema::SaveSpecialValue(db, Schema::EValueIds::OwnerPath, OwnerPath);
    }

    for (auto& createTable : proto.GetTables()) {
        RunEnsureTable(createTable, version, txc);
    }
}

void TColumnShard::RunEnsureTable(const NKikimrTxColumnShard::TCreateTable& tableProto, const TRowVersion& version,
                                  NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    const ui64 pathId = tableProto.GetPathId();
    if (TablesManager.HasTable(pathId)) {
        LOG_S_DEBUG("EnsureTable for existed pathId: " << pathId << " at tablet " << TabletID());
        return;
    }

    LOG_S_DEBUG("EnsureTable for pathId: " << pathId
        << " ttl settings: " << tableProto.GetTtlSettings()
        << " at tablet " << TabletID());

    TTableInfo::TTableVersionInfo tableVerProto;
    tableVerProto.SetPathId(pathId);

    if (tableProto.HasSchemaPreset()) {
        Y_VERIFY(!tableProto.HasSchema(), "Tables has either schema or preset");

        TSchemaPreset preset;
        preset.Deserialize(tableProto.GetSchemaPreset());
        Y_VERIFY(!preset.IsStandaloneTable());
        tableVerProto.SetSchemaPresetId(preset.GetId());

        if (TablesManager.RegisterSchemaPreset(preset, db)) {
            TablesManager.AddPresetVersion(tableProto.GetSchemaPreset().GetId(), version, tableProto.GetSchemaPreset().GetSchema(), db);
        }
    } else {
        Y_VERIFY(tableProto.HasSchema(), "Tables has either schema or preset");
        *tableVerProto.MutableSchema() = tableProto.GetSchema();
    }

    TTableInfo table(pathId);
    if (tableProto.HasTtlSettings()) {
        const auto& ttlSettings = tableProto.GetTtlSettings();
        *tableVerProto.MutableTtlSettings() = ttlSettings;
        if (ttlSettings.HasUseTiering()) {
            table.SetTieringUsage(ttlSettings.GetUseTiering());
            ActivateTiering(pathId, table.GetTieringUsage());
        }
    }

    tableVerProto.SetSchemaPresetVersionAdj(tableProto.GetSchemaPresetVersionAdj());
    tableVerProto.SetTtlSettingsPresetVersionAdj(tableProto.GetTtlSettingsPresetVersionAdj());

    TablesManager.RegisterTable(std::move(table), db);
    TablesManager.AddTableVersion(pathId, version, tableVerProto, db);

    SetCounter(COUNTER_TABLES, TablesManager.GetTables().size());
    SetCounter(COUNTER_TABLE_PRESETS, TablesManager.GetSchemaPresets().size());
    SetCounter(COUNTER_TABLE_TTLS, TablesManager.GetTtl().PathsCount());
}

void TColumnShard::RunAlterTable(const NKikimrTxColumnShard::TAlterTable& alterProto, const TRowVersion& version,
                                 NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    const ui64 pathId = alterProto.GetPathId();
    Y_VERIFY(TablesManager.HasTable(pathId), "AlterTable on a dropped or non-existent table");

    LOG_S_DEBUG("AlterTable for pathId: " << pathId
        << " schema: " << alterProto.GetSchema()
        << " ttl settings: " << alterProto.GetTtlSettings()
        << " at tablet " << TabletID());

    TTableInfo::TTableVersionInfo tableVerProto;
    if (alterProto.HasSchemaPreset()) {
        tableVerProto.SetSchemaPresetId(alterProto.GetSchemaPreset().GetId());
        TablesManager.AddPresetVersion(alterProto.GetSchemaPreset().GetId(), version, alterProto.GetSchemaPreset().GetSchema(), db);
    } else if (alterProto.HasSchema()) {
        *tableVerProto.MutableSchema() = alterProto.GetSchema();
    }

    const auto& ttlSettings = alterProto.GetTtlSettings(); // Note: Not valid behaviour for full alter implementation
    const TString& tieringUsage = ttlSettings.GetUseTiering();
    if (alterProto.HasTtlSettings()) {
        const auto& ttlSettings = alterProto.GetTtlSettings();
        *tableVerProto.MutableTtlSettings() = ttlSettings;
    }
    ActivateTiering(pathId, tieringUsage);
    Schema::SaveTableInfo(db, pathId, tieringUsage);

    tableVerProto.SetSchemaPresetVersionAdj(alterProto.GetSchemaPresetVersionAdj());
    TablesManager.AddTableVersion(pathId, version, tableVerProto, db);
    TablesManager.OnTtlUpdate();
}

void TColumnShard::RunDropTable(const NKikimrTxColumnShard::TDropTable& dropProto, const TRowVersion& version,
                                NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    const ui64 pathId = dropProto.GetPathId();
    if (!TablesManager.HasTable(pathId)) {
        LOG_S_DEBUG("DropTable for unknown or deleted pathId: " << pathId << " at tablet " << TabletID());
        return;
    }

    LOG_S_DEBUG("DropTable for pathId: " << pathId << " at tablet " << TabletID());
    TablesManager.DropTable(pathId, version, db);

    // TODO: Allow to read old snapshots after DROP
    TBlobGroupSelector dsGroupSelector(Info());
    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);
    THashSet<TWriteId> writesToAbort = InsertTable->DropPath(dbTable, pathId);

    TryAbortWrites(db, dbTable, std::move(writesToAbort));
}

void TColumnShard::RunAlterStore(const NKikimrTxColumnShard::TAlterStore& proto, const TRowVersion& version,
                                 NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    if (proto.HasStorePathId()) {
        OwnerPathId = proto.GetStorePathId();
        Schema::SaveSpecialValue(db, Schema::EValueIds::OwnerPathId, OwnerPathId);
    }

    for (ui32 id : proto.GetDroppedSchemaPresets()) {
        if (!TablesManager.HasPreset(id)) {
            continue;
        }
        TablesManager.DropPreset(id, version, db);
    }

    for (const auto& presetProto : proto.GetSchemaPresets()) {
        if (!TablesManager.HasPreset(presetProto.GetId())) {
            continue; // we don't update presets that we don't use
        }
        TablesManager.AddPresetVersion(presetProto.GetId(), version, presetProto.GetSchema(), db);
    }
}

void TColumnShard::ScheduleNextGC(const TActorContext& ctx, bool cleanupOnly) {
    LOG_S_DEBUG("Scheduling GC at tablet " << TabletID());

    UpdateBlobMangerCounters();
    if (BlobManager->CanCollectGarbage(cleanupOnly)) {
        BlobManager->GetCounters().StartCollection->Add(1);
        Execute(CreateTxRunGc(), ctx);
    } else {
        BlobManager->GetCounters().SkipCollection->Add(1);
    }
}

void TColumnShard::EnqueueBackgroundActivities(bool periodic, TBackgroundActivity activity) {
    if (periodic) {
        if (LastPeriodicBackActivation > TInstant::Now() - ActivationPeriod) {
            CSCounters.OnTooEarly();
            return;
        }
        LastPeriodicBackActivation = TInstant::Now();
    }
    CSCounters.OnStartBackground();

    SendPeriodicStats();

    if (!TablesManager.HasPrimaryIndex()) {
        LOG_S_NOTICE("Background activities cannot be started: no index at tablet " << TabletID());
        return;
    }

    const TActorContext& ctx = ActorContext();
    // Schedule either indexing or compaction.
    if (activity.HasIndexation() || activity.HasCompaction()) {
        [&] {
            if (BackgroundController.IsIndexingActive()) {
                return;
            }
            if (activity.HasCompaction()) {
                SetupCompaction();
            }
            if (activity.HasIndexation()) {
                SetupIndexation();
            }
        }();
    }

    if (activity.HasCleanup()) {
        if (auto event = SetupCleanup()) {
            ctx.Send(SelfId(), event.release());
        } else {
            // Small cleanup (no index changes)
            CleanForgottenBlobs(ctx);
        }
    }

    if (activity.HasTtl()) {
        if (auto event = SetupTtl()) {
            if (event->NeedDataReadWrite()) {
                ctx.Send(EvictionActor, event.release());
            } else {
                ctx.Send(SelfId(), event->TxEvent.release());
            }
        }
    }
}

void TColumnShard::SetupIndexation() {
    CSCounters.OnSetupIndexation();
    ui32 blobs = 0;
    ui32 ignored = 0;
    ui64 size = 0;
    ui64 bytesToIndex = 0;
    std::vector<const NOlap::TInsertedData*> dataToIndex;
    dataToIndex.reserve(TLimits::MIN_SMALL_BLOBS_TO_INSERT);
    for (auto it = InsertTable->GetPathPriorities().rbegin(); it != InsertTable->GetPathPriorities().rend(); ++it) {
        for (auto* pathInfo : it->second) {
            const bool hasSplitCompaction = BackgroundController.HasSplitCompaction(pathInfo->GetPathId());
            const bool granulesOverloaded = TablesManager.GetPrimaryIndex()->HasOverloadedGranules(pathInfo->GetPathId());
            for (auto& data : pathInfo->GetCommitted()) {
                ui32 dataSize = data.BlobSize();
                Y_VERIFY(dataSize);

                size += dataSize;
                if (bytesToIndex && (bytesToIndex + dataSize) > (ui64)Limits.MaxInsertBytes) {
                    continue;
                }
                if (granulesOverloaded) {
                    ++ignored;
                    CSCounters.SkipIndexationInputDueToGranuleOverload(dataSize);
                    continue;
                }
                if (hasSplitCompaction) {
                    ++ignored;
                    CSCounters.SkipIndexationInputDueToSplitCompaction(dataSize);
                    continue;
                }
                ++blobs;
                bytesToIndex += dataSize;
                dataToIndex.push_back(&data);
            }
        }
    }

    if (bytesToIndex < (ui64)Limits.MinInsertBytes && blobs < TLimits::MIN_SMALL_BLOBS_TO_INSERT) {
        LOG_S_DEBUG("Few data for indexation (" << bytesToIndex << " bytes in " << blobs << " blobs, ignored "
            << ignored << ") at tablet " << TabletID());

        // Force small indexations sometimes to keep BatchCache smaller
        if (!bytesToIndex || SkippedIndexations < TSettings::MAX_INDEXATIONS_TO_SKIP) {
            ++SkippedIndexations;
            return;
        }
    }
    CSCounters.IndexationInput(bytesToIndex);
    SkippedIndexations = 0;

    LOG_S_DEBUG("Prepare indexing " << bytesToIndex << " bytes in " << dataToIndex.size() << " batches of committed "
        << size << " bytes in " << blobs << " blobs ignored " << ignored
        << " at tablet " << TabletID());

    std::vector<NOlap::TInsertedData> data;
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
    auto indexChanges = TablesManager.MutablePrimaryIndex().StartInsert(CompactionLimits.Get(), std::move(data));
    if (!indexChanges) {
        LOG_S_NOTICE("Cannot prepare indexing at tablet " << TabletID());
        return;
    }

    auto actualIndexInfo = TablesManager.GetPrimaryIndex()->GetVersionedIndex();
    BackgroundController.StartIndexing();
    auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(std::move(actualIndexInfo), indexChanges,
        Settings.CacheDataAfterIndexing, std::move(cachedBlobs));
    if (Tiers) {
        ev->SetTiering(Tiers->GetTiering());
    }

    ActorContext().Send(IndexingActor, std::make_unique<TEvPrivate::TEvIndexing>(std::move(ev)));
}

void TColumnShard::SetupCompaction() {
    CSCounters.OnSetupCompaction();

    while (BackgroundController.GetCompactionsCount() < TSettings::MAX_ACTIVE_COMPACTIONS) {
        auto limits = CompactionLimits.Get();
        auto compactionInfo = TablesManager.MutablePrimaryIndex().Compact(limits);
        if (!compactionInfo) {
            if (!BackgroundController.GetCompactionsCount()) {
                LOG_S_DEBUG("Compaction not started: no portions to compact at tablet " << TabletID());
            }
            break;
        }

        LOG_S_DEBUG("Prepare " << *compactionInfo << " at tablet " << TabletID());

        auto& g = compactionInfo->GetObject<NOlap::TGranuleMeta>();
        if (compactionInfo->InGranule()) {
            CSCounters.OnInternalCompactionInfo(g.GetAdditiveSummary().GetOther().GetPortionsSize(), g.GetAdditiveSummary().GetOther().GetPortionsCount());
        } else {
            CSCounters.OnSplitCompactionInfo(g.GetAdditiveSummary().GetOther().GetPortionsSize(), g.GetAdditiveSummary().GetOther().GetPortionsCount());
        }

        ui64 outdatedStep = GetOutdatedStep();
        const NOlap::TPlanCompactionInfo planInfo = compactionInfo->GetPlanCompaction();
        auto indexChanges = TablesManager.MutablePrimaryIndex().StartCompaction(std::move(compactionInfo), NOlap::TSnapshot(outdatedStep, 0), limits);
        if (!indexChanges) {
            if (!BackgroundController.GetCompactionsCount()) {
                LOG_S_DEBUG("Compaction not started: cannot prepare compaction at tablet " << TabletID());
            }
            break;
        }

        BackgroundController.StartCompaction(planInfo);
        auto actualIndexInfo = TablesManager.GetPrimaryIndex()->GetVersionedIndex();
        auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(std::move(actualIndexInfo), indexChanges,
            Settings.CacheDataAfterCompaction);
        if (Tiers) {
            ev->SetTiering(Tiers->GetTiering());
        }

        ActorContext().Send(CompactionActor, std::make_unique<TEvPrivate::TEvCompaction>(std::move(ev), *BlobManager));
    }

    LOG_S_DEBUG("ActiveCompactions: " << BackgroundController.GetCompactionsCount() << " at tablet " << TabletID());
}

std::unique_ptr<TEvPrivate::TEvEviction> TColumnShard::SetupTtl(const THashMap<ui64, NOlap::TTiering>& pathTtls,
                                                                bool force) {
    CSCounters.OnSetupTtl();
    if (BackgroundController.IsTtlActive()) {
        LOG_S_DEBUG("TTL already in progress at tablet " << TabletID());
        return {};
    }
    if (ActiveEvictions) {
        LOG_S_DEBUG("Do not start TTL while eviction is in progress at tablet " << TabletID());
        return {};
    }

    THashMap<ui64, NOlap::TTiering> eviction = pathTtls;
    if (eviction.empty()) {
        if (Tiers) {
            eviction = Tiers->GetTiering(); // TODO: pathIds
        }
        TablesManager.AddTtls(eviction, AppData()->TimeProvider->Now(), force);
    }

    if (eviction.empty()) {
        if (Tiers || TablesManager.GetTtl().PathsCount()) {
            LOG_S_DEBUG("TTL not started. No tables to activate it on (or delayed) at tablet " << TabletID());
        }
        return {};
    }

    for (auto&& i : eviction) {
        LOG_S_DEBUG("Prepare TTL evicting path " << i.first << " with " << i.second.GetDebugString()
            << " at tablet " << TabletID());
    }

    auto actualIndexInfo = TablesManager.GetPrimaryIndex()->GetVersionedIndex();
    std::shared_ptr<NOlap::TColumnEngineChanges> indexChanges;
    indexChanges = TablesManager.MutablePrimaryIndex().StartTtl(eviction, actualIndexInfo.GetLastSchema()->GetIndexInfo().ArrowSchema());

    if (!indexChanges) {
        LOG_S_INFO("Cannot prepare TTL at tablet " << TabletID());
        return {};
    }
    if (indexChanges->NeedRepeat) {
        TablesManager.OnTtlUpdate();
    }

    bool needWrites = !indexChanges->PortionsToEvict.empty();
    LOG_S_INFO("TTL" << (needWrites ? " with writes" : "" ) << " prepared at tablet " << TabletID());

    BackgroundController.StartTtl();
    auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(std::move(actualIndexInfo), indexChanges, false);
    ev->SetTiering(eviction);
    return std::make_unique<TEvPrivate::TEvEviction>(std::move(ev), *BlobManager, needWrites);
}

std::unique_ptr<TEvPrivate::TEvWriteIndex> TColumnShard::SetupCleanup() {
    CSCounters.OnSetupCleanup();
    if (BackgroundController.IsCleanupActive()) {
        LOG_S_DEBUG("Cleanup already in progress at tablet " << TabletID());
        return {};
    }

    NOlap::TSnapshot cleanupSnapshot{GetMinReadStep(), 0};

    auto changes = TablesManager.StartIndexCleanup(cleanupSnapshot, CompactionLimits.Get(), TLimits::MAX_TX_RECORDS);
    if (!changes) {
        LOG_S_INFO("Cannot prepare cleanup at tablet " << TabletID());
        return {};
    }

    Y_VERIFY(!changes->CompactionInfo);
    Y_VERIFY(changes->DataToIndex.empty());
    Y_VERIFY(changes->AppendedPortions.empty());

    // Filter PortionsToDrop
    std::vector<NOlap::TPortionInfo> portionsCanBedropped;
    THashSet<ui64> excludedPortions;
    for (const auto& portionInfo : changes->PortionsToDrop) {
        ui64 portionId = portionInfo.Records.front().Portion;
        // Exclude portions that are used by in-flight reads/scans
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

    auto actualIndexInfo = TablesManager.GetPrimaryIndex()->GetVersionedIndex();
#if 0 // No need for now
    if (Tiers) {
        ...
    }
#endif

    auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(std::move(actualIndexInfo), changes, false);
    ev->SetPutStatus(NKikimrProto::OK); // No new blobs to write

    BackgroundController.StartCleanup();
    return ev;
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

    auto exported = std::make_shared<THashSet<TUnifiedBlobId>>();

    for (auto& blobId : uniqBlobs) {
        TEvictMetadata meta;
        auto evicted = BlobManager->GetEvicted(blobId, meta);
        if (evicted.IsExternal()) {
            exported->insert(blobId);
        }
    }

    if (!exported->empty()) {
        metadata.ExternBlobs = exported;
    }
}

void TColumnShard::CleanForgottenBlobs(const TActorContext& ctx) {
    THashMap<TString, THashSet<NOlap::TEvictedBlob>> tierBlobsToForget;
    BlobManager->GetCleanupBlobs(tierBlobsToForget);
    ForgetBlobs(ctx, tierBlobsToForget);
}

void TColumnShard::Reexport(const TActorContext& ctx) {
    THashMap<TString, THashSet<NOlap::TEvictedBlob>> tierBlobsToReexport;
    BlobManager->GetReexportBlobs(tierBlobsToReexport);

    ui64 exportNo = LastExportNo;
    LastExportNo += tierBlobsToReexport.size(); // TODO: persist it?

    for (auto& [tierName, evictSet] : tierBlobsToReexport) {
        ++exportNo;
        LOG_S_INFO("Reexport " << exportNo << " at tablet " << TabletID());
        ExportBlobs(ctx, std::make_unique<TEvPrivate::TEvExport>(exportNo, tierName, evictSet));
    }
}

void TColumnShard::ExportBlobs(const TActorContext& ctx, std::unique_ptr<TEvPrivate::TEvExport>&& event) {
    Y_VERIFY(event);
    Y_VERIFY(event->ExportNo);
    Y_VERIFY(event->Blobs.size());
    Y_VERIFY(event->SrcToDstBlobs.size() == event->Blobs.size());

    const auto& tierName = event->TierName;
    if (auto s3 = GetS3ActorForTier(tierName)) {
        TStringBuilder strBlobs;
        for (auto& [blobId, _] : event->Blobs) {
            strBlobs << "'" << blobId.ToStringNew() << "' ";
        }

        event->DstActor = s3;
        LOG_S_NOTICE("Export blobs " << strBlobs << "(tier '" << tierName << "') at tablet " << TabletID());
        ctx.Register(CreateExportActor(TabletID(), SelfId(), event.release()));
    } else {
        LOG_S_INFO("Cannot export blobs (no S3 actor for tier '" << tierName << "') at tablet " << TabletID());
    }
}

// It should be called from ForgetBlobs() only to log all S3 activity
void TColumnShard::ForgetTierBlobs(const TActorContext& ctx, const TString& tierName, std::vector<NOlap::TEvictedBlob>&& blobs) const {
    if (auto s3 = GetS3ActorForTier(tierName)) {
        auto forget = std::make_unique<TEvPrivate::TEvForget>();
        forget->Evicted = std::move(blobs);
        ctx.Send(s3, forget.release());
    }
}

void TColumnShard::ForgetBlobs(const TActorContext& ctx, const THashMap<TString, THashSet<NOlap::TEvictedBlob>>& evictedBlobs) {
    TStringBuilder strBlobs;
    TStringBuilder strBlobsDelayed;

    for (const auto& [tierName, evictSet] : evictedBlobs) {
        std::vector<NOlap::TEvictedBlob> tierBlobs;

        for (const auto& ev : evictSet) {
            auto& blobId = ev.Blob;
            if (BlobManager->BlobInUse(blobId)) {
                LOG_S_DEBUG("Blob '" << blobId.ToStringNew() << "' is in use at tablet " << TabletID());
                strBlobsDelayed << "'" << blobId.ToStringNew() << "' ";
                continue;
            }

            TEvictMetadata meta;
            auto evict = BlobManager->GetDropped(blobId, meta);
            if (tierName != meta.GetTierName()) {
                LOG_S_ERROR("Forget with unexpected tier name '" << meta.GetTierName() << "' at tablet " << TabletID());
                continue;
            }

            if (evict.State == EEvictState::UNKNOWN) {
                LOG_S_ERROR("Forget unknown blob '" << blobId.ToStringNew() << "' at tablet " << TabletID());
            } else if (NOlap::CouldBeExported(evict.State)) {
                Y_VERIFY(evict.Blob == blobId);
                strBlobs << "'" << blobId.ToStringNew() << "' ";
                tierBlobs.emplace_back(std::move(evict));
            } else {
                Y_VERIFY(evict.Blob == blobId);
                strBlobsDelayed << "'" << blobId.ToStringNew() << "' ";
            }
        }

        if (tierBlobs.size()) {
            ForgetTierBlobs(ctx, tierName, std::move(tierBlobs));
        }
    }

    if (strBlobs.size()) {
        LOG_S_NOTICE("Forget blobs " << strBlobs << "at tablet " << TabletID());
    }
    if (strBlobsDelayed.size()) {
        LOG_S_NOTICE("Forget blobs (deleyed) " << strBlobsDelayed << "at tablet " << TabletID());
    }
}

bool TColumnShard::GetExportedBlob(const TActorContext& ctx, TActorId dst, ui64 cookie, const TString& tierName,
                                   NOlap::TEvictedBlob&& evicted, std::vector<NOlap::TBlobRange>&& ranges) {
    if (auto s3 = GetS3ActorForTier(tierName)) {
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

void TColumnShard::Die(const TActorContext& ctx) {
    // TODO
    if (!!Tiers) {
        Tiers->Stop();
    }
    NTabletPipe::CloseAndForgetClient(SelfId(), StatsReportPipe);
    UnregisterMediatorTimeCast();
    return IActor::Die(ctx);
}

TActorId TColumnShard::GetS3ActorForTier(const TString& tierId) const {
    if (!Tiers) {
        return {};
    }
    return Tiers->GetStorageActorId(tierId);
}

void TColumnShard::Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    Y_VERIFY(Tiers);
    ALS_INFO(NKikimrServices::TX_COLUMNSHARD) << "test handle NMetadata::NProvider::TEvRefreshSubscriberData"
        << ev->Get()->GetSnapshot()->SerializeToString();
    Tiers->TakeConfigs(ev->Get()->GetSnapshot(), nullptr);
}

void TColumnShard::ActivateTiering(const ui64 pathId, const TString& useTiering) {
    if (!Tiers) {
        Tiers = std::make_shared<TTiersManager>(TabletID(), SelfId(),
            [this](const TActorContext& ctx){
                CleanForgottenBlobs(ctx);
                Reexport(ctx);
            });
        Tiers->Start(Tiers);
    }
    if (!!Tiers) {
        if (useTiering) {
            Tiers->EnablePathId(pathId, useTiering);
        } else {
            Tiers->DisablePathId(pathId);
        }
    }
}

}
