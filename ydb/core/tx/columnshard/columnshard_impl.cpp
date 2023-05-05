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

        if (name == firstKeyColumn && !supportedTypes.count(column.GetTypeId())) {
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

bool TColumnShard::TAlterMeta::Validate() const {
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
        case NKikimrTxColumnShard::TSchemaTxBody::kDropTable:
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterStore:
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

TWriteId TColumnShard::HasLongTxWrite(const NLongTxService::TLongTxId& longTxId) {
    auto it = LongTxWritesByUniqueId.find(longTxId.UniqueId);
    if (it != LongTxWritesByUniqueId.end()) {
        return (TWriteId)it->second->WriteId;
    }
    return (TWriteId)0;
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

ui32 TColumnShard::EnsureSchemaPreset(NIceDb::TNiceDb& db, ui32 presetId, const TString& name,
                                      const NKikimrSchemeOp::TColumnTableSchema& schemaProto,
                                      const TRowVersion& version) {
    if (!SchemaPresets.contains(presetId)) {
        LOG_S_DEBUG("EnsureSchemaPreset " << presetId << " at tablet " << TabletID());

        auto& preset = SchemaPresets[presetId];
        preset.Id = presetId;
        preset.Name = name;
        auto& info = preset.Versions[version];
        info.SetId(preset.Id);
        info.SetSinceStep(version.Step);
        info.SetSinceTxId(version.TxId);
        *info.MutableSchema() = schemaProto;

        Schema::SaveSchemaPresetInfo(db, preset.Id, preset.Name);
        Schema::SaveSchemaPresetVersionInfo(db, preset.Id, version, info);
        SetCounter(COUNTER_TABLE_PRESETS, SchemaPresets.size());
    } else {
        LOG_S_DEBUG("EnsureSchemaPreset for existed preset " << presetId << " at tablet " << TabletID());
    }

    return presetId;
}

ui32 TColumnShard::EnsureSchemaPreset(NIceDb::TNiceDb& db, const NKikimrSchemeOp::TColumnTableSchemaPreset& presetProto,
                                      const TRowVersion& version) {
    Y_VERIFY(presetProto.GetName() == "default", "Only schema preset named 'default' is supported");

    return EnsureSchemaPreset(db, presetProto.GetId(), presetProto.GetName(), presetProto.GetSchema(), version);
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
    if (!Tables.contains(pathId)) {
        LOG_S_DEBUG("EnsureTable for pathId: " << pathId
            << " ttl settings: " << tableProto.GetTtlSettings()
            << " at tablet " << TabletID());

        ui32 schemaPresetId = 0;
        if (tableProto.HasSchemaPreset()) {
            Y_VERIFY(!tableProto.HasSchema(), "Tables has either schema or preset");

            schemaPresetId = EnsureSchemaPreset(db, tableProto.GetSchemaPreset(), version);
            Y_VERIFY(schemaPresetId);
        } else {
            Y_VERIFY(tableProto.HasSchema(), "Tables has either schema or preset");

            // Save first table schema as common one with schemaPresetId == 0

            if (SchemaPresets.count(0)) {
                LOG_S_WARN("Colocated standalone tables are not supported. "
                    << "EnsureTable failed at tablet " << TabletID());
                return;
            }

            schemaPresetId = EnsureSchemaPreset(db, 0, "", tableProto.GetSchema(), version);
            Y_VERIFY(!schemaPresetId);
        }

        auto& table = Tables[pathId];
        table.PathId = pathId;
        auto& tableVerProto = table.Versions[version];
        tableVerProto.SetPathId(pathId);
        tableVerProto.SetSchemaPresetId(schemaPresetId);

        if (tableProto.HasTtlSettings()) {
            *tableVerProto.MutableTtlSettings() = tableProto.GetTtlSettings();
            auto& ttlInfo = tableProto.GetTtlSettings();
            if (ttlInfo.HasEnabled()) {
                Ttl.SetPathTtl(pathId, TTtl::TDescription(ttlInfo.GetEnabled()));
                SetCounter(COUNTER_TABLE_TTLS, Ttl.PathsCount());
            }
            if (ttlInfo.HasUseTiering()) {
                table.TieringUsage = ttlInfo.GetUseTiering();
                ActivateTiering(pathId, table.TieringUsage);
            }
        }

        if (!PrimaryIndex) {
            TMap<NOlap::TSnapshot, NOlap::TIndexInfo> schemaHistory;

            auto& schemaPresetVerProto = SchemaPresets[schemaPresetId].Versions[version];
            schemaHistory.emplace(NOlap::TSnapshot{version.Step, version.TxId},
                                    ConvertSchema(schemaPresetVerProto.GetSchema()));

            SetPrimaryIndex(std::move(schemaHistory));
        }

        tableVerProto.SetSchemaPresetVersionAdj(tableProto.GetSchemaPresetVersionAdj());
        tableVerProto.SetTtlSettingsPresetVersionAdj(tableProto.GetTtlSettingsPresetVersionAdj());

        Schema::SaveTableInfo(db, table.PathId, table.TieringUsage);
        Schema::SaveTableVersionInfo(db, table.PathId, version, tableVerProto);
        SetCounter(COUNTER_TABLES, Tables.size());
    } else {
        LOG_S_DEBUG("EnsureTable for existed pathId: " << pathId << " at tablet " << TabletID());
    }
}

void TColumnShard::RunAlterTable(const NKikimrTxColumnShard::TAlterTable& alterProto, const TRowVersion& version,
                                 NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    const ui64 pathId = alterProto.GetPathId();
    auto* tablePtr = Tables.FindPtr(pathId);
    Y_VERIFY(tablePtr && !tablePtr->IsDropped(), "AlterTable on a dropped or non-existent table");
    auto& table = *tablePtr;
    auto& ttlSettings = alterProto.GetTtlSettings();

    LOG_S_DEBUG("AlterTable for pathId: " << pathId
        << " schema: " << alterProto.GetSchema()
        << " ttl settings: " << ttlSettings
        << " at tablet " << TabletID());

    auto& info = table.Versions[version];

    if (alterProto.HasSchemaPreset()) {
        info.SetSchemaPresetId(EnsureSchemaPreset(db, alterProto.GetSchemaPreset(), version));
    }

    const TString& tieringUsage = ttlSettings.GetUseTiering();
    ActivateTiering(pathId, tieringUsage);
    if (alterProto.HasTtlSettings()) {
        *info.MutableTtlSettings() = ttlSettings;
        if (ttlSettings.HasEnabled()) {
            Ttl.SetPathTtl(pathId, TTtl::TDescription(ttlSettings.GetEnabled()));
        } else {
            Ttl.DropPathTtl(pathId);
        }
    } else {
        Ttl.DropPathTtl(pathId);
    }
    Ttl.Repeat(); // Atler TTL triggers TTL activity

    info.SetSchemaPresetVersionAdj(alterProto.GetSchemaPresetVersionAdj());
    Schema::SaveTableInfo(db, table.PathId, tieringUsage);
    Schema::SaveTableVersionInfo(db, table.PathId, version, info);
}

void TColumnShard::RunDropTable(const NKikimrTxColumnShard::TDropTable& dropProto, const TRowVersion& version,
                                NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    const ui64 pathId = dropProto.GetPathId();
    auto* table = Tables.FindPtr(pathId);
    Y_VERIFY_DEBUG(table && !table->IsDropped());
    if (table && !table->IsDropped()) {
        LOG_S_DEBUG("DropTable for pathId: " << pathId << " at tablet " << TabletID());

        PathsToDrop.insert(pathId);
        Ttl.DropPathTtl(pathId);

        // TODO: Allow to read old snapshots after DROP
        TBlobGroupSelector dsGroupSelector(Info());
        NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);
        THashSet<TWriteId> writesToAbort = InsertTable->DropPath(dbTable, pathId);
        TryAbortWrites(db, dbTable, std::move(writesToAbort));

        table->DropVersion = version;
        Schema::SaveTableDropVersion(db, pathId, version.Step, version.TxId);
    } else {
        LOG_S_DEBUG("DropTable for unknown or deleted pathId: " << pathId << " at tablet " << TabletID());
    }
}

void TColumnShard::RunAlterStore(const NKikimrTxColumnShard::TAlterStore& proto, const TRowVersion& version,
                                 NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    if (proto.HasStorePathId()) {
        OwnerPathId = proto.GetStorePathId();
        Schema::SaveSpecialValue(db, Schema::EValueIds::OwnerPathId, OwnerPathId);
    }

    TMap<NOlap::TSnapshot, NOlap::TIndexInfo> schemaHistory;

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
            schemaHistory.emplace(NOlap::TSnapshot{version.Step, version.TxId}, ConvertSchema(info.GetSchema()));
        }

        Schema::SaveSchemaPresetVersionInfo(db, preset.Id, version, info);
    }

    if (!schemaHistory.empty()) {
        SetPrimaryIndex(std::move(schemaHistory));
    }
}

void TColumnShard::SetPrimaryIndex(TMap<NOlap::TSnapshot, NOlap::TIndexInfo>&& schemaVersions) {
    for (auto& [snap, indexInfo] : schemaVersions) {
        if (!PrimaryIndex) {
            PrimaryIndex = std::make_unique<NOlap::TColumnEngineForLogs>(std::move(indexInfo), TabletID());
            SetCounter(COUNTER_INDEXES, 1);
        } else {
            PrimaryIndex->UpdateDefaultSchema(snap, std::move(indexInfo));
        }
    }

    for (auto& columnName : Ttl.TtlColumns()) {
        PrimaryIndex->GetIndexInfo().CheckTtlColumn(columnName);
    }
}

void TColumnShard::ScheduleNextGC(const TActorContext& ctx, bool cleanupOnly) {
    LOG_S_DEBUG("Scheduling GC at tablet " << TabletID());

    UpdateBlobMangerCounters();
    if (BlobManager->CanCollectGarbage(cleanupOnly)) {
        Execute(CreateTxRunGc(), ctx);
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
    } else {
        // Small cleanup (no index changes)
        THashSet<NOlap::TEvictedBlob> blobsToForget;
        BlobManager->GetCleanupBlobs(blobsToForget);
        ForgetBlobs(ctx, blobsToForget);
    }

    if (auto event = SetupTtl()) {
        if (event->NeedDataReadWrite()) {
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

    auto actualIndexInfo = PrimaryIndex->GetIndexInfo();
    if (Tiers) {
        auto pathTiering = Tiers->GetTiering(); // TODO: pathIds
        actualIndexInfo.UpdatePathTiering(pathTiering);
        actualIndexInfo.SetPathTiering(std::move(pathTiering));
    }

    ActiveIndexingOrCompaction = true;
    auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(std::move(actualIndexInfo), indexChanges,
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

    auto actualIndexInfo = PrimaryIndex->GetIndexInfo();
    if (Tiers) {
        auto pathTiering = Tiers->GetTiering(); // TODO: pathIds
        actualIndexInfo.UpdatePathTiering(pathTiering);
        actualIndexInfo.SetPathTiering(std::move(pathTiering));
    }

    ActiveIndexingOrCompaction = true;
    auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(std::move(actualIndexInfo), indexChanges,
        Settings.CacheDataAfterCompaction);
    return std::make_unique<TEvPrivate::TEvCompaction>(std::move(ev), *BlobManager);
}

std::unique_ptr<TEvPrivate::TEvEviction> TColumnShard::SetupTtl(const THashMap<ui64, NOlap::TTiering>& pathTtls,
                                                                bool force) {
    if (ActiveTtl) {
        LOG_S_DEBUG("TTL already in progress at tablet " << TabletID());
        return {};
    }
    if (ActiveEvictions) {
        LOG_S_DEBUG("Do not start TTL while eviction is in progress at tablet " << TabletID());
        return {};
    }
    if (!PrimaryIndex) {
        LOG_S_NOTICE("TTL not started. No index for TTL at tablet " << TabletID());
        return {};
    }

    THashMap<ui64, NOlap::TTiering> eviction = pathTtls;
    if (eviction.empty()) {
        if (Tiers) {
            eviction = Tiers->GetTiering(); // TODO: pathIds
        }
        Ttl.AddTtls(eviction, TInstant::Now(), force);
    }

    if (eviction.empty()) {
        LOG_S_TRACE("TTL not started. No tables to activate it on (or delayed) at tablet " << TabletID());
        return {};
    }

    LOG_S_DEBUG("Prepare TTL at tablet " << TabletID());

    for (auto&& i : eviction) {
        LOG_S_DEBUG("Evicting path " << i.first << " with " << i.second.GetDebugString() << " at tablet " << TabletID());
    }

    auto actualIndexInfo = PrimaryIndex->GetIndexInfo();
    actualIndexInfo.UpdatePathTiering(eviction);

    std::shared_ptr<NOlap::TColumnEngineChanges> indexChanges;
    indexChanges = PrimaryIndex->StartTtl(eviction);

    actualIndexInfo.SetPathTiering(std::move(eviction));

    if (!indexChanges) {
        LOG_S_DEBUG("Cannot prepare TTL at tablet " << TabletID());
        return {};
    }
    if (indexChanges->NeedRepeat) {
        Ttl.Repeat();
    }

    bool needWrites = !indexChanges->PortionsToEvict.empty();

    ActiveTtl = true;
    auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(std::move(actualIndexInfo), indexChanges, false);
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

    auto changes = PrimaryIndex->StartCleanup(cleanupSnapshot, PathsToDrop, TLimits::MAX_TX_RECORDS);
    if (!changes) {
        LOG_S_NOTICE("Cannot prepare cleanup at tablet " << TabletID());
        return {};
    }

    Y_VERIFY(!changes->CompactionInfo);
    Y_VERIFY(changes->DataToIndex.empty());
    Y_VERIFY(changes->AppendedPortions.empty());

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

    auto actualIndexInfo = PrimaryIndex->GetIndexInfo();
#if 0 // No need for now
    if (Tiers) {
        ...
    }
#endif

    auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(std::move(actualIndexInfo), changes, false);
    ev->PutStatus = NKikimrProto::OK; // No new blobs to write

    ActiveCleanup = true;
    return ev;
}

NOlap::TIndexInfo TColumnShard::ConvertSchema(const NKikimrSchemeOp::TColumnTableSchema& schema) {
    Y_VERIFY(schema.GetEngine() == NKikimrSchemeOp::COLUMN_ENGINE_REPLACING_TIMESERIES);

    ui32 indexId = 0;
    NOlap::TIndexInfo indexInfo("", indexId);

    for (const auto& col : schema.GetColumns()) {
        const ui32 id = col.GetId();
        const TString& name = col.GetName();
        auto typeInfo = NScheme::TypeInfoFromProtoColumnType(col.GetTypeId(),
            col.HasTypeInfo() ? &col.GetTypeInfo() : nullptr);
        indexInfo.Columns[id] = NTable::TColumn(name, id, typeInfo);
        indexInfo.ColumnNames[name] = id;
    }

    for (const auto& keyName : schema.GetKeyColumnNames()) {
        Y_VERIFY(indexInfo.ColumnNames.count(keyName));
        indexInfo.KeyColumns.push_back(indexInfo.ColumnNames[keyName]);
    }

    if (schema.HasDefaultCompression()) {
        NOlap::TCompression compression = NTiers::ConvertCompression(schema.GetDefaultCompression());
        indexInfo.SetDefaultCompression(compression);
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

void TColumnShard::ExportBlobs(const TActorContext& ctx, ui64 exportNo, const TString& tierName, ui64 pathId,
                               TEvPrivate::TEvExport::TBlobDataMap&& blobsInfo) const {
    Y_VERIFY(blobsInfo.size());

    TString strBlobs;
    for (auto& [blobId, _] : blobsInfo) {
        strBlobs += "'" + blobId.ToStringNew() + "' ";
    }
    LOG_S_NOTICE("Export blobs " << strBlobs << "at tablet " << TabletID());

    if (auto s3 = GetS3ActorForTier(tierName)) {
        auto event = std::make_unique<TEvPrivate::TEvExport>(exportNo, tierName, pathId, s3, std::move(blobsInfo));
        ctx.Register(CreateExportActor(TabletID(), ctx.SelfID, event.release()));
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

void TColumnShard::ForgetBlobs(const TActorContext& ctx, const THashSet<NOlap::TEvictedBlob>& evictedBlobs) {
    THashMap<TString, std::vector<NOlap::TEvictedBlob>> tierBlobs;

    TString strBlobs;
    TString strBlobsDelayed;

    for (const auto& ev : evictedBlobs) {
        auto& blobId = ev.Blob;
        if (BlobManager->BlobInUse(blobId)) {
            LOG_S_DEBUG("Blob '" << blobId.ToStringNew() << "' in use at tablet " << TabletID());
            strBlobsDelayed += "'" + blobId.ToStringNew() + "' ";
            continue;
        }

        TEvictMetadata meta;
        auto evict = BlobManager->GetDropped(blobId, meta);

        if (evict.State == EEvictState::UNKNOWN) {
            LOG_S_ERROR("Forget unknown blob '" << blobId.ToStringNew() << "' at tablet " << TabletID());
        } else if (NOlap::CouldBeExported(evict.State)) {
            Y_VERIFY(evict.Blob == blobId);
            strBlobs += "'" + blobId.ToStringNew() + "' ";
            tierBlobs[meta.GetTierName()].emplace_back(std::move(evict));
        } else {
            Y_VERIFY(evict.Blob == blobId);
            strBlobsDelayed += "'" + blobId.ToStringNew() + "' ";
        }
    }

    if (strBlobs.size()) {
        LOG_S_NOTICE("Forget blobs " << strBlobs << "at tablet " << TabletID());
    }
    if (strBlobsDelayed.size()) {
        LOG_S_NOTICE("Forget blobs (deleyed) " << strBlobsDelayed << "at tablet " << TabletID());
    }

    for (auto& [tierName, blobs] : tierBlobs) {
        ForgetTierBlobs(ctx, tierName, std::move(blobs));
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
        Tiers = std::make_shared<TTiersManager>(TabletID(), SelfId());
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
