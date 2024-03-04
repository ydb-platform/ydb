#include "columnshard_impl.h"
#include "columnshard_schema.h"
#include "blobs_reader/task.h"
#include "blobs_reader/events.h"
#include "engines/changes/ttl.h"
#include "engines/changes/cleanup.h"
#include "blobs_action/bs/storage.h"
#include "resource_subscriber/task.h"

#ifndef KIKIMR_DISABLE_S3_OPS
#include "blobs_action/tier/storage.h"
#endif

#include "blobs_action/transaction/tx_gc_insert_table.h"
#include "blobs_action/transaction/tx_gc_indexed.h"
#include "hooks/abstract/abstract.h"
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tx/tiering/external_data.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/services/metadata/service.h>
#include <ydb/core/tx/tiering/manager.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include "resource_subscriber/counters.h"
#include "blobs_reader/actor.h"


#include <ydb/core/tx/columnshard/normalizer/granule/normalizer.h>
#include <ydb/core/tx/columnshard/normalizer/portion/min_max.h>
#include <ydb/core/tx/columnshard/normalizer/portion/chunks.h>

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

bool TColumnShard::TAlterMeta::Validate(const NOlap::ISnapshotSchema::TPtr& /*schema*/) const {
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
            return true;
        case NKikimrTxColumnShard::TSchemaTxBody::kAlterStore:
            return true;
        case NKikimrTxColumnShard::TSchemaTxBody::kDropTable:
        case NKikimrTxColumnShard::TSchemaTxBody::TXBODY_NOT_SET:
            break;
    }
    return true;
}

class TColumnShard::TStoragesManager: public NOlap::IStoragesManager {
private:
    using TBase = NOlap::IStoragesManager;
    TColumnShard& Shard;
protected:
    virtual std::shared_ptr<NOlap::IBlobsStorageOperator> DoBuildOperator(const TString& storageId) override {
        if (storageId == TBase::DefaultStorageId) {
            return std::make_shared<NOlap::NBlobOperations::NBlobStorage::TOperator>(storageId, Shard.SelfId(), Shard.Info(), Shard.Executor()->Generation());
        } else if (!Shard.Tiers) {
            return nullptr;
        } else {
#ifndef KIKIMR_DISABLE_S3_OPS
            return std::make_shared<NOlap::NBlobOperations::NTier::TOperator>(storageId, Shard);
#else
            return nullptr;
#endif
        }
    }
public:
    TStoragesManager(TColumnShard& shard)
        : Shard(shard) {

    }
};

TColumnShard::TColumnShard(TTabletStorageInfo* info, const TActorId& tablet)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, nullptr)
    , ProgressTxController(std::make_unique<TTxController>(*this))
    , StoragesManager(std::make_shared<TStoragesManager>(*this))
    , InFlightReadsTracker(StoragesManager)
    , TablesManager(StoragesManager, info->TabletID)
    , PipeClientCache(NTabletPipe::CreateBoundedClientCache(new NTabletPipe::TBoundedClientCacheConfig(), GetPipeClientConfig()))
    , InsertTable(std::make_unique<NOlap::TInsertTable>())
    , SubscribeCounters(std::make_shared<NOlap::NResourceBroker::NSubscribe::TSubscriberCounters>())
    , InsertTaskSubscription(NOlap::TInsertColumnEngineChanges::StaticTypeName(), SubscribeCounters)
    , CompactTaskSubscription(NOlap::TCompactColumnEngineChanges::StaticTypeName(), SubscribeCounters)
    , ReadCounters("Read")
    , ScanCounters("Scan")
    , WritesMonitor(*this)
    , NormalizerController(StoragesManager, SubscribeCounters)
{
    TabletCountersPtr.reset(new TProtobufTabletCounters<
        ESimpleCounters_descriptor,
        ECumulativeCounters_descriptor,
        EPercentileCounters_descriptor,
        ETxTypes_descriptor
    >());
    TabletCounters = TabletCountersPtr.get();

    NormalizerController.RegisterNormalizer(std::make_shared<NOlap::TGranulesNormalizer>());
    NormalizerController.RegisterNormalizer(std::make_shared<NOlap::TChunksNormalizer>(Info()));
    NormalizerController.RegisterNormalizer(std::make_shared<NOlap::TPortionsNormalizer>(Info()));
}

void TColumnShard::OnDetach(const TActorContext& ctx) {
    CleanupActors(ctx);
    Die(ctx);
}

void TColumnShard::OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ev);
    CleanupActors(ctx);
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
    auto plannedTx = ProgressTxController->GetPlannedTx();
    if (plannedTx) {
        // We may only read just before the first transaction in the queue
        return TRowVersion(plannedTx->Step, plannedTx->TxId).Prev();
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

ui64 TColumnShard::GetMinReadStep() const {
    ui64 delayMillisec = MaxReadStaleness.MilliSeconds();
    ui64 passedStep = GetOutdatedStep();
    ui64 minReadStep = (passedStep > delayMillisec ? passedStep - delayMillisec : 0);
    return minReadStep;
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

    TWriteId writeId = BuildNextWriteId(db);
    auto& lw = LongTxWrites[writeId];
    lw.WriteId = (ui64)writeId;
    lw.WritePartId = partId;
    lw.LongTxId = longTxId;
    it->second[partId] = &lw;

    Schema::SaveLongTxWrite(db, writeId, partId, longTxId);
    return writeId;
}

TWriteId TColumnShard::BuildNextWriteId(NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);
    return BuildNextWriteId(db);
}

TWriteId TColumnShard::BuildNextWriteId(NIceDb::TNiceDb& db) {
    TWriteId writeId = ++LastWriteId;
    Schema::SaveSpecialValue(db, Schema::EValueIds::LastWriteId, (ui64)writeId);
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

bool TColumnShard::AbortTx(const ui64 txId, const NKikimrTxColumnShard::ETransactionKind& txKind, NTabletFlatExecutor::TTransactionContext& txc) {
    switch (txKind) {
        case NKikimrTxColumnShard::TX_KIND_SCHEMA: {
            AltersInFlight.erase(txId);
            break;
        }
        case NKikimrTxColumnShard::TX_KIND_COMMIT: {
            NIceDb::TNiceDb db(txc.DB);
            if (auto* meta = CommitsInFlight.FindPtr(txId)) {
                for (TWriteId writeId : meta->WriteIds) {
                    // TODO: we probably need to have more complex
                    // logic in the future, when there are multiple
                    // inflight commits for the same writeId.
                    RemoveLongTxWrite(db, writeId, txId);
                }
                TBlobGroupSelector dsGroupSelector(Info());
                NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);
                InsertTable->Abort(dbTable, meta->WriteIds);

                CommitsInFlight.erase(txId);
            }
            break;
        }
        case NKikimrTxColumnShard::TX_KIND_COMMIT_WRITE: {
            if (!OperationsManager->AbortTransaction(*this, txId, txc)) {
                return false;
            }
            break;
        }
        default: {
            Y_ABORT("Unsupported TxKind");
        }
    }
    return true;
}

void TColumnShard::TryAbortWrites(NIceDb::TNiceDb& db, NOlap::TDbWrapper& dbTable, THashSet<TWriteId>&& writesToAbort) {
    std::vector<TWriteId> failedAborts;
    for (auto& writeId : writesToAbort) {
        if (!RemoveLongTxWrite(db, writeId)) {
            failedAborts.push_back(writeId);
        }
    }
    for (auto& writeId : failedAborts) {
        writesToAbort.erase(writeId);
    }
    if (!writesToAbort.empty()) {
        InsertTable->Abort(dbTable, writesToAbort);
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

    Y_ABORT("Unsupported schema tx type");
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

    // check schema changed

    if (tableProto.HasSchemaPreset()) {
        Y_ABORT_UNLESS(!tableProto.HasSchema(), "Tables has either schema or preset");

        TSchemaPreset preset;
        preset.Deserialize(tableProto.GetSchemaPreset());
        Y_ABORT_UNLESS(!preset.IsStandaloneTable());
        tableVerProto.SetSchemaPresetId(preset.GetId());

        if (TablesManager.RegisterSchemaPreset(preset, db)) {
            TablesManager.AddSchemaVersion(tableProto.GetSchemaPreset().GetId(), version, tableProto.GetSchemaPreset().GetSchema(), db);
        }
    } else {
        Y_ABORT_UNLESS(tableProto.HasSchema(), "Tables has either schema or preset");
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
    Y_ABORT_UNLESS(TablesManager.HasTable(pathId), "AlterTable on a dropped or non-existent table");

    LOG_S_DEBUG("AlterTable for pathId: " << pathId
        << " schema: " << alterProto.GetSchema()
        << " ttl settings: " << alterProto.GetTtlSettings()
        << " at tablet " << TabletID());

    TTableInfo::TTableVersionInfo tableVerProto;
    if (alterProto.HasSchemaPreset()) {
        tableVerProto.SetSchemaPresetId(alterProto.GetSchemaPreset().GetId());
        TablesManager.AddSchemaVersion(alterProto.GetSchemaPreset().GetId(), version, alterProto.GetSchemaPreset().GetSchema(), db);
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
        TablesManager.AddSchemaVersion(presetProto.GetId(), version, presetProto.GetSchema(), db);
    }
}

void TColumnShard::EnqueueBackgroundActivities(bool periodic, TBackgroundActivity activity) {
    TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletID()));
    ACFL_DEBUG("event", "EnqueueBackgroundActivities")("periodic", periodic)("activity", activity.DebugString());
    CSCounters.OnStartBackground();

    SendPeriodicStats();

    if (!TablesManager.HasPrimaryIndex()) {
        LOG_S_NOTICE("Background activities cannot be started: no index at tablet " << TabletID());
        return;
    }

    if (activity.HasIndexation()) {
        SetupIndexation();
    }

    if (activity.HasCompaction()) {
        SetupCompaction();
    }

    if (activity.HasCleanup()) {
        SetupCleanup();
    }

    if (activity.HasTtl()) {
        SetupTtl();
    }

    SetupGC();

    SetupCleanupInsertTable();
}

class TChangesTask: public NConveyor::ITask {
private:
    std::unique_ptr<TEvPrivate::TEvWriteIndex> TxEvent;
    const TIndexationCounters Counters;
    const ui64 TabletId;
    const TActorId ParentActorId;
    TString ClassId;
protected:
    virtual bool DoExecute() override {
        NActors::TLogContextGuard g(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletId)("parent_id", ParentActorId));
        {
            auto guard = TxEvent->PutResult->StartCpuGuard();
            NOlap::TConstructionContext context(TxEvent->IndexInfo, Counters);
            Y_ABORT_UNLESS(TxEvent->IndexChanges->ConstructBlobs(context).Ok());
            if (!TxEvent->IndexChanges->GetWritePortionsCount()) {
                TxEvent->SetPutStatus(NKikimrProto::OK);
            }
        }
        TActorContext::AsActorContext().Send(ParentActorId, std::move(TxEvent));
        return true;
    }
public:
    virtual TString GetTaskClassIdentifier() const override {
        return ClassId;
    }

    TChangesTask(std::unique_ptr<TEvPrivate::TEvWriteIndex>&& txEvent, const TIndexationCounters& counters, const ui64 tabletId, const TActorId parentActorId)
        : TxEvent(std::move(txEvent))
        , Counters(counters)
        , TabletId(tabletId)
        , ParentActorId(parentActorId)
    {
        Y_ABORT_UNLESS(TxEvent);
        Y_ABORT_UNLESS(TxEvent->IndexChanges);
        ClassId = "Changes::ConstructBlobs::" + TxEvent->IndexChanges->TypeString();
    }
};

class TChangesReadTask: public NOlap::NBlobOperations::NRead::ITask {
private:
    using TBase = NOlap::NBlobOperations::NRead::ITask;
    const TActorId ParentActorId;
    const ui64 TabletId;
    std::unique_ptr<TEvPrivate::TEvWriteIndex> TxEvent;
    TIndexationCounters Counters;
protected:
    virtual void DoOnDataReady(const std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard) override {
        TxEvent->IndexChanges->Blobs = ExtractBlobsData();
        TxEvent->IndexChanges->ResourcesGuard = resourcesGuard;
        const bool isInsert = !!dynamic_pointer_cast<NOlap::TInsertColumnEngineChanges>(TxEvent->IndexChanges);
        std::shared_ptr<NConveyor::ITask> task = std::make_shared<TChangesTask>(std::move(TxEvent), Counters, TabletId, ParentActorId);
        if (isInsert) {
            NConveyor::TInsertServiceOperator::SendTaskToExecute(task);
        } else {
            NConveyor::TCompServiceOperator::SendTaskToExecute(task);
        }
    }
    virtual bool DoOnError(const TBlobRange& range, const NOlap::IBlobsReadingAction::TErrorStatus& status) override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "DoOnError")("blob_id", range)("status", status.GetErrorMessage())("status_code", status.GetStatus());
        AFL_VERIFY(false)("blob_id", range)("status", status.GetStatus());
        TxEvent->SetPutStatus(NKikimrProto::ERROR);
        TActorContext::AsActorContext().Send(ParentActorId, std::move(TxEvent));
        return false;
    }
public:
    TChangesReadTask(std::unique_ptr<TEvPrivate::TEvWriteIndex>&& event, const TActorId parentActorId, const ui64 tabletId, const TIndexationCounters& counters)
        : TBase(event->IndexChanges->GetReadingActions(), event->IndexChanges->TypeString(), event->IndexChanges->GetTaskIdentifier())
        , ParentActorId(parentActorId)
        , TabletId(tabletId)
        , TxEvent(std::move(event))
        , Counters(counters)
    {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "start_changes")("type", TxEvent->IndexChanges->TypeString())("task_id", TxEvent->IndexChanges->GetTaskIdentifier());
    }
};

void TColumnShard::StartIndexTask(std::vector<const NOlap::TInsertedData*>&& dataToIndex, const i64 bytesToIndex) {
    CSCounters.IndexationInput(bytesToIndex);

    std::vector<NOlap::TInsertedData> data;
    data.reserve(dataToIndex.size());
    for (auto& ptr : dataToIndex) {
        data.push_back(*ptr);
    }

    Y_ABORT_UNLESS(data.size());
    auto indexChanges = TablesManager.MutablePrimaryIndex().StartInsert(std::move(data));
    Y_ABORT_UNLESS(indexChanges);

    auto actualIndexInfo = TablesManager.GetPrimaryIndex()->GetVersionedIndex();
    indexChanges->Start(*this);
    auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(std::move(actualIndexInfo), indexChanges, Settings.CacheDataAfterIndexing);

    const TString externalTaskId = indexChanges->GetTaskIdentifier();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "indexation")("bytes", bytesToIndex)("blobs_count", dataToIndex.size())("max_limit", (i64)Limits.MaxInsertBytes)
        ("has_more", bytesToIndex >= Limits.MaxInsertBytes)("external_task_id", externalTaskId);

    NOlap::NResourceBroker::NSubscribe::ITask::StartResourceSubscription(
        ResourceSubscribeActor, std::make_shared<NOlap::NBlobOperations::NRead::ITask::TReadSubscriber>(
                                std::make_shared<TChangesReadTask>(std::move(ev), SelfId(), TabletID(), IndexationCounters), 0, indexChanges->CalcMemoryForUsage(), externalTaskId, InsertTaskSubscription));
}

void TColumnShard::SetupIndexation() {
    if (!AppDataVerified().ColumnShardConfig.GetIndexationEnabled()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_indexation")("reason", "disabled");
        return;
    }
    BackgroundController.CheckDeadlinesIndexation();
    if (BackgroundController.GetIndexingActiveCount()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "skip_indexation")("reason", "in_progress")
            ("count", BackgroundController.GetIndexingActiveCount())("insert_overload_size", InsertTable->GetCountersCommitted().Bytes)
            ("indexing_debug", BackgroundController.DebugStringIndexation());
        return;
    }

    bool force = false;
    if (InsertTable->GetPathPriorities().size() && InsertTable->GetPathPriorities().rbegin()->first.GetCategory() == NOlap::TPathInfoIndexPriority::EIndexationPriority::PreventOverload) {
        force = true;
    }
    const ui64 bytesLimit = NYDBTest::TControllers::GetColumnShardController()->GetGuaranteeIndexationStartBytesLimit(TSettings::GuaranteeIndexationStartBytesLimit);
    const TDuration durationLimit = NYDBTest::TControllers::GetColumnShardController()->GetGuaranteeIndexationInterval(TSettings::GuaranteeIndexationInterval);
    if (!force && InsertTable->GetCountersCommitted().Bytes < bytesLimit &&
        TMonotonic::Now() < BackgroundController.GetLastIndexationInstant() + durationLimit) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "skip_indexation")("reason", "not_enough_data_and_too_frequency")
            ("insert_size", InsertTable->GetCountersCommitted().Bytes);
        return;
    }

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "start_indexation_tasks")("insert_overload_size", InsertTable->GetCountersCommitted().Bytes);
    CSCounters.OnSetupIndexation();
    i64 bytesToIndex = 0;
    std::vector<const NOlap::TInsertedData*> dataToIndex;
    dataToIndex.reserve(TLimits::MIN_SMALL_BLOBS_TO_INSERT);
    for (auto it = InsertTable->GetPathPriorities().rbegin(); it != InsertTable->GetPathPriorities().rend(); ++it) {
        for (auto* pathInfo : it->second) {
            for (auto& data : pathInfo->GetCommitted()) {
                Y_ABORT_UNLESS(data.BlobSize());
                bytesToIndex += data.BlobSize();
                dataToIndex.push_back(&data);
                if (bytesToIndex >= Limits.MaxInsertBytes) {
                    StartIndexTask(std::move(dataToIndex), bytesToIndex);
                    dataToIndex.clear();
                    bytesToIndex = 0;
                }
            }
        }
    }
    if (dataToIndex.size()) {
        StartIndexTask(std::move(dataToIndex), bytesToIndex);
    }
}

void TColumnShard::SetupCompaction() {
    if (!AppDataVerified().ColumnShardConfig.GetCompactionEnabled()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_compaction")("reason", "disabled");
        return;
    }
    CSCounters.OnSetupCompaction();

    BackgroundController.CheckDeadlines();
    while (BackgroundController.GetCompactionsCount() < TSettings::MAX_ACTIVE_COMPACTIONS) {
        auto limits = CompactionLimits.Get();
        auto indexChanges = TablesManager.MutablePrimaryIndex().StartCompaction(limits, BackgroundController.GetConflictCompactionPortions());
        if (!indexChanges) {
            LOG_S_DEBUG("Compaction not started: cannot prepare compaction at tablet " << TabletID());
            break;
        }

        indexChanges->Start(*this);

        auto actualIndexInfo = TablesManager.GetPrimaryIndex()->GetVersionedIndex();
        auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(std::move(actualIndexInfo), indexChanges, Settings.CacheDataAfterCompaction);
        const TString externalTaskId = indexChanges->GetTaskIdentifier();
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "compaction")("external_task_id", externalTaskId);

        NOlap::NResourceBroker::NSubscribe::ITask::StartResourceSubscription(
            ResourceSubscribeActor, std::make_shared<NOlap::NBlobOperations::NRead::ITask::TReadSubscriber>(
                std::make_shared<TChangesReadTask>(std::move(ev), SelfId(), TabletID(), CompactionCounters), 0, indexChanges->CalcMemoryForUsage(), externalTaskId, CompactTaskSubscription));
    }

    LOG_S_DEBUG("ActiveCompactions: " << BackgroundController.GetCompactionsCount() << " at tablet " << TabletID());
}

bool TColumnShard::SetupTtl(const THashMap<ui64, NOlap::TTiering>& pathTtls, const bool force) {
    if (!AppDataVerified().ColumnShardConfig.GetTTLEnabled()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_ttl")("reason", "disabled");
        return false;
    }
    CSCounters.OnSetupTtl();
    if (BackgroundController.IsTtlActive()) {
        ACFL_DEBUG("background", "ttl")("skip_reason", "in_progress");
        return false;
    }
    if (force) {
        TablesManager.MutablePrimaryIndex().OnTieringModified(Tiers, TablesManager.GetTtl());
    }
    THashMap<ui64, NOlap::TTiering> eviction = pathTtls;
    for (auto&& i : eviction) {
        ACFL_DEBUG("background", "ttl")("path", i.first)("info", i.second.GetDebugString());
    }

    auto actualIndexInfo = TablesManager.GetPrimaryIndex()->GetVersionedIndex();
    std::shared_ptr<NOlap::TTTLColumnEngineChanges> indexChanges = TablesManager.MutablePrimaryIndex().StartTtl(eviction, BackgroundController.GetConflictTTLPortions());

    if (!indexChanges) {
        ACFL_DEBUG("background", "ttl")("skip_reason", "no_changes");
        return false;
    }

    const bool needWrites = indexChanges->NeedConstruction();
    ACFL_DEBUG("background", "ttl")("need_writes", needWrites);

    indexChanges->Start(*this);
    auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(std::move(actualIndexInfo), indexChanges, false);
    NYDBTest::TControllers::GetColumnShardController()->OnWriteIndexStart(TabletID(), indexChanges->TypeString());
    if (needWrites) {
        ActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(std::make_shared<TChangesReadTask>(std::move(ev), SelfId(), TabletID(), CompactionCounters)));
    } else {
        ev->SetPutStatus(NKikimrProto::OK);
        ActorContext().Send(SelfId(), std::move(ev));
    }
    return true;
}

void TColumnShard::SetupCleanup() {
    CSCounters.OnSetupCleanup();
    if (BackgroundController.IsCleanupActive()) {
        ACFL_DEBUG("background", "cleanup")("skip_reason", "in_progress");
        return;
    }

    NOlap::TSnapshot cleanupSnapshot{GetMinReadStep(), 0};

    auto changes =
        TablesManager.MutablePrimaryIndex().StartCleanup(cleanupSnapshot, TablesManager.MutablePathsToDrop(), TLimits::MAX_TX_RECORDS);
    if (!changes) {
        ACFL_DEBUG("background", "cleanup")("skip_reason", "no_changes");
        return;
    }

    ACFL_DEBUG("background", "cleanup")("changes_info", changes->DebugString());
    auto actualIndexInfo = TablesManager.GetPrimaryIndex()->GetVersionedIndex();
    auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(std::move(actualIndexInfo), changes, false);
    ev->SetPutStatus(NKikimrProto::OK); // No new blobs to write

    changes->Start(*this);

    Send(SelfId(), ev.release());
}

void TColumnShard::SetupGC() {
    for (auto&& i : StoragesManager->GetStorages()) {
        i.second->StartGC();
    }
}

void TColumnShard::Handle(TEvPrivate::TEvGarbageCollectionFinished::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxGarbageCollectionFinished(this, ev->Get()->Action), ctx);
}

void TColumnShard::SetupCleanupInsertTable() {
    auto writeIdsToCleanup = InsertTable->OldWritesToAbort(AppData()->TimeProvider->Now());

    if (!InsertTable->GetAborted().size() && !writeIdsToCleanup.size()) {
        return;
    }

    Execute(new TTxInsertTableCleanup(this, std::move(writeIdsToCleanup)), TActorContext::AsActorContext());
}

void TColumnShard::Die(const TActorContext& ctx) {
    // TODO
    CleanupActors(ctx);
    NTabletPipe::CloseAndForgetClient(SelfId(), StatsReportPipe);
    UnregisterMediatorTimeCast();
    return IActor::Die(ctx);
}

void TColumnShard::Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    Y_ABORT_UNLESS(Tiers);
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "TEvRefreshSubscriberData")("snapshot", ev->Get()->GetSnapshot()->SerializeToString());
    Tiers->TakeConfigs(ev->Get()->GetSnapshot(), nullptr);
}

void TColumnShard::ActivateTiering(const ui64 pathId, const TString& useTiering, const bool onTabletInit) {
    Y_ABORT_UNLESS(!!Tiers);
    if (!!Tiers) {
        if (useTiering) {
            Tiers->EnablePathId(pathId, useTiering);
        } else {
            Tiers->DisablePathId(pathId);
        }
    }
    if (!onTabletInit) {
        OnTieringModified();
    }
}

void TColumnShard::Enqueue(STFUNC_SIG) {
    const TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletID())("self_id", SelfId());
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvPrivate::TEvTieringModified, Handle);
        HFunc(TEvPrivate::TEvNormalizerResult, Handle);
        default:
            return NTabletFlatExecutor::TTabletExecutedFlat::Enqueue(ev);
    }
}

void TColumnShard::OnTieringModified() {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "OnTieringModified");
    StoragesManager->OnTieringModified(Tiers);
    if (TablesManager.HasPrimaryIndex()) {
        TablesManager.MutablePrimaryIndex().OnTieringModified(Tiers, TablesManager.GetTtl());
    }
}

}
