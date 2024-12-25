#include "blob.h"
#include "columnshard_impl.h"
#include "columnshard_schema.h"

#include "blobs_action/bs/storage.h"
#include "blobs_reader/events.h"
#include "blobs_reader/task.h"
#include "common/tablet_id.h"
#include "resource_subscriber/task.h"

#ifndef KIKIMR_DISABLE_S3_OPS
#include "blobs_action/tier/storage.h"
#endif

#include "bg_tasks/adapter/adapter.h"
#include "bg_tasks/events/events.h"
#include "bg_tasks/manager/manager.h"
#include "blobs_action/storages_manager/manager.h"
#include "blobs_action/transaction/tx_gc_indexed.h"
#include "blobs_action/transaction/tx_gc_insert_table.h"
#include "blobs_action/transaction/tx_remove_blobs.h"
#include "blobs_action/transaction/tx_gc_insert_table.h"
#include "blobs_action/transaction/tx_gc_indexed.h"
#include "blobs_reader/actor.h"
#include "bg_tasks/events/events.h"

#include "data_accessor/manager.h"
#include "data_sharing/destination/session/destination.h"
#include "data_sharing/source/session/source.h"
#include "engines/changes/cleanup_portions.h"
#include "engines/changes/cleanup_tables.h"
#include "engines/changes/general_compaction.h"
#include "engines/changes/indexation.h"
#include "engines/changes/ttl.h"
#include "hooks/abstract/abstract.h"
#include "resource_subscriber/counters.h"
#include "transactions/operators/ev_write/sync.h"

#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/scheme/schema_version.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/priorities/usage/abstract.h>
#include <ydb/core/tx/priorities/usage/events.h>
#include <ydb/core/tx/priorities/usage/service.h>
#include <ydb/core/tx/tiering/manager.h>

#include <ydb/services/metadata/service.h>

#include <util/generic/object_counter.h>

namespace NKikimr::NColumnShard {

// NOTE: We really want to batch log records by default in columnshards!
// But in unittests we want to test both scenarios
bool gAllowLogBatchingDefaultValue = true;

namespace {

NTabletPipe::TClientConfig GetPipeClientConfig() {
    NTabletPipe::TClientConfig config;
    config.RetryPolicy = {
        .MinRetryTime = TDuration::MilliSeconds(50),
        .MaxRetryTime = TDuration::Seconds(2),
    };
    return config;
}

} // namespace

TColumnShard::TColumnShard(TTabletStorageInfo* info, const TActorId& tablet)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
    , TabletCountersHolder(new TProtobufTabletCounters<ESimpleCounters_descriptor, ECumulativeCounters_descriptor,
          EPercentileCounters_descriptor, ETxTypes_descriptor>())
    , Counters(*TabletCountersHolder)
    , ProgressTxController(std::make_unique<TTxController>(*this))
    , StoragesManager(std::make_shared<NOlap::TStoragesManager>(*this))
    , DataLocksManager(std::make_shared<NOlap::NDataLocks::TManager>())
    , PeriodicWakeupActivationPeriod(NYDBTest::TControllers::GetColumnShardController()->GetPeriodicWakeupActivationPeriod())
    , StatsReportInterval(NYDBTest::TControllers::GetColumnShardController()->GetStatsReportInterval())
    , InFlightReadsTracker(StoragesManager, Counters.GetRequestsTracingCounters())
    , TablesManager(StoragesManager, std::make_shared<NOlap::NDataAccessorControl::TLocalManager>(nullptr),
          std::make_shared<NOlap::TSchemaObjectsCache>(), info->TabletID)
    , Subscribers(std::make_shared<NSubscriber::TManager>(*this))
    , PipeClientCache(NTabletPipe::CreateBoundedClientCache(new NTabletPipe::TBoundedClientCacheConfig(), GetPipeClientConfig()))
    , InsertTable(std::make_unique<NOlap::TInsertTable>())
    , InsertTaskSubscription(NOlap::TInsertColumnEngineChanges::StaticTypeName(), Counters.GetSubscribeCounters())
    , CompactTaskSubscription(NOlap::TCompactColumnEngineChanges::StaticTypeName(), Counters.GetSubscribeCounters())
    , TTLTaskSubscription(NOlap::TTTLColumnEngineChanges::StaticTypeName(), Counters.GetSubscribeCounters())
    , BackgroundController(Counters.GetBackgroundControllerCounters())
    , NormalizerController(StoragesManager, Counters.GetSubscribeCounters())
    , SysLocks(this) {
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
            step < *MediatorTimeCastWaitingSteps.begin()) {
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
    NOlap::TSnapshot maxReadVersion = GetMaxReadVersion();
    for (auto it = WaitingScans.begin(); it != WaitingScans.end();) {
        if (maxReadVersion < it->first) {
            minWaitingStep = Min(minWaitingStep, it->first.GetPlanStep());
            break;
        }
        TActivationContext::Send(it->second.Release());
        it = WaitingScans.erase(it);
    }
    if (minWaitingStep != Max<ui64>()) {
        WaitPlanStep(minWaitingStep);
    }
}

NOlap::TSnapshot TColumnShard::GetMaxReadVersion() const {
    auto plannedTx = ProgressTxController->GetPlannedTx();
    if (plannedTx) {
        // We may only read just before the first transaction in the queue
        auto maxReadVersion = TRowVersion(plannedTx->Step, plannedTx->TxId).Prev();
        return NOlap::TSnapshot(maxReadVersion.Step, maxReadVersion.TxId);
    }
    ui64 step = LastPlannedStep;
    if (MediatorTimeCastEntry) {
        ui64 mediatorStep = MediatorTimeCastEntry->Get(TabletID());
        step = Max(step, mediatorStep);
    }
    return NOlap::TSnapshot(step, Max<ui64>());
}

ui64 TColumnShard::GetOutdatedStep() const {
    ui64 step = LastPlannedStep;
    if (MediatorTimeCastEntry) {
        step = Max(step, MediatorTimeCastEntry->Get(TabletID()));
    }
    return step;
}

NOlap::TSnapshot TColumnShard::GetMinReadSnapshot() const {
    ui64 delayMillisec = NYDBTest::TControllers::GetColumnShardController()->GetMaxReadStaleness().MilliSeconds();
    ui64 passedStep = GetOutdatedStep();
    ui64 minReadStep = (passedStep > delayMillisec ? passedStep - delayMillisec : 0);

    if (auto ssClean = InFlightReadsTracker.GetSnapshotToClean()) {
        if (ssClean->GetPlanStep() < minReadStep) {
            Counters.GetRequestsTracingCounters()->OnDefaultMinSnapshotInstant(TInstant::MilliSeconds(ssClean->GetPlanStep()));
            return *ssClean;
        }
    }
    Counters.GetRequestsTracingCounters()->OnDefaultMinSnapshotInstant(TInstant::MilliSeconds(minReadStep));
    return NOlap::TSnapshot::MaxForPlanStep(minReadStep);
}

TInsertWriteId TColumnShard::HasLongTxWrite(const NLongTxService::TLongTxId& longTxId, const ui32 partId) const {
    auto it = LongTxWritesByUniqueId.find(longTxId.UniqueId);
    if (it != LongTxWritesByUniqueId.end()) {
        auto itPart = it->second.find(partId);
        if (itPart != it->second.end()) {
            return itPart->second->InsertWriteId;
        }
    }
    return (TInsertWriteId)0;
}

TInsertWriteId TColumnShard::GetLongTxWrite(NIceDb::TNiceDb& db, const NLongTxService::TLongTxId& longTxId, const ui32 partId, const std::optional<ui32> granuleShardingVersionId) {
    auto it = LongTxWritesByUniqueId.find(longTxId.UniqueId);
    if (it != LongTxWritesByUniqueId.end()) {
        auto itPart = it->second.find(partId);
        if (itPart != it->second.end()) {
            return itPart->second->InsertWriteId;
        }
    } else {
        it = LongTxWritesByUniqueId.emplace(longTxId.UniqueId, TPartsForLTXShard()).first;
    }

    TInsertWriteId insertWriteId = InsertTable->BuildNextWriteId(db);
    auto& lw = LongTxWrites[insertWriteId];
    lw.InsertWriteId = insertWriteId;
    lw.WritePartId = partId;
    lw.LongTxId = longTxId;
    lw.GranuleShardingVersionId = granuleShardingVersionId;
    it->second[partId] = &lw;

    Schema::SaveLongTxWrite(db, insertWriteId, partId, longTxId, granuleShardingVersionId);
    return insertWriteId;
}

void TColumnShard::AddLongTxWrite(const TInsertWriteId writeId, ui64 txId) {
    auto it = LongTxWrites.find(writeId);
    AFL_VERIFY(it != LongTxWrites.end());
    it->second.PreparedTxId = txId;
}

void TColumnShard::LoadLongTxWrite(const TInsertWriteId writeId, const ui32 writePartId, const NLongTxService::TLongTxId& longTxId, const std::optional<ui32> granuleShardingVersion) {
    auto& lw = LongTxWrites[writeId];
    lw.WritePartId = writePartId;
    lw.InsertWriteId = writeId;
    lw.LongTxId = longTxId;
    lw.GranuleShardingVersionId = granuleShardingVersion;
    LongTxWritesByUniqueId[longTxId.UniqueId][writePartId] = &lw;
}

bool TColumnShard::RemoveLongTxWrite(NIceDb::TNiceDb& db, const TInsertWriteId writeId, const ui64 txId) {
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
        } else {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_remove_prepared_tx_insertion")("write_id", (ui64)writeId)("tx_id", txId);
            return false;
        }
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_remove_removed_tx_insertion")("write_id", (ui64)writeId)("tx_id", txId);
        return true;
    }
}

void TColumnShard::TryAbortWrites(NIceDb::TNiceDb& db, NOlap::TDbWrapper& dbTable, THashSet<TInsertWriteId>&& writesToAbort) {
    std::vector<TInsertWriteId> failedAborts;
    for (auto& writeId : writesToAbort) {
        if (!RemoveLongTxWrite(db, writeId, 0)) {
            failedAborts.push_back(writeId);
        }
    }
    if (failedAborts.size()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "failed_aborts")("count", failedAborts.size())("writes_count", writesToAbort.size());
    }
    for (auto& writeId : failedAborts) {
        InsertTable->MarkAsNotAbortable(writeId);
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

void TColumnShard::RunSchemaTx(const NKikimrTxColumnShard::TSchemaTxBody& body, const NOlap::TSnapshot& version,
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

void TColumnShard::RunInit(const NKikimrTxColumnShard::TInitShard& proto, const NOlap::TSnapshot& version,
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

void TColumnShard::RunEnsureTable(const NKikimrTxColumnShard::TCreateTable& tableProto, const NOlap::TSnapshot& version,
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

    NKikimrTxColumnShard::TTableVersionInfo tableVerProto;
    tableVerProto.SetPathId(pathId);

    // check schema changed

    std::optional<NKikimrSchemeOp::TColumnTableSchema> schema;
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
        schema = tableProto.GetSchema();
    }

    {
        THashSet<TString> usedTiers;
        TTableInfo table(pathId);
        if (tableProto.HasTtlSettings()) {
            const auto& ttlSettings = tableProto.GetTtlSettings();
            *tableVerProto.MutableTtlSettings() = ttlSettings;
            if (ttlSettings.HasEnabled()) {
                usedTiers = NOlap::TTiering::GetUsedTiers(ttlSettings.GetEnabled());
            }
        }
        TablesManager.RegisterTable(std::move(table), db);
        if (!usedTiers.empty()) {
            ActivateTiering(pathId, usedTiers);
        }
    }

    tableVerProto.SetSchemaPresetVersionAdj(tableProto.GetSchemaPresetVersionAdj());

    TablesManager.AddTableVersion(pathId, version, tableVerProto, schema, db);
    InsertTable->RegisterPathInfo(pathId);

    Counters.GetTabletCounters()->SetCounter(COUNTER_TABLES, TablesManager.GetTables().size());
    Counters.GetTabletCounters()->SetCounter(COUNTER_TABLE_PRESETS, TablesManager.GetSchemaPresets().size());
    Counters.GetTabletCounters()->SetCounter(COUNTER_TABLE_TTLS, TablesManager.GetTtl().size());
}

void TColumnShard::RunAlterTable(const NKikimrTxColumnShard::TAlterTable& alterProto, const NOlap::TSnapshot& version,
    NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    const ui64 pathId = alterProto.GetPathId();
    Y_ABORT_UNLESS(TablesManager.HasTable(pathId), "AlterTable on a dropped or non-existent table");

    LOG_S_DEBUG("AlterTable for pathId: " << pathId
                                          << " schema: " << alterProto.GetSchema()
                                          << " ttl settings: " << alterProto.GetTtlSettings()
                                          << " at tablet " << TabletID());

    NKikimrTxColumnShard::TTableVersionInfo tableVerProto;
    std::optional<NKikimrSchemeOp::TColumnTableSchema> schema;
    if (alterProto.HasSchemaPreset()) {
        tableVerProto.SetSchemaPresetId(alterProto.GetSchemaPreset().GetId());
        TablesManager.AddSchemaVersion(alterProto.GetSchemaPreset().GetId(), version, alterProto.GetSchemaPreset().GetSchema(), db);
    } else if (alterProto.HasSchema()) {
        schema = alterProto.GetSchema();
    }

    THashSet<TString> usedTiers;
    if (alterProto.HasTtlSettings()) {
        const auto& ttlSettings = alterProto.GetTtlSettings();
        *tableVerProto.MutableTtlSettings() = ttlSettings;

        if (ttlSettings.HasEnabled()) {
            usedTiers = NOlap::TTiering::GetUsedTiers(ttlSettings.GetEnabled());
        }
    }
    ActivateTiering(pathId, usedTiers);

    tableVerProto.SetSchemaPresetVersionAdj(alterProto.GetSchemaPresetVersionAdj());
    TablesManager.AddTableVersion(pathId, version, tableVerProto, schema, db);
}

void TColumnShard::RunDropTable(const NKikimrTxColumnShard::TDropTable& dropProto, const NOlap::TSnapshot& version,
    NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    const ui64 pathId = dropProto.GetPathId();
    if (!TablesManager.HasTable(pathId)) {
        LOG_S_DEBUG("DropTable for unknown or deleted pathId: " << pathId << " at tablet " << TabletID());
        return;
    }

    LOG_S_DEBUG("DropTable for pathId: " << pathId << " at tablet " << TabletID());
    TablesManager.DropTable(pathId, version, db);
}

void TColumnShard::RunAlterStore(const NKikimrTxColumnShard::TAlterStore& proto, const NOlap::TSnapshot& version,
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

void TColumnShard::EnqueueBackgroundActivities(const bool periodic) {
    TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletID()));
    ACFL_DEBUG("event", "EnqueueBackgroundActivities")("periodic", periodic);
    StoragesManager->GetOperatorVerified(NOlap::IStoragesManager::DefaultStorageId);
    StoragesManager->GetSharedBlobsManager()->GetStorageManagerVerified(NOlap::IStoragesManager::DefaultStorageId);
    Counters.GetCSCounters().OnStartBackground();

    if (!TablesManager.HasPrimaryIndex()) {
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("problem", "Background activities cannot be started: no index at tablet");
        return;
    }
    //  !!!!!! MUST BE FIRST THROUGH DATA HAVE TO BE SAME IN SESSIONS AFTER TABLET RESTART
    SharingSessionsManager->Start(*this);

    SetupIndexation();
    SetupCompaction({});
    SetupCleanupPortions();
    SetupCleanupTables();
    SetupMetadata();
    SetupTtl();
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
    NOlap::TSnapshot LastCompletedTx;

protected:
    virtual TConclusionStatus DoExecute(const std::shared_ptr<NConveyor::ITask>& /*taskPtr*/) override {
        NActors::TLogContextGuard g(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletId)("parent_id", ParentActorId));
        {
            NOlap::TConstructionContext context(*TxEvent->IndexInfo, Counters, LastCompletedTx);
            Y_ABORT_UNLESS(TxEvent->IndexChanges->ConstructBlobs(context).Ok());
            if (!TxEvent->IndexChanges->GetWritePortionsCount()) {
                TxEvent->SetPutStatus(NKikimrProto::OK);
            }
        }
        TActorContext::AsActorContext().Send(ParentActorId, std::move(TxEvent));
        return TConclusionStatus::Success();
    }

public:
    virtual TString GetTaskClassIdentifier() const override {
        return ClassId;
    }

    TChangesTask(std::unique_ptr<TEvPrivate::TEvWriteIndex>&& txEvent, const TIndexationCounters& counters, const ui64 tabletId, const TActorId parentActorId, NOlap::TSnapshot lastCompletedTx)
        : TxEvent(std::move(txEvent))
        , Counters(counters)
        , TabletId(tabletId)
        , ParentActorId(parentActorId)
        , LastCompletedTx(lastCompletedTx) {
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
    NOlap::TSnapshot LastCompletedTx;

protected:
    virtual void DoOnDataReady(const std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard) override {
        if (!!resourcesGuard) {
            AFL_VERIFY(!TxEvent->IndexChanges->ResourcesGuard);
            TxEvent->IndexChanges->ResourcesGuard = resourcesGuard;
        } else {
            AFL_VERIFY(TxEvent->IndexChanges->ResourcesGuard);
        }
        TxEvent->IndexChanges->Blobs = ExtractBlobsData();
        const bool isInsert = !!dynamic_pointer_cast<NOlap::TInsertColumnEngineChanges>(TxEvent->IndexChanges);
        std::shared_ptr<NConveyor::ITask> task = std::make_shared<TChangesTask>(std::move(TxEvent), Counters, TabletId, ParentActorId, LastCompletedTx);
        if (isInsert) {
            NConveyor::TInsertServiceOperator::SendTaskToExecute(task);
        } else {
            NConveyor::TCompServiceOperator::SendTaskToExecute(task);
        }
    }
    virtual bool DoOnError(const TString& storageId, const NOlap::TBlobRange& range, const NOlap::IBlobsReadingAction::TErrorStatus& status) override {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "DoOnError")("storage_id", storageId)("blob_id", range)("status", status.GetErrorMessage())("status_code", status.GetStatus());
        AFL_VERIFY(status.GetStatus() != NKikimrProto::EReplyStatus::NODATA)("blob_id", range)("status", status.GetStatus())("error", status.GetErrorMessage())("type", TxEvent->IndexChanges->TypeString())("task_id", TxEvent->IndexChanges->GetTaskIdentifier())("debug", TxEvent->IndexChanges->DebugString());
        TxEvent->SetPutStatus(NKikimrProto::ERROR);
        Counters.ReadErrors->Add(1);
        TActorContext::AsActorContext().Send(ParentActorId, std::move(TxEvent));
        return false;
    }

public:
    TChangesReadTask(std::unique_ptr<TEvPrivate::TEvWriteIndex>&& event, const TActorId parentActorId, const ui64 tabletId, const TIndexationCounters& counters, NOlap::TSnapshot lastCompletedTx)
        : TBase(event->IndexChanges->GetReadingActions(), event->IndexChanges->TypeString(), event->IndexChanges->GetTaskIdentifier())
        , ParentActorId(parentActorId)
        , TabletId(tabletId)
        , TxEvent(std::move(event))
        , Counters(counters)
        , LastCompletedTx(lastCompletedTx) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "start_changes")("type", TxEvent->IndexChanges->TypeString())("task_id", TxEvent->IndexChanges->GetTaskIdentifier());
    }
};

class TDataAccessorsSubscriberBase: public NOlap::IDataAccessorRequestsSubscriber {
private:
    std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard> ResourcesGuard;
    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return Default<std::shared_ptr<const TAtomicCounter>>();
    }

    virtual void DoOnRequestsFinished(NOlap::TDataAccessorsResult&& result) override final {
        AFL_VERIFY(ResourcesGuard);
        DoOnRequestsFinished(std::move(result), std::move(ResourcesGuard));
    }

protected:
    virtual void DoOnRequestsFinished(NOlap::TDataAccessorsResult&& result, std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>&& guard) = 0;

public:
    void SetResourcesGuard(const std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>& guard) {
        AFL_VERIFY(!ResourcesGuard);
        AFL_VERIFY(guard);
        ResourcesGuard = guard;
    }
};

class TDataAccessorsSubscriber: public TDataAccessorsSubscriberBase {
protected:
    const NActors::TActorId ShardActorId;
    std::shared_ptr<NOlap::TColumnEngineChanges> Changes;
    std::shared_ptr<NOlap::TVersionedIndex> VersionedIndex;
    std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard> ResourcesGuard;

    virtual void DoOnRequestsFinishedImpl() = 0;

    virtual void DoOnRequestsFinished(NOlap::TDataAccessorsResult&& result, std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>&& guard) override final {
        Changes->SetFetchedDataAccessors(std::move(result), NOlap::TDataAccessorsInitializationContext(VersionedIndex));
        Changes->ResourcesGuard = std::move(guard);
        DoOnRequestsFinishedImpl();
    }

public:
    void SetResourcesGuard(const std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>& guard) {
        AFL_VERIFY(!ResourcesGuard);
        ResourcesGuard = guard;
    }

    std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>&& ExtractResourcesGuard() {
        AFL_VERIFY(ResourcesGuard);
        return std::move(ResourcesGuard);
    }

    TDataAccessorsSubscriber(const NActors::TActorId& shardActorId, const std::shared_ptr<NOlap::TColumnEngineChanges>& changes,
        const std::shared_ptr<NOlap::TVersionedIndex>& versionedIndex)
        : ShardActorId(shardActorId)
        , Changes(changes)
        , VersionedIndex(versionedIndex) {
    }
};

class TDataAccessorsSubscriberWithRead: public TDataAccessorsSubscriber {
private:
    using TBase = TDataAccessorsSubscriber;

protected:
    const bool CacheDataAfterWrite = false;
    const ui64 ShardTabletId;
    TIndexationCounters Counters;
    NOlap::TSnapshot SnapshotModification;
    const NActors::TActorId ResourceSubscribeActor;
    const NOlap::NResourceBroker::NSubscribe::TTaskContext TaskSubscriptionContext;

public:
    TDataAccessorsSubscriberWithRead(const NActors::TActorId& resourceSubscribeActor, const std::shared_ptr<NOlap::TColumnEngineChanges>& changes,
        const std::shared_ptr<NOlap::TVersionedIndex>& versionedIndex, const bool cacheAfterWrite, const NActors::TActorId& shardActorId,
        const ui64 shardTabletId, const TIndexationCounters& counters, const NOlap::TSnapshot& snapshotModification,
        const NOlap::NResourceBroker::NSubscribe::TTaskContext& taskSubscriptionContext)
        : TBase(shardActorId, changes, versionedIndex)
        , CacheDataAfterWrite(cacheAfterWrite)
        , ShardTabletId(shardTabletId)
        , Counters(counters)
        , SnapshotModification(snapshotModification)
        , ResourceSubscribeActor(resourceSubscribeActor)
        , TaskSubscriptionContext(taskSubscriptionContext)
    {
    }
};

class TInsertChangesReadTask: public TChangesReadTask, public TMonitoringObjectsCounter<TInsertChangesReadTask> {
private:
    using TBase = TChangesReadTask;

public:
    using TBase::TBase;
};

class TCompactChangesReadTask: public TChangesReadTask, public TMonitoringObjectsCounter<TCompactChangesReadTask> {
private:
    using TBase = TChangesReadTask;

public:
    using TBase::TBase;
};

class TTTLChangesReadTask: public TChangesReadTask, public TMonitoringObjectsCounter<TTTLChangesReadTask> {
private:
    using TBase = TChangesReadTask;

public:
    using TBase::TBase;
};

void TColumnShard::StartIndexTask(std::vector<const NOlap::TCommittedData*>&& dataToIndex, const i64 bytesToIndex) {
    Counters.GetCSCounters().IndexationInput(bytesToIndex);

    std::vector<NOlap::TCommittedData> data;
    data.reserve(dataToIndex.size());
    for (auto& ptr : dataToIndex) {
        data.push_back(*ptr);
        if (!TablesManager.HasTable(data.back().GetPathId())) {
            data.back().SetRemove();
        }
    }

    Y_ABORT_UNLESS(data.size());
    auto indexChanges = TablesManager.MutablePrimaryIndex().StartInsert(std::move(data));
    Y_ABORT_UNLESS(indexChanges);

    auto actualIndexInfo = TablesManager.GetPrimaryIndex()->GetVersionedIndexReadonlyCopy();
    indexChanges->Start(*this);
    auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(actualIndexInfo, indexChanges, Settings.CacheDataAfterIndexing);

    const TString externalTaskId = indexChanges->GetTaskIdentifier();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "indexation")("bytes", bytesToIndex)("blobs_count", dataToIndex.size())("max_limit", (i64)Limits.MaxInsertBytes)("has_more", bytesToIndex >= Limits.MaxInsertBytes)("external_task_id", externalTaskId);

    NOlap::NResourceBroker::NSubscribe::ITask::StartResourceSubscription(
        ResourceSubscribeActor, std::make_shared<NOlap::NBlobOperations::NRead::ITask::TReadSubscriber>(
                                    std::make_shared<TInsertChangesReadTask>(std::move(ev), SelfId(), TabletID(), Counters.GetIndexationCounters(), GetLastCompletedTx()),
                                    0, indexChanges->CalcMemoryForUsage(), externalTaskId, InsertTaskSubscription));
}

void TColumnShard::SetupIndexation() {
    if (!AppDataVerified().ColumnShardConfig.GetIndexationEnabled() || !NYDBTest::TControllers::GetColumnShardController()->IsBackgroundEnabled(NYDBTest::ICSController::EBackground::Indexation)) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_indexation")("reason", "disabled");
        return;
    }
    BackgroundController.CheckDeadlinesIndexation();
    if (BackgroundController.GetIndexingActiveCount()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "skip_indexation")("reason", "in_progress")("count", BackgroundController.GetIndexingActiveCount())("insert_overload_size", InsertTable->GetCountersCommitted().Bytes)("indexing_debug", BackgroundController.DebugStringIndexation());
        return;
    }

    bool force = false;
    if (InsertTable->GetPathPriorities().size() && InsertTable->GetPathPriorities().rbegin()->first.GetCategory() == NOlap::TPathInfoIndexPriority::EIndexationPriority::PreventOverload) {
        force = true;
    }
    const ui64 bytesLimit = NYDBTest::TControllers::GetColumnShardController()->GetGuaranteeIndexationStartBytesLimit();
    const TDuration durationLimit = NYDBTest::TControllers::GetColumnShardController()->GetGuaranteeIndexationInterval();
    if (!force && InsertTable->GetCountersCommitted().Bytes < bytesLimit &&
        TMonotonic::Now() < BackgroundController.GetLastIndexationInstant() + durationLimit) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "skip_indexation")("reason", "not_enough_data_and_too_frequency")("insert_size", InsertTable->GetCountersCommitted().Bytes);
        return;
    }

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "start_indexation_tasks")("insert_overload_size", InsertTable->GetCountersCommitted().Bytes);
    Counters.GetCSCounters().OnSetupIndexation();
    ui64 bytesToIndex = 0;
    ui64 txBytesWrite = 0;
    std::vector<const NOlap::TCommittedData*> dataToIndex;
    dataToIndex.reserve(TLimits::MIN_SMALL_BLOBS_TO_INSERT);
    for (auto it = InsertTable->GetPathPriorities().rbegin(); it != InsertTable->GetPathPriorities().rend(); ++it) {
        for (auto* pathInfo : it->second) {
            for (auto& data : pathInfo->GetCommitted()) {
                Y_ABORT_UNLESS(data.BlobSize());
                bytesToIndex += data.BlobSize();
                txBytesWrite += data.GetTxVolume();
                dataToIndex.push_back(&data);
                if (bytesToIndex >= (ui64)Limits.MaxInsertBytes || txBytesWrite >= NOlap::TGlobalLimits::TxWriteLimitBytes || dataToIndex.size() > 500) {
                    StartIndexTask(std::move(dataToIndex), bytesToIndex);
                    dataToIndex.clear();
                    bytesToIndex = 0;
                    txBytesWrite = 0;
                }
            }
        }
    }
    if (dataToIndex.size()) {
        StartIndexTask(std::move(dataToIndex), bytesToIndex);
    }
}

namespace {
class TCompactionAllocated: public NPrioritiesQueue::IRequest {
private:
    const NActors::TActorId TabletActorId;
    virtual void DoOnAllocated(const std::shared_ptr<NPrioritiesQueue::TAllocationGuard>& guard) override {
        NActors::TActorContext::AsActorContext().Send(TabletActorId, new TEvPrivate::TEvStartCompaction(guard));
    }

public:
    TCompactionAllocated(const NActors::TActorId& tabletActorId)
        : TabletActorId(tabletActorId) {
    }
};

}   // namespace

void TColumnShard::SetupCompaction(const std::set<ui64>& pathIds) {
    if (!AppDataVerified().ColumnShardConfig.GetCompactionEnabled() ||
        !NYDBTest::TControllers::GetColumnShardController()->IsBackgroundEnabled(NYDBTest::ICSController::EBackground::Compaction)) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_compaction")("reason", "disabled");
        return;
    }

    BackgroundController.CheckDeadlines();
    if (BackgroundController.GetCompactionsCount()) {
        return;
    }
    const ui64 priority = TablesManager.MutablePrimaryIndex().GetCompactionPriority(DataLocksManager, pathIds, BackgroundController.GetWaitingPriorityOptional());
    if (priority) {
        BackgroundController.UpdateWaitingPriority(priority);
        if (pathIds.size()) {
            NPrioritiesQueue::TCompServiceOperator::AskMax(PrioritizationClientId, priority, std::make_shared<TCompactionAllocated>(SelfId()));
        } else {
            NPrioritiesQueue::TCompServiceOperator::Ask(PrioritizationClientId, priority, std::make_shared<TCompactionAllocated>(SelfId()));
        }
    }
}

class TAccessorsMemorySubscriber: public NOlap::NResourceBroker::NSubscribe::ITask {
private:
    using TBase = NOlap::NResourceBroker::NSubscribe::ITask;
    std::shared_ptr<NOlap::TDataAccessorsRequest> Request;
    std::shared_ptr<TDataAccessorsSubscriberBase> Subscriber;
    std::shared_ptr<NOlap::NDataAccessorControl::IDataAccessorsManager> DataAccessorsManager;

    virtual void DoOnAllocationSuccess(const std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>& guard) override {
        Subscriber->SetResourcesGuard(guard);
        Request->RegisterSubscriber(Subscriber);
        DataAccessorsManager->AskData(Request);
    }

public:
    TAccessorsMemorySubscriber(const ui64 memory, const TString& externalTaskId, const NOlap::NResourceBroker::NSubscribe::TTaskContext& context,
        std::shared_ptr<NOlap::TDataAccessorsRequest>&& request, const std::shared_ptr<TDataAccessorsSubscriberBase>& subscriber,
        const std::shared_ptr<NOlap::NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager)
        : TBase(0, memory, externalTaskId, context)
        , Request(std::move(request))
        , Subscriber(subscriber)
        , DataAccessorsManager(dataAccessorsManager) {
    }
};

class TCompactionDataAccessorsSubscriber: public TDataAccessorsSubscriberWithRead {
private:
    using TBase = TDataAccessorsSubscriberWithRead;

protected:
    virtual void DoOnRequestsFinishedImpl() override {
        const TString externalTaskId = Changes->GetTaskIdentifier();
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "compaction")("external_task_id", externalTaskId);

        auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(VersionedIndex, Changes, CacheDataAfterWrite);
        TActorContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(
            std::make_shared<TCompactChangesReadTask>(std::move(ev), ShardActorId, ShardTabletId, Counters, SnapshotModification)));
    }

public:
    using TBase::TBase;
};

void TColumnShard::StartCompaction(const std::shared_ptr<NPrioritiesQueue::TAllocationGuard>& guard) {
    Counters.GetCSCounters().OnSetupCompaction();
    BackgroundController.ResetWaitingPriority();

    auto indexChanges = TablesManager.MutablePrimaryIndex().StartCompaction(DataLocksManager);
    if (!indexChanges) {
        LOG_S_DEBUG("Compaction not started: cannot prepare compaction at tablet " << TabletID());
        return;
    }

    auto compaction = dynamic_pointer_cast<NOlap::NCompaction::TGeneralCompactColumnEngineChanges>(indexChanges);
    compaction->SetQueueGuard(guard);
    compaction->Start(*this);

    auto actualIndexInfo = TablesManager.GetPrimaryIndex()->GetVersionedIndexReadonlyCopy();
    auto request = compaction->ExtractDataAccessorsRequest();
    const ui64 accessorsMemory = request->PredictAccessorsMemory(TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetLastSchema()) +
                                 indexChanges->CalcMemoryForUsage();
    const auto subscriber = std::make_shared<TCompactionDataAccessorsSubscriber>(ResourceSubscribeActor, indexChanges, actualIndexInfo,
        Settings.CacheDataAfterCompaction, SelfId(), TabletID(), Counters.GetCompactionCounters(), GetLastCompletedTx(),
        CompactTaskSubscription);
    NOlap::NResourceBroker::NSubscribe::ITask::StartResourceSubscription(
        ResourceSubscribeActor, std::make_shared<TAccessorsMemorySubscriber>(accessorsMemory, indexChanges->GetTaskIdentifier(),
                                    CompactTaskSubscription, std::move(request), subscriber, DataAccessorsManager.GetObjectPtrVerified()));
}

class TWriteEvictPortionsDataAccessorsSubscriber: public TDataAccessorsSubscriberWithRead {
private:
    using TBase = TDataAccessorsSubscriberWithRead;

protected:
    virtual void DoOnRequestsFinishedImpl() override {
        ACFL_DEBUG("background", "ttl")("need_writes", true);
        auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(VersionedIndex, Changes, false);
        TActorContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(
            std::make_shared<TTTLChangesReadTask>(std::move(ev), ShardActorId, ShardTabletId, Counters, SnapshotModification)));
    }

public:
    using TBase::TBase;
};

class TNoWriteEvictPortionsDataAccessorsSubscriber: public TDataAccessorsSubscriber {
private:
    using TBase = TDataAccessorsSubscriber;

protected:
    virtual void DoOnRequestsFinishedImpl() override {
        ACFL_DEBUG("background", "ttl")("need_writes", false);
        auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(VersionedIndex, Changes, false);
        ev->SetPutStatus(NKikimrProto::OK);
        NActors::TActivationContext::Send(ShardActorId, std::move(ev));
    }

public:
    using TBase::TBase;
};

class TCSMetadataSubscriber: public TDataAccessorsSubscriberBase, public TObjectCounter<TCSMetadataSubscriber> {
private:
    NActors::TActorId TabletActorId;
    const std::shared_ptr<NOlap::IMetadataAccessorResultProcessor> Processor;
    const ui64 Generation;
    virtual void DoOnRequestsFinished(
        NOlap::TDataAccessorsResult&& result, std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>&& guard) override {
        NActors::TActivationContext::Send(
            TabletActorId, std::make_unique<TEvPrivate::TEvMetadataAccessorsInfo>(Processor, Generation,
                               NOlap::NResourceBroker::NSubscribe::TResourceContainer(std::move(result), std::move(guard))));
    }

public:
    TCSMetadataSubscriber(
        const NActors::TActorId& tabletActorId, const std::shared_ptr<NOlap::IMetadataAccessorResultProcessor>& processor, const ui64 gen)
        : TabletActorId(tabletActorId)
        , Processor(processor)
        , Generation(gen)
    {

    }
};

void TColumnShard::SetupMetadata() {
    if (TObjectCounter<TCSMetadataSubscriber>::ObjectCount()) {
        return;
    }
    std::vector<NOlap::TCSMetadataRequest> requests = TablesManager.MutablePrimaryIndex().CollectMetadataRequests();
    for (auto&& i : requests) {
        const ui64 accessorsMemory =
            i.GetRequest()->PredictAccessorsMemory(TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetLastSchema());
        NOlap::NResourceBroker::NSubscribe::ITask::StartResourceSubscription(ResourceSubscribeActor,
            std::make_shared<TAccessorsMemorySubscriber>(accessorsMemory, i.GetRequest()->GetTaskId(), TTLTaskSubscription,
                std::shared_ptr<NOlap::TDataAccessorsRequest>(i.GetRequest()),
                std::make_shared<TCSMetadataSubscriber>(SelfId(), i.GetProcessor(), Generation()), DataAccessorsManager.GetObjectPtrVerified()));
    }
}

bool TColumnShard::SetupTtl(const THashMap<ui64, NOlap::TTiering>& pathTtls) {
    if (!AppDataVerified().ColumnShardConfig.GetTTLEnabled() ||
        !NYDBTest::TControllers::GetColumnShardController()->IsBackgroundEnabled(NYDBTest::ICSController::EBackground::TTL)) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_ttl")("reason", "disabled");
        return false;
    }
    Counters.GetCSCounters().OnSetupTtl();
    THashMap<ui64, NOlap::TTiering> eviction = pathTtls;
    for (auto&& i : eviction) {
        ACFL_DEBUG("background", "ttl")("path", i.first)("info", i.second.GetDebugString());
    }

    const ui64 memoryUsageLimit = HasAppData() ? AppDataVerified().ColumnShardConfig.GetTieringsMemoryLimit() : ((ui64)512 * 1024 * 1024);
    std::vector<std::shared_ptr<NOlap::TTTLColumnEngineChanges>> indexChanges =
        TablesManager.MutablePrimaryIndex().StartTtl(eviction, DataLocksManager, memoryUsageLimit);

    if (indexChanges.empty()) {
        ACFL_DEBUG("background", "ttl")("skip_reason", "no_changes");
        return false;
    }

    auto actualIndexInfo = TablesManager.GetPrimaryIndex()->GetVersionedIndexReadonlyCopy();
    for (auto&& i : indexChanges) {
        i->Start(*this);
        auto request = i->ExtractDataAccessorsRequest();
        ui64 memoryUsage = 0;
        std::shared_ptr<TDataAccessorsSubscriber> subscriber;
        if (i->NeedConstruction()) {
            subscriber = std::make_shared<TWriteEvictPortionsDataAccessorsSubscriber>(ResourceSubscribeActor, i, actualIndexInfo,
                Settings.CacheDataAfterCompaction, SelfId(), TabletID(), Counters.GetEvictionCounters(), GetLastCompletedTx(),
                TTLTaskSubscription);
            memoryUsage = i->CalcMemoryForUsage();
        } else {
            subscriber = std::make_shared<TNoWriteEvictPortionsDataAccessorsSubscriber>(SelfId(), i, actualIndexInfo);
        }
        const ui64 accessorsMemory =
            request->PredictAccessorsMemory(TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetLastSchema()) + memoryUsage;
        NOlap::NResourceBroker::NSubscribe::ITask::StartResourceSubscription(
            ResourceSubscribeActor, std::make_shared<TAccessorsMemorySubscriber>(accessorsMemory, i->GetTaskIdentifier(), TTLTaskSubscription,
                                        std::move(request), subscriber, DataAccessorsManager.GetObjectPtrVerified()));
    }
    return true;
}

class TCleanupPortionsDataAccessorsSubscriber: public TDataAccessorsSubscriber {
private:
    using TBase = TDataAccessorsSubscriber;

protected:
    virtual void DoOnRequestsFinishedImpl() override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("background", "cleanup")("changes_info", Changes->DebugString());
        auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(VersionedIndex, Changes, false);
        ev->SetPutStatus(NKikimrProto::OK);   // No new blobs to write
        NActors::TActivationContext::Send(ShardActorId, std::move(ev));
    }

public:
    using TBase::TBase;
};

void TColumnShard::SetupCleanupPortions() {
    Counters.GetCSCounters().OnSetupCleanup();
    if (!AppDataVerified().ColumnShardConfig.GetCleanupEnabled() || !NYDBTest::TControllers::GetColumnShardController()->IsBackgroundEnabled(NYDBTest::ICSController::EBackground::Cleanup)) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_cleanup")("reason", "disabled");
        return;
    }
    if (BackgroundController.IsCleanupPortionsActive()) {
        ACFL_DEBUG("background", "cleanup")("skip_reason", "in_progress");
        return;
    }

    const NOlap::TSnapshot minReadSnapshot = GetMinReadSnapshot();
    THashSet<ui64> pathsToDrop = TablesManager.GetPathsToDrop(minReadSnapshot);

    auto changes = TablesManager.MutablePrimaryIndex().StartCleanupPortions(minReadSnapshot, pathsToDrop, DataLocksManager);
    if (!changes) {
        ACFL_DEBUG("background", "cleanup")("skip_reason", "no_changes");
        return;
    }
    changes->Start(*this);

    auto request = changes->ExtractDataAccessorsRequest();
    auto actualIndexInfo = TablesManager.GetPrimaryIndex()->GetVersionedIndexReadonlyCopy();
    const ui64 accessorsMemory = request->PredictAccessorsMemory(TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetLastSchema());
    const auto subscriber = std::make_shared<TCleanupPortionsDataAccessorsSubscriber>(SelfId(), changes, actualIndexInfo);

    NOlap::NResourceBroker::NSubscribe::ITask::StartResourceSubscription(
        ResourceSubscribeActor, std::make_shared<TAccessorsMemorySubscriber>(accessorsMemory, changes->GetTaskIdentifier(), TTLTaskSubscription,
                                    std::move(request), subscriber, DataAccessorsManager.GetObjectPtrVerified()));
}

void TColumnShard::SetupCleanupTables() {
    Counters.GetCSCounters().OnSetupCleanup();
    if (BackgroundController.IsCleanupTablesActive()) {
        ACFL_DEBUG("background", "cleanup")("skip_reason", "in_progress");
        return;
    }

    THashSet<ui64> pathIdsEmptyInInsertTable;
    for (auto&& i : TablesManager.GetPathsToDrop(GetMinReadSnapshot())) {
        if (InsertTable->HasPathIdData(i)) {
            continue;
        }
        pathIdsEmptyInInsertTable.emplace(i);
    }

    auto changes = TablesManager.MutablePrimaryIndex().StartCleanupTables(pathIdsEmptyInInsertTable);
    if (!changes) {
        ACFL_DEBUG("background", "cleanup")("skip_reason", "no_changes");
        return;
    }

    ACFL_DEBUG("background", "cleanup")("changes_info", changes->DebugString());
    auto actualIndexInfo = TablesManager.GetPrimaryIndex()->GetVersionedIndexReadonlyCopy();
    auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(actualIndexInfo, changes, false);
    ev->SetPutStatus(NKikimrProto::OK); // No new blobs to write

    changes->Start(*this);

    Send(SelfId(), ev.release());
}

void TColumnShard::SetupGC() {
    if (!NYDBTest::TControllers::GetColumnShardController()->IsBackgroundEnabled(NYDBTest::ICSController::EBackground::GC)) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_gc")("reason", "disabled");
        return;
    }
    for (auto&& i : StoragesManager->GetStorages()) {
        auto gcTask = i.second->CreateGC();
        if (!gcTask) {
            continue;
        }
        Execute(new TTxGarbageCollectionStart(this, gcTask, i.second), NActors::TActivationContext::AsActorContext());
    }
}

void TColumnShard::Handle(TEvPrivate::TEvStartCompaction::TPtr& ev, const TActorContext& /*ctx*/) {
    StartCompaction(ev->Get()->GetGuard());
}

void TColumnShard::Handle(TEvPrivate::TEvMetadataAccessorsInfo::TPtr& ev, const TActorContext& /*ctx*/) {
    AFL_VERIFY(ev->Get()->GetGeneration() == Generation())("ev", ev->Get()->GetGeneration())("tablet", Generation());
    ev->Get()->GetProcessor()->ApplyResult(
        ev->Get()->ExtractResult(), TablesManager.MutablePrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>());
    SetupMetadata();
}

void TColumnShard::Handle(TEvPrivate::TEvGarbageCollectionFinished::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxGarbageCollectionFinished(this, ev->Get()->Action), ctx);
}

void TColumnShard::SetupCleanupInsertTable() {
    auto writeIdsToCleanup = InsertTable->OldWritesToAbort(AppData()->TimeProvider->Now());

    if (BackgroundController.IsCleanupInsertTableActive()) {
        ACFL_DEBUG("background", "cleanup_insert_table")("skip_reason", "in_progress");
        return;
    }

    if (!InsertTable->GetAborted().size() && !writeIdsToCleanup.size()) {
        return;
    }
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "cleanup_started")("aborted", InsertTable->GetAborted().size())("to_cleanup", writeIdsToCleanup.size());
    BackgroundController.StartCleanupInsertTable();
    Execute(new TTxInsertTableCleanup(this, std::move(writeIdsToCleanup)), TActorContext::AsActorContext());
}

void TColumnShard::Die(const TActorContext& ctx) {
    CleanupActors(ctx);
    NTabletPipe::CloseAndForgetClient(SelfId(), StatsReportPipe);
    UnregisterMediatorTimeCast();
    NYDBTest::TControllers::GetColumnShardController()->OnTabletStopped(*this);
    return IActor::Die(ctx);
}

void TColumnShard::Handle(NActors::TEvents::TEvUndelivered::TPtr& ev, const TActorContext&) {
    ui32 eventType = ev->Get()->SourceType;
    switch (eventType) {
        case NOlap::NDataSharing::NEvents::TEvSendDataFromSource::EventType:
        case NOlap::NDataSharing::NEvents::TEvAckDataToSource::EventType:
        case NOlap::NDataSharing::NEvents::TEvApplyLinksModification::EventType:
        case NOlap::NDataSharing::NEvents::TEvStartToSource::EventType:
        case NOlap::NDataSharing::NEvents::TEvAckFinishToSource::EventType:
        case NOlap::NDataSharing::NEvents::TEvFinishedFromSource::EventType:
            SharingSessionsManager->InitializeEventsExchange(*this, ev->Cookie);
            break;
    }
}

void TColumnShard::Handle(TEvTxProcessing::TEvReadSet::TPtr& ev, const TActorContext& ctx) {
    const ui64 txId = ev->Get()->Record.GetTxId();
    if (!GetProgressTxController().GetTxOperatorOptional(txId)) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "read_set_ignored")("proto", ev->Get()->Record.DebugString());
        Send(MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(
                new TEvTxProcessing::TEvReadSetAck(0, txId, TabletID(), ev->Get()->Record.GetTabletProducer(), TabletID(), 0),
                ev->Get()->Record.GetTabletProducer(), true),
            IEventHandle::FlagTrackDelivery, txId);
        return;
    }
    auto op = GetProgressTxController().GetTxOperatorVerifiedAs<TEvWriteCommitSyncTransactionOperator>(txId);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "read_set")("proto", ev->Get()->Record.DebugString())("lock_id", op->GetLockId());
    NKikimrTx::TReadSetData data;
    AFL_VERIFY(data.ParseFromArray(ev->Get()->Record.GetReadSet().data(), ev->Get()->Record.GetReadSet().size()));
    auto tx = op->CreateReceiveBrokenFlagTx(
        *this, ev->Get()->Record.GetTabletProducer(), data.GetDecision() != NKikimrTx::TReadSetData::DECISION_COMMIT);
    Execute(tx.release(), ctx);
}

void TColumnShard::Handle(TEvTxProcessing::TEvReadSetAck::TPtr& ev, const TActorContext& ctx) {
    auto opPtr = GetProgressTxController().GetTxOperatorOptional(ev->Get()->Record.GetTxId());
    if (!opPtr) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "missed_read_set_ack")("proto", ev->Get()->Record.DebugString())(
            "tx_id", ev->Get()->Record.GetTxId());
        return;
    }
    auto op = TValidator::CheckNotNull(dynamic_pointer_cast<TEvWriteCommitSyncTransactionOperator>(opPtr));
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "read_set_ack")("proto", ev->Get()->Record.DebugString())("lock_id", op->GetLockId());
    auto tx = op->CreateReceiveResultAckTx(*this, ev->Get()->Record.GetTabletConsumer());
    Execute(tx.release(), ctx);
}

void TColumnShard::Handle(NOlap::NDataSharing::NEvents::TEvProposeFromInitiator::TPtr& ev, const TActorContext& ctx) {
    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("process", "BlobsSharing")("event", "TEvProposeFromInitiator");
    auto reqSession = std::make_shared<NOlap::NDataSharing::TDestinationSession>();
    auto conclusion = reqSession->DeserializeDataFromProto(ev->Get()->Record.GetSession(), TablesManager.GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>());
    if (!conclusion) {
        if (!reqSession->GetInitiatorController()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_parse_start_data_sharing_from_initiator");
        } else {
            reqSession->GetInitiatorController().ProposeError(ev->Get()->Record.GetSession().GetSessionId(), conclusion.GetErrorMessage());
        }
        return;
    }

    auto currentSession = SharingSessionsManager->GetDestinationSession(reqSession->GetSessionId());
    if (currentSession) {
        reqSession->GetInitiatorController().ProposeError(ev->Get()->Record.GetSession().GetSessionId(), "Session exists already");
        return;
    }

    auto txConclusion = SharingSessionsManager->ProposeDestSession(this, reqSession);
    Execute(txConclusion.release(), ctx);
}

void TColumnShard::Handle(NOlap::NDataSharing::NEvents::TEvConfirmFromInitiator::TPtr& ev, const TActorContext& ctx) {
    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("process", "BlobsSharing")("event", "TEvConfirmFromInitiator");
    auto currentSession = SharingSessionsManager->GetDestinationSession(ev->Get()->Record.GetSessionId());
    if (!currentSession) {
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("process", "BlobsSharing")("event", "TEvStartFromInitiator")("problem", "not_exists_session")("session_id", ev->Get()->Record.GetSessionId());
        return;
    }
    if (currentSession->IsConfirmed()) {
        currentSession->GetInitiatorController().ConfirmSuccess(ev->Get()->Record.GetSessionId());
    } else {
        auto txConclusion = SharingSessionsManager->ConfirmDestSession(this, currentSession);
        Execute(txConclusion.release(), ctx);
    }
}

void TColumnShard::Handle(NOlap::NDataSharing::NEvents::TEvStartToSource::TPtr& ev, const TActorContext& ctx) {
    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("process", "BlobsSharing")("event", "TEvStartToSource");
    auto reqSession = std::make_shared<NOlap::NDataSharing::TSourceSession>((NOlap::TTabletId)TabletID());
    reqSession->DeserializeFromProto(ev->Get()->Record.GetSession(), {}, {}).Validate();

    auto currentSession = SharingSessionsManager->GetSourceSession(reqSession->GetSessionId());
    if (currentSession) {
        AFL_VERIFY(currentSession->IsEqualTo(*reqSession))("session_current", currentSession->DebugString())("session_new", reqSession->DebugString());
        return;
    }

    auto txConclusion = SharingSessionsManager->InitializeSourceSession(this, reqSession);
    Execute(txConclusion.release(), ctx);
};

void TColumnShard::Handle(NOlap::NDataSharing::NEvents::TEvSendDataFromSource::TPtr& ev, const TActorContext& ctx) {
    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("process", "BlobsSharing")("event", "TEvSendDataFromSource");
    auto currentSession = SharingSessionsManager->GetDestinationSession(ev->Get()->Record.GetSessionId());
    if (!currentSession) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "ignore_inactual_sharing_session")("sesion_id", ev->Get()->Record.GetSessionId());
        return;
    }

    // in current implementation the next loop will crash if schemas will be sent in the same package with the data, so adding this verify to ensure consistent behaviour
    AFL_VERIFY(ev->Get()->Record.GetPathIdData().empty() || ev->Get()->Record.GetSchemeHistory().empty())("reason", "can not send schemas and data in the same package");

    THashMap<ui64, NOlap::NDataSharing::NEvents::TPathIdData> dataByPathId;
    TBlobGroupSelector dsGroupSelector(Info());
    for (auto&& i : ev->Get()->Record.GetPathIdData()) {
        auto data = NOlap::NDataSharing::NEvents::TPathIdData::BuildFromProto(i, TablesManager.GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>().GetVersionedIndex(), dsGroupSelector);
        AFL_VERIFY(data.IsSuccess())("error", data.GetErrorMessage());
        AFL_VERIFY(dataByPathId.emplace(i.GetPathId(), data.DetachResult()).second);
    }

    const auto& schemeHistoryProto = ev->Get()->Record.GetSchemeHistory();

    std::vector<NOlap::TSchemaPresetVersionInfo> schemas;

    for (auto&& i : schemeHistoryProto) {
        schemas.emplace_back(i);
    }

    auto txConclusion = currentSession->ReceiveData(this, std::move(dataByPathId), std::move(schemas), ev->Get()->Record.GetPackIdx(), (NOlap::TTabletId)ev->Get()->Record.GetSourceTabletId(), currentSession);
    if (!txConclusion) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_received_data");
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "on_received_data");
        Execute(txConclusion->release(), ctx);
    }
};

void TColumnShard::Handle(NOlap::NDataSharing::NEvents::TEvAckDataToSource::TPtr& ev, const TActorContext& ctx) {
    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("process", "BlobsSharing")("event", "TEvAckDataToSource");
    auto currentSession = SharingSessionsManager->GetSourceSession(ev->Get()->Record.GetSessionId());
    if (!currentSession) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "ignore_inactual_sharing_session")("sesion_id", ev->Get()->Record.GetSessionId());
        return;
    }

    auto txConclusion = currentSession->AckData(this, ev->Get()->Record.GetPackIdx(), currentSession);
    if (!txConclusion) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_ack_data");
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "on_ack_data");
        Execute(txConclusion->release(), ctx);
    }
};

void TColumnShard::Handle(NOlap::NDataSharing::NEvents::TEvAckFinishToSource::TPtr& ev, const TActorContext& ctx) {
    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("process", "BlobsSharing")("event", "TEvAckFinishToSource");
    auto currentSession = SharingSessionsManager->GetSourceSession(ev->Get()->Record.GetSessionId());
    if (!currentSession) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "ignore_inactual_sharing_session")("sesion_id", ev->Get()->Record.GetSessionId());
        return;
    }

    auto txConclusion = currentSession->AckFinished(this, currentSession);
    if (!txConclusion) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_ack_finish");
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "on_ack_finish");
        Execute(txConclusion->release(), ctx);
    }
};

void TColumnShard::Handle(NOlap::NDataSharing::NEvents::TEvFinishedFromSource::TPtr& ev, const TActorContext& ctx) {
    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("process", "BlobsSharing")("event", "TEvFinishedFromSource");
    auto currentSession = SharingSessionsManager->GetDestinationSession(ev->Get()->Record.GetSessionId());
    if (!currentSession) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "ignore_inactual_sharing_session")("sesion_id", ev->Get()->Record.GetSessionId());
        return;
    }

    auto txConclusion = currentSession->ReceiveFinished(this, (NOlap::TTabletId)ev->Get()->Record.GetSourceTabletId(), currentSession);
    if (!txConclusion) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_finished_data")("error", txConclusion.GetErrorMessage());
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "on_finished_data");
        Execute(txConclusion->release(), ctx);
    }
};

class TPortionConstructorV2 {
private:
    NOlap::TPortionInfo::TConstPtr PortionInfo;
    std::optional<NOlap::TColumnChunkLoadContextV2> Records;
    std::optional<std::vector<NOlap::TIndexChunkLoadContext>> Indexes;

public:
    TPortionConstructorV2(const NOlap::TPortionInfo::TConstPtr& portionInfo)
        : PortionInfo(portionInfo) {
    }

    bool IsReady() const {
        return HasRecords() && HasIndexes();
    }

    bool HasRecords() const {
        return !!Records;
    }

    bool HasIndexes() const {
        return !!Indexes;
    }

    void SetRecords(NOlap::TColumnChunkLoadContextV2&& records) {
        AFL_VERIFY(!Records);
        Records = std::move(records);
    }

    void SetIndexes(std::vector<NOlap::TIndexChunkLoadContext>&& indexes) {
        AFL_VERIFY(!Indexes);
        Indexes = std::move(indexes);
    }

    NOlap::TPortionDataAccessor BuildAccessor() {
        AFL_VERIFY(PortionInfo);
        AFL_VERIFY(Records)("portion_id", PortionInfo->GetPortionId())("path_id", PortionInfo->GetPathId());
        AFL_VERIFY(Indexes)("portion_id", PortionInfo->GetPortionId())("path_id", PortionInfo->GetPathId());
        std::vector<NOlap::TColumnChunkLoadContextV1> records = Records->BuildRecordsV1();
        return NOlap::TPortionAccessorConstructor::BuildForLoading(std::move(PortionInfo), std::move(records), std::move(*Indexes));
    }
};

class TAccessorsParsingTask: public NConveyor::ITask {
private:
    std::shared_ptr<NOlap::NDataAccessorControl::IAccessorCallback> FetchCallback;
    std::vector<TPortionConstructorV2> Portions;

    virtual TConclusionStatus DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override {
        std::vector<NOlap::TPortionDataAccessor> accessors;
        accessors.reserve(Portions.size());
        for (auto&& i : Portions) {
            accessors.emplace_back(i.BuildAccessor());
        }
        FetchCallback->OnAccessorsFetched(std::move(accessors));
        return TConclusionStatus::Success();
    }
    virtual void DoOnCannotExecute(const TString& reason) override {
        AFL_VERIFY(false)("cannot parse metadata", reason);
    }

public:
    virtual TString GetTaskClassIdentifier() const override {
        return "ASKED_METADATA_PARSER";
    }

    TAccessorsParsingTask(
        const std::shared_ptr<NOlap::NDataAccessorControl::IAccessorCallback>& callback, std::vector<TPortionConstructorV2>&& portions)
        : FetchCallback(callback)
        , Portions(std::move(portions))
    {

    }
};

class TTxAskPortionChunks: public TTransactionBase<TColumnShard> {
private:
    using TBase = TTransactionBase<TColumnShard>;
    std::shared_ptr<NOlap::NDataAccessorControl::IAccessorCallback> FetchCallback;
    THashMap<ui64, std::vector<NOlap::TPortionInfo::TConstPtr>> PortionsByPath;
    std::vector<TPortionConstructorV2> FetchedAccessors;
    const TString Consumer;
    THashMap<NOlap::TPortionAddress, TPortionConstructorV2> Constructors;

public:
    TTxAskPortionChunks(TColumnShard* self, const std::shared_ptr<NOlap::NDataAccessorControl::IAccessorCallback>& fetchCallback,
        std::vector<NOlap::TPortionInfo::TConstPtr>&& portions, const TString& consumer)
        : TBase(self)
        , FetchCallback(fetchCallback)
        , Consumer(consumer)
    {
        for (auto&& i : portions) {
            PortionsByPath[i->GetPathId()].emplace_back(i);
        }
    }

    bool Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        NIceDb::TNiceDb db(txc.DB);
        
        TBlobGroupSelector selector(Self->Info());
        bool reask = false;
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("consumer", Consumer)("event", "TTxAskPortionChunks::Execute");
        for (auto&& i : PortionsByPath) {
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("size", i.second.size())("path_id", i.first);
            for (auto&& p : i.second) {
                auto itPortionConstructor = Constructors.find(p->GetAddress());
                if (itPortionConstructor == Constructors.end()) {
                    TPortionConstructorV2 constructor(p);
                    itPortionConstructor = Constructors.emplace(p->GetAddress(), std::move(constructor)).first;
                } else if (itPortionConstructor->second.IsReady()) {
                    continue;
                }
                if (!itPortionConstructor->second.HasRecords()) {
                    auto rowset = db.Table<NColumnShard::Schema::IndexColumnsV2>().Key(p->GetPathId(), p->GetPortionId()).Select();
                    if (!rowset.IsReady()) {
                        reask = true;
                    } else {
                        AFL_VERIFY(!rowset.EndOfSet())("path_id", p->GetPathId())("portion_id", p->GetPortionId())(
                            "debug", p->DebugString(true));
                        NOlap::TColumnChunkLoadContextV2 info(rowset);
                        itPortionConstructor->second.SetRecords(std::move(info));
                    }
                }
                if (!itPortionConstructor->second.HasIndexes()) {
                    if (!p->GetSchema(Self->GetIndexAs<NOlap::TColumnEngineForLogs>().GetVersionedIndex())->GetIndexesCount()) {
                        itPortionConstructor->second.SetIndexes({});
                    } else {
                        auto rowset = db.Table<NColumnShard::Schema::IndexIndexes>().Prefix(p->GetPathId(), p->GetPortionId()).Select();
                        if (!rowset.IsReady()) {
                            reask = true;
                        } else {
                            std::vector<NOlap::TIndexChunkLoadContext> indexes;
                            bool localReask = false;
                            while (!localReask && !rowset.EndOfSet()) {
                                indexes.emplace_back(NOlap::TIndexChunkLoadContext(rowset, &selector));
                                if (!rowset.Next()) {
                                    reask = true;
                                    localReask = true;
                                }
                            }
                            itPortionConstructor->second.SetIndexes(std::move(indexes));
                        }
                    }
                }
            }
        }
        if (reask) {
            return false;
        }

        for (auto&& i : Constructors) {
            FetchedAccessors.emplace_back(std::move(i.second));
        }

        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("stage", "finished");
        NConveyor::TInsertServiceOperator::AsyncTaskToExecute(std::make_shared<TAccessorsParsingTask>(FetchCallback, std::move(FetchedAccessors)));
        return true;
    }
    void Complete(const TActorContext& /*ctx*/) override {
    }
    TTxType GetTxType() const override {
        return TXTYPE_ASK_PORTION_METADATA;
    }
};

void TColumnShard::Handle(NOlap::NDataAccessorControl::TEvAskTabletDataAccessors::TPtr& ev, const TActorContext& /*ctx*/) {
    Execute(new TTxAskPortionChunks(this, ev->Get()->GetCallback(), std::move(ev->Get()->MutablePortions()), ev->Get()->GetConsumer()));
}

void TColumnShard::Handle(NOlap::NDataSharing::NEvents::TEvAckFinishFromInitiator::TPtr& ev, const TActorContext& ctx) {
    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("process", "BlobsSharing")("event", "TEvAckFinishFromInitiator");
    auto currentSession = SharingSessionsManager->GetDestinationSession(ev->Get()->Record.GetSessionId());
    if (!currentSession) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "ignore_inactual_sharing_session")("sesion_id", ev->Get()->Record.GetSessionId());
        return;
    }

    auto txConclusion = currentSession->AckInitiatorFinished(this, currentSession);
    if (!txConclusion) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_initiator_ack_finished_data");
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "on_initiator_ack_finished_data");
        Execute(txConclusion->release(), ctx);
    }
};

void TColumnShard::Handle(NOlap::NDataSharing::NEvents::TEvApplyLinksModification::TPtr& ev, const TActorContext& ctx) {
    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("process", "BlobsSharing")("event", "TEvApplyLinksModification")("info", ev->Get()->Record.DebugString());
    NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletID())("event", "TEvChangeBlobsOwning");

    auto task = std::make_shared<NOlap::NDataSharing::TTaskForTablet>((NOlap::TTabletId)TabletID());
    auto parsed = task->DeserializeFromProto(ev->Get()->Record.GetTask());
    AFL_VERIFY(!!parsed)("error", parsed.GetErrorMessage());

    AFL_VERIFY(task->GetTabletId() == (NOlap::TTabletId)TabletID());
    auto txConclusion = task->BuildModificationTransaction(this, (NOlap::TTabletId)ev->Get()->Record.GetInitiatorTabletId(), ev->Get()->Record.GetSessionId(), ev->Get()->Record.GetPackIdx(), task);
    AFL_VERIFY(!!txConclusion)("error", txConclusion.GetErrorMessage());
    Execute(txConclusion->release(), ctx);
}

void TColumnShard::Handle(NOlap::NDataSharing::NEvents::TEvApplyLinksModificationFinished::TPtr& ev, const TActorContext& ctx) {
    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("process", "BlobsSharing")("event", "TEvApplyLinksModificationFinished");
    auto currentSession = SharingSessionsManager->GetSourceSession(ev->Get()->Record.GetSessionId());
    if (!currentSession) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "ignore_inactual_sharing_session")("sesion_id", ev->Get()->Record.GetSessionId());
        return;
    }
    const NOlap::TTabletId modifiedTabletId = (NOlap::TTabletId)ev->Get()->Record.GetModifiedTabletId();
    auto txConclusion = currentSession->AckLinks(this, modifiedTabletId, ev->Get()->Record.GetPackIdx(), currentSession);

    if (!txConclusion) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_change_links_finish")("error", txConclusion.GetErrorMessage())("tablet_id", modifiedTabletId);
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "on_change_links_finish")("tablet_id", modifiedTabletId);
        Execute(txConclusion->release(), ctx);
    }
}

void TColumnShard::Handle(TAutoPtr<TEventHandle<NOlap::NBackground::TEvExecuteGeneralLocalTransaction>>& ev, const TActorContext& ctx) {
    Execute(ev->Get()->ExtractTransaction().release(), ctx);
}

void TColumnShard::Handle(NOlap::NBlobOperations::NEvents::TEvDeleteSharedBlobs::TPtr& ev, const TActorContext& ctx) {
    if (SharingSessionsManager->IsSharingInProgress()) {
        ctx.Send(NActors::ActorIdFromProto(ev->Get()->Record.GetSourceActorId()),
            new NOlap::NBlobOperations::NEvents::TEvDeleteSharedBlobsFinished(
                (NOlap::TTabletId)TabletID(), NKikimrColumnShardBlobOperationsProto::TEvDeleteSharedBlobsFinished::DestinationCurrenlyLocked));
        for (auto&& i : ev->Get()->Record.GetBlobIds()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS)("event", "sharing_in_progress")("blob_id", i)(
                "from_tablet", ev->Get()->Record.GetSourceTabletId());
        }

        return;
    }

    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("process", "BlobsSharing")("event", "TEvDeleteSharedBlobs");
    NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletID())("event", "TEvDeleteSharedBlobs");
    NOlap::TTabletsByBlob blobs;
    for (auto&& i : ev->Get()->Record.GetBlobIds()) {
        auto blobId = NOlap::TUnifiedBlobId::BuildFromString(i, nullptr);
        AFL_VERIFY(!!blobId)("problem", blobId.GetErrorMessage());
        AFL_VERIFY(blobs.Add((NOlap::TTabletId)ev->Get()->Record.GetSourceTabletId(), blobId.DetachResult()));
    }
    Execute(new TTxRemoveSharedBlobs(this, blobs, NActors::ActorIdFromProto(ev->Get()->Record.GetSourceActorId()), ev->Get()->Record.GetStorageId()), ctx);
}

void TColumnShard::ActivateTiering(const ui64 pathId, const THashSet<TString>& usedTiers) {
    AFL_VERIFY(Tiers);
    if (!usedTiers.empty()) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "activate_tiering")("path_id", pathId)("tiers", JoinStrings(usedTiers.begin(), usedTiers.end(), ","));
        Tiers->EnablePathId(pathId, usedTiers);
    } else {
        Tiers->DisablePathId(pathId);
    }
    OnTieringModified(pathId);
}

void TColumnShard::Enqueue(STFUNC_SIG) {
    const TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletID())(
        "self_id", SelfId())("process", "Enqueue")("ev", ev->GetTypeName());
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvPrivate::TEvTieringModified, HandleInit);
        HFunc(TEvPrivate::TEvNormalizerResult, Handle);
        HFunc(NOlap::NDataAccessorControl::TEvAskTabletDataAccessors, Handle);
        default:
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "unexpected event in enqueue");
            return NTabletFlatExecutor::TTabletExecutedFlat::Enqueue(ev);
    }
}

void TColumnShard::OnTieringModified(const std::optional<ui64> pathId) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "OnTieringModified")("path_id", pathId);
    StoragesManager->OnTieringModified(Tiers);
    if (TablesManager.HasPrimaryIndex()) {
        if (pathId) {
            std::optional<NOlap::TTiering> tableTtl;
            if (auto* findTtl = TablesManager.GetTtl().FindPtr(*pathId)) {
                tableTtl = *findTtl;
            }
            TablesManager.MutablePrimaryIndex().OnTieringModified(tableTtl, *pathId);
        } else {
            TablesManager.MutablePrimaryIndex().OnTieringModified(TablesManager.GetTtl());
        }
    }
}

const NKikimr::NColumnShard::NTiers::TManager* TColumnShard::GetTierManagerPointer(const TString& tierId) const {
    Y_ABORT_UNLESS(!!Tiers);
    return Tiers->GetManagerOptional(tierId);
}

} // namespace NKikimr::NColumnShard
