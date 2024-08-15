#include "columnshard_impl.h"
#include "blob.h"
#include "columnshard_schema.h"
#include "common/tablet_id.h"
#include "blobs_reader/task.h"
#include "blobs_reader/events.h"
#include "blobs_action/bs/storage.h"
#include "resource_subscriber/task.h"

#ifndef KIKIMR_DISABLE_S3_OPS
#include "blobs_action/tier/storage.h"
#endif

#include "blobs_reader/actor.h"
#include "blobs_action/storages_manager/manager.h"
#include "blobs_action/transaction/tx_remove_blobs.h"
#include "blobs_action/transaction/tx_gc_insert_table.h"
#include "blobs_action/transaction/tx_gc_indexed.h"
#include "bg_tasks/events/events.h"

#include "data_sharing/destination/session/destination.h"
#include "data_sharing/source/session/source.h"
#include "data_sharing/common/transactions/tx_extension.h"

#include "engines/changes/indexation.h"
#include "engines/changes/cleanup_portions.h"
#include "engines/changes/cleanup_tables.h"
#include "engines/changes/ttl.h"

#include "resource_subscriber/counters.h"

#include "bg_tasks/adapter/adapter.h"
#include "bg_tasks/manager/manager.h"
#include "hooks/abstract/abstract.h"

#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tx/tiering/external_data.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/services/metadata/service.h>
#include <ydb/core/tx/tiering/manager.h>
#include <ydb/core/tx/conveyor/usage/service.h>

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
    , TabletCountersHolder(new TProtobufTabletCounters<ESimpleCounters_descriptor, ECumulativeCounters_descriptor,
          EPercentileCounters_descriptor, ETxTypes_descriptor>())
    , Counters(*TabletCountersHolder)
    , ProgressTxController(std::make_unique<TTxController>(*this))
    , StoragesManager(std::make_shared<NOlap::TStoragesManager>(*this))
    , DataLocksManager(std::make_shared<NOlap::NDataLocks::TManager>())
    , PeriodicWakeupActivationPeriod(NYDBTest::TControllers::GetColumnShardController()->GetPeriodicWakeupActivationPeriod(
          TSettings::DefaultPeriodicWakeupActivationPeriod))
    , StatsReportInterval(NYDBTest::TControllers::GetColumnShardController()->GetStatsReportInterval(TSettings::DefaultStatsReportInterval))
    , InFlightReadsTracker(StoragesManager, Counters.GetRequestsTracingCounters())
    , TablesManager(StoragesManager, info->TabletID)
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
    ui64 delayMillisec = GetMaxReadStaleness().MilliSeconds();
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

TWriteId TColumnShard::HasLongTxWrite(const NLongTxService::TLongTxId& longTxId, const ui32 partId) const {
    auto it = LongTxWritesByUniqueId.find(longTxId.UniqueId);
    if (it != LongTxWritesByUniqueId.end()) {
        auto itPart = it->second.find(partId);
        if (itPart != it->second.end()) {
            return (TWriteId)itPart->second->WriteId;
        }
    }
    return (TWriteId)0;
}

TWriteId TColumnShard::GetLongTxWrite(NIceDb::TNiceDb& db, const NLongTxService::TLongTxId& longTxId, const ui32 partId, const std::optional<ui32> granuleShardingVersionId) {
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
    lw.GranuleShardingVersionId = granuleShardingVersionId;
    it->second[partId] = &lw;

    Schema::SaveLongTxWrite(db, writeId, partId, longTxId, granuleShardingVersionId);
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

void TColumnShard::LoadLongTxWrite(TWriteId writeId, const ui32 writePartId, const NLongTxService::TLongTxId& longTxId, const std::optional<ui32> granuleShardingVersion) {
    auto& lw = LongTxWrites[writeId];
    lw.WritePartId = writePartId;
    lw.WriteId = (ui64)writeId;
    lw.LongTxId = longTxId;
    lw.GranuleShardingVersionId = granuleShardingVersion;
    LongTxWritesByUniqueId[longTxId.UniqueId][writePartId] = &lw;
}

bool TColumnShard::RemoveLongTxWrite(NIceDb::TNiceDb& db, const TWriteId writeId, const ui64 txId) {
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
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_remove_prepared_tx_insertion")("write_id", writeId)("tx_id", txId);
        }
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_remove_removed_tx_insertion")("write_id", writeId)("tx_id", txId);
    }
    return false;
}

void TColumnShard::TryAbortWrites(NIceDb::TNiceDb& db, NOlap::TDbWrapper& dbTable, THashSet<TWriteId>&& writesToAbort) {
    std::vector<TWriteId> failedAborts;
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

    if (tableProto.HasSchemaPreset()) {
        Y_ABORT_UNLESS(!tableProto.HasSchema(), "Tables has either schema or preset");

        TSchemaPreset preset;
        preset.Deserialize(tableProto.GetSchemaPreset());
        Y_ABORT_UNLESS(!preset.IsStandaloneTable());
        tableVerProto.SetSchemaPresetId(preset.GetId());

        if (TablesManager.RegisterSchemaPreset(preset, db)) {
            TablesManager.AddSchemaVersion(tableProto.GetSchemaPreset().GetId(), version, tableProto.GetSchemaPreset().GetSchema(), db, Tiers);
        }
    } else {
        Y_ABORT_UNLESS(tableProto.HasSchema(), "Tables has either schema or preset");
        *tableVerProto.MutableSchema() = tableProto.GetSchema();
    }

    {
        bool needTieringActivation = false;
        TTableInfo table(pathId);
        if (tableProto.HasTtlSettings()) {
            const auto& ttlSettings = tableProto.GetTtlSettings();
            *tableVerProto.MutableTtlSettings() = ttlSettings;
            if (ttlSettings.HasUseTiering()) {
                table.SetTieringUsage(ttlSettings.GetUseTiering());
                needTieringActivation = true;
            }
        }
        const TString tieringName = table.GetTieringUsage();
        TablesManager.RegisterTable(std::move(table), db);
        if (needTieringActivation) {
            ActivateTiering(pathId, tieringName);
        }
    }

    tableVerProto.SetSchemaPresetVersionAdj(tableProto.GetSchemaPresetVersionAdj());

    TablesManager.AddTableVersion(pathId, version, tableVerProto, db, Tiers);

    Counters.GetTabletCounters()->SetCounter(COUNTER_TABLES, TablesManager.GetTables().size());
    Counters.GetTabletCounters()->SetCounter(COUNTER_TABLE_PRESETS, TablesManager.GetSchemaPresets().size());
    Counters.GetTabletCounters()->SetCounter(COUNTER_TABLE_TTLS, TablesManager.GetTtl().PathsCount());
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
    if (alterProto.HasSchemaPreset()) {
        tableVerProto.SetSchemaPresetId(alterProto.GetSchemaPreset().GetId());
        TablesManager.AddSchemaVersion(alterProto.GetSchemaPreset().GetId(), version, alterProto.GetSchemaPreset().GetSchema(), db, Tiers);
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
    TablesManager.AddTableVersion(pathId, version, tableVerProto, db, Tiers);
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

    // TODO: Allow to read old snapshots after DROP
    TBlobGroupSelector dsGroupSelector(Info());
    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);
    THashSet<TWriteId> writesToAbort = InsertTable->DropPath(dbTable, pathId);

    TryAbortWrites(db, dbTable, std::move(writesToAbort));
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
        TablesManager.AddSchemaVersion(presetProto.GetId(), version, presetProto.GetSchema(), db, Tiers);
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
    SetupCompaction();
    SetupCleanupPortions();
    SetupCleanupTables();
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
        , LastCompletedTx(lastCompletedTx)
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
    NOlap::TSnapshot LastCompletedTx;
protected:
    virtual void DoOnDataReady(const std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard) override {
        TxEvent->IndexChanges->Blobs = ExtractBlobsData();
        TxEvent->IndexChanges->ResourcesGuard = resourcesGuard;
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
        AFL_VERIFY(status.GetStatus() != NKikimrProto::EReplyStatus::NODATA)("blob_id", range)("status", status.GetStatus())
            ("error", status.GetErrorMessage())("type", TxEvent->IndexChanges->TypeString())("task_id", TxEvent->IndexChanges->GetTaskIdentifier())
            ("debug", TxEvent->IndexChanges->DebugString());
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
        , LastCompletedTx(lastCompletedTx)
    {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "start_changes")("type", TxEvent->IndexChanges->TypeString())("task_id", TxEvent->IndexChanges->GetTaskIdentifier());
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

void TColumnShard::StartIndexTask(std::vector<const NOlap::TInsertedData*>&& dataToIndex, const i64 bytesToIndex) {
    Counters.GetCSCounters().IndexationInput(bytesToIndex);

    std::vector<NOlap::TInsertedData> data;
    data.reserve(dataToIndex.size());
    for (auto& ptr : dataToIndex) {
        data.push_back(*ptr);
    }

    Y_ABORT_UNLESS(data.size());
    auto indexChanges = TablesManager.MutablePrimaryIndex().StartInsert(std::move(data));
    Y_ABORT_UNLESS(indexChanges);

    auto actualIndexInfo = std::make_shared<NOlap::TVersionedIndex>(TablesManager.GetPrimaryIndex()->GetVersionedIndex());
    indexChanges->Start(*this);
    auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(actualIndexInfo, indexChanges, Settings.CacheDataAfterIndexing);

    const TString externalTaskId = indexChanges->GetTaskIdentifier();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "indexation")("bytes", bytesToIndex)("blobs_count", dataToIndex.size())("max_limit", (i64)Limits.MaxInsertBytes)
        ("has_more", bytesToIndex >= Limits.MaxInsertBytes)("external_task_id", externalTaskId);

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
    Counters.GetCSCounters().OnSetupIndexation();
    ui64 bytesToIndex = 0;
    ui64 txBytesWrite = 0;
    std::vector<const NOlap::TInsertedData*> dataToIndex;
    dataToIndex.reserve(TLimits::MIN_SMALL_BLOBS_TO_INSERT);
    for (auto it = InsertTable->GetPathPriorities().rbegin(); it != InsertTable->GetPathPriorities().rend(); ++it) {
        for (auto* pathInfo : it->second) {
            for (auto& data : pathInfo->GetCommitted()) {
                Y_ABORT_UNLESS(data.BlobSize());
                bytesToIndex += data.BlobSize();
                txBytesWrite += data.GetTxVolume();
                dataToIndex.push_back(&data);
                if (bytesToIndex >= (ui64)Limits.MaxInsertBytes || txBytesWrite >= NOlap::TGlobalLimits::TxWriteLimitBytes) {
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

void TColumnShard::SetupCompaction() {
    if (!AppDataVerified().ColumnShardConfig.GetCompactionEnabled() || !NYDBTest::TControllers::GetColumnShardController()->IsBackgroundEnabled(NYDBTest::ICSController::EBackground::Compaction)) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_compaction")("reason", "disabled");
        return;
    }
    Counters.GetCSCounters().OnSetupCompaction();

    BackgroundController.CheckDeadlines();
    while (BackgroundController.GetCompactionsCount() < TSettings::MAX_ACTIVE_COMPACTIONS) {
        auto indexChanges = TablesManager.MutablePrimaryIndex().StartCompaction(DataLocksManager);
        if (!indexChanges) {
            LOG_S_DEBUG("Compaction not started: cannot prepare compaction at tablet " << TabletID());
            break;
        }

        indexChanges->Start(*this);

        auto actualIndexInfo = std::make_shared<NOlap::TVersionedIndex>(TablesManager.GetPrimaryIndex()->GetVersionedIndex());
        auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(actualIndexInfo, indexChanges, Settings.CacheDataAfterCompaction);
        const TString externalTaskId = indexChanges->GetTaskIdentifier();
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "compaction")("external_task_id", externalTaskId);

        NOlap::NResourceBroker::NSubscribe::ITask::StartResourceSubscription(
            ResourceSubscribeActor, std::make_shared<NOlap::NBlobOperations::NRead::ITask::TReadSubscriber>(
                std::make_shared<TCompactChangesReadTask>(std::move(ev), SelfId(), TabletID(), Counters.GetCompactionCounters(), GetLastCompletedTx()), 0, indexChanges->CalcMemoryForUsage(), externalTaskId, CompactTaskSubscription));
    }

    LOG_S_DEBUG("ActiveCompactions: " << BackgroundController.GetCompactionsCount() << " at tablet " << TabletID());
}

bool TColumnShard::SetupTtl(const THashMap<ui64, NOlap::TTiering>& pathTtls) {
    if (!AppDataVerified().ColumnShardConfig.GetTTLEnabled() || !NYDBTest::TControllers::GetColumnShardController()->IsBackgroundEnabled(NYDBTest::ICSController::EBackground::TTL)) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_ttl")("reason", "disabled");
        return false;
    }
    Counters.GetCSCounters().OnSetupTtl();
    THashMap<ui64, NOlap::TTiering> eviction = pathTtls;
    for (auto&& i : eviction) {
        ACFL_DEBUG("background", "ttl")("path", i.first)("info", i.second.GetDebugString());
    }

    const ui64 memoryUsageLimit = HasAppData() ? AppDataVerified().ColumnShardConfig.GetTieringsMemoryLimit() : ((ui64)512 * 1024 * 1024);
    std::vector<std::shared_ptr<NOlap::TTTLColumnEngineChanges>> indexChanges = TablesManager.MutablePrimaryIndex().StartTtl(eviction, DataLocksManager, memoryUsageLimit);

    if (indexChanges.empty()) {
        ACFL_DEBUG("background", "ttl")("skip_reason", "no_changes");
        return false;
    }

    auto actualIndexInfo = std::make_shared<NOlap::TVersionedIndex>(TablesManager.GetPrimaryIndex()->GetVersionedIndex());
    for (auto&& i : indexChanges) {
        const TString externalTaskId = i->GetTaskIdentifier();
        const bool needWrites = i->NeedConstruction();
        ACFL_DEBUG("background", "ttl")("need_writes", needWrites);
        i->Start(*this);
        auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(actualIndexInfo, i, false);
        if (needWrites) {
            NOlap::NResourceBroker::NSubscribe::ITask::StartResourceSubscription(
                ResourceSubscribeActor, std::make_shared<NOlap::NBlobOperations::NRead::ITask::TReadSubscriber>(
                    std::make_shared<TTTLChangesReadTask>(std::move(ev), SelfId(), TabletID(), Counters.GetCompactionCounters(), GetLastCompletedTx()),
                    0, i->CalcMemoryForUsage(), externalTaskId, TTLTaskSubscription));
        } else {
            ev->SetPutStatus(NKikimrProto::OK);
            ActorContext().Send(SelfId(), std::move(ev));
        }
    }
    return true;
}

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

    auto changes =
        TablesManager.MutablePrimaryIndex().StartCleanupPortions(GetMinReadSnapshot(), TablesManager.GetPathsToDrop(), DataLocksManager);
    if (!changes) {
        ACFL_DEBUG("background", "cleanup")("skip_reason", "no_changes");
        return;
    }

    ACFL_DEBUG("background", "cleanup")("changes_info", changes->DebugString());
    auto actualIndexInfo = std::make_shared<NOlap::TVersionedIndex>(TablesManager.GetPrimaryIndex()->GetVersionedIndex());
    auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(actualIndexInfo, changes, false);
    ev->SetPutStatus(NKikimrProto::OK); // No new blobs to write

    changes->Start(*this);

    Send(SelfId(), ev.release());
}

void TColumnShard::SetupCleanupTables() {
    Counters.GetCSCounters().OnSetupCleanup();
    if (BackgroundController.IsCleanupTablesActive()) {
        ACFL_DEBUG("background", "cleanup")("skip_reason", "in_progress");
        return;
    }

    auto changes = TablesManager.MutablePrimaryIndex().StartCleanupTables(TablesManager.MutablePathsToDrop());
    if (!changes) {
        ACFL_DEBUG("background", "cleanup")("skip_reason", "no_changes");
        return;
    }

    ACFL_DEBUG("background", "cleanup")("changes_info", changes->DebugString());
    auto actualIndexInfo = std::make_shared<NOlap::TVersionedIndex>(TablesManager.GetPrimaryIndex()->GetVersionedIndex());
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

    THashMap<ui64, NOlap::NDataSharing::NEvents::TPathIdData> dataByPathId;
    for (auto&& i : ev->Get()->Record.GetPathIdData()) {
        auto schema = TablesManager.GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>().GetVersionedIndex().GetLastSchema();
        AFL_VERIFY(schema);
        auto data = NOlap::NDataSharing::NEvents::TPathIdData::BuildFromProto(i, schema->GetIndexInfo());
        AFL_VERIFY(data.IsSuccess())("error", data.GetErrorMessage());
        AFL_VERIFY(dataByPathId.emplace(i.GetPathId(), data.DetachResult()).second);
    }

    auto txConclusion = currentSession->ReceiveData(this, dataByPathId, ev->Get()->Record.GetPackIdx(), (NOlap::TTabletId)ev->Get()->Record.GetSourceTabletId(), currentSession);
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

void TColumnShard::Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    Y_ABORT_UNLESS(Tiers);
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "TEvRefreshSubscriberData")("snapshot", ev->Get()->GetSnapshot()->SerializeToString());
    Tiers->TakeConfigs(ev->Get()->GetSnapshot(), nullptr);
}

void TColumnShard::ActivateTiering(const ui64 pathId, const TString& useTiering) {
    AFL_VERIFY(Tiers);
    if (useTiering) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "activate_tiering")("path_id", pathId)("tiering", useTiering);
    }
    if (useTiering) {
        Tiers->EnablePathId(pathId, useTiering);
    } else {
        Tiers->DisablePathId(pathId);
    }
    OnTieringModified(pathId);
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

void TColumnShard::OnTieringModified(const std::optional<ui64> pathId) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "OnTieringModified")("path_id", pathId);
    if (Tiers->IsReady()) {
        StoragesManager->OnTieringModified(Tiers);
        if (TablesManager.HasPrimaryIndex()) {
            TablesManager.MutablePrimaryIndex().OnTieringModified(Tiers, TablesManager.GetTtl(), pathId);
        }
    }
}

const NKikimr::NColumnShard::NTiers::TManager* TColumnShard::GetTierManagerPointer(const TString& tierId) const {
    Y_ABORT_UNLESS(!!Tiers);
    return Tiers->GetManagerOptional(tierId);
}

TDuration TColumnShard::GetMaxReadStaleness() {
    return NYDBTest::TControllers::GetColumnShardController()->GetReadTimeoutClean(
        TDuration::MilliSeconds(AppDataVerified().ColumnShardConfig.GetMaxReadStaleness_ms()));
}

}
