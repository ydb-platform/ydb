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
#include "blobs_action/transaction/tx_remove_blobs.h"
#include "blobs_reader/actor.h"
#include "column_fetching/cache_policy.h"
#include "data_accessor/manager.h"
#include "data_reader/contexts.h"
#include "data_reader/fetcher.h"
#include "data_sharing/destination/session/destination.h"
#include "data_sharing/source/session/source.h"
#include "engines/changes/cleanup_portions.h"
#include "engines/changes/cleanup_tables.h"
#include "engines/changes/general_compaction.h"
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
#include <ydb/core/tx/columnshard/tablet/write_queue.h>
#include <ydb/core/tx/columnshard/tracing/probes.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/core/tx/priorities/usage/abstract.h>
#include <ydb/core/tx/priorities/usage/events.h>
#include <ydb/core/tx/priorities/usage/service.h>
#include <ydb/core/tx/tiering/manager.h>

#include <ydb/services/metadata/service.h>

#include <util/generic/object_counter.h>

namespace NKikimr::NColumnShard {

LWTRACE_USING(YDB_CS);

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
    , WriteTasksQueue(std::make_unique<TWriteTasksQueue>(this))
    , ProgressTxController(std::make_unique<TTxController>(*this))
    , StoragesManager(std::make_shared<NOlap::TStoragesManager>(*this))
    , DataLocksManager(std::make_shared<NOlap::NDataLocks::TManager>())
    , PeriodicWakeupActivationPeriod(NYDBTest::TControllers::GetColumnShardController()->GetPeriodicWakeupActivationPeriod())
    , StatsReportInterval(NYDBTest::TControllers::GetColumnShardController()->GetStatsReportInterval())
    , InFlightReadsTracker(StoragesManager, Counters.GetRequestsTracingCounters())
    , TablesManager(StoragesManager, nullptr, Counters.GetPortionIndexCounters(), info->TabletID)
    , Subscribers(std::make_shared<NSubscriber::TManager>(*this))
    , PipeClientCache(NTabletPipe::CreateBoundedClientCache(new NTabletPipe::TBoundedClientCacheConfig(), GetPipeClientConfig()))
    , CompactTaskSubscription(NOlap::TCompactColumnEngineChanges::StaticTypeName(), Counters.GetSubscribeCounters())
    , TTLTaskSubscription(NOlap::TTTLColumnEngineChanges::StaticTypeName(), Counters.GetSubscribeCounters())
    , BackgroundController(Counters.GetBackgroundControllerCounters())
    , NormalizerController(StoragesManager, Counters.GetSubscribeCounters())
    , SysLocks(this) {
    AFL_VERIFY(TabletActivityImpl->Inc() == 1);
    SpaceWatcher = new TSpaceWatcher(this);
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
        return;   // already registered
    }

    if (!ProcessingParams) {
        return;   // cannot register without processing params
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
        auto firstPlannedSnapshot = NOlap::TSnapshot(plannedTx->Step, plannedTx->TxId);
        return firstPlannedSnapshot.GetPreviousSnapshot();
    } else {
        // the same snapshot is used by bulk upsert and aborts
        // aborts are fine, but be careful with bulk upsert,
        // it must correctly break conflicting serializable txs
        return GetCurrentSnapshotForInternalModification();
    }
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
        case NKikimrTxColumnShard::TSchemaTxBody::kMoveTable: {
            RunMoveTable(body.GetMoveTable(), version, txc);
            return;
        }
        case NKikimrTxColumnShard::TSchemaTxBody::kCopyTable: {
            RunCopyTable(body.GetCopyTable(), version, txc);
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
    NIceDb::TNiceDb db(txc.DB);
    AFL_VERIFY(proto.HasOwnerPathId());
    const auto& tabletSchemeShardLocalPathId = NColumnShard::TSchemeShardLocalPathId::FromProto(proto);
    TablesManager.Init(db, tabletSchemeShardLocalPathId, Info());
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

    const auto& schemeShardLocalPathId = TSchemeShardLocalPathId::FromProto(tableProto);

    if (const auto& internalPathId = TablesManager.ResolveInternalPathId(schemeShardLocalPathId, false);
        internalPathId && TablesManager.HasTable(*internalPathId, true)) {
        LOG_S_DEBUG("EnsureTable for existed pathId: " << TUnifiedOptionalPathId(internalPathId, schemeShardLocalPathId)
                                                       << " at tablet "
                                                       << TabletID());
        return;
    }
    const auto internalPathId = TablesManager.GetOrCreateInternalPathId(schemeShardLocalPathId);

    LOG_S_INFO("EnsureTable for pathId: " << TUnifiedPathId::BuildValid(internalPathId, schemeShardLocalPathId)
                                           << " ttl settings: " << tableProto.GetTtlSettings()
                                           << " at tablet " << TabletID());

    NKikimrTxColumnShard::TTableVersionInfo tableVerProto;
    internalPathId.ToProto(tableVerProto);

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
        THashSet<NTiers::TExternalStorageId> usedTiers;
        TTableInfo table({TUnifiedPathId::BuildValid(internalPathId, schemeShardLocalPathId)});
        if (tableProto.HasTtlSettings()) {
            const auto& ttlSettings = tableProto.GetTtlSettings();
            *tableVerProto.MutableTtlSettings() = ttlSettings;
            if (ttlSettings.HasEnabled()) {
                usedTiers = NOlap::TTiering::GetUsedTiers(ttlSettings.GetEnabled());
            }
        }
        TablesManager.RegisterTable(std::move(table), db);
        if (!usedTiers.empty()) {
            ActivateTiering(internalPathId, usedTiers);
        }
    }

    tableVerProto.SetSchemaPresetVersionAdj(tableProto.GetSchemaPresetVersionAdj());

    TablesManager.AddTableVersion(internalPathId, version, tableVerProto, schema, db);

    Counters.GetTabletCounters()->SetCounter(COUNTER_TABLES, TablesManager.GetTables().size());
    Counters.GetTabletCounters()->SetCounter(COUNTER_TABLE_PRESETS, TablesManager.GetSchemaPresets().size());
    Counters.GetTabletCounters()->SetCounter(COUNTER_TABLE_TTLS, TablesManager.GetTtl().size());
}

void TColumnShard::RunAlterTable(const NKikimrTxColumnShard::TAlterTable& alterProto, const NOlap::TSnapshot& version,
    NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    const auto& schemeShardLocalPathId = TSchemeShardLocalPathId::FromProto(alterProto);
    const auto& internalPathId = TablesManager.ResolveInternalPathIdVerified(schemeShardLocalPathId, false);
    Y_ABORT_UNLESS(TablesManager.HasTable(internalPathId), "AlterTable on a dropped or non-existent table");
    const auto& pathId = TUnifiedPathId::BuildValid(internalPathId, schemeShardLocalPathId);
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

    if (alterProto.HasTtlSettings()) {
        const auto& ttlSettings = alterProto.GetTtlSettings();
        *tableVerProto.MutableTtlSettings() = ttlSettings;

        THashSet<NTiers::TExternalStorageId> usedTiers;
        if (ttlSettings.HasEnabled()) {
            usedTiers = NOlap::TTiering::GetUsedTiers(ttlSettings.GetEnabled());
        }
        ActivateTiering(internalPathId, usedTiers);
    }

    tableVerProto.SetSchemaPresetVersionAdj(alterProto.GetSchemaPresetVersionAdj());
    TablesManager.AddTableVersion(internalPathId, version, tableVerProto, schema, db);
}

void TColumnShard::RunDropTable(const NKikimrTxColumnShard::TDropTable& dropProto, const NOlap::TSnapshot& version,
    NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    const auto& schemeShardLocalPathId = TSchemeShardLocalPathId::FromProto(dropProto);
    const auto& internalPathId = TablesManager.ResolveInternalPathId(schemeShardLocalPathId, false);

    if (!internalPathId) {
        LOG_S_DEBUG("DropTable for unknown or deleted scheme shard pathId: " << schemeShardLocalPathId << " at tablet " << TabletID());
        return;
    }

    const auto& pathId = TUnifiedPathId::BuildValid(*internalPathId, schemeShardLocalPathId);
    if (!TablesManager.HasTable(*internalPathId)) {
        LOG_S_DEBUG("DropTable for unknown or deleted pathId: " << pathId << " at tablet " << TabletID());
        return;
    }

    LOG_S_DEBUG("DropTable for pathId: " << pathId << " at tablet " << TabletID());
    TablesManager.DropTable(schemeShardLocalPathId, *internalPathId, version, db);
}

void TColumnShard::RunMoveTable(const NKikimrTxColumnShard::TMoveTable& proto, const NOlap::TSnapshot& /*version*/,
                                 NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    const auto srcPathId = TSchemeShardLocalPathId::FromRawValue(proto.GetSrcPathId());
    const auto dstPathId = TSchemeShardLocalPathId::FromRawValue(proto.GetDstPathId());
    if (proto.HasDstPath()) { //This check can be deleted in 25.3 or later
        OwnerPath = proto.GetDstPath();
        Schema::SaveSpecialValue(db, Schema::EValueIds::OwnerPath, OwnerPath);
    }
    TablesManager.MoveTableProgress(db, srcPathId, dstPathId);
}

void TColumnShard::RunCopyTable(const NKikimrTxColumnShard::TCopyTable& proto, const NOlap::TSnapshot& version,
                                 NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);
    const auto srcPathId = TSchemeShardLocalPathId::FromRawValue(proto.GetSrcPathId());
    const auto dstPathId = TSchemeShardLocalPathId::FromRawValue(proto.GetDstPathId());
    TablesManager.CopyTableProgress(db, version, srcPathId, dstPathId);
}

void TColumnShard::RunAlterStore(const NKikimrTxColumnShard::TAlterStore& proto, const NOlap::TSnapshot& version,
    NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    AFL_VERIFY(proto.HasStorePathId());
    const auto& storeSchemeShardLocalPathId = NColumnShard::TSchemeShardLocalPathId::FromProto(proto);
    const auto& tabletSchemeShardLocalPathId = TablesManager.GetTabletPathIdVerified().SchemeShardLocalPathId;
    AFL_VERIFY(tabletSchemeShardLocalPathId == storeSchemeShardLocalPathId)("tablet_path_id", tabletSchemeShardLocalPathId)(
        "store_path_id", storeSchemeShardLocalPathId);

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

    SetupCompaction({});
    SetupCleanupSchemas();
    SetupCleanupPortions();
    SetupCleanupTables();
    SetupMetadata();
    SetupTtl();
    SetupGC();
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

void TColumnShard::SetupCompaction(const std::set<TInternalPathId>& pathIds) {
    if (!AppDataVerified().ColumnShardConfig.GetCompactionEnabled() ||
        !NYDBTest::TControllers::GetColumnShardController()->IsBackgroundEnabled(NYDBTest::ICSController::EBackground::Compaction)) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_compaction")("reason", "disabled");
        return;
    }

    BackgroundController.CheckDeadlines();
    if (BackgroundController.GetCompactionsCount()) {
        return;
    }
    const ui64 priority = TablesManager.GetPrimaryIndexSafe().GetCompactionPriority(DataLocksManager, pathIds, BackgroundController.GetWaitingPriorityOptional());
    if (priority) {
        BackgroundController.UpdateWaitingPriority(priority);
        if (pathIds.size()) {
            NPrioritiesQueue::TCompServiceOperator::AskMax(PrioritizationClientId, priority, std::make_shared<TCompactionAllocated>(SelfId()));
        } else {
            NPrioritiesQueue::TCompServiceOperator::Ask(PrioritizationClientId, priority, std::make_shared<TCompactionAllocated>(SelfId()));
        }
    }
}

class TCompactionExecutor: public NOlap::NDataFetcher::IFetchCallback {
private:
    const ui64 TabletId;
    const NActors::TActorId ParentActorId;
    std::shared_ptr<NOlap::TColumnEngineChanges> Changes;
    std::shared_ptr<const NOlap::TVersionedIndex> VersionedIndex;
    std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard> ResourcesGuard;
    const TIndexationCounters Counters;
    const NOlap::TSnapshot SnapshotModification;
    const bool NeedBlobs = true;
    const std::shared_ptr<TAtomicCounter> TabletActivity;

    virtual bool IsAborted() const override {
        return !TabletActivity->Val();
    }

    virtual TString GetClassName() const override {
        return Changes->TypeString();
    }

    virtual void OnStageStarting(const NOlap::NDataFetcher::EFetchingStage stage) override {
        switch (stage) {
            case NOlap::NDataFetcher::EFetchingStage::AskAccessors:
                Changes->SetStage(NOlap::NChanges::EStage::AskAccessors);
                break;
            case NOlap::NDataFetcher::EFetchingStage::AskDataResources:
            case NOlap::NDataFetcher::EFetchingStage::AskGeneralResources:
                Changes->SetStage(NOlap::NChanges::EStage::AskDataResources);
                break;
            case NOlap::NDataFetcher::EFetchingStage::AskAccessorResources:
                Changes->SetStage(NOlap::NChanges::EStage::AskAccessorResources);
                break;
            case NOlap::NDataFetcher::EFetchingStage::ReadBlobs:
                Changes->SetStage(NOlap::NChanges::EStage::ReadBlobs);
                break;
            case NOlap::NDataFetcher::EFetchingStage::Finished:
                Changes->SetStage(NOlap::NChanges::EStage::ReadyForConstruct);
                break;
            default:
                break;
        }
    }

    virtual void DoOnFinished(NOlap::NDataFetcher::TCurrentContext&& context) override {
        NActors::TLogContextGuard g(
            NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletId)("parent_id", ParentActorId));
        if (NeedBlobs) {
            AFL_VERIFY(context.GetResourceGuards().size() == 3);
        } else {
            AFL_VERIFY(context.GetResourceGuards().size() == 2);
        }
        if (NeedBlobs) {
            Changes->Blobs = context.ExtractBlobs();
        }
        Changes->SetFetchedDataAccessors(
            NOlap::TDataAccessorsResult(context.ExtractPortionAccessors()), NOlap::TDataAccessorsInitializationContext(VersionedIndex));

        auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(Changes, false);
        Changes->FetchingContext.emplace(std::move(context));
        {
            NOlap::TConstructionContext context(*VersionedIndex, Counters, SnapshotModification);
            if (NeedBlobs) {
                Changes->ConstructBlobs(context).Validate();
            }
            if (!Changes->GetWritePortionsCount()) {
                ev->SetPutStatus(NKikimrProto::OK);
            }
        }
        TActorContext::AsActorContext().Send(ParentActorId, std::move(ev));
    }

    virtual ui64 GetNecessaryDataMemory(const std::shared_ptr<NOlap::NReader::NCommon::TColumnsSetIds>& columnIds,
        const std::vector<std::shared_ptr<NOlap::TPortionDataAccessor>>& acc) const override {
        AFL_VERIFY(!columnIds);
        THashMap<ui32, ui64> memoryByColumns;
        for (auto&& a : acc) {
            THashMap<ui32, ui64> memoryByPortionColumns;
            for (auto&& c : a->GetRecordsVerified()) {
                const ui64 current = memoryByPortionColumns[c.GetEntityId()];
                memoryByPortionColumns[c.GetEntityId()] = std::max<ui64>(current, c.GetMeta().GetRawBytes());
            }
            for (auto&& c : memoryByPortionColumns) {
                memoryByColumns[c.first] += c.second;
            }
        }
        ui64 max = 0;
        for (auto&& c : memoryByColumns) {
            max = std::max(max, c.second);
        }
        return max;
    }

    virtual void DoOnError(const TString& errorMessage) override {
        auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(Changes, false);
        ev->SetPutStatus(NKikimrProto::ERROR);
        ev->ErrorMessage = errorMessage;
        TActorContext::AsActorContext().Send(ParentActorId, std::move(ev));
    }

public:
    TCompactionExecutor(const ui64 tabletId, const NActors::TActorId parentActorId, const std::shared_ptr<NOlap::TColumnEngineChanges>& changes,
        const std::shared_ptr<const NOlap::TVersionedIndex>& versionedIndex, const TIndexationCounters& counters,
        const NOlap::TSnapshot snapshotModification, const std::shared_ptr<TAtomicCounter>& tabletActivity, const bool needBlobs = true)
        : TabletId(tabletId)
        , ParentActorId(parentActorId)
        , Changes(changes)
        , VersionedIndex(versionedIndex)
        , Counters(counters)
        , SnapshotModification(snapshotModification)
        , NeedBlobs(needBlobs)
        , TabletActivity(tabletActivity) {
    }
};

void TColumnShard::StartCompaction(const std::shared_ptr<NPrioritiesQueue::TAllocationGuard>& guard) {
    Counters.GetCSCounters().OnSetupCompaction();
    BackgroundController.ResetWaitingPriority();

    auto indexChangesList = TablesManager.MutablePrimaryIndex().StartCompaction(DataLocksManager);

    if (indexChangesList.empty()) {
        LOG_S_DEBUG("Compaction not started: cannot prepare compaction at tablet " << TabletID());
        return;
    }

    const ui64 compactionStageMemoryLimit = HasAppData() ? AppDataVerified().ColumnShardConfig.GetCompactionStageMemoryLimit() : NOlap::TGlobalLimits::GeneralCompactionMemoryLimit;
    for (const auto& indexChanges : indexChangesList) {
        auto& compaction = *VerifyDynamicCast<NOlap::NCompaction::TGeneralCompactColumnEngineChanges*>(indexChanges.get());
        compaction.SetActivityFlag(GetTabletActivity());
        compaction.SetQueueGuard(guard);
        compaction.Start(*this);

        auto actualIndexInfo = TablesManager.GetPrimaryIndex()->GetVersionedIndexReadonlyCopy();
        static std::shared_ptr<NOlap::NGroupedMemoryManager::TStageFeatures> stageFeatures =
            NOlap::NGroupedMemoryManager::TCompMemoryLimiterOperator::BuildStageFeatures("COMPACTION", compactionStageMemoryLimit);
        auto processGuard = NOlap::NGroupedMemoryManager::TCompMemoryLimiterOperator::BuildProcessGuard({ stageFeatures });
        NOlap::NDataFetcher::TRequestInput rInput(compaction.GetSwitchedPortions(), actualIndexInfo,
            NOlap::NBlobOperations::EConsumer::GENERAL_COMPACTION, compaction.GetTaskIdentifier(), processGuard);
        auto env = std::make_shared<NOlap::NDataFetcher::TEnvironment>(DataAccessorsManager.GetObjectPtrVerified(), StoragesManager);
        NOlap::NDataFetcher::TPortionsDataFetcher::StartFullPortionsFetching(std::move(rInput),
            std::make_shared<TCompactionExecutor>(
                TabletID(), SelfId(), indexChanges, actualIndexInfo, Counters.GetIndexationCounters(), GetLastCompletedTx(), TabletActivityImpl),
            env, NConveyorComposite::ESpecialTaskCategory::Compaction);
    }
}

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
    virtual void DoOnRequestsFinished(
        NOlap::TDataAccessorsResult&& result, std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>&& guard) = 0;

public:
    void SetResourcesGuard(const std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>& guard) {
        AFL_VERIFY(!ResourcesGuard);
        AFL_VERIFY(guard);
        ResourcesGuard = guard;
    }
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

class TAccessorsMemorySubscriber: public NOlap::NResourceBroker::NSubscribe::ITask {
private:
    using TBase = NOlap::NResourceBroker::NSubscribe::ITask;
    std::shared_ptr<NOlap::TColumnEngineChanges> ChangeTask;
    std::shared_ptr<NOlap::TDataAccessorsRequest> Request;
    std::shared_ptr<TDataAccessorsSubscriberBase> Subscriber;
    std::shared_ptr<NOlap::NDataAccessorControl::IDataAccessorsManager> DataAccessorsManager;

    virtual void DoOnAllocationSuccess(const std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>& guard) override {
        Subscriber->SetResourcesGuard(guard);
        Request->RegisterSubscriber(Subscriber);
        if (ChangeTask) {
            ChangeTask->SetStage(NOlap::NChanges::EStage::AskAccessors);
        }
        DataAccessorsManager->AskData(Request);
    }

public:
    TAccessorsMemorySubscriber(const ui64 memory, const TString& externalTaskId, const NOlap::NResourceBroker::NSubscribe::TTaskContext& context,
        std::shared_ptr<NOlap::TDataAccessorsRequest>&& request, const std::shared_ptr<TDataAccessorsSubscriberBase>& subscriber,
        const std::shared_ptr<NOlap::NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager,
        const std::shared_ptr<NOlap::TColumnEngineChanges>& changeTask)
        : TBase(0, memory, externalTaskId, context)
        , ChangeTask(changeTask)
        , Request(std::move(request))
        , Subscriber(subscriber)
        , DataAccessorsManager(dataAccessorsManager) {
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
                std::make_shared<TCSMetadataSubscriber>(SelfId(), i.GetProcessor(), Generation()), DataAccessorsManager.GetObjectPtrVerified(), nullptr));
    }
}

bool TColumnShard::SetupTtl() {
    if (!AppDataVerified().ColumnShardConfig.GetTTLEnabled() ||
        !NYDBTest::TControllers::GetColumnShardController()->IsBackgroundEnabled(NYDBTest::ICSController::EBackground::TTL)) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_ttl")("reason", "disabled");
        return false;
    }
    Counters.GetCSCounters().OnSetupTtl();

    const ui64 memoryUsageLimit = HasAppData() ? AppDataVerified().ColumnShardConfig.GetTieringsMemoryLimit() : ((ui64)512 * 1024 * 1024);
    std::vector<std::shared_ptr<NOlap::TTTLColumnEngineChanges>> indexChanges =
        TablesManager.MutablePrimaryIndex().StartTtl({}, DataLocksManager, memoryUsageLimit);

    if (indexChanges.empty()) {
        ACFL_DEBUG("background", "ttl")("skip_reason", "no_changes");
        return false;
    }

    auto actualIndexInfo = TablesManager.GetPrimaryIndex()->GetVersionedIndexReadonlyCopy();
    const ui64 tieringStageMemoryLimit = HasAppData() ? AppDataVerified().ColumnShardConfig.GetTieringStageMemoryLimit() : 1000000000;
    for (auto&& i : indexChanges) {
        i->Start(*this);
        static std::shared_ptr<NOlap::NGroupedMemoryManager::TStageFeatures> stageFeatures =
            NOlap::NGroupedMemoryManager::TCompMemoryLimiterOperator::BuildStageFeatures("TTL", tieringStageMemoryLimit);
        auto processGuard = NOlap::NGroupedMemoryManager::TCompMemoryLimiterOperator::BuildProcessGuard({ stageFeatures });
        NOlap::NDataFetcher::TRequestInput rInput(
            i->GetPortionsInfo(), actualIndexInfo, NOlap::NBlobOperations::EConsumer::TTL, i->GetTaskIdentifier(), processGuard);
        auto env = std::make_shared<NOlap::NDataFetcher::TEnvironment>(DataAccessorsManager.GetObjectPtrVerified(), StoragesManager);
        if (i->NeedConstruction()) {
            NOlap::NDataFetcher::TPortionsDataFetcher::StartFullPortionsFetching(std::move(rInput),
                std::make_shared<TCompactionExecutor>(
                    TabletID(), SelfId(), i, actualIndexInfo, Counters.GetIndexationCounters(), GetLastCompletedTx(), TabletActivityImpl, true),
                env, NConveyorComposite::ESpecialTaskCategory::Compaction);
        } else {
            NOlap::NDataFetcher::TPortionsDataFetcher::StartAccessorPortionsFetching(std::move(rInput),
                std::make_shared<TCompactionExecutor>(
                    TabletID(), SelfId(), i, actualIndexInfo, Counters.GetIndexationCounters(), GetLastCompletedTx(), TabletActivityImpl, false),
                env, NConveyorComposite::ESpecialTaskCategory::Compaction);
        }
    }
    return true;
}

void TColumnShard::SetupCleanupPortions() {
    Counters.GetCSCounters().OnSetupCleanup();
    if (!AppDataVerified().ColumnShardConfig.GetCleanupEnabled() ||
        !NYDBTest::TControllers::GetColumnShardController()->IsBackgroundEnabled(NYDBTest::ICSController::EBackground::Cleanup)) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_cleanup")("reason", "disabled");
        return;
    }
    if (BackgroundController.IsCleanupPortionsActive()) {
        ACFL_DEBUG("background", "cleanup_portions")("skip_reason", "in_progress");
        Counters.GetCSCounters().OnSetupCleanupSkippedByInProgress();
        return;
    }

    const NOlap::TSnapshot minReadSnapshot = GetMinReadSnapshot();
    const auto& pathsToDrop = TablesManager.GetPathsToDrop(minReadSnapshot);

    auto changes = TablesManager.MutablePrimaryIndex().StartCleanupPortions(minReadSnapshot, pathsToDrop, DataLocksManager);
    if (!changes) {
        ACFL_DEBUG("background", "cleanup")("skip_reason", "no_changes");
        return;
    }
    changes->Start(*this);
    const ui64 cleanupPortionsStageMemoryLimit = HasAppData() ? AppDataVerified().ColumnShardConfig.GetCleanupPortionsStageMemoryLimit() : 1000000000;
    auto actualIndexInfo = TablesManager.GetPrimaryIndex()->GetVersionedIndexReadonlyCopy();
    static std::shared_ptr<NOlap::NGroupedMemoryManager::TStageFeatures> stageFeatures =
        NOlap::NGroupedMemoryManager::TCompMemoryLimiterOperator::BuildStageFeatures("CLEANUP_PORTIONS", cleanupPortionsStageMemoryLimit);
    auto processGuard = NOlap::NGroupedMemoryManager::TCompMemoryLimiterOperator::BuildProcessGuard({ stageFeatures });
    NOlap::NDataFetcher::TRequestInput rInput(changes->GetPortionsToAccess(), actualIndexInfo,
        NOlap::NBlobOperations::EConsumer::CLEANUP_PORTIONS, changes->GetTaskIdentifier(), processGuard);
    auto env = std::make_shared<NOlap::NDataFetcher::TEnvironment>(DataAccessorsManager.GetObjectPtrVerified(), StoragesManager);
    NOlap::NDataFetcher::TPortionsDataFetcher::StartAccessorPortionsFetching(std::move(rInput),
        std::make_shared<TCompactionExecutor>(
            TabletID(), SelfId(), changes, actualIndexInfo, Counters.GetIndexationCounters(), GetLastCompletedTx(), TabletActivityImpl, false),
        env, NConveyorComposite::ESpecialTaskCategory::Compaction);
}

void TColumnShard::SetupCleanupTables() {
    Counters.GetCSCounters().OnSetupCleanup();
    if (BackgroundController.IsCleanupTablesActive()) {
        ACFL_DEBUG("background", "cleanup_tables")("skip_reason", "in_progress");
        return;
    }

    THashSet<TInternalPathId> pathIdsEmptyInInsertTable;
    for (auto&& i : TablesManager.GetPathsToDrop(GetMinReadSnapshot())) {
        pathIdsEmptyInInsertTable.emplace(i);
    }

    auto changes = TablesManager.MutablePrimaryIndex().StartCleanupTables(pathIdsEmptyInInsertTable);
    if (!changes) {
        ACFL_DEBUG("background", "cleanup")("skip_reason", "no_changes");
        return;
    }

    ACFL_DEBUG("background", "cleanup")("changes_info", changes->DebugString());
    auto actualIndexInfo = TablesManager.GetPrimaryIndex()->GetVersionedIndexReadonlyCopy();
    auto ev = std::make_unique<TEvPrivate::TEvWriteIndex>(changes, false);
    ev->SetPutStatus(NKikimrProto::OK); // No new blobs to write

    changes->Start(*this);

    Send(SelfId(), ev.release());
}

class TTxCleanupSchemasWithnoData: public TTransactionBase<TColumnShard> {
private:
    std::vector<TTablesManager::TSchemasChain> ChainsToClean;
    std::set<TTablesManager::TSchemaAddress> AddressesToFetch;
    THashMap<TTablesManager::TSchemaAddress, TSchemaPreset::TSchemaPresetVersionInfo> Fetched;

    TSchemaPreset::TSchemaPresetVersionInfo ExtractFetched(const TTablesManager::TSchemaAddress addr) {
        auto it = Fetched.find(addr);
        AFL_VERIFY(it != Fetched.end());
        auto result = std::move(it->second);
        Fetched.erase(it);
        return result;
    }

public:
    TTxCleanupSchemasWithnoData(TColumnShard* self, std::vector<TTablesManager::TSchemasChain>&& chainsToClean)
        : TBase(self)
        , ChainsToClean(std::move(chainsToClean)) {
        for (auto&& i : ChainsToClean) {
            i.FillAddressesTo(AddressesToFetch);
        }
    }

    virtual bool Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        NIceDb::TNiceDb db(txc.DB);
        using SchemaPresetVersionInfo = NColumnShard::Schema::SchemaPresetVersionInfo;
        std::vector<TTablesManager::TSchemaAddress> toRemove;
        for (auto&& i : AddressesToFetch) {
            auto rowset =
                db.Table<SchemaPresetVersionInfo>().Key(i.GetPresetId(), i.GetSnapshot().GetPlanStep(), i.GetSnapshot().GetTxId()).Select();
            if (rowset.IsReady()) {
                toRemove.emplace_back(i);
                AFL_VERIFY(!rowset.EndOfSet())("address", i.DebugString());
                TSchemaPreset::TSchemaPresetVersionInfo info;
                Y_ABORT_UNLESS(info.ParseFromString(rowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));
                Fetched.emplace(i, std::move(info));
            }
        }
        for (auto&& i : toRemove) {
            AddressesToFetch.erase(i);
        }
        if (AddressesToFetch.size()) {
            return false;
        }
        for (auto&& i : ChainsToClean) {
            std::vector<TSchemaPreset::TSchemaPresetVersionInfo> schemasProto;
            for (auto&& del : i.GetToRemove()) {
                schemasProto.emplace_back(ExtractFetched(del));
            }
            schemasProto.emplace_back(ExtractFetched(i.GetFinish()));
            auto finalProto = NOlap::TSchemaDiffView::Merge(schemasProto);
            AFL_VERIFY(schemasProto.back().HasSchema());
            *finalProto.MutableSchema() = schemasProto.back().GetSchema();

            ui32 idx = 0;
            for (auto&& del : i.GetToRemove()) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "useless_schema_removed")("address", del.DebugString())(
                    "is_diff", schemasProto[idx].HasDiff())("version",
                    schemasProto[idx].HasDiff() ? schemasProto[idx].GetDiff().GetVersion() : schemasProto[idx].GetSchema().GetVersion());
                ++idx;
                db.Table<SchemaPresetVersionInfo>()
                    .Key(del.GetPresetId(), del.GetSnapshot().GetPlanStep(), del.GetSnapshot().GetTxId())
                    .Delete();
            }
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "schema_updated")("address", i.GetFinish().DebugString())(
                "new_schema", finalProto.DebugString());
            db.Table<SchemaPresetVersionInfo>()
                .Key(i.GetFinish().GetPresetId(), i.GetFinish().GetSnapshot().GetPlanStep(), i.GetFinish().GetSnapshot().GetTxId())
                .Update(NIceDb::TUpdate<SchemaPresetVersionInfo::InfoProto>(finalProto.SerializeAsString()));
        }
        return true;
    }
    virtual void Complete(const TActorContext& /*ctx*/) override {
        NYDBTest::TControllers::GetColumnShardController()->OnCleanupSchemasFinished();
        Self->BackgroundController.OnCleanupSchemasFinished();
    }
    TTxType GetTxType() const override {
        return TXTYPE_CLEANUP_SCHEMAS;
    }
};

void TColumnShard::SetupCleanupSchemas() {
    if (!NYDBTest::TControllers::GetColumnShardController()->IsBackgroundEnabled(NYDBTest::ICSController::EBackground::CleanupSchemas)) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_schemas_cleanup")("reason", "disabled");
        return;
    }
    if (!AppDataVerified().FeatureFlags.GetEnableCSSchemasCollapsing()) {
        return;
    }
    Counters.GetCSCounters().OnSetupCleanup();
    if (BackgroundController.IsCleanupSchemasActive()) {
        ACFL_DEBUG("background", "cleanup_schemas")("skip_reason", "in_progress");
        return;
    }

    std::vector<TTablesManager::TSchemasChain> chainsToClean = TablesManager.ExtractSchemasToClean();
    if (chainsToClean.empty()) {
        ACFL_DEBUG("background", "cleanup_schemas")("skip_reason", "no_changes");
        return;
    }

    BackgroundController.OnCleanupSchemasStarted();
    Execute(new TTxCleanupSchemasWithnoData(this, std::move(chainsToClean)), NActors::TActivationContext::AsActorContext());
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

void TColumnShard::Die(const TActorContext& ctx) {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "tablet_die");
    AFL_VERIFY(TabletActivityImpl->Dec() == 0);
    CleanupActors(ctx);
    NTabletPipe::CloseAndForgetClient(SelfId(), StatsReportPipe);
    UnregisterMediatorTimeCast();
    NYDBTest::TControllers::GetColumnShardController()->OnTabletStopped(*this);
    if (SpaceWatcherId == TActorId{}) {
        delete SpaceWatcher;
        SpaceWatcher = nullptr;
    } else {
        Send(SpaceWatcherId, new NActors::TEvents::TEvPoison);
    }
    Send(NOverload::TOverloadManagerServiceOperator::MakeServiceId(),
        std::make_unique<NOverload::TEvOverloadColumnShardDied>(NOverload::TColumnShardInfo{.ColumnShardId = SelfId(), .TabletId = TabletID()}));
    IActor::Die(ctx);
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
    const ui64 tabletDest = ev->Get()->Record.GetTabletProducer();
    if (!GetProgressTxController().GetTxOperatorOptional(txId)) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "read_set_ignored")("proto", ev->Get()->Record.DebugString());
        TEvWriteCommitSyncTransactionOperator::SendBrokenFlagAck(*this, ev->Get()->Record.GetStep(), txId, tabletDest);
        return;
    }
    auto op = GetProgressTxController().GetTxOperatorVerifiedAs<TEvWriteCommitSyncTransactionOperator>(txId);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "read_set")("proto", ev->Get()->Record.DebugString())("lock_id", op->GetLockId());
    NKikimrTx::TReadSetData data;
    AFL_VERIFY(data.ParseFromArray(ev->Get()->Record.GetReadSet().data(), ev->Get()->Record.GetReadSet().size()));
    auto tx = op->CreateReceiveBrokenFlagTx(*this, tabletDest, data.GetDecision() != NKikimrTx::TReadSetData::DECISION_COMMIT);
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

    THashMap<TInternalPathId, NOlap::NDataSharing::NEvents::TPathIdData> dataByPathId;
    TBlobGroupSelector dsGroupSelector(Info());
    for (auto&& i : ev->Get()->Record.GetPathIdData()) {
        auto data = NOlap::NDataSharing::NEvents::TPathIdData::BuildFromProto(i, TablesManager.GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>().GetVersionedIndex(), dsGroupSelector);
        AFL_VERIFY(data.IsSuccess())("error", data.GetErrorMessage());
        AFL_VERIFY(dataByPathId.emplace(TInternalPathId::FromProto(i), data.DetachResult()).second);
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
    YDB_READONLY_DEF(NOlap::TPortionInfo::TConstPtr, PortionInfo);
    std::optional<NOlap::TColumnChunkLoadContextV2> Records;
    std::optional<std::vector<NOlap::TIndexChunkLoadContext>> Indexes;

public:
    TPortionConstructorV2(const NOlap::TPortionInfo::TConstPtr& portionInfo)
        : PortionInfo(portionInfo) {
        AFL_VERIFY(PortionInfo);
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

    std::shared_ptr<NOlap::TPortionDataAccessor> BuildAccessor() {
        AFL_VERIFY(PortionInfo);
        AFL_VERIFY(Records)("portion_id", PortionInfo->GetPortionId())("path_id", PortionInfo->GetPathId());
        AFL_VERIFY(Indexes)("portion_id", PortionInfo->GetPortionId())("path_id", PortionInfo->GetPathId());
        return NOlap::TPortionAccessorConstructor::BuildForLoading(std::move(PortionInfo), Records->CreateBuildInfo(), std::move(*Indexes));
    }
};

class TAccessorsParsingTask: public NConveyor::ITask {
private:
    std::shared_ptr<NOlap::NDataAccessorControl::IAccessorCallback> FetchCallback;
    std::vector<TPortionConstructorV2> Portions;

    virtual void DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override {
        TMemoryProfileGuard mpg("TAccessorsParsingTask::Execute");
        std::vector<std::shared_ptr<NOlap::TPortionDataAccessor>> accessors;
        accessors.reserve(Portions.size());
        for (auto&& i : Portions) {
            accessors.emplace_back(i.BuildAccessor());
        }
        FetchCallback->OnAccessorsFetched(std::move(accessors));
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
        , Portions(std::move(portions)) {
    }
};

class TTxAskPortionChunks: public TTransactionBase<TColumnShard> {
private:
    using TBase = TTransactionBase<TColumnShard>;
    std::shared_ptr<NOlap::NDataAccessorControl::IAccessorCallback> FetchCallback;
    THashMap<TInternalPathId, NOlap::NDataAccessorControl::TPortionsByConsumer> PortionsByPath;
    std::vector<TPortionConstructorV2> FetchedAccessors;
    THashMap<NOlap::TPortionAddress, TPortionConstructorV2> Constructors;
    TInstant StartTime;

public:
    TTxAskPortionChunks(TColumnShard* self, const std::shared_ptr<NOlap::NDataAccessorControl::IAccessorCallback>& fetchCallback,
        THashMap<TInternalPathId, NOlap::NDataAccessorControl::TPortionsByConsumer>&& portions)
        : TBase(self)
        , FetchCallback(fetchCallback)
        , PortionsByPath(std::move(portions))
        , StartTime(TInstant::Now()) {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        TInstant startTransactionTime = TInstant::Now();
        NIceDb::TNiceDb db(txc.DB);

        TBlobGroupSelector selector(Self->Info());
        bool reask = false;
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("event", "TTxAskPortionChunks::Execute");
        for (auto&& i : PortionsByPath) {
            const auto& granule = Self->GetIndexAs<NOlap::TColumnEngineForLogs>().GetGranuleVerified(i.first);
            for (auto&& c : i.second.GetConsumers()) {
                NActors::TLogContextGuard lcGuard = NActors::TLogContextBuilder::Build()("consumer", c.first)("path_id", i.first);
                AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("size", c.second.GetPortionsCount());
                for (auto&& portion : c.second.GetPortions(granule)) {
                    const ui64 p = portion->GetPortionId();
                    const NOlap::TPortionAddress pAddress = portion->GetAddress();
                    auto itPortionConstructor = Constructors.find(pAddress);
                    if (itPortionConstructor == Constructors.end()) {
                        TPortionConstructorV2 constructor(portion);
                        itPortionConstructor = Constructors.emplace(pAddress, std::move(constructor)).first;
                    } else if (itPortionConstructor->second.IsReady()) {
                        continue;
                    }
                    if (!itPortionConstructor->second.HasRecords()) {
                        auto rowset = db.Table<NColumnShard::Schema::IndexColumnsV2>().Key(i.first.GetRawValue(), p).Select();
                        if (!rowset.IsReady()) {
                            reask = true;
                        } else {
                            AFL_VERIFY(!rowset.EndOfSet())("path_id", i.first)("portion_id", p)(
                                "debug", itPortionConstructor->second.GetPortionInfo()->DebugString(true));
                            NOlap::TColumnChunkLoadContextV2 info(rowset, selector);
                            itPortionConstructor->second.SetRecords(std::move(info));
                        }
                    }
                    if (!itPortionConstructor->second.HasIndexes()) {
                        if (!itPortionConstructor->second.GetPortionInfo()
                                ->GetSchema(Self->GetIndexAs<NOlap::TColumnEngineForLogs>().GetVersionedIndex())
                                ->GetIndexesCount()) {
                            itPortionConstructor->second.SetIndexes({});
                        } else {
                            auto rowset = db.Table<NColumnShard::Schema::IndexIndexes>().Prefix(i.first.GetRawValue(), p).Select();
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
        }
        if (reask) {
            return false;
        }

        for (auto&& i : Constructors) {
            FetchedAccessors.emplace_back(std::move(i.second));
        }

        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("stage", "finished");
        NConveyorComposite::TScanServiceOperator::SendTaskToExecute(
            std::make_shared<TAccessorsParsingTask>(FetchCallback, std::move(FetchedAccessors)), 0);
        TDuration transactionTime = TInstant::Now() - startTransactionTime;
        TDuration totalTime = TInstant::Now() - StartTime;
        LWPROBE(TxAskPortionChunks, Self->TabletID(), transactionTime, totalTime, PortionsByPath.size());
        return true;
    }
    void Complete(const TActorContext& /*ctx*/) override {
    }
    TTxType GetTxType() const override {
        return TXTYPE_ASK_PORTION_METADATA;
    }
};

void TColumnShard::Handle(NColumnShard::TEvPrivate::TEvAskTabletDataAccessors::TPtr& ev, const TActorContext& /*ctx*/) {
    Execute(new TTxAskPortionChunks(this, ev->Get()->GetCallback(), std::move(ev->Get()->DetachPortions())));
}

void TColumnShard::Handle(NColumnShard::TEvPrivate::TEvAskColumnData::TPtr& ev, const TActorContext& /*ctx*/) {
    class TExecutor: public NOlap::NDataFetcher::IFetchCallback {
    private:
        std::shared_ptr<NKikimr::NGeneralCache::NSource::IObjectsProcessor<NOlap::NGeneralCache::TColumnDataCachePolicy>> CacheCallback;
        NOlap::TPortionAddress Portion;
        TActorId Owner;
        NOlap::ISnapshotSchema::TPtr Schema;

        virtual bool IsAborted() const override {
            return false;
        }

        virtual TString GetClassName() const override {
            return "ColumnData";
        }

        virtual void DoOnFinished(NOlap::NDataFetcher::TCurrentContext&& context) override {
            THashMap<NOlap::NGeneralCache::TGlobalColumnAddress, NOlap::NGeneralCache::TColumnDataCachePolicy::TObject> result;

            auto portionsData = context.ExtractAssembledData();
            AFL_VERIFY(portionsData.size() == 1);
            std::shared_ptr<NArrow::TGeneralContainer> container = std::make_shared<NArrow::TGeneralContainer>(std::move(portionsData.front()));
            {
                for (const ui32 columnId : Schema->GetColumnIds()) {
                    auto address = NOlap::NGeneralCache::TGlobalColumnAddress(Owner, Portion, columnId);
                    auto columnData = container->GetAccessorByNameVerified(Schema->GetFieldByColumnIdVerified(columnId)->name());
                    AFL_VERIFY(Schema->GetFieldByColumnIdVerified(columnId)->type()->Equals(columnData->GetDataType()))(
                        "schema", Schema->GetFieldByColumnIdVerified(columnId)->ToString())("data", columnData->GetDataType()->ToString());
                    AFL_VERIFY(result.emplace(address, columnData).second);
                }
            }

            CacheCallback->OnReceiveData(Owner, std::move(result), {}, {});
        }

        virtual void DoOnError(const TString& errorMessage) override {
            THashMap<NOlap::NGeneralCache::TGlobalColumnAddress, TString> errorAddresses;
            for (const ui32 columnId : Schema->GetColumnIds()) {
                errorAddresses.emplace(NOlap::NGeneralCache::TGlobalColumnAddress(Owner, Portion, columnId), errorMessage);
            }
            CacheCallback->OnReceiveData(Owner, {}, {}, std::move(errorAddresses));
        }

    public:
        TExecutor(const std::shared_ptr<NKikimr::NGeneralCache::NSource::IObjectsProcessor<NOlap::NGeneralCache::TColumnDataCachePolicy>>&
                      cacheCallback, const NOlap::TPortionAddress& portion, const TActorId& owner, const NOlap::ISnapshotSchema::TPtr& schema)
            : CacheCallback(cacheCallback)
            , Portion(std::move(portion))
            , Owner(owner)
            , Schema(schema)
        {
        }
    };

    for (const auto& [portion, columns] : ev->Get()->GetRequests()) {
        auto actualIndexInfo = TablesManager.GetPrimaryIndex()->GetVersionedIndexReadonlyCopy();
        auto portionInfo = TablesManager.MutablePrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>()
                               .GetGranuleVerified(portion.GetPortionAddress().GetPathId())
                               .GetPortionVerifiedPtr(portion.GetPortionAddress().GetPortionId(), false);
        NOlap::NDataFetcher::TRequestInput rInput({ portionInfo }, actualIndexInfo, portion.GetConsumer(), "", nullptr);
        auto env = std::make_shared<NOlap::NDataFetcher::TEnvironment>(DataAccessorsManager.GetObjectPtrVerified(), StoragesManager);

        NOlap::NDataFetcher::TPortionsDataFetcher::StartAssembledColumnsFetchingNoAllocation(std::move(rInput),
            std::make_shared<NOlap::NReader::NCommon::TColumnsSetIds>(columns),
            std::make_shared<TExecutor>(ev->Get()->GetCallback(), portion.GetPortionAddress(), SelfId(),
                std::make_shared<NOlap::TFilteredSnapshotSchema>(portionInfo->GetSchema(*actualIndexInfo), columns)), env,
            NConveyorComposite::ESpecialTaskCategory::Scan);
    }
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

void TColumnShard::ActivateTiering(const TInternalPathId pathId, const THashSet<NTiers::TExternalStorageId>& usedTiers) {
    AFL_VERIFY(Tiers);
    Tiers->ActivateTiers(usedTiers, true);
    OnTieringModified(pathId);
}

void TColumnShard::Enqueue(STFUNC_SIG) {
    const TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletID())(
        "self_id", SelfId())("process", "Enqueue")("ev", ev->GetTypeName());
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvPrivate::TEvTieringModified, HandleInit);
        HFunc(TEvPrivate::TEvNormalizerResult, Handle);
        HFunc(TEvPrivate::TEvAskTabletDataAccessors, Handle);
        HFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);
        default:
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "unexpected event in enqueue");
            return NTabletFlatExecutor::TTabletExecutedFlat::Enqueue(ev);
    }
}

void TColumnShard::OnTieringModified(const std::optional<TInternalPathId> pathId) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "OnTieringModified")("path_id", pathId);
    StoragesManager->OnTieringModified(Tiers);
    if (TablesManager.HasPrimaryIndex()) {
        if (pathId) {
            TablesManager.MutablePrimaryIndex().OnTieringModified(TablesManager.GetTableTtl(*pathId), *pathId);
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
