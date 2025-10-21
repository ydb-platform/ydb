#include "columnshard_impl.h"
#include "columnshard_private_events.h"
#include "columnshard_schema.h"

#include "bg_tasks/adapter/adapter.h"
#include "bg_tasks/manager/manager.h"
#include "blobs_action/storages_manager/manager.h"
#include "data_accessor/manager.h"
#include "engines/column_engine_logs.h"
#include "hooks/abstract/abstract.h"
#include "loading/stages.h"
#include "tx_reader/abstract.h"
#include "tx_reader/composite.h"

#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/operations/write.h>
#include <ydb/core/tx/columnshard/transactions/locks_db.h>
#include <ydb/core/tx/tiering/manager.h>

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

class TTxInit: public TTransactionBase<TColumnShard> {
private:
    const TMonotonic StartInstant = TMonotonic::Now();

public:
    TTxInit(TColumnShard* self)
        : TBase(self) {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override {
        return TXTYPE_INIT;
    }

private:
    std::shared_ptr<ITxReader> StartReader;
    void SetDefaults();
    std::shared_ptr<ITxReader> BuildReader();
};

void TTxInit::SetDefaults() {
    Self->CurrentSchemeShardId = 0;
    Self->LastSchemaSeqNo = {};
    Self->ProcessingParams.reset();
    Self->LastPlannedStep = 0;
    Self->LastPlannedTxId = 0;
    Self->LastCompletedTx = NOlap::TSnapshot::Zero();
    Self->OwnerPath.clear();
}

std::shared_ptr<ITxReader> TTxInit::BuildReader() {
    auto result = std::make_shared<TTxCompositeReader>("composite_init");
    result->AddChildren(std::make_shared<NLoading::TSpecialValuesInitializer>("special_values", Self));
    result->AddChildren(std::make_shared<NLoading::TTablesManagerInitializer>("tables_manager", Self));
    result->AddChildren(std::make_shared<NLoading::TTxControllerInitializer>("tx_controller", Self));
    result->AddChildren(std::make_shared<NLoading::TOperationsManagerInitializer>("operations_manager", Self));
    result->AddChildren(std::make_shared<NLoading::TStoragesManagerInitializer>("storages_manager", Self));
    result->AddChildren(std::make_shared<NLoading::TDBLocksInitializer>("db_locks", Self));
    result->AddChildren(std::make_shared<NLoading::TBackgroundSessionsInitializer>("bg_sessions", Self));
    result->AddChildren(std::make_shared<NLoading::TSharingSessionsInitializer>("sharing_sessions", Self));
    result->AddChildren(std::make_shared<NLoading::TInFlightReadsInitializer>("in_flight_reads", Self));
    result->AddChildren(std::make_shared<NLoading::TTiersManagerInitializer>("tiers_manager", Self));
    return result;
}

bool TTxInit::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    NActors::TLogContextGuard gLogging =
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID())("event", "initialize_shard");
    LOG_S_DEBUG("TTxInit.Execute at tablet " << Self->TabletID());

    try {
        if (!StartReader) {
            SetDefaults();
            StartReader = BuildReader();
        }
        if (!StartReader->Execute(txc, ctx)) {
            return false;
        }
        StartReader = nullptr;
        Self->UpdateIndexCounters();
        Self->UpdateResourceMetrics(ctx, {});
    } catch (const TNotReadyTabletException&) {
        ACFL_ERROR("event", "tablet not ready");
        return false;
    } catch (const TSchemeErrorTabletException& ex) {
        Y_UNUSED(ex);
        Y_ABORT();
    } catch (...) {
        Y_ABORT("there must be no leaked exceptions");
    }

    return true;
}

void TTxInit::Complete(const TActorContext& ctx) {
    Self->Counters.GetCSCounters().Initialization.OnTxInitFinished(TMonotonic::Now() - StartInstant);
    AFL_VERIFY(!Self->IsTxInitFinished);
    Self->IsTxInitFinished = true;
    Self->TrySwitchToWork(ctx);
    if (Self->SpaceWatcher->SubDomainPathId) {
        Self->SpaceWatcher->StartWatchingSubDomainPathId();
    } else {
        Self->SpaceWatcher->StartFindSubDomainPathId();
    }
}

class TTxUpdateSchema: public TTransactionBase<TColumnShard> {
    std::vector<NOlap::INormalizerTask::TPtr> NormalizerTasks;
    const TMonotonic StartInstant = TMonotonic::Now();

public:
    TTxUpdateSchema(TColumnShard* self)
        : TBase(self) {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override {
        return TXTYPE_UPDATE_SCHEMA;
    }
};

bool TTxUpdateSchema::Execute(TTransactionContext& txc, const TActorContext&) {
    NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID())(
        "process", "TTxUpdateSchema::Execute");
    ACFL_INFO("step", "TTxUpdateSchema.Execute_Start")("details", Self->NormalizerController.DebugString());

    while (!Self->NormalizerController.IsNormalizationFinished()) {
        auto normalizer = Self->NormalizerController.GetNormalizer();
        auto result = normalizer->Init(Self->NormalizerController, txc);
        if (result.IsSuccess()) {
            NormalizerTasks = result.DetachResult();
            if (!NormalizerTasks.empty()) {
                ACFL_WARN("normalizer_controller", Self->NormalizerController.DebugString())("tasks_count", NormalizerTasks.size());
                break;
            }
            NIceDb::TNiceDb db(txc.DB);
            Self->NormalizerController.OnNormalizerFinished(db);
            Self->NormalizerController.SwitchNormalizer();
        } else {
            Self->NormalizerController.GetCounters().OnNormalizerFails();
            ACFL_INFO("step", "TTxUpdateSchema.Execute_Failed")("details", Self->NormalizerController.DebugString());
            return false;
        }
    }
    ACFL_INFO("step", "TTxUpdateSchema.Execute_Finish");
    return true;
}

void TTxUpdateSchema::Complete(const TActorContext& ctx) {
    NActors::TLogContextGuard gLogging =
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID())("process", "TTxUpdateSchema::Complete");
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("step", "TTxUpdateSchema.Complete");
    Self->Counters.GetCSCounters().Initialization.OnTxUpdateSchemaFinished(TMonotonic::Now() - StartInstant);
    if (NormalizerTasks.empty()) {
        AFL_VERIFY(Self->NormalizerController.IsNormalizationFinished())("details", Self->NormalizerController.DebugString());
        Self->Execute(new TTxInit(Self), ctx);
        return;
    }

    NOlap::TNormalizationContext nCtx;
    nCtx.SetShardActor(Self->SelfId());
    nCtx.SetResourceSubscribeActor(Self->ResourceSubscribeActor);

    for (auto&& task : NormalizerTasks) {
        Self->NormalizerController.GetCounters().OnNormalizerStart();
        task->Start(Self->NormalizerController, nCtx);
    }
}

class TTxApplyNormalizer: public TTransactionBase<TColumnShard> {
public:
    TTxApplyNormalizer(TColumnShard* self, NOlap::INormalizerChanges::TPtr changes)
        : TBase(self)
        , IsDryRun(self->NormalizerController.GetNormalizer()->GetIsDryRun())
        , Changes(changes) {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override {
        return TXTYPE_APPLY_NORMALIZER;
    }

private:
    const bool IsDryRun;
    bool NormalizerFinished = false;
    NOlap::INormalizerChanges::TPtr Changes;
};

bool TTxApplyNormalizer::Execute(TTransactionContext& txc, const TActorContext&) {
    NActors::TLogContextGuard gLogging =
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID())("event", "TTxApplyNormalizer::Execute");
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("step", "TTxApplyNormalizer.Execute")("details", Self->NormalizerController.DebugString())(
        "dry_run", IsDryRun);
    if (!IsDryRun) {
        if (!Changes->ApplyOnExecute(txc, Self->NormalizerController)) {
            return false;
        }
    }

    if (Self->NormalizerController.GetNormalizer()->DecActiveCounters() == 0) {
        NormalizerFinished = true;
        NIceDb::TNiceDb db(txc.DB);
        Self->NormalizerController.OnNormalizerFinished(db);
    }
    return true;
}

void TTxApplyNormalizer::Complete(const TActorContext& ctx) {
    NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID())(
        "event", "TTxApplyNormalizer::Complete");
    AFL_VERIFY(!Self->NormalizerController.IsNormalizationFinished())("details", Self->NormalizerController.DebugString());
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "apply_normalizer_changes")("details", Self->NormalizerController.DebugString())(
        "size", Changes->GetSize())("dry_run", IsDryRun);
    if (IsDryRun) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "normalizer_changes_dry_run")(
            "normalizer", Self->NormalizerController.GetNormalizer()->GetClassName())("changes", Changes->DebugString());
    } else {
        Changes->ApplyOnComplete(Self->NormalizerController);
    }
    if (!NormalizerFinished) {
        return;
    }

    if (Self->NormalizerController.SwitchNormalizer()) {
        Self->Execute(new TTxUpdateSchema(Self), ctx);
    } else {
        AFL_VERIFY(Self->NormalizerController.IsNormalizationFinished())("details", Self->NormalizerController.DebugString());
        Self->Execute(new TTxInit(Self), ctx);
    }
}

/// Create local database on tablet start if none
class TTxInitSchema: public TTransactionBase<TColumnShard> {
private:
    const TMonotonic StartInstant = TMonotonic::Now();

public:
    TTxInitSchema(TColumnShard* self)
        : TBase(self) {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override {
        return TXTYPE_INIT_SCHEMA;
    }
};

bool TTxInitSchema::Execute(TTransactionContext& txc, const TActorContext&) {
    NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID())(
        "process", "TTxInitSchema::Execute");
    LOG_S_DEBUG("TxInitSchema.Execute at tablet " << Self->TabletID());

    const bool isFirstRun = txc.DB.GetScheme().IsEmpty();
    NIceDb::TNiceDb(txc.DB).Materialize<Schema>();

    if (!NYDBTest::TControllers::GetColumnShardController()->BuildLocalBaseModifier()) {
        NIceDb::TNiceDb db(txc.DB);
        if (!Self->NormalizerController.InitControllerState(db)) {
            return false;
        }
    }
    {
        NOlap::TNormalizationController::TInitContext initCtx(Self->Info(), Self->TabletID(), Self->SelfId());
        Self->NormalizerController.InitNormalizers(initCtx);
    }

    if (isFirstRun) {
        txc.DB.Alter().SetExecutorAllowLogBatching(gAllowLogBatchingDefaultValue);
        txc.DB.Alter().SetExecutorLogFlushPeriod(TDuration::MicroSeconds(500));
        txc.DB.Alter().SetExecutorCacheSize(500000);
    } else {
        auto localBaseModifier = NYDBTest::TControllers::GetColumnShardController()->BuildLocalBaseModifier();
        if (localBaseModifier) {
            localBaseModifier->Apply(txc);
        }
    }

    // Enable compression for the SmallBlobs table
    const auto* smallBlobsDefaultColumnFamily = txc.DB.GetScheme().DefaultFamilyFor(Schema::SmallBlobs::TableId);
    if (!smallBlobsDefaultColumnFamily || smallBlobsDefaultColumnFamily->Codec != NTable::TAlter::ECodec::LZ4) {
        txc.DB.Alter().SetFamilyCompression(Schema::SmallBlobs::TableId, 0, NTable::TAlter::ECodec::LZ4);
    }

    // SmallBlobs table has compaction policy suitable for a big table
    const auto* smallBlobsTable = txc.DB.GetScheme().GetTableInfo(Schema::SmallBlobs::TableId);
    NLocalDb::TCompactionPolicyPtr bigTableCompactionPolicy = NLocalDb::CreateDefaultUserTablePolicy();
    bigTableCompactionPolicy->MinDataPageSize = 32 * 1024;
    if (!smallBlobsTable || !smallBlobsTable->CompactionPolicy ||
        smallBlobsTable->CompactionPolicy->Generations.size() != bigTableCompactionPolicy->Generations.size()) {
        txc.DB.Alter().SetCompactionPolicy(Schema::SmallBlobs::TableId, *bigTableCompactionPolicy);
    }

    return true;
}

void TTxInitSchema::Complete(const TActorContext& ctx) {
    NActors::TLogContextGuard gLogging =
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID())("process", "TTxInitSchema::Complete");
    Self->Counters.GetCSCounters().Initialization.OnTxInitSchemaFinished(TMonotonic::Now() - StartInstant);
    LOG_S_DEBUG("TxInitSchema.Complete at tablet " << Self->TabletID(););
    Self->Execute(new TTxUpdateSchema(Self), ctx);
}

ITransaction* TColumnShard::CreateTxInitSchema() {
    return new TTxInitSchema(this);
}

void TColumnShard::Handle(TEvPrivate::TEvNormalizerResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxApplyNormalizer(this, ev->Get()->GetChanges()), ctx);
}

}   // namespace NKikimr::NColumnShard
