#include "columnshard_impl.h"
#include "columnshard_ttl.h"
#include "columnshard_private_events.h"
#include "columnshard_schema.h"
#include "hooks/abstract/abstract.h"
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>

#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/tx/columnshard/operations/write.h>


namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

class TTxInit : public TTransactionBase<TColumnShard> {
public:
    TTxInit(TColumnShard* self)
        : TBase(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_INIT; }

private:
    bool Precharge(TTransactionContext& txc);
    void SetDefaults();
    bool ReadEverything(TTransactionContext& txc, const TActorContext& ctx);
};

void TTxInit::SetDefaults() {
    Self->CurrentSchemeShardId = 0;
    Self->LastSchemaSeqNo = { };
    Self->ProcessingParams.reset();
    Self->LastWriteId = TWriteId{0};
    Self->LastPlannedStep = 0;
    Self->LastPlannedTxId = 0;
    Self->OwnerPathId = 0;
    Self->OwnerPath.clear();
    Self->AltersInFlight.clear();
    Self->CommitsInFlight.clear();
    Self->LongTxWrites.clear();
    Self->LongTxWritesByUniqueId.clear();
}

bool TTxInit::Precharge(TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    bool ready = true;
    ready = ready & Schema::Precharge<Schema::Value>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::TxInfo>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::SchemaPresetInfo>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::SchemaPresetVersionInfo>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::TtlSettingsPresetInfo>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::TtlSettingsPresetVersionInfo>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::TableInfo>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::TableVersionInfo>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::LongTxWrites>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::BlobsToKeep>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::BlobsToDelete>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::IndexCounters>(db, txc.DB.GetScheme());

    ready = ready && Schema::GetSpecialValue(db, Schema::EValueIds::CurrentSchemeShardId, Self->CurrentSchemeShardId);
    ready = ready && Schema::GetSpecialValue(db, Schema::EValueIds::LastSchemaSeqNoGeneration, Self->LastSchemaSeqNo.Generation);
    ready = ready && Schema::GetSpecialValue(db, Schema::EValueIds::LastSchemaSeqNoRound, Self->LastSchemaSeqNo.Round);
    ready = ready && Schema::GetSpecialProtoValue(db, Schema::EValueIds::ProcessingParams, Self->ProcessingParams);
    ready = ready && Schema::GetSpecialValue(db, Schema::EValueIds::LastWriteId, Self->LastWriteId);
    ready = ready && Schema::GetSpecialValue(db, Schema::EValueIds::LastPlannedStep, Self->LastPlannedStep);
    ready = ready && Schema::GetSpecialValue(db, Schema::EValueIds::LastPlannedTxId, Self->LastPlannedTxId);
    ready = ready && Schema::GetSpecialValue(db, Schema::EValueIds::LastExportNumber, Self->LastExportNo);
    ready = ready && Schema::GetSpecialValue(db, Schema::EValueIds::OwnerPathId, Self->OwnerPathId);
    ready = ready && Schema::GetSpecialValue(db, Schema::EValueIds::OwnerPath, Self->OwnerPath);

    if (!ready) {
        return false;
    }
    return true;
}

bool TTxInit::ReadEverything(TTransactionContext& txc, const TActorContext& ctx) {
    if (!Precharge(txc)) {
        return false;
    }

    NIceDb::TNiceDb db(txc.DB);
    TBlobGroupSelector dsGroupSelector(Self->Info());
    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);
    {
        ACFL_INFO("step", "TInsertTable::Load_Start");
        auto localInsertTable = std::make_unique<NOlap::TInsertTable>();
        if (!localInsertTable->Load(dbTable, TAppData::TimeProvider->Now())) {
            ACFL_ERROR("step", "TInsertTable::Load_Fails");
            return false;
        }
        ACFL_INFO("step", "TInsertTable::Load_Finish");
        Self->InsertTable.swap(localInsertTable);
    }

    {
        ACFL_INFO("step", "TTxController::Load_Start");
        auto localTxController = std::make_unique<TTxController>(*Self);
         if (!localTxController->Load(txc)) {
            ACFL_ERROR("step", "TTxController::Load_Fails");
            return false;
        }
        ACFL_INFO("step", "TTxController::Load_Finish");
        Self->ProgressTxController.swap(localTxController);
    }

    {
        ACFL_INFO("step", "TOperationsManager::Load_Start");
        auto localOperationsManager = std::make_unique<TOperationsManager>();
         if (!localOperationsManager->Load(txc)) {
            ACFL_ERROR("step", "TOperationsManager::Load_Fails");
            return false;
        }
        ACFL_INFO("step", "TOperationsManager::Load_Finish");
        Self->OperationsManager.swap(localOperationsManager);
    }

    {
        TBlobManagerDb blobManagerDb(txc.DB);
        for (auto&& i : Self->StoragesManager->GetStorages()) {
            if (!i.second->Load(blobManagerDb)) {
                ACFL_ERROR("event", "storages manager load")("storage", i.first);
                return false;
            }
        }
    }
    {
        ACFL_INFO("step", "TTablesManager::Load_Start");
        TTablesManager tManagerLocal(Self->StoragesManager, Self->TabletID());
        if (!tManagerLocal.InitFromDB(db)) {
            ACFL_ERROR("step", "TTablesManager::InitFromDB_Fails");
            return false;
        }
        if (!tManagerLocal.LoadIndex(dbTable)) {
            ACFL_ERROR("step", "TTablesManager::LoadIndex_Fails");
            return false;
        }
        Self->TablesManager = std::move(tManagerLocal);

        Self->SetCounter(COUNTER_TABLES, Self->TablesManager.GetTables().size());
        Self->SetCounter(COUNTER_TABLE_PRESETS, Self->TablesManager.GetSchemaPresets().size());
        Self->SetCounter(COUNTER_TABLE_TTLS, Self->TablesManager.GetTtl().PathsCount());
        ACFL_INFO("step", "TTablesManager::Load_Finish");
    }

    {
        auto rowset = db.Table<Schema::LongTxWrites>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const TWriteId writeId = TWriteId{ rowset.GetValue<Schema::LongTxWrites::WriteId>() };
            const ui32 writePartId = rowset.GetValue<Schema::LongTxWrites::WritePartId>();
            NKikimrLongTxService::TLongTxId proto;
            Y_ABORT_UNLESS(proto.ParseFromString(rowset.GetValue<Schema::LongTxWrites::LongTxId>()));
            const auto longTxId = NLongTxService::TLongTxId::FromProto(proto);

            Self->LoadLongTxWrite(writeId, writePartId, longTxId);

            if (!rowset.Next()) {
                return false;
            }
        }
    }

    for (const auto& pr : Self->CommitsInFlight) {
        ui64 txId = pr.first;
        for (TWriteId writeId : pr.second.WriteIds) {
            Y_ABORT_UNLESS(Self->LongTxWrites.contains(writeId),
                "TTxInit at %" PRIu64 " : Commit %" PRIu64 " references local write %" PRIu64 " that doesn't exist",
                Self->TabletID(), txId, writeId);
            Self->AddLongTxWrite(writeId, txId);
        }
    }
    Self->UpdateInsertTableCounters();
    Self->UpdateIndexCounters();
    Self->UpdateResourceMetrics(ctx, {});
    return true;
}

bool TTxInit::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID())("event", "initialize_shard");
    LOG_S_DEBUG("TTxInit.Execute at tablet " << Self->TabletID());

    try {
        SetDefaults();
        return ReadEverything(txc, ctx);
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
    Self->SwitchToWork(ctx);
}

class TTxUpdateSchema : public TTransactionBase<TColumnShard> {
    std::vector<NOlap::INormalizerTask::TPtr> NormalizerTasks;
public:
    TTxUpdateSchema(TColumnShard* self)
        : TBase(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_UPDATE_SCHEMA; }
};

bool TTxUpdateSchema::Execute(TTransactionContext& txc, const TActorContext&) {
    NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID())("event", "initialize_shard");
    ACFL_INFO("step", "TTxUpdateSchema.Execute_Start")("details", Self->NormalizerController.DebugString());

    while (!Self->NormalizerController.IsNormalizationFinished()) {
        auto normalizer = Self->NormalizerController.GetNormalizer();
        auto result = normalizer->Init(Self->NormalizerController, txc);
        if (result.IsSuccess()) {
            NormalizerTasks = result.DetachResult();
            if (!NormalizerTasks.empty()) {
                break;
            }
            Self->NormalizerController.SwitchNormalizer();
        } else {
            Self->NormalizerController.GetCounters().OnNormalizerFails();
            return false;
        }
    }
    ACFL_INFO("step", "TTxUpdateSchema.Execute_Finish");
    return true;
}

void TTxUpdateSchema::Complete(const TActorContext& ctx) {
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("step", "TTxUpdateSchema.Complete");
    if (NormalizerTasks.empty()) {
        Self->Execute(new TTxInit(Self), ctx);
        return;
    }

    NOlap::TNormalizationContext nCtx;
    nCtx.SetColumnshardActor(Self->SelfId());
    nCtx.SetResourceSubscribeActor(Self->ResourceSubscribeActor);

    for (auto&& task : NormalizerTasks) {
        Self->NormalizerController.GetCounters().OnNormalizerStart();
        task->Start(Self->NormalizerController, nCtx);
    }
}

class TTxApplyNormalizer : public TTransactionBase<TColumnShard> {
public:
    TTxApplyNormalizer(TColumnShard* self, NOlap::INormalizerChanges::TPtr changes)
        : TBase(self)
        , Changes(changes)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_UPDATE_SCHEMA; }

private:
    NOlap::INormalizerChanges::TPtr Changes;
};

bool TTxApplyNormalizer::Execute(TTransactionContext& txc, const TActorContext&) {
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("step", "TTxApplyNormalizer.Execute")("details", Self->NormalizerController.DebugString());;
    Self->NormalizerController.GetNormalizer()->OnResultReady();
    return Changes->Apply(txc, Self->NormalizerController);
}

void TTxApplyNormalizer::Complete(const TActorContext& ctx) {
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("step", "TTxApplyNormalizer.Complete");

    if (Self->NormalizerController.GetNormalizer()->WaitResult()) {
        return;
    }

    if (Self->NormalizerController.SwitchNormalizer()) {
        Self->Execute(new TTxUpdateSchema(Self), ctx);
    } else {
        Self->Execute(new TTxInit(Self), ctx);
    }
}

/// Create local database on tablet start if none
class TTxInitSchema : public TTransactionBase<TColumnShard> {
public:
    TTxInitSchema(TColumnShard* self)
        : TBase(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_INIT_SCHEMA; }
};

bool TTxInitSchema::Execute(TTransactionContext& txc, const TActorContext&) {
    LOG_S_DEBUG("TxInitSchema.Execute at tablet " << Self->TabletID());

    const bool isFirstRun = txc.DB.GetScheme().IsEmpty();
    NIceDb::TNiceDb(txc.DB).Materialize<Schema>();

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
    if (!smallBlobsDefaultColumnFamily ||
        smallBlobsDefaultColumnFamily->Codec != NTable::TAlter::ECodec::LZ4)
    {
        txc.DB.Alter().SetFamily(Schema::SmallBlobs::TableId, 0,
            NTable::TAlter::ECache::None, NTable::TAlter::ECodec::LZ4);
    }

    // SmallBlobs table has compaction policy suitable for a big table
    const auto* smallBlobsTable = txc.DB.GetScheme().GetTableInfo(Schema::SmallBlobs::TableId);
    NLocalDb::TCompactionPolicyPtr bigTableCompactionPolicy = NLocalDb::CreateDefaultUserTablePolicy();
    bigTableCompactionPolicy->MinDataPageSize = 32 * 1024;
    if (!smallBlobsTable ||
        !smallBlobsTable->CompactionPolicy ||
        smallBlobsTable->CompactionPolicy->Generations.size() != bigTableCompactionPolicy->Generations.size())
    {
        txc.DB.Alter().SetCompactionPolicy(Schema::SmallBlobs::TableId, *bigTableCompactionPolicy);
    }

    return true;
}

void TTxInitSchema::Complete(const TActorContext& ctx) {
    LOG_S_DEBUG("TxInitSchema.Complete at tablet " << Self->TabletID();)
    Self->Execute(new TTxUpdateSchema(Self), ctx);
}

ITransaction* TColumnShard::CreateTxInitSchema() {
    return new TTxInitSchema(this);
}

void TColumnShard::Handle(TEvPrivate::TEvNormalizerResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxApplyNormalizer(this, ev->Get()->GetChanges()), ctx);
}

bool TColumnShard::LoadTx(const ui64 txId, const NKikimrTxColumnShard::ETransactionKind& txKind, const TString& txBody) {
    switch (txKind) {
        case NKikimrTxColumnShard::TX_KIND_SCHEMA: {
            TColumnShard::TAlterMeta meta;
            Y_ABORT_UNLESS(meta.Body.ParseFromString(txBody));
            AltersInFlight.emplace(txId, std::move(meta));
            break;
        }
        case NKikimrTxColumnShard::TX_KIND_COMMIT: {
            NKikimrTxColumnShard::TCommitTxBody body;
            Y_ABORT_UNLESS(body.ParseFromString(txBody));

            TColumnShard::TCommitMeta meta;
            for (auto& id : body.GetWriteIds()) {
                meta.AddWriteId(TWriteId{id});
            }

            CommitsInFlight.emplace(txId, std::move(meta));
            break;
        }
        default: {
            Y_ABORT("Unsupported TxKind stored in the TxInfo table");
        }
    }
    return true;
}

}
