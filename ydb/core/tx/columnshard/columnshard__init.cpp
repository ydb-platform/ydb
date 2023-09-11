#include "columnshard_impl.h"
#include "columnshard_ttl.h"
#include "columnshard_private_events.h"
#include "columnshard_schema.h"
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>

#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/tx/columnshard/operations/write.h>

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

// TTxInit => SwitchToWork

/// Load data from local database
class TTxInit : public TTransactionBase<TColumnShard> {
public:
    TTxInit(TColumnShard* self)
        : TBase(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_INIT; }

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
    Self->ProgressTxController.Clear();
    Self->AltersInFlight.clear();
    Self->CommitsInFlight.clear();
    Self->TablesManager.Clear();
    Self->LongTxWrites.clear();
    Self->LongTxWritesByUniqueId.clear();
}

bool TTxInit::ReadEverything(TTransactionContext& txc, const TActorContext& ctx)
{
    // Load InsertTable
    TBlobGroupSelector dsGroupSelector(Self->Info());
    NOlap::TDbWrapper dbTable(txc.DB, &dsGroupSelector);
    if (!Self->InsertTable->Load(dbTable, TAppData::TimeProvider->Now())) {
        return false;
    }

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

    if (!ready)
        return false;

    if (!Self->ProgressTxController.Load(txc)) {
        return false;
    }

    if (!Self->OperationsManager.Init(txc)) {
        return false;
    }

    Self->TablesManager.InitFromDB(db, Self->TabletID());
    Self->SetCounter(COUNTER_TABLES, Self->TablesManager.GetTables().size());
    Self->SetCounter(COUNTER_TABLE_PRESETS, Self->TablesManager.GetSchemaPresets().size());
    Self->SetCounter(COUNTER_TABLE_TTLS, Self->TablesManager.GetTtl().PathsCount());

    { // Load long tx writes
        auto rowset = db.Table<Schema::LongTxWrites>().Select();
        if (!rowset.IsReady())
            return false;

        while (!rowset.EndOfSet()) {
            const TWriteId writeId = TWriteId{ rowset.GetValue<Schema::LongTxWrites::WriteId>() };
            const ui32 writePartId = rowset.GetValue<Schema::LongTxWrites::WritePartId>();
            NKikimrLongTxService::TLongTxId proto;
            Y_VERIFY(proto.ParseFromString(rowset.GetValue<Schema::LongTxWrites::LongTxId>()));
            const auto longTxId = NLongTxService::TLongTxId::FromProto(proto);

            Self->LoadLongTxWrite(writeId, writePartId, longTxId);

            if (!rowset.Next())
                return false;
        }
    }

    for (const auto& pr : Self->CommitsInFlight) {
        ui64 txId = pr.first;
        for (TWriteId writeId : pr.second.WriteIds) {
            Y_VERIFY(Self->LongTxWrites.contains(writeId),
                "TTxInit at %" PRIu64 " : Commit %" PRIu64 " references local write %" PRIu64 " that doesn't exist",
                Self->TabletID(), txId, writeId);
            Self->AddLongTxWrite(writeId, txId);
        }
    }

    // There could be extern blobs that are evicting & dropped.
    // Load info from export tables and check if we have such blobs in index to find them
    THashSet<TUnifiedBlobId> lostEvictions;
    TBlobManagerDb blobManagerDb(txc.DB);

    // Initialize the BlobManager
    {
        if (!Self->BlobManager->LoadState(blobManagerDb)) {
            return false;
        }
        if (!Self->BlobManager->LoadOneToOneExport(blobManagerDb, lostEvictions)) {
            return false;
        }
    }

    if (!Self->TablesManager.LoadIndex(dbTable, lostEvictions)) {
        return false;
    }

    // Set dropped evicting records to be erased in future cleanups
    TString strBlobs;
    for (auto& blobId : lostEvictions) {
        TEvictMetadata meta;
        auto evict = Self->BlobManager->GetDropped(blobId, meta);
        Y_VERIFY(evict.State == EEvictState::EVICTING);
        evict.State = EEvictState::ERASING;

        if (meta.GetTierName().empty()) {
            LOG_S_ERROR("Blob " << evict.Blob << " eviction with empty tier name at tablet " << Self->TabletID());
        }

        bool dropped;
        bool present = Self->BlobManager->UpdateOneToOne(evict, blobManagerDb, dropped);
        if (present) {
            strBlobs += "'" + evict.Blob.ToStringNew() + "' ";
        } else {
            LOG_S_ERROR("Unknown dropped evicting blob " << evict.Blob << " at tablet " << Self->TabletID());
        }
    }
    if (!strBlobs.empty()) {
        LOG_S_NOTICE("Erasing potentially exported blobs " << strBlobs << "at tablet " << Self->TabletID());
    }

    Self->UpdateInsertTableCounters();
    Self->UpdateIndexCounters();
    Self->UpdateResourceMetrics(ctx, {});
    return true;
}

bool TTxInit::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    Y_UNUSED(txc);
    LOG_S_DEBUG("TTxInit.Execute at tablet " << Self->TabletID());

    try {
        SetDefaults();
        return ReadEverything(txc, ctx);
    } catch (const TNotReadyTabletException&) {
        return false;
    } catch (const TSchemeErrorTabletException& ex) {
        Y_UNUSED(ex);
        Y_FAIL();
    } catch (...) {
        Y_FAIL("there must be no leaked exceptions");
    }

    return true;
}

void TTxInit::Complete(const TActorContext& ctx) {
    LOG_S_DEBUG("TTxInit.Complete at tablet " << Self->TabletID());
    Self->SwitchToWork(ctx);
    Self->TryRegisterMediatorTimeCast();

    // Trigger progress: planned or outdated tx
    Self->EnqueueProgressTx(ctx);
    Self->EnqueueBackgroundActivities();

    // Start periodic wakeups
    ctx.Schedule(Self->ActivationPeriod, new TEvPrivate::TEvPeriodicWakeup());
}

// TTxUpdateSchema => TTxInit

/// Update local database on tablet start
class TTxUpdateSchema : public TTransactionBase<TColumnShard> {
public:
    TTxUpdateSchema(TColumnShard* self)
        : TBase(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_UPDATE_SCHEMA; }
};

bool TTxUpdateSchema::Execute(TTransactionContext& txc, const TActorContext&) {
    Y_UNUSED(txc);
    LOG_S_DEBUG("TTxUpdateSchema.Execute at tablet " << Self->TabletID());
    return true;
}

void TTxUpdateSchema::Complete(const TActorContext& ctx) {
    LOG_S_DEBUG("TTxUpdateSchema.Complete at tablet " << Self->TabletID());
    Self->Execute(new TTxInit(Self), ctx);
}

// TTxInitSchema => TTxUpdateSchema

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

    bool isCreate = txc.DB.GetScheme().IsEmpty();
    NIceDb::TNiceDb(txc.DB).Materialize<Schema>();

    if (isCreate) {
        txc.DB.Alter().SetExecutorAllowLogBatching(gAllowLogBatchingDefaultValue);
        txc.DB.Alter().SetExecutorLogFlushPeriod(TDuration::MicroSeconds(500));
        txc.DB.Alter().SetExecutorCacheSize(500000);
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
    LOG_S_DEBUG("TxInitSchema.Complete at tablet " << Self->TabletID());
    Self->Execute(new TTxUpdateSchema(Self), ctx);
}

ITransaction* TColumnShard::CreateTxInitSchema() {
    return new TTxInitSchema(this);
}

bool TColumnShard::LoadTx(const ui64 txId, const NKikimrTxColumnShard::ETransactionKind& txKind, const TString& txBody) {
    switch (txKind) {
        case NKikimrTxColumnShard::TX_KIND_SCHEMA: {
            TColumnShard::TAlterMeta meta;
            Y_VERIFY(meta.Body.ParseFromString(txBody));
            AltersInFlight.emplace(txId, std::move(meta));
            break;
        }
        case NKikimrTxColumnShard::TX_KIND_COMMIT: {
            NKikimrTxColumnShard::TCommitTxBody body;
            Y_VERIFY(body.ParseFromString(txBody));

            TColumnShard::TCommitMeta meta;
            for (auto& id : body.GetWriteIds()) {
                meta.AddWriteId(TWriteId{id});
            }

            CommitsInFlight.emplace(txId, std::move(meta));
            break;
        }
        default: {
            Y_FAIL("Unsupported TxKind stored in the TxInfo table");
        }
    }
    return true;
}

}
