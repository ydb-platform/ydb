#include "columnshard_impl.h"
#include "columnshard_ttl.h"
#include "columnshard_private_events.h"
#include "columnshard_schema.h"
#include "blob_manager_db.h"

#include <ydb/core/tablet/tablet_exception.h>

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
    Self->BasicTxInfo.clear();
    Self->DeadlineQueue.clear();
    Self->PlanQueue.clear();
    Self->AltersInFlight.clear();
    Self->CommitsInFlight.clear();
    Self->SchemaPresets.clear();
    Self->Tables.clear();
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

    { // Load transactions
        auto rowset = db.Table<Schema::TxInfo>().GreaterOrEqual(0).Select();
        if (!rowset.IsReady())
            return false;

        while (!rowset.EndOfSet()) {
            ui64 txId = rowset.GetValue<Schema::TxInfo::TxId>();
            auto& txInfo = Self->BasicTxInfo[txId];
            txInfo.TxId = txId;
            txInfo.MaxStep = rowset.GetValue<Schema::TxInfo::MaxStep>();
            txInfo.PlanStep = rowset.GetValueOrDefault<Schema::TxInfo::PlanStep>(0);
            txInfo.Source = rowset.GetValue<Schema::TxInfo::Source>();
            txInfo.Cookie = rowset.GetValue<Schema::TxInfo::Cookie>();
            txInfo.TxKind = rowset.GetValue<Schema::TxInfo::TxKind>();

            if (txInfo.PlanStep != 0) {
                Self->PlanQueue.emplace(txInfo.PlanStep, txInfo.TxId);
            } else if (txInfo.MaxStep != Max<ui64>()) {
                Self->DeadlineQueue.emplace(txInfo.MaxStep, txInfo.TxId);
            }

            switch (txInfo.TxKind) {
                case NKikimrTxColumnShard::TX_KIND_SCHEMA: {
                    TColumnShard::TAlterMeta meta;
                    Y_VERIFY(meta.Body.ParseFromString(rowset.GetValue<Schema::TxInfo::TxBody>()));

                    Self->AltersInFlight.emplace(txId, std::move(meta));
                    break;
                }
                case NKikimrTxColumnShard::TX_KIND_COMMIT: {
                    NKikimrTxColumnShard::TCommitTxBody body;
                    Y_VERIFY(body.ParseFromString(rowset.GetValue<Schema::TxInfo::TxBody>()));

                    TColumnShard::TCommitMeta meta;
                    meta.MetaShard = body.GetTxInitiator();
                    for (auto& id : body.GetWriteIds()) {
                        meta.AddWriteId(TWriteId{id});
                    }

                    Self->CommitsInFlight.emplace(txId, std::move(meta));
                    break;
                }
                default: {
                    Y_FAIL("Unsupported TxKind stored in the TxInfo table");
                }
            }

            if (!rowset.Next())
                return false;
        }
    }

    // Primary index defaut schema and TTL (both are versioned)
    TMap<NOlap::TSnapshot, NOlap::TIndexInfo> schemaPreset;
    TMap<NOlap::TSnapshot, NOlap::TIndexInfo> commonSchema;
    THashMap<ui64, TMap<TRowVersion, TTtl::TDescription>> ttls;

    { // Load schema presets
        auto rowset = db.Table<Schema::SchemaPresetInfo>().Select();
        if (!rowset.IsReady())
            return false;

        while (!rowset.EndOfSet()) {
            const ui32 id = rowset.GetValue<Schema::SchemaPresetInfo::Id>();
            auto& preset = Self->SchemaPresets[id];
            preset.Id = id;
            if (id) {
                preset.Name = rowset.GetValue<Schema::SchemaPresetInfo::Name>();
            }
            Y_VERIFY(!id || preset.Name == "default", "Unsupported preset at load time");

            if (rowset.HaveValue<Schema::SchemaPresetInfo::DropStep>() &&
                rowset.HaveValue<Schema::SchemaPresetInfo::DropTxId>())
            {
                preset.DropVersion.Step = rowset.GetValue<Schema::SchemaPresetInfo::DropStep>();
                preset.DropVersion.TxId = rowset.GetValue<Schema::SchemaPresetInfo::DropTxId>();
            }

            if (!rowset.Next())
                return false;
        }
    }

    { // Load schema preset versions
        auto rowset = db.Table<Schema::SchemaPresetVersionInfo>().Select();
        if (!rowset.IsReady())
            return false;

        while (!rowset.EndOfSet()) {
            const ui32 id = rowset.GetValue<Schema::SchemaPresetVersionInfo::Id>();
            Y_VERIFY(Self->SchemaPresets.contains(id));
            auto& preset = Self->SchemaPresets.at(id);
            TRowVersion version(
                rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceStep>(),
                rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceTxId>());
            auto& info = preset.Versions[version];
            Y_VERIFY(info.ParseFromString(rowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));

            NOlap::TSnapshot snap{version.Step, version.TxId};
            if (!id) {
                commonSchema.emplace(snap, Self->ConvertSchema(info.GetSchema()));
            } else if (preset.Name == "default") {
                schemaPreset.emplace(snap, Self->ConvertSchema(info.GetSchema()));
            }

            if (!rowset.Next())
                return false;
        }
    }

    { // Load tables
        auto rowset = db.Table<Schema::TableInfo>().Select();
        if (!rowset.IsReady())
            return false;

        while (!rowset.EndOfSet()) {
            const ui64 pathId = rowset.GetValue<Schema::TableInfo::PathId>();
            auto& table = Self->Tables[pathId];
            table.PathId = pathId;
            table.TieringUsage = rowset.GetValue<Schema::TableInfo::TieringUsage>();
            if (rowset.HaveValue<Schema::TableInfo::DropStep>() &&
                rowset.HaveValue<Schema::TableInfo::DropTxId>())
            {
                table.DropVersion.Step = rowset.GetValue<Schema::TableInfo::DropStep>();
                table.DropVersion.TxId = rowset.GetValue<Schema::TableInfo::DropTxId>();
                Self->PathsToDrop.insert(pathId);
            }

            if (!rowset.Next())
                return false;
        }
    }

    { // Load table versions
        auto rowset = db.Table<Schema::TableVersionInfo>().Select();
        if (!rowset.IsReady())
            return false;

        while (!rowset.EndOfSet()) {
            const ui64 pathId = rowset.GetValue<Schema::TableVersionInfo::PathId>();
            Y_VERIFY(Self->Tables.contains(pathId));
            auto& table = Self->Tables.at(pathId);
            TRowVersion version(
                rowset.GetValue<Schema::TableVersionInfo::SinceStep>(),
                rowset.GetValue<Schema::TableVersionInfo::SinceTxId>());
            auto& info = table.Versions[version];
            Y_VERIFY(info.ParseFromString(rowset.GetValue<Schema::TableVersionInfo::InfoProto>()));

            if (!Self->PathsToDrop.count(pathId)) {
                auto& ttlSettings = info.GetTtlSettings();
                if (ttlSettings.HasEnabled()) {
                    ttls[pathId].emplace(version, TTtl::TDescription(ttlSettings.GetEnabled()));
                }
            }

            if (!rowset.Next())
                return false;
        }
    }

    for (auto& [pathId, map] : ttls) {
        auto& description = map.rbegin()->second; // last version if many
        Self->Ttl.SetPathTtl(pathId, std::move(description));
    }

    Self->SetCounter(COUNTER_TABLES, Self->Tables.size());
    Self->SetCounter(COUNTER_TABLE_PRESETS, Self->SchemaPresets.size());
    Self->SetCounter(COUNTER_TABLE_TTLS, ttls.size());

    if (!schemaPreset.empty()) {
        Y_VERIFY(commonSchema.empty(), "Mix of schema preset and common schema");
        Self->SetPrimaryIndex(std::move(schemaPreset));
    } else if (!commonSchema.empty()) {
        Self->SetPrimaryIndex(std::move(commonSchema));
    } else {
        Y_VERIFY(Self->Tables.empty());
        Y_VERIFY(Self->SchemaPresets.empty());
    }

    { // Load long tx writes
        auto rowset = db.Table<Schema::LongTxWrites>().Select();
        if (!rowset.IsReady())
            return false;

        while (!rowset.EndOfSet()) {
            const TWriteId writeId = TWriteId{rowset.GetValue<Schema::LongTxWrites::WriteId>()};
            NKikimrLongTxService::TLongTxId proto;
            Y_VERIFY(proto.ParseFromString(rowset.GetValue<Schema::LongTxWrites::LongTxId>()));
            const auto longTxId = NLongTxService::TLongTxId::FromProto(proto);

            Self->LoadLongTxWrite(writeId, longTxId);

            if (!rowset.Next())
                return false;
        }
    }

    for (const auto& pr : Self->CommitsInFlight) {
        ui64 txId = pr.first;
        if (pr.second.MetaShard == 0) {
            for (TWriteId writeId : pr.second.WriteIds) {
                Y_VERIFY(Self->LongTxWrites.contains(writeId),
                    "TTxInit at %" PRIu64 " : Commit %" PRIu64 " references local write %" PRIu64 " that doesn't exist",
                    Self->TabletID(), txId, writeId);
                Self->AddLongTxWrite(writeId, txId);
            }
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

    // Load primary index
    if (Self->PrimaryIndex) {
        TBlobGroupSelector dsGroupSelector(Self->Info());
        NOlap::TDbWrapper idxDB(txc.DB, &dsGroupSelector);
        if (!Self->PrimaryIndex->Load(idxDB, lostEvictions, Self->PathsToDrop)) {
            return false;
        }
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
        bool present = Self->BlobManager->UpdateOneToOne(std::move(evict), blobManagerDb, dropped);
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

}
