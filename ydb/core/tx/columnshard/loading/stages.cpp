#include "stages.h"

#include <ydb/core/tx/columnshard/bg_tasks/manager/manager.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/transactions/locks_db.h>
#include <ydb/core/tx/tiering/manager.h>

namespace NKikimr::NColumnShard::NLoading {

bool TTxControllerInitializer::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    auto localTxController = std::make_unique<TTxController>(*Self);
    if (!localTxController->Load(txc)) {
        return false;
    }
    Self->ProgressTxController.swap(localTxController);
    return true;
}

bool TTxControllerInitializer::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return Schema::Precharge<Schema::TxInfo>(db, txc.DB.GetScheme());
}

bool TOperationsManagerInitializer::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    auto localOperationsManager = std::make_unique<TOperationsManager>();
    if (!localOperationsManager->Load(txc)) {
        return false;
    }
    Self->OperationsManager.swap(localOperationsManager);
    return true;
}

bool TOperationsManagerInitializer::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return (int)Schema::Precharge<Schema::Operations>(db, txc.DB.GetScheme()) &
           (int)Schema::Precharge<Schema::OperationTxIds>(db, txc.DB.GetScheme());
}

bool TStoragesManagerInitializer::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    AFL_VERIFY(Self->StoragesManager);
    return Self->StoragesManager->LoadIdempotency(txc.DB);
}

bool TStoragesManagerInitializer::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return (int)Schema::Precharge<Schema::BlobsToKeep>(db, txc.DB.GetScheme()) &
           (int)Schema::Precharge<Schema::BlobsToDelete>(db, txc.DB.GetScheme()) &
           (int)Schema::Precharge<Schema::BlobsToDeleteWT>(db, txc.DB.GetScheme()) &
           (int)Schema::Precharge<Schema::SharedBlobIds>(db, txc.DB.GetScheme()) &
           (int)Schema::Precharge<Schema::BorrowedBlobIds>(db, txc.DB.GetScheme());
}

bool TDBLocksInitializer::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    if (txc.DB.GetScheme().GetTableInfo(Schema::Locks::TableId)) {
        TColumnShardLocksDb locksDb(*Self, txc);
        if (!Self->SysLocks.Load(locksDb)) {
            return false;
        }
    }
    return true;
}

bool TDBLocksInitializer::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return Schema::Precharge<NColumnShard::Schema::Locks>(db, txc.DB.GetScheme());
}

bool TBackgroundSessionsInitializer::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    return Self->BackgroundSessionsManager->LoadIdempotency(txc);
}

bool TSharingSessionsInitializer::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    auto local = std::make_shared<NOlap::NDataSharing::TSessionsManager>();
    if (!local->Load(txc.DB, Self->TablesManager.GetPrimaryIndexAsOptional<NOlap::TColumnEngineForLogs>())) {
        return false;
    }
    Self->SharingSessionsManager = local;
    return true;
}

bool TInFlightReadsInitializer::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TInFlightReadsTracker local(Self->StoragesManager, Self->Counters.GetRequestsTracingCounters());
    if (!local.LoadFromDatabase(txc.DB)) {
        return false;
    }
    Self->InFlightReadsTracker = std::move(local);
    return true;
}

bool TSpecialValuesInitializer::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    if (!Schema::GetSpecialValueOpt(db, Schema::EValueIds::CurrentSchemeShardId, Self->CurrentSchemeShardId)) {
        return false;
    }
    if (!Schema::GetSpecialValueOpt(db, Schema::EValueIds::LastSchemaSeqNoGeneration, Self->LastSchemaSeqNo.Generation)) {
        return false;
    }
    if (!Schema::GetSpecialValueOpt(db, Schema::EValueIds::LastSchemaSeqNoRound, Self->LastSchemaSeqNo.Round)) {
        return false;
    }
    if (!Schema::GetSpecialProtoValue(db, Schema::EValueIds::ProcessingParams, Self->ProcessingParams)) {
        return false;
    }
    if (!Schema::GetSpecialValueOpt(db, Schema::EValueIds::LastPlannedStep, Self->LastPlannedStep)) {
        return false;
    }
    if (!Schema::GetSpecialValueOpt(db, Schema::EValueIds::LastPlannedTxId, Self->LastPlannedTxId)) {
        return false;
    }
    if (!Schema::GetSpecialValueOpt(db, Schema::EValueIds::LastExportNumber, Self->LastExportNo)) {
        return false;
    }
    if (!Schema::GetSpecialValueOpt(db, Schema::EValueIds::OwnerPath, Self->OwnerPath)) {
        return false;
    }

    if (!Schema::GetSpecialValueOpt(db, Schema::EValueIds::SubDomainLocalPathId, Self->SpaceWatcher->SubDomainPathId)) {
        return false;
    }

    ui64 outOfSpace = 0;
    if (!Schema::GetSpecialValueOpt(db, Schema::EValueIds::SubDomainOutOfSpace, outOfSpace)) {
        return false;
    }
    Self->SpaceWatcher->SubDomainOutOfSpace = outOfSpace;

    {
        ui64 lastCompletedStep = 0;
        ui64 lastCompletedTx = 0;
        if (!Schema::GetSpecialValueOpt(db, Schema::EValueIds::LastCompletedStep, lastCompletedStep)) {
            return false;
        }
        if (!Schema::GetSpecialValueOpt(db, Schema::EValueIds::LastCompletedTxId, lastCompletedTx)) {
            return false;
        }
        Self->LastCompletedTx = NOlap::TSnapshot(lastCompletedStep, lastCompletedTx);
    }

    return true;
}

bool TSpecialValuesInitializer::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return Schema::Precharge<Schema::Value>(db, txc.DB.GetScheme());
}

bool TTablesManagerInitializer::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    TTablesManager tablesManagerLocal(Self->StoragesManager, Self->DataAccessorsManager.GetObjectPtrVerified(),
        Self->Counters.GetPortionIndexCounters(), Self->TabletID());
    {
        TMemoryProfileGuard g("TTxInit/TTablesManager");
        if (!tablesManagerLocal.InitFromDB(db, Self->Info())) {
            return false;
        }
    }
    Self->Counters.GetTabletCounters()->SetCounter(COUNTER_TABLES, tablesManagerLocal.GetTables().size());
    Self->Counters.GetTabletCounters()->SetCounter(COUNTER_TABLE_PRESETS, tablesManagerLocal.GetSchemaPresets().size());
    Self->Counters.GetTabletCounters()->SetCounter(COUNTER_TABLE_TTLS, tablesManagerLocal.GetTtl().size());

    Self->TablesManager = std::move(tablesManagerLocal);
    return true;
}

std::shared_ptr<NKikimr::ITxReader> TTablesManagerInitializer::BuildNextReaderAfterLoad() {
    if (Self->TablesManager.HasPrimaryIndex()) {
        return Self->TablesManager.MutablePrimaryIndex().BuildLoader(std::make_shared<TBlobGroupSelector>(Self->Info()));
    } else {
        return nullptr;
    }
}

bool TTablesManagerInitializer::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return (int)Schema::Precharge<Schema::SchemaPresetInfo>(db, txc.DB.GetScheme()) &
           (int)Schema::Precharge<Schema::SchemaPresetVersionInfo>(db, txc.DB.GetScheme()) &
           (int)Schema::Precharge<Schema::TableInfo>(db, txc.DB.GetScheme()) &
           (int)Schema::Precharge<Schema::TableVersionInfo>(db, txc.DB.GetScheme()) &
           (int)Schema::Precharge<Schema::TtlSettingsPresetInfo>(db, txc.DB.GetScheme()) &
           (int)Schema::Precharge<Schema::TtlSettingsPresetVersionInfo>(db, txc.DB.GetScheme());
}

bool TTiersManagerInitializer::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    auto rowset = db.Table<Schema::TableVersionInfo>().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        NKikimrTxColumnShard::TTableVersionInfo versionInfo;
        AFL_VERIFY(versionInfo.ParseFromString(rowset.GetValue<Schema::TableVersionInfo::InfoProto>()));
        if (versionInfo.GetTtlSettings().HasEnabled()) {
            NOlap::TTiering tiering;
            tiering.DeserializeFromProto(versionInfo.GetTtlSettings().GetEnabled()).Validate();
            Self->Tiers->ActivateTiers(tiering.GetUsedTiers());
        }

        if (!rowset.Next()) {
            return false;
        }
    }
    return true;
}

bool TTiersManagerInitializer::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return Schema::Precharge<Schema::TableVersionInfo>(db, txc.DB.GetScheme());
}

}   // namespace NKikimr::NColumnShard::NLoading
