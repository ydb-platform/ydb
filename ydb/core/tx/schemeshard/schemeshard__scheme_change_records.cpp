#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

ui64 TSchemeShard::AllocateSchemeChangeSequenceId(NIceDb::TNiceDb& db) {
    ui64 id = ++NextSchemeChangeSequenceId;
    PersistUpdateNextSchemeChangeSequenceId(db);
    return id;
}

void TSchemeShard::PersistSchemeChangeRecord(NIceDb::TNiceDb& db, const TSchemeChangeRecordData& entry)
{
    using T = Schema::SchemeChangeRecords;
    db.Table<T>().Key(entry.SequenceId).Update(
        NIceDb::TUpdate<T::TxId>(ui64(entry.TxId)),
        NIceDb::TUpdate<T::OperationType>(ui32(entry.TxType)),
        NIceDb::TUpdate<T::PathOwnerId>(entry.PathId.OwnerId),
        NIceDb::TUpdate<T::PathLocalId>(entry.PathId.LocalPathId),
        NIceDb::TUpdate<T::PathName>(entry.PathName),
        NIceDb::TUpdate<T::ObjectType>(ui32(entry.ObjectType)),
        NIceDb::TUpdate<T::Status>(ui32(entry.Status)),
        NIceDb::TUpdate<T::UserSID>(entry.UserSid),
        NIceDb::TUpdate<T::SchemaVersion>(entry.SchemaVersion),
        NIceDb::TUpdate<T::CompletedAt>(entry.CompletedAt.MicroSeconds()),
        NIceDb::TUpdate<T::PlanStep>(ui64(entry.PlanStep))
    );
    ++SchemeChangeRecordCount;
    PersistUpdateSchemeChangeRecordCount(db);
}

ui64 TSchemeShard::GetTypeSpecificAlterVersion(TPathId pathId, NKikimrSchemeOp::EPathType pathType) const {
    switch (pathType) {
        case NKikimrSchemeOp::EPathTypeTable:
            if (auto it = Tables.find(pathId); it != Tables.end()) {
                return it->second->AlterVersion;
            }
            break;
        case NKikimrSchemeOp::EPathTypePersQueueGroup:
            if (auto it = Topics.find(pathId); it != Topics.end()) {
                return it->second->AlterVersion;
            }
            break;
        case NKikimrSchemeOp::EPathTypeBlockStoreVolume:
            if (auto it = BlockStoreVolumes.find(pathId); it != BlockStoreVolumes.end()) {
                return it->second->AlterVersion;
            }
            break;
        case NKikimrSchemeOp::EPathTypeFileStore:
            if (auto it = FileStoreInfos.find(pathId); it != FileStoreInfos.end()) {
                return it->second->Version;
            }
            break;
        case NKikimrSchemeOp::EPathTypeKesus:
            if (auto it = KesusInfos.find(pathId); it != KesusInfos.end()) {
                return it->second->Version;
            }
            break;
        case NKikimrSchemeOp::EPathTypeSolomonVolume:
            if (auto it = SolomonVolumes.find(pathId); it != SolomonVolumes.end()) {
                return it->second->Version;
            }
            break;
        case NKikimrSchemeOp::EPathTypeTableIndex:
            if (auto it = Indexes.find(pathId); it != Indexes.end()) {
                return it->second->AlterVersion;
            }
            break;
        case NKikimrSchemeOp::EPathTypeColumnStore:
            if (auto it = OlapStores.find(pathId); it != OlapStores.end()) {
                return it->second->GetAlterVersion();
            }
            break;
        case NKikimrSchemeOp::EPathTypeColumnTable:
            if (ColumnTables.contains(pathId)) {
                return ColumnTables.at(pathId)->AlterVersion;
            }
            break;
        case NKikimrSchemeOp::EPathTypeCdcStream:
            if (auto it = CdcStreams.find(pathId); it != CdcStreams.end()) {
                return it->second->AlterVersion;
            }
            break;
        case NKikimrSchemeOp::EPathTypeSequence:
            if (auto it = Sequences.find(pathId); it != Sequences.end()) {
                return it->second->AlterVersion;
            }
            break;
        case NKikimrSchemeOp::EPathTypeReplication:
        case NKikimrSchemeOp::EPathTypeTransfer:
            if (auto it = Replications.find(pathId); it != Replications.end()) {
                return it->second->AlterVersion;
            }
            break;
        case NKikimrSchemeOp::EPathTypeBlobDepot:
            if (auto it = BlobDepots.find(pathId); it != BlobDepots.end()) {
                return it->second->AlterVersion;
            }
            break;
        case NKikimrSchemeOp::EPathTypeExternalTable:
            if (auto it = ExternalTables.find(pathId); it != ExternalTables.end()) {
                return it->second->AlterVersion;
            }
            break;
        case NKikimrSchemeOp::EPathTypeExternalDataSource:
            if (auto it = ExternalDataSources.find(pathId); it != ExternalDataSources.end()) {
                return it->second->AlterVersion;
            }
            break;
        case NKikimrSchemeOp::EPathTypeView:
            if (auto it = Views.find(pathId); it != Views.end()) {
                return it->second->AlterVersion;
            }
            break;
        case NKikimrSchemeOp::EPathTypeResourcePool:
            if (auto it = ResourcePools.find(pathId); it != ResourcePools.end()) {
                return it->second->AlterVersion;
            }
            break;
        case NKikimrSchemeOp::EPathTypeBackupCollection:
            if (auto it = BackupCollections.find(pathId); it != BackupCollections.end()) {
                return it->second->AlterVersion;
            }
            break;
        case NKikimrSchemeOp::EPathTypeSysView:
            if (auto it = SysViews.find(pathId); it != SysViews.end()) {
                return it->second->AlterVersion;
            }
            break;
        case NKikimrSchemeOp::EPathTypeStreamingQuery:
            if (auto it = StreamingQueries.find(pathId); it != StreamingQueries.end()) {
                return it->second->AlterVersion;
            }
            break;
        case NKikimrSchemeOp::EPathTypeSecret:
            if (auto it = Secrets.find(pathId); it != Secrets.end()) {
                return it->second->AlterVersion;
            }
            break;
        case NKikimrSchemeOp::EPathTypeDir:
        case NKikimrSchemeOp::EPathTypeSubDomain:
        case NKikimrSchemeOp::EPathTypeExtSubDomain:
            if (SubDomains.contains(pathId)) {
                return SubDomains.at(pathId)->GetVersion();
            }
            return 0;
        case NKikimrSchemeOp::EPathTypeRtmrVolume:
            return 1;
        case NKikimrSchemeOp::EPathTypeInvalid:
            return 0;
    }
    return 0;
}

void TSchemeShard::Handle(TEvSchemeShard::TEvInternalReadSchemeChangeRecords::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxInternalReadSchemeChangeRecords(ev), ctx);
}

struct TTxInternalReadSchemeChangeRecords : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TEvSchemeShard::TEvInternalReadSchemeChangeRecords::TPtr Request;
    THolder<TEvSchemeShard::TEvInternalReadSchemeChangeRecordsResult> Result;

    TTxInternalReadSchemeChangeRecords(TSchemeShard* self, TEvSchemeShard::TEvInternalReadSchemeChangeRecords::TPtr& ev)
        : TTransactionBase(self)
        , Request(ev)
        , Result(MakeHolder<TEvSchemeShard::TEvInternalReadSchemeChangeRecordsResult>())
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);

        using NL = Schema::SchemeChangeRecords;
        auto rowset = db.Table<NL>().Range().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            TEvSchemeShard::TEvInternalReadSchemeChangeRecordsResult::TEntry entry;
            entry.SequenceId = rowset.GetValue<NL::SequenceId>();
            entry.TxId = rowset.GetValue<NL::TxId>();
            entry.OperationType = rowset.GetValue<NL::OperationType>();
            entry.PathOwnerId = rowset.GetValue<NL::PathOwnerId>();
            entry.PathLocalId = rowset.GetValue<NL::PathLocalId>();
            entry.PathName = rowset.GetValue<NL::PathName>();
            entry.ObjectType = rowset.GetValue<NL::ObjectType>();
            entry.Status = rowset.GetValue<NL::Status>();
            entry.UserSID = rowset.GetValue<NL::UserSID>();
            entry.SchemaVersion = rowset.GetValue<NL::SchemaVersion>();
            entry.CompletedAt = rowset.GetValue<NL::CompletedAt>();
            entry.PlanStep = rowset.GetValueOrDefault<NL::PlanStep>(0);
            Result->Entries.push_back(std::move(entry));

            if (!rowset.Next()) {
                return false;
            }
        }

        ui64 minInFlightPlanStep = 0;
        for (const auto& [opId, txState] : Self->TxInFlight) {
            if (txState.PlanStep != InvalidStepId
                && txState.State != TTxState::Done
                && txState.State != TTxState::Aborted) {
                ui64 ps = ui64(txState.PlanStep.GetValue());
                if (minInFlightPlanStep == 0 || ps < minInFlightPlanStep) {
                    minInFlightPlanStep = ps;
                }
            }
        }
        Result->MinInFlightPlanStep = minInFlightPlanStep;

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Request->Sender, Result.Release());
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxInternalReadSchemeChangeRecords(TEvSchemeShard::TEvInternalReadSchemeChangeRecords::TPtr& ev) {
    return new TTxInternalReadSchemeChangeRecords(this, ev);
}

} // namespace NKikimr::NSchemeShard
