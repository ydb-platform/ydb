#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#include <ydb/public/sdk/cpp/src/library/operation_id/protos/operation_id.pb.h>

namespace NKikimr::NSchemeShard {

// Storage adapter for TModifyScheme admission. Legacy RPC handlers provide
// their existing operation tables/indexes directly to TOperationUidAdmission.

TMaybe<TOperationUidRecord> TSchemeShard::FindSchemeOperationByUid(const TOperationUidKey& key) const {
    const auto* id = FindOperationByUid(SchemeOperationsByUid, key);
    if (!id) {
        return Nothing();
    }
    switch (key.first) {
        case Ydb::TOperationId::FULL_BACKUP: {
            const auto& info = *FullBackups.at(*id);
            return TOperationUidRecord{*id, {}, info.UserSID.GetOrElse(TString()), info.OriginalDdl};
        }
        case Ydb::TOperationId::INCREMENTAL_BACKUP: {
            const auto& info = *IncrementalBackups.at(*id);
            return TOperationUidRecord{*id, {}, info.UserSID.GetOrElse(TString()), info.OriginalDdl};
        }
        case Ydb::TOperationId::RESTORE: {
            const auto& info = IncrementalRestoreStates.at(*id);
            return TOperationUidRecord{*id, {}, info.UserSID, info.OriginalDdl};
        }
        default:
            Y_ABORT("Unsupported scheme operation UID storage kind");
    }
}

void TSchemeShard::BindSchemeOperationUid(const TOperationUidKey& key, ui64 id,
    const NKikimrSchemeOp::TModifyScheme& tx, const TString& userSID)
{
    const auto& ddl = tx.GetOperationIdempotency().GetOriginalDdl();
    switch (key.first) {
        case Ydb::TOperationId::FULL_BACKUP: {
            auto& info = *FullBackups.at(id);
            info.Uid = key.second;
            info.OriginalDdl = ddl;
            info.UserSID = userSID;
            break;
        }
        case Ydb::TOperationId::INCREMENTAL_BACKUP: {
            auto& info = *IncrementalBackups.at(id);
            info.Uid = key.second;
            info.OriginalDdl = ddl;
            info.UserSID = userSID;
            break;
        }
        case Ydb::TOperationId::RESTORE: {
            Y_ABORT_UNLESS(!IncrementalRestoreStates.contains(id));
            auto& info = IncrementalRestoreStates[id];
            info.Uid = key.second;
            info.OriginalDdl = ddl;
            info.UserSID = userSID;
            info.OriginalOperationId = id;
            const auto& name = tx.GetRestoreBackupCollection().GetName();
            const auto path = TPath::Resolve(name.StartsWith('/') ? name : tx.GetWorkingDir() + "/" + name, this);
            Y_ABORT_UNLESS(path.IsResolved());
            info.BackupCollectionPathId = path.Base()->PathId;
            info.AwaitingInitialRestore = true;
            break;
        }
        default:
            Y_ABORT("Unsupported scheme operation UID storage kind");
    }
    Y_ABORT_UNLESS(SchemeOperationsByUid.emplace(key, id).second);
}

void TSchemeShard::PersistSchemeOperationUidKey(NIceDb::TNiceDb& db, const TOperationUidKey& key) {
    const auto id = SchemeOperationsByUid.at(key);
    switch (key.first) {
        case Ydb::TOperationId::FULL_BACKUP: {
            const auto& info = *FullBackups.at(id);
            db.Table<Schema::FullBackups>().Key(id).Update(
                NIceDb::TUpdate<Schema::FullBackups::Uid>(info.Uid),
                NIceDb::TUpdate<Schema::FullBackups::OriginalDdl>(info.OriginalDdl));
            break;
        }
        case Ydb::TOperationId::INCREMENTAL_BACKUP: {
            const auto& info = *IncrementalBackups.at(id);
            db.Table<Schema::IncrementalBackups>().Key(id).Update(
                NIceDb::TUpdate<Schema::IncrementalBackups::Uid>(info.Uid),
                NIceDb::TUpdate<Schema::IncrementalBackups::OriginalDdl>(info.OriginalDdl));
            break;
        }
        case Ydb::TOperationId::RESTORE: {
            const auto& info = IncrementalRestoreStates.at(id);
            using T = Schema::IncrementalRestoreState;
            db.Table<T>().Key(id).Update(
                NIceDb::TUpdate<T::Uid>(info.Uid),
                NIceDb::TUpdate<T::OriginalDdl>(info.OriginalDdl),
                NIceDb::TUpdate<T::UserSID>(info.UserSID),
                NIceDb::TUpdate<T::BackupCollectionPathOwnerId>(info.BackupCollectionPathId.OwnerId),
                NIceDb::TUpdate<T::BackupCollectionPathId>(info.BackupCollectionPathId.LocalPathId),
                NIceDb::TUpdate<T::State>(static_cast<ui32>(info.State)),
                NIceDb::TUpdate<T::CurrentIncrementalIdx>(info.CurrentIncrementalIdx),
                NIceDb::TUpdate<T::AwaitingInitialRestore>(info.AwaitingInitialRestore));
            break;
        }
        default:
            Y_ABORT("Unsupported scheme operation UID storage kind");
    }
}

} // namespace NKikimr::NSchemeShard
