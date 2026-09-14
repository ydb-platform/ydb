#include "schemeshard_impl.h"

#include <ydb/public/api/protos/ydb_operation.pb.h>

namespace NKikimr::NSchemeShard {

TString GetUid(const Ydb::Operations::OperationParams& operationParams) {
    if (const auto* uid = FindOperationByUid(operationParams.labels(), "uid")) {
        return *uid;
    }
    return {};
}

EUidReplayMatch CompareOperationUid(const TOperationUidIdentity& stored, const TOperationUidIdentity& requested) {
    if (requested.UserSID && stored.UserSID != requested.UserSID) {
        return EUidReplayMatch::OwnerMismatch;
    }
    if (requested.DomainPathId && stored.DomainPathId != requested.DomainPathId) {
        return EUidReplayMatch::DomainMismatch;
    }
    if (requested.RequestBody && stored.RequestBody != requested.RequestBody) {
        return EUidReplayMatch::RequestMismatch;
    }
    return EUidReplayMatch::Match;
}


TMaybe<TBackupOperationReplay> TSchemeShard::FindBackupOperationByUid(const TBackupOperationUidKey& key) const {
    const auto* id = FindOperationByUid(BackupOperationsByUid, key);
    if (!id) {
        return Nothing();
    }
    switch (key.first) {
        case NKikimrSchemeOp::ESchemeOpBackupBackupCollection: {
            const auto& info = *FullBackups.at(*id);
            return TBackupOperationReplay{*id, info.OriginalDdl, info.UserSID.GetOrElse(TString())};
        }
        case NKikimrSchemeOp::ESchemeOpBackupIncrementalBackupCollection: {
            const auto& info = *IncrementalBackups.at(*id);
            return TBackupOperationReplay{*id, info.OriginalDdl, info.UserSID.GetOrElse(TString())};
        }
        case NKikimrSchemeOp::ESchemeOpRestoreBackupCollection: {
            const auto& info = IncrementalRestoreStates.at(*id);
            return TBackupOperationReplay{*id, info.OriginalDdl, info.UserSID};
        }
        default:
            Y_ABORT("Unexpected backup operation type");
    }
}

void TSchemeShard::BindBackupOperationUid(const TBackupOperationUidKey& key, ui64 id,
    const NKikimrSchemeOp::TModifyScheme& tx, const TString& userSID)
{
    const auto& ddl = tx.GetOperationIdempotency().GetOriginalDdl();
    switch (key.first) {
        case NKikimrSchemeOp::ESchemeOpBackupBackupCollection: {
            auto& info = *FullBackups.at(id);
            info.Uid = key.second;
            info.OriginalDdl = ddl;
            info.UserSID = userSID;
            break;
        }
        case NKikimrSchemeOp::ESchemeOpBackupIncrementalBackupCollection: {
            auto& info = *IncrementalBackups.at(id);
            info.Uid = key.second;
            info.OriginalDdl = ddl;
            info.UserSID = userSID;
            break;
        }
        case NKikimrSchemeOp::ESchemeOpRestoreBackupCollection: {
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
            Y_ABORT("Unexpected backup operation type");
    }
    Y_ABORT_UNLESS(BackupOperationsByUid.emplace(key, id).second);
}

void TSchemeShard::PersistBackupOperationUidKey(NIceDb::TNiceDb& db, const TBackupOperationUidKey& key) {
    const auto id = BackupOperationsByUid.at(key);
    switch (key.first) {
        case NKikimrSchemeOp::ESchemeOpBackupBackupCollection: {
            const auto& info = *FullBackups.at(id);
            db.Table<Schema::FullBackups>().Key(id).Update(
                NIceDb::TUpdate<Schema::FullBackups::Uid>(info.Uid),
                NIceDb::TUpdate<Schema::FullBackups::OriginalDdl>(info.OriginalDdl));
            break;
        }
        case NKikimrSchemeOp::ESchemeOpBackupIncrementalBackupCollection: {
            const auto& info = *IncrementalBackups.at(id);
            db.Table<Schema::IncrementalBackups>().Key(id).Update(
                NIceDb::TUpdate<Schema::IncrementalBackups::Uid>(info.Uid),
                NIceDb::TUpdate<Schema::IncrementalBackups::OriginalDdl>(info.OriginalDdl));
            break;
        }
        case NKikimrSchemeOp::ESchemeOpRestoreBackupCollection: {
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
            Y_ABORT("Unexpected backup operation type");
    }
}

} // namespace NKikimr::NSchemeShard
