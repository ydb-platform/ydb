#include "schemeshard_impl.h"
#include "schemeshard__local_index_migration.h"
#include "schemeshard_svp_migration.h"

#include "olap/bg_tasks/adapter/adapter.h"
#include "olap/bg_tasks/events/global.h"
#include "olap/operations/local_index_helpers.h"
#include "schemeshard.h"
#include "schemeshard__root_shred_manager.h"
#include "schemeshard__tenant_shred_manager.h"
#include "schemeshard_svp_migration.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/protos/schemeshard_config.pb.h>
#include <ydb/core/protos/table_stats.pb.h>  // for TStoragePoolsStats
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/sys_view/common/path.h>
#include <ydb/core/sys_view/common/resolver.h>
#include <ydb/core/sys_view/partition_stats/partition_stats.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tablet_flat/bloom_filter_defaults.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/test_tablet/events.h>
#include <ydb/core/tx/columnshard/bg_tasks/events/events.h>
#include <ydb/core/tx/scheme_board/events_schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>
#include <ydb/core/tx/schemeshard/schemeshard_sysviews_update.h>

#include <ydb/library/login/account_lockout/account_lockout.h>
#include <ydb/library/login/password_checker/password_checker.h>

#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <util/random/random.h>
#include <util/system/byteorder.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NSchemeShard {
bool TSchemeShard::CheckLocks(const TPathId pathId, const NKikimrSchemeOp::TModifyScheme &scheme, TString &errStr) const {
    if (scheme.HasLockGuard() && scheme.GetLockGuard().HasOwnerTxId()) {
        return CheckLocks(pathId, TTxId(scheme.GetLockGuard().GetOwnerTxId()), errStr);
    }

    return CheckLocks(pathId, InvalidTxId, errStr);
}

bool TSchemeShard::CheckLocks(const TPathId pathId, const TTxId lockTxId, TString& errStr) const {
    if (lockTxId == InvalidTxId) {
        // check lock is free
        if (LockedPaths.contains(pathId)) {
            auto explain = TStringBuilder()
                << "path '" << pathId << "'"
                << " has been locked by tx: " << LockedPaths.at(pathId);
            errStr.append(explain);
            return false;
        }

        return true;
    }

    // check lock is correct

    if (!LockedPaths.contains(pathId)) {
        auto explain = TStringBuilder()
            << "path '" << pathId << "'"
            << " hasn't been locked at all"
            << " but it is declared that it should be locked by tx: " << lockTxId;
        errStr.append(explain);
        return false;
    }

    if (LockedPaths.at(pathId) != lockTxId) {
        auto explain = TStringBuilder()
            << "path '" << pathId << "'"
            << " has been locked by tx: " << LockedPaths.at(pathId)
            << " but it is declared that it should be locked by: " << lockTxId;
        errStr.append(explain);
        return false;
    }

    return true;
}

bool TSchemeShard::CheckInFlightLimit(TTxState::ETxType txType, TString& errStr) const {
    auto it = InFlightLimits.find(txType);
    if (it == InFlightLimits.end()) {
        return true;
    }

    if (it->second != 0 && TabletCounters->Simple()[TxTypeInFlightCounter(txType)].Get() >= it->second) {
        errStr = TStringBuilder() << "the limit of operations with type " << TTxState::TypeName(txType)
            << " has been exceeded"
            << ", limit: " << it->second;
        return false;
    }

    return true;
}

bool TSchemeShard::CheckInFlightLimit(NKikimrSchemeOp::EOperationType opType, TString& errStr) const {
    if (const auto txType = ConvertToTxType(opType); txType != TTxState::TxInvalid) {
        return CheckInFlightLimit(txType, errStr);
    }

    return true;
}

bool TSchemeShard::CanCreateSnapshot(const TPathId& tablePathId, TTxId txId, NKikimrScheme::EStatus& status, TString& errStr) const {
    auto it = TablesWithSnapshots.find(tablePathId);
    if (it == TablesWithSnapshots.end()) {
        return true;
    }

    const auto& snapshotTxId = it->second;
    TStepId snapshotStepId;

    if (auto sit = SnapshotsStepIds.find(snapshotTxId); sit != SnapshotsStepIds.end()) {
        snapshotStepId = sit->second;
    }

    if (txId == snapshotTxId) {
        status = NKikimrScheme::StatusAlreadyExists;
        errStr = TStringBuilder()
            << "Snapshot with the same txId already presents for table"
            << ", tableId:" << tablePathId
            << ", txId: " << txId
            << ", snapshotTxId: " << snapshotTxId
            << ", snapshotStepId: " << snapshotStepId;
    } else {
        status = NKikimrScheme::StatusSchemeError;
        errStr = TStringBuilder()
            << "Snapshot with another txId already presents for table, only one snapshot is allowed for table for now"
            << ", tableId:" << tablePathId
            << ", txId: " << txId
            << ", snapshotTxId: " << snapshotTxId
            << ", snapshotStepId: " << snapshotStepId;
    }

    return false;
}

TShardIdx TSchemeShard::ReserveShardIdxs(ui64 count) {
    auto idx = TLocalShardIdx(NextLocalShardIdx);
    NextLocalShardIdx += count;
    return MakeLocalId(idx);
}

TShardIdx TSchemeShard::NextShardIdx(const TShardIdx& shardIdx, ui64 inc) const {
    Y_ABORT_UNLESS(shardIdx.GetOwnerId() == TabletID());

    ui64 nextLocalId = ui64(shardIdx.GetLocalId()) + inc;
    Y_VERIFY_S(nextLocalId < NextLocalShardIdx, "what: nextLocalId: " << nextLocalId << " NextLocalShardIdx: " << NextLocalShardIdx);

    return MakeLocalId(TLocalShardIdx(nextLocalId));
}

const TTableInfo* TSchemeShard::GetMainTableForIndex(TPathId indexTableId) const {
    if (!Tables.contains(indexTableId))
        return nullptr;

    auto pathEl = PathsById.FindPtr(indexTableId);
    if (!pathEl)
        return nullptr;

    TPathId parentId = (*pathEl)->ParentPathId;
    auto parentEl = PathsById.FindPtr(parentId);

    if (!parentEl || !(*parentEl)->IsTableIndex())
        return nullptr;

    TPathId grandParentId = (*parentEl)->ParentPathId;

    if (!Tables.contains(grandParentId))
        return nullptr;

    return Tables.FindPtr(grandParentId)->Get();
}

bool TSchemeShard::IsBackupTable(TPathId pathId) const {
    auto it = Tables.find(pathId);
    if (it == Tables.end()) {
        return false;
    }

    Y_ABORT_UNLESS(it->second);
    return it->second->IsBackup;
}

TPathElement::EPathState TSchemeShard::CalcPathState(TTxState::ETxType txType, TPathElement::EPathState oldState) {
    // Do not change state if PathId is dropped. It can't become alive.
    switch (oldState) {
    case TPathElement::EPathState::EPathStateNotExist:
    case TPathElement::EPathState::EPathStateDrop: // there could be multiple TXs, preserve StateDrop
        return oldState;
    case TPathElement::EPathState::EPathStateUpgrade:
        return oldState;
    default:
        break;
    }

    switch (txType) {
    case TTxState::TxMkDir:
    case TTxState::TxCreateTable:
    case TTxState::TxCopyTable:
    case TTxState::TxReadOnlyCopyColumnTable:
    case TTxState::TxCreatePQGroup:
    case TTxState::TxCreateSubDomain:
    case TTxState::TxCreateExtSubDomain:
    case TTxState::TxCreateBlockStoreVolume:
    case TTxState::TxCreateFileStore:
    case TTxState::TxCreateKesus:
    case TTxState::TxCreateSolomonVolume:
    case TTxState::TxCreateRtmrVolume:
    case TTxState::TxCreateTableIndex:
    case TTxState::TxCreateLocalIndex:
    case TTxState::TxCreateOlapStore:
    case TTxState::TxCreateColumnTable:
    case TTxState::TxCreateCdcStream:
    case TTxState::TxCreateSequence:
    case TTxState::TxCopySequence:
    case TTxState::TxCreateReplication:
    case TTxState::TxCreateTransfer:
    case TTxState::TxCreateBlobDepot:
    case TTxState::TxCreateExternalTable:
    case TTxState::TxCreateExternalDataSource:
    case TTxState::TxCreateView:
    case TTxState::TxCreateContinuousBackup:
    case TTxState::TxCreateResourcePool:
    case TTxState::TxCreateBackupCollection:
    case TTxState::TxCreateSysView:
    case TTxState::TxCreateLongIncrementalBackupOp:
    case TTxState::TxCreateSecret:
    case TTxState::TxCreateStreamingQuery:
    case TTxState::TxCreateTestShardSet:
        return TPathElement::EPathState::EPathStateCreate;
    case TTxState::TxAlterPQGroup:
    case TTxState::TxAlterTable:
    case TTxState::TxAlterBlockStoreVolume:
    case TTxState::TxAlterFileStore:
    case TTxState::TxAlterKesus:
    case TTxState::TxAlterSubDomain:
    case TTxState::TxAlterExtSubDomain:
    case TTxState::TxAlterExtSubDomainCreateHive:
    case TTxState::TxAlterUserAttributes:
    case TTxState::TxInitializeBuildIndex:
    case TTxState::TxPrepareIndexValidation:
    case TTxState::TxFinalizeBuildIndex:
    case TTxState::TxCreateLock:
    case TTxState::TxDropLock:
    case TTxState::TxAlterTableIndex:
    case TTxState::TxAlterLocalIndex:
    case TTxState::TxAlterSolomonVolume:
    case TTxState::TxDropTableIndexAtMainTable:
    case TTxState::TxAlterOlapStore:
    case TTxState::TxAlterColumnTable:
    case TTxState::TxAlterCdcStream:
    case TTxState::TxAlterCdcStreamAtTable:
    case TTxState::TxAlterCdcStreamAtTableDropSnapshot:
    case TTxState::TxCreateCdcStreamAtTable:
    case TTxState::TxCreateCdcStreamAtTableWithInitialScan:
    case TTxState::TxDropCdcStreamAtTable:
    case TTxState::TxDropCdcStreamAtTableDropSnapshot:
    case TTxState::TxRotateCdcStreamAtTable:
    case TTxState::TxAlterSequence:
    case TTxState::TxAlterReplication:
    case TTxState::TxAlterTransfer:
    case TTxState::TxAlterBlobDepot:
    case TTxState::TxUpdateMainTableOnIndexMove:
    case TTxState::TxAlterExternalTable:
    case TTxState::TxAlterExternalDataSource:
    case TTxState::TxAlterView:
    case TTxState::TxAlterContinuousBackup:
    case TTxState::TxAlterResourcePool:
    case TTxState::TxAlterBackupCollection:
    case TTxState::TxChangePathState:
    case TTxState::TxAlterSecret:
    case TTxState::TxAlterStreamingQuery:
    case TTxState::TxTruncateTable:
        return TPathElement::EPathState::EPathStateAlter;
    case TTxState::TxDropTable:
    case TTxState::TxDropPQGroup:
    case TTxState::TxRmDir:
    case TTxState::TxDropSubDomain:
    case TTxState::TxForceDropSubDomain:
    case TTxState::TxForceDropExtSubDomain:
    case TTxState::TxDropBlockStoreVolume:
    case TTxState::TxDropFileStore:
    case TTxState::TxDropKesus:
    case TTxState::TxDropSolomonVolume:
    case TTxState::TxDropTableIndex:
    case TTxState::TxDropLocalIndex:
    case TTxState::TxDropOlapStore:
    case TTxState::TxDropColumnTable:
    case TTxState::TxDropCdcStream:
    case TTxState::TxDropSequence:
    case TTxState::TxDropReplication:
    case TTxState::TxDropReplicationCascade:
    case TTxState::TxDropTransfer:
    case TTxState::TxDropTransferCascade:
    case TTxState::TxDropBlobDepot:
    case TTxState::TxDropExternalTable:
    case TTxState::TxDropExternalDataSource:
    case TTxState::TxDropView:
    case TTxState::TxDropContinuousBackup:
    case TTxState::TxDropResourcePool:
    case TTxState::TxDropBackupCollection:
    case TTxState::TxDropSysView:
    case TTxState::TxDropSecret:
    case TTxState::TxDropStreamingQuery:
    case TTxState::TxDropTestShardSet:
        return TPathElement::EPathState::EPathStateDrop;
    case TTxState::TxBackup:
        return TPathElement::EPathState::EPathStateBackup;
    case TTxState::TxRestore:
        return TPathElement::EPathState::EPathStateRestore;
    case TTxState::TxUpgradeSubDomain:
        return TPathElement::EPathState::EPathStateUpgrade;
    case TTxState::TxUpgradeSubDomainDecision:
        return TPathElement::EPathState::EPathStateAlter; // if only TxUpgradeSubDomainDecision hangs under path it is considered just as Alter
    case TTxState::TxSplitTablePartition:
    case TTxState::TxMergeTablePartition:
        break;
    case TTxState::TxFillIndex:
    case TTxState::TxAllocatePQ:
        Y_ABORT("deprecated");
    case TTxState::TxModifyACL:
    case TTxState::TxInvalid:
    case TTxState::TxAssignBlockStoreVolume:
        Y_UNREACHABLE();
    case TTxState::TxMoveTable:
    case TTxState::TxMoveTableIndex:
    case TTxState::TxMoveLocalIndex:
    case TTxState::TxMoveSequence:
    case TTxState::TxRotateCdcStream:
        return TPathElement::EPathState::EPathStateCreate;
    case TTxState::TxRestoreIncrementalBackupAtTable:
    case TTxState::TxCreateLongIncrementalRestoreOp: // Set this state for now, maybe we need to be more precise
        return TPathElement::EPathState::EPathStateOutgoingIncrementalRestore;
    case TTxState::TxIncrementalRestoreFinalize:
        return TPathElement::EPathState::EPathStateAlter; // Finalization is an alter operation to normalize path states
    case TTxState::TxCreateFullBackupOp:
        // The control op must not flip the backup-collection path to
        // EPathStateCreate; concurrent-backup exclusion is enforced via
        // BCPathToFullBackup, not path-state.
        return oldState;
    }
    return oldState;
}

bool TSchemeShard::TRwTxBase::Execute(NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx) {
    THPTimer cpuTimer;

    // Transactions don't read anything from the DB, they all use in-mem structures and do writes to the DB
    // That's why transactions should never be retried
    txc.DB.NoMoreReadsForTx();

    try {
        DoExecute(txc, ctx);
    } catch (const std::exception& ex) {
        Y_FAIL_S("there must be no leaked exceptions: " << ex.what() << ", at schemeshard: " << Self->TabletID());
    } catch (...) {
        Y_FAIL_S("there must be no leaked exceptions, at schemeshard: " << Self->TabletID());
    }

    ExecuteDuration = TDuration::Seconds(cpuTimer.Passed());
    return true;
}

void TSchemeShard::TRwTxBase::Complete(const TActorContext &ctx) {
    DoComplete(ctx);
}

void TSchemeShard::BumpIncompatibleChanges(NIceDb::TNiceDb& db, ui64 incompatibleChange) {
    if (MaxIncompatibleChange < incompatibleChange) {
        Y_VERIFY_S(incompatibleChange <= Schema::MaxIncompatibleChangeSupported,
            "Attempting to bump incompatible changes to " << incompatibleChange <<
            ", but maximum supported change is " << Schema::MaxIncompatibleChangeSupported);
        // We add a special path on the first incompatible change, which breaks
        // all versions that don't know about incompatible changes. Newer
        // versions will just skip this non-sensible entry.
        if (MaxIncompatibleChange == 0) {
            db.Table<Schema::Paths>().Key(0).Update(
                NIceDb::TUpdate<Schema::Paths::ParentId>(0),
                NIceDb::TUpdate<Schema::Paths::Name>("/incompatible/"));
        }
        // Persist a new maximum incompatible change, this will cause older
        // versions to stop gracefully instead of working inconsistently.
        db.Table<Schema::SysParams>().Key(Schema::SysParam_MaxIncompatibleChange).Update(
            NIceDb::TUpdate<Schema::SysParams::Value>(ToString(incompatibleChange)));
        MaxIncompatibleChange = incompatibleChange;
    }
}

void TSchemeShard::PersistTableIndex(NIceDb::TNiceDb& db, const TPathId& pathId) {
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    TPathElement::TPtr element = PathsById.at(pathId);

    Y_ABORT_UNLESS(Indexes.contains(pathId));
    TTableIndexInfo::TPtr index = Indexes.at(pathId);

    Y_ABORT_UNLESS(IsLocalId(element->PathId));
    Y_ABORT_UNLESS(element->IsTableIndex());

    TTableIndexInfo::TPtr alterData = index->AlterData;
    Y_ABORT_UNLESS(alterData);
    // Relaxed to <= to allow convergence when multiple operations use CoordinatedSchemaVersion
    Y_ABORT_UNLESS(index->AlterVersion <= alterData->AlterVersion);

    db.Table<Schema::TableIndex>().Key(element->PathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::TableIndex::AlterVersion>(alterData->AlterVersion),
                NIceDb::TUpdate<Schema::TableIndex::IndexType>(alterData->Type),
                NIceDb::TUpdate<Schema::TableIndex::State>(alterData->State),
                NIceDb::TUpdate<Schema::TableIndex::Description>(alterData->SerializeDescription()));

    db.Table<Schema::TableIndexAlterData>().Key(element->PathId.LocalPathId).Delete();

    for (ui32 keyIdx = 0; keyIdx < alterData->IndexKeys.size(); ++keyIdx) {
        db.Table<Schema::TableIndexKeys>().Key(element->PathId.LocalPathId, keyIdx).Update(
                    NIceDb::TUpdate<Schema::TableIndexKeys::KeyName>(alterData->IndexKeys[keyIdx]));

        db.Table<Schema::TableIndexKeysAlterData>().Key(element->PathId.LocalPathId, keyIdx).Delete();
    }

    for (ui32 dataColIdx = 0; dataColIdx < alterData->IndexDataColumns.size(); ++dataColIdx) {
        db.Table<Schema::TableIndexDataColumns>().Key(element->PathId.OwnerId, element->PathId.LocalPathId, dataColIdx).Update(
                    NIceDb::TUpdate<Schema::TableIndexDataColumns::DataColumnName>(alterData->IndexDataColumns[dataColIdx]));

        db.Table<Schema::TableIndexDataColumnsAlterData>().Key(element->PathId.OwnerId, element->PathId.LocalPathId, dataColIdx).Delete();
    }
}

void TSchemeShard::PersistTableIndexAlterData(NIceDb::TNiceDb& db, const TPathId& pathId) {
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    TPathElement::TPtr elem = PathsById.at(pathId);

    Y_ABORT_UNLESS(Indexes.contains(pathId));
    TTableIndexInfo::TPtr index = Indexes.at(pathId);

    Y_ABORT_UNLESS(IsLocalId(pathId));
    Y_ABORT_UNLESS(elem->IsTableIndex());

    TTableIndexInfo::TPtr alterData = index->AlterData;
    Y_ABORT_UNLESS(alterData);

    db.Table<Schema::TableIndexAlterData>().Key(elem->PathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::TableIndexAlterData::AlterVersion>(alterData->AlterVersion),
                NIceDb::TUpdate<Schema::TableIndexAlterData::IndexType>(alterData->Type),
                NIceDb::TUpdate<Schema::TableIndexAlterData::State>(alterData->State),
                NIceDb::TUpdate<Schema::TableIndexAlterData::Description>(alterData->SerializeDescription()));

    for (ui32 keyIdx = 0; keyIdx < alterData->IndexKeys.size(); ++keyIdx) {
        db.Table<Schema::TableIndexKeysAlterData>().Key(elem->PathId.LocalPathId, keyIdx).Update(
                    NIceDb::TUpdate<Schema::TableIndexKeysAlterData::KeyName>(alterData->IndexKeys[keyIdx]));
    }

    for (ui32 dataColIdx = 0; dataColIdx < alterData->IndexDataColumns.size(); ++dataColIdx) {
        db.Table<Schema::TableIndexDataColumnsAlterData>().Key(elem->PathId.OwnerId, elem->PathId.LocalPathId, dataColIdx).Update(
                    NIceDb::TUpdate<Schema::TableIndexDataColumnsAlterData::DataColumnName>(alterData->IndexDataColumns[dataColIdx]));
    }
}

void TSchemeShard::PersistTableIndexAlterVersion(NIceDb::TNiceDb& db, const TPathId& pathId, const TTableIndexInfo::TPtr indexInfo) {
    if (IsLocalId(pathId)) {
        db.Table<Schema::TableIndex>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::TableIndex::AlterVersion>(indexInfo->AlterVersion)
        );
    } else {
        db.Table<Schema::MigratedTableIndex>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedTableIndex::AlterVersion>(indexInfo->AlterVersion)
        );
    }
}

void TSchemeShard::PersistCdcStream(NIceDb::TNiceDb& db, const TPathId& pathId) {
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    auto path = PathsById.at(pathId);

    Y_ABORT_UNLESS(CdcStreams.contains(pathId));
    auto stream = CdcStreams.at(pathId);

    Y_ABORT_UNLESS(IsLocalId(pathId));
    Y_ABORT_UNLESS(path->IsCdcStream());

    auto alterData = stream->AlterData;
    Y_ABORT_UNLESS(alterData);
    Y_ABORT_UNLESS(stream->AlterVersion < alterData->AlterVersion);

    db.Table<Schema::CdcStream>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::CdcStream::AlterVersion>(alterData->AlterVersion),
        NIceDb::TUpdate<Schema::CdcStream::Mode>(alterData->Mode),
        NIceDb::TUpdate<Schema::CdcStream::Format>(alterData->Format),
        NIceDb::TUpdate<Schema::CdcStream::VirtualTimestamps>(alterData->VirtualTimestamps),
        NIceDb::TUpdate<Schema::CdcStream::ResolvedTimestampsIntervalMs>(alterData->ResolvedTimestamps.MilliSeconds()),
        NIceDb::TUpdate<Schema::CdcStream::SchemaChanges>(alterData->SchemaChanges),
        NIceDb::TUpdate<Schema::CdcStream::AwsRegion>(alterData->AwsRegion),
        NIceDb::TUpdate<Schema::CdcStream::State>(alterData->State),
        NIceDb::TUpdate<Schema::CdcStream::UserSIDs>(alterData->UserSIDs),
        NIceDb::TUpdate<Schema::CdcStream::TraceIds>(alterData->TraceIds)
    );

    db.Table<Schema::CdcStreamAlterData>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistCdcStreamAlterData(NIceDb::TNiceDb& db, const TPathId& pathId) {
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    auto path = PathsById.at(pathId);

    Y_ABORT_UNLESS(CdcStreams.contains(pathId));
    auto stream = CdcStreams.at(pathId);

    Y_ABORT_UNLESS(IsLocalId(pathId));
    Y_ABORT_UNLESS(path->IsCdcStream());

    auto alterData = stream->AlterData;
    Y_ABORT_UNLESS(alterData);

    db.Table<Schema::CdcStreamAlterData>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::CdcStreamAlterData::AlterVersion>(alterData->AlterVersion),
        NIceDb::TUpdate<Schema::CdcStreamAlterData::Mode>(alterData->Mode),
        NIceDb::TUpdate<Schema::CdcStreamAlterData::Format>(alterData->Format),
        NIceDb::TUpdate<Schema::CdcStreamAlterData::VirtualTimestamps>(alterData->VirtualTimestamps),
        NIceDb::TUpdate<Schema::CdcStreamAlterData::ResolvedTimestampsIntervalMs>(alterData->ResolvedTimestamps.MilliSeconds()),
        NIceDb::TUpdate<Schema::CdcStreamAlterData::SchemaChanges>(alterData->SchemaChanges),
        NIceDb::TUpdate<Schema::CdcStreamAlterData::AwsRegion>(alterData->AwsRegion),
        NIceDb::TUpdate<Schema::CdcStreamAlterData::State>(alterData->State),
        NIceDb::TUpdate<Schema::CdcStreamAlterData::UserSIDs>(alterData->UserSIDs),
        NIceDb::TUpdate<Schema::CdcStreamAlterData::TraceIds>(alterData->TraceIds)
    );
}

void TSchemeShard::PersistRemoveCdcStream(NIceDb::TNiceDb &db, const TPathId& pathId) {
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    auto path = PathsById.at(pathId);

    if (!CdcStreams.contains(pathId)) {
        return;
    }

    auto stream = CdcStreams.at(pathId);
    if (stream->AlterData) {
        db.Table<Schema::CdcStreamAlterData>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
    }

    db.Table<Schema::CdcStream>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();

    for (const auto& [shardIdx, _] : stream->ScanShards) {
        RemoveCdcStreamScanShardStatus(db, pathId, shardIdx);
    }

    CdcStreams.erase(pathId);
    DecrementPathDbRefCount(pathId);
}

void TSchemeShard::PersistAlterUserAttributes(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    TPathElement::TPtr element = PathsById.at(pathId);

    if (!element->UserAttrs->AlterData) {
        return;
    }

    for (auto& item: element->UserAttrs->AlterData->Attrs) {
        const auto& name = item.first;
        const auto& value = item.second;
        if (pathId.OwnerId == TabletID()) {
            db.Table<Schema::UserAttributesAlterData>().Key(pathId.LocalPathId, name).Update(
                    NIceDb::TUpdate<Schema::UserAttributesAlterData::AttrValue>(value));
        } else {
            db.Table<Schema::MigratedUserAttributesAlterData>().Key(pathId.OwnerId, pathId.LocalPathId, name).Update(
                NIceDb::TUpdate<Schema::MigratedUserAttributesAlterData::AttrValue>(value));
        }
    }
}

void TSchemeShard::ApplyAndPersistUserAttrs(NIceDb::TNiceDb& db, const TPathId& pathId) {
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    TPathElement::TPtr element = PathsById.at(pathId);
    Y_ABORT_UNLESS(element->UserAttrs);

    if (!element->UserAttrs->AlterData) {
        return;
    }

    TabletCounters->Simple()[COUNTER_USER_ATTRIBUTES_COUNT].Add(element->UserAttrs->AlterData->Size());
    TabletCounters->Simple()[COUNTER_USER_ATTRIBUTES_COUNT].Sub(element->UserAttrs->Size());

    PersistUserAttributes(db, pathId, element->UserAttrs, element->UserAttrs->AlterData);

    element->UserAttrs = element->UserAttrs->AlterData;
    element->UserAttrs->AlterData.Reset();
    element->ApplySpecialAttributes();
}


void TSchemeShard::PersistUserAttributes(NIceDb::TNiceDb& db, TPathId pathId,
                                             TUserAttributes::TPtr oldAttrs, TUserAttributes::TPtr alterAttrs) {
    //remove old version
    if (oldAttrs) {
        for (auto& item: oldAttrs->Attrs) {
            const auto& name = item.first;
            if (pathId.OwnerId == TabletID()) {
                db.Table<Schema::UserAttributes>().Key(pathId.LocalPathId, name).Delete();
            }

            db.Table<Schema::MigratedUserAttributes>().Key(pathId.OwnerId, pathId.LocalPathId, name).Delete();
        }
    }
    //apply new version and clear UserAttributesAlterData
    if (!alterAttrs) {
        return;
    }
    for (auto& item: alterAttrs->Attrs) {
        const auto& name = item.first;
        const auto& value = item.second;
        if (pathId.OwnerId == TabletID()) {
            db.Table<Schema::UserAttributes>().Key(pathId.LocalPathId, name).Update(
                    NIceDb::TUpdate<Schema::UserAttributes::AttrValue>(value));

            db.Table<Schema::UserAttributesAlterData>().Key(pathId.LocalPathId, name).Delete();
        } else {
            db.Table<Schema::MigratedUserAttributes>().Key(pathId.OwnerId, pathId.LocalPathId, name).Update(
                NIceDb::TUpdate<Schema::MigratedUserAttributes::AttrValue>(value));
        }

        db.Table<Schema::MigratedUserAttributesAlterData>().Key(pathId.OwnerId, pathId.LocalPathId, name).Delete();
    }

    if (pathId.OwnerId == TabletID()) {
        //update UserAttrs's AlterVersion in Paths table
        db.Table<Schema::Paths>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::Paths::UserAttrsAlterVersion>(alterAttrs->AlterVersion));
    } else {
        db.Table<Schema::MigratedPaths>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedPaths::UserAttrsAlterVersion>(alterAttrs->AlterVersion));
    }
}


void TSchemeShard::PersistLastTxId(NIceDb::TNiceDb& db, const TPathElement::TPtr path) {
    if (path->PathId.OwnerId == TabletID()) {
        db.Table<Schema::Paths>().Key(path->PathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::Paths::LastTxId>(path->LastTxId));
    } else {
        db.Table<Schema::MigratedPaths>().Key(path->PathId.OwnerId, path->PathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::MigratedPaths::LastTxId>(path->LastTxId));
    }
}

void TSchemeShard::PersistPath(NIceDb::TNiceDb& db, const TPathId& pathId) {
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    TPathElement::TPtr elem = PathsById.at(pathId);
    if (IsLocalId(pathId)) {
        db.Table<Schema::Paths>().Key(pathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::Paths::ParentOwnerId>(elem->ParentPathId.OwnerId),
                    NIceDb::TUpdate<Schema::Paths::ParentId>(elem->ParentPathId.LocalPathId),
                    NIceDb::TUpdate<Schema::Paths::Name>(elem->Name),
                    NIceDb::TUpdate<Schema::Paths::PathType>(elem->PathType),
                    NIceDb::TUpdate<Schema::Paths::StepCreated>(elem->StepCreated),
                    NIceDb::TUpdate<Schema::Paths::CreateTxId>(elem->CreateTxId),
                    NIceDb::TUpdate<Schema::Paths::StepDropped>(elem->StepDropped),
                    NIceDb::TUpdate<Schema::Paths::DropTxId>(elem->DropTxId),
                    NIceDb::TUpdate<Schema::Paths::Owner>(elem->Owner),
                    NIceDb::TUpdate<Schema::Paths::ACL>(elem->ACL),
                    NIceDb::TUpdate<Schema::Paths::LastTxId>(elem->LastTxId),
                    NIceDb::TUpdate<Schema::Paths::DirAlterVersion>(elem->DirAlterVersion),
                    NIceDb::TUpdate<Schema::Paths::UserAttrsAlterVersion>(elem->UserAttrs->AlterVersion),
                    NIceDb::TUpdate<Schema::Paths::ACLVersion>(elem->ACLVersion),
                    NIceDb::TUpdate<Schema::Paths::TempDirOwnerActorIdRaw>(elem->TempDirOwnerActorId)
                    );
    } else {
        db.Table<Schema::MigratedPaths>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::MigratedPaths::ParentOwnerId>(elem->ParentPathId.OwnerId),
                    NIceDb::TUpdate<Schema::MigratedPaths::ParentLocalId>(elem->ParentPathId.LocalPathId),
                    NIceDb::TUpdate<Schema::MigratedPaths::Name>(elem->Name),
                    NIceDb::TUpdate<Schema::MigratedPaths::PathType>(elem->PathType),
                    NIceDb::TUpdate<Schema::MigratedPaths::StepCreated>(elem->StepCreated),
                    NIceDb::TUpdate<Schema::MigratedPaths::CreateTxId>(elem->CreateTxId),
                    NIceDb::TUpdate<Schema::MigratedPaths::StepDropped>(elem->StepDropped),
                    NIceDb::TUpdate<Schema::MigratedPaths::DropTxId>(elem->DropTxId),
                    NIceDb::TUpdate<Schema::MigratedPaths::Owner>(elem->Owner),
                    NIceDb::TUpdate<Schema::MigratedPaths::ACL>(elem->ACL),
                    NIceDb::TUpdate<Schema::MigratedPaths::LastTxId>(elem->LastTxId),
                    NIceDb::TUpdate<Schema::MigratedPaths::DirAlterVersion>(elem->DirAlterVersion),
                    NIceDb::TUpdate<Schema::MigratedPaths::UserAttrsAlterVersion>(elem->UserAttrs->AlterVersion),
                    NIceDb::TUpdate<Schema::MigratedPaths::ACLVersion>(elem->ACLVersion),
                    NIceDb::TUpdate<Schema::MigratedPaths::TempDirOwnerActorIdRaw>(elem->TempDirOwnerActorId)
                    );
    }
}

void TSchemeShard::PersistRemovePath(NIceDb::TNiceDb& db, const TPathElement::TPtr path) {
    Y_ABORT_UNLESS(path->Dropped() && path->DbRefCount == 0);

    // Make sure to cleanup any leftover user attributes for this path
    for (auto& item : path->UserAttrs->Attrs) {
        const auto& name = item.first;
        if (IsLocalId(path->PathId)) {
            db.Table<Schema::UserAttributes>().Key(path->PathId.LocalPathId, name).Delete();
        } else {
            db.Table<Schema::MigratedUserAttributes>().Key(path->PathId.OwnerId, path->PathId.LocalPathId, name).Delete();
        }
    }

    if (IsLocalId(path->PathId)) {
        db.Table<Schema::Paths>().Key(path->PathId.LocalPathId).Delete();
    } else {
        db.Table<Schema::MigratedPaths>().Key(path->PathId.OwnerId, path->PathId.LocalPathId).Delete();
    }
    PathsById.erase(path->PathId);

    auto itParent = PathsById.find(path->ParentPathId);
    Y_DEBUG_ABORT_UNLESS(itParent != PathsById.end());
    if (itParent != PathsById.end()) {
        itParent->second->RemoveChild(path->Name, path->PathId);
        // placeholders are never attached to their parent and never counted
        if (!path->IsOrphanPlaceholder) {
            Y_ABORT_UNLESS(itParent->second->AllChildrenCount > 0);
            --itParent->second->AllChildrenCount;
            DecrementPathDbRefCount(path->ParentPathId, "remove path");
        }
    }
}

void TSchemeShard::PersistPathDirAlterVersion(NIceDb::TNiceDb& db, const TPathElement::TPtr path) {
    if (path->PathId.OwnerId == TabletID()) {
        db.Table<Schema::Paths>().Key(path->PathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::Paths::DirAlterVersion>(path->DirAlterVersion));
    } else {
        db.Table<Schema::MigratedPaths>().Key(path->PathId.OwnerId, path->PathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedPaths::DirAlterVersion>(path->DirAlterVersion));
    }
}

template <typename PersistentTable>
void PersistSchemeLimitsImpl(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    const auto& limits = subDomain.GetSchemeLimits();

    db.Table<PersistentTable>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<typename PersistentTable::DepthLimit>(limits.MaxDepth),
        NIceDb::TUpdate<typename PersistentTable::PathsLimit>(limits.MaxPaths),
        NIceDb::TUpdate<typename PersistentTable::ChildrenLimit>(limits.MaxChildrenInDir),
        NIceDb::TUpdate<typename PersistentTable::ShardsLimit>(limits.MaxShards),
        NIceDb::TUpdate<typename PersistentTable::PathShardsLimit>(limits.MaxShardsInPath),
        NIceDb::TUpdate<typename PersistentTable::TableColumnsLimit>(limits.MaxTableColumns),
        NIceDb::TUpdate<typename PersistentTable::TableColumnNameLengthLimit>(limits.MaxTableColumnNameLength),
        NIceDb::TUpdate<typename PersistentTable::TableKeyColumnsLimit>(limits.MaxTableKeyColumns),
        NIceDb::TUpdate<typename PersistentTable::TableIndicesLimit>(limits.MaxTableIndices),
        NIceDb::TUpdate<typename PersistentTable::AclByteSizeLimit>(limits.MaxAclBytesSize),
        NIceDb::TUpdate<typename PersistentTable::ConsistentCopyingTargetsLimit>(limits.MaxConsistentCopyTargets),
        NIceDb::TUpdate<typename PersistentTable::PathElementLength>(limits.MaxPathElementLength),
        NIceDb::TUpdate<typename PersistentTable::ExtraPathSymbolsAllowed>(limits.ExtraPathSymbolsAllowed),
        NIceDb::TUpdate<typename PersistentTable::PQPartitionsLimit>(limits.MaxPQPartitions),
        NIceDb::TUpdate<typename PersistentTable::TableCdcStreamsLimit>(limits.MaxTableCdcStreams),
        NIceDb::TUpdate<typename PersistentTable::ExportsLimit>(limits.MaxExports),
        NIceDb::TUpdate<typename PersistentTable::ImportsLimit>(limits.MaxImports),
        NIceDb::TUpdate<typename PersistentTable::ColumnTableColumnsLimit>(limits.MaxColumnTableColumns)
    );
}

void TSchemeShard::PersistSchemeLimits(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));
    PersistSchemeLimitsImpl<Schema::SubDomains>(db, pathId, subDomain);
}

void TSchemeShard::PersistSubDomainSchemeLimitsAlter(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));
    PersistSchemeLimitsImpl<Schema::SubDomainsAlterData>(db, pathId, subDomain);
}

void TSchemeShard::PersistStoragePools(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    for (auto pool: subDomain.GetStoragePools()) {
        db.Table<Schema::StoragePools>().Key(pathId.LocalPathId, pool.GetName(), pool.GetKind()).Update();
        db.Table<Schema::StoragePoolsAlterData>().Key(pathId.LocalPathId, pool.GetName(), pool.GetKind()).Delete();
    }
    db.Table<Schema::SubDomains>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::SubDomains::AlterVersion>(subDomain.GetVersion()));
}

void TSchemeShard::PersistInitState(NIceDb::TNiceDb& db) {
    db.Table<Schema::SysParams>().Key(Schema::SysParam_TenantInitState).Update(
        NIceDb::TUpdate<Schema::SysParams::Value>(ToString((ui64)InitState)));
}

void TSchemeShard::PersistStorageBillingTime(NIceDb::TNiceDb &db) {
    db.Table<Schema::SysParams>().Key(Schema::SysParam_ServerlessStorageLastBillTime).Update(
        NIceDb::TUpdate<Schema::SysParams::Value>(ToString(this->ServerlessStorageLastBillTime.Seconds())));
}

void TSchemeShard::PersistSubDomainAlter(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::SubDomainsAlterData>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::SubDomainsAlterData::AlterVersion>(subDomain.GetVersion()),
                NIceDb::TUpdate<Schema::SubDomainsAlterData::PlanResolution>(subDomain.GetPlanResolution()),
                NIceDb::TUpdate<Schema::SubDomainsAlterData::TimeCastBuckets>(subDomain.GetTCB()),
                NIceDb::TUpdate<Schema::SubDomainsAlterData::ResourcesDomainOwnerPathId>(subDomain.GetResourcesDomainId().OwnerId),
                NIceDb::TUpdate<Schema::SubDomainsAlterData::ResourcesDomainLocalPathId>(subDomain.GetResourcesDomainId().LocalPathId),
                NIceDb::TUpdate<Schema::SubDomainsAlterData::SharedHiveId>(subDomain.GetSharedHive()));

    if (subDomain.GetDeclaredSchemeQuotas()) {
        TString declaredSchemeQuotas;
        Y_ABORT_UNLESS(subDomain.GetDeclaredSchemeQuotas()->SerializeToString(&declaredSchemeQuotas));
        db.Table<Schema::SubDomainsAlterData>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::SubDomainsAlterData::DeclaredSchemeQuotas>(declaredSchemeQuotas));
    } else {
        db.Table<Schema::SubDomainsAlterData>().Key(pathId.LocalPathId).Update(
                NIceDb::TNull<Schema::SubDomainsAlterData::DeclaredSchemeQuotas>());
    }

    if (const auto& databaseQuotas = subDomain.GetDatabaseQuotas()) {
        TString serialized;
        Y_ABORT_UNLESS(databaseQuotas->SerializeToString(&serialized));
        db.Table<Schema::SubDomainsAlterData>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::SubDomainsAlterData::DatabaseQuotas>(serialized));
    } else {
        db.Table<Schema::SubDomainsAlterData>().Key(pathId.LocalPathId).Update(
                NIceDb::TNull<Schema::SubDomainsAlterData::DatabaseQuotas>());
    }

    if (subDomain.GetTenantSchemeShardID()) {
        // Scheme limits protect SchemeShard tablets from overload.
        // Regular (non-external) subdomains use the root SchemeShard and its limits,
        // so they don't need to persist their scheme limits.
        PersistSubDomainSchemeLimitsAlter(db, pathId, subDomain);
    }
    PersistSubDomainAuditSettingsAlter(db, pathId, subDomain);
    PersistSubDomainServerlessComputeResourcesModeAlter(db, pathId, subDomain);

    for (auto shardIdx: subDomain.GetPrivateShards()) {
        db.Table<Schema::SubDomainShardsAlterData>().Key(pathId.LocalPathId, shardIdx.GetLocalId()).Update();
    }

    for (auto pool: subDomain.GetStoragePools()) {
        db.Table<Schema::StoragePoolsAlterData>().Key(pathId.LocalPathId, pool.GetName(), pool.GetKind()).Update();
    }
}

void TSchemeShard::PersistDeleteSubDomainAlter(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& alterDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::SubDomainsAlterData>().Key(pathId.LocalPathId).Delete();

    for (auto shardIdx: alterDomain.GetPrivateShards()) {
        db.Table<Schema::SubDomainShardsAlterData>().Key(pathId.LocalPathId, shardIdx.GetLocalId()).Delete();
    }

    for (auto pool: alterDomain.GetStoragePools()) {
        db.Table<Schema::StoragePoolsAlterData>().Key(pathId.LocalPathId, pool.GetName(), pool.GetKind()).Delete();
    }
}

void TSchemeShard::PersistSubDomainVersion(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::SubDomains>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::SubDomains::AlterVersion>(subDomain.GetVersion()));
}

void TSchemeShard::PersistSubDomainSecurityStateVersion(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::SubDomains>()
        .Key(pathId.LocalPathId)
        .Update<Schema::SubDomains::SecurityStateVersion>(subDomain.GetSecurityStateVersion());
}

void TSchemeShard::PersistSubDomainPrivateShards(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    for (auto shardIdx: subDomain.GetPrivateShards()) {
        db.Table<Schema::SubDomainShards>().Key(pathId.LocalPathId, shardIdx.GetLocalId()).Update();
    }
}

void TSchemeShard::PersistSubDomain(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::SubDomains>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::SubDomains::AlterVersion>(subDomain.GetVersion()),
        NIceDb::TUpdate<Schema::SubDomains::PlanResolution>(subDomain.GetPlanResolution()),
        NIceDb::TUpdate<Schema::SubDomains::TimeCastBuckets>(subDomain.GetTCB()),
        NIceDb::TUpdate<Schema::SubDomains::ResourcesDomainOwnerPathId>(subDomain.GetResourcesDomainId().OwnerId),
        NIceDb::TUpdate<Schema::SubDomains::ResourcesDomainLocalPathId>(subDomain.GetResourcesDomainId().LocalPathId),
        NIceDb::TUpdate<Schema::SubDomains::SharedHiveId>(subDomain.GetSharedHive()));

    PersistSubDomainDeclaredSchemeQuotas(db, pathId, subDomain);
    PersistSubDomainDatabaseQuotas(db, pathId, subDomain);
    if (subDomain.GetTenantSchemeShardID()) {
        // Scheme limits protect SchemeShard tablets from overload.
        // Regular (non-external) subdomains use the root SchemeShard and its limits,
        // so they don't need to persist their scheme limits.
        PersistSchemeLimits(db, pathId, subDomain);
    }
    PersistSubDomainState(db, pathId, subDomain);

    PersistSubDomainAuditSettings(db, pathId, subDomain);
    PersistSubDomainServerlessComputeResourcesMode(db, pathId, subDomain);

    db.Table<Schema::SubDomainsAlterData>().Key(pathId.LocalPathId).Delete();

    for (auto shardIdx: subDomain.GetPrivateShards()) {
        db.Table<Schema::SubDomainShards>().Key(pathId.LocalPathId, shardIdx.GetLocalId()).Update();
        db.Table<Schema::SubDomainShardsAlterData>().Key(pathId.LocalPathId, shardIdx.GetLocalId()).Delete();
    }

    PersistStoragePools(db, pathId, subDomain);
}

void TSchemeShard::PersistSubDomainDeclaredSchemeQuotas(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    if (subDomain.GetDeclaredSchemeQuotas()) {
        TString declaredSchemeQuotas;
        Y_ABORT_UNLESS(subDomain.GetDeclaredSchemeQuotas()->SerializeToString(&declaredSchemeQuotas));
        db.Table<Schema::SubDomains>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::SubDomains::DeclaredSchemeQuotas>(declaredSchemeQuotas));
    } else {
        db.Table<Schema::SubDomains>().Key(pathId.LocalPathId).Update(
                NIceDb::TNull<Schema::SubDomains::DeclaredSchemeQuotas>());
    }
}

void TSchemeShard::PersistSubDomainDatabaseQuotas(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    if (const auto& databaseQuotas = subDomain.GetDatabaseQuotas()) {
        TString serialized;
        Y_ABORT_UNLESS(databaseQuotas->SerializeToString(&serialized));
        db.Table<Schema::SubDomains>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::SubDomains::DatabaseQuotas>(serialized));
    } else {
        db.Table<Schema::SubDomains>().Key(pathId.LocalPathId).Update(
                NIceDb::TNull<Schema::SubDomains::DatabaseQuotas>());
    }
}

void TSchemeShard::PersistSubDomainState(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::SubDomains>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::SubDomains::StateVersion>(subDomain.GetDomainStateVersion()),
            NIceDb::TUpdate<Schema::SubDomains::DiskQuotaExceeded>(subDomain.GetDiskQuotaExceeded()),
            NIceDb::TUpdate<Schema::SubDomains::SmallBlobsQuotaExceeded>(subDomain.GetSmallBlobsQuotaExceeded()));
}

void TSchemeShard::PersistSubDomainSchemeQuotas(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    const auto& quotas = subDomain.GetSchemeQuotas();

    ui64 idx = 0;
    for (const auto& quota : quotas) {
        if (quota.Dirty) {
            db.Table<Schema::SubDomainSchemeQuotas>().Key(pathId.LocalPathId, idx).Update(
                NIceDb::TUpdate<Schema::SubDomainSchemeQuotas::BucketSize>(quota.BucketSize),
                NIceDb::TUpdate<Schema::SubDomainSchemeQuotas::BucketDurationUs>(quota.BucketDuration.MicroSeconds()),
                NIceDb::TUpdate<Schema::SubDomainSchemeQuotas::Available>(quota.Available),
                NIceDb::TUpdate<Schema::SubDomainSchemeQuotas::LastUpdateUs>(quota.LastUpdate.MicroSeconds()));
            quota.Dirty = false;
        }
        ++idx;
    }

    while (idx < quotas.LastKnownSize) {
        db.Table<Schema::SubDomainSchemeQuotas>().Key(pathId.LocalPathId, idx).Delete();
        ++idx;
    }
    quotas.LastKnownSize = quotas.size();
}

void TSchemeShard::PersistRemoveSubDomain(NIceDb::TNiceDb& db, const TPathId& pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    auto it = SubDomains.find(pathId);
    if (it != SubDomains.end()) {
        TSubDomainInfo::TPtr subDomain = it->second;

        if (subDomain->GetAlter()) {
            PersistDeleteSubDomainAlter(db, pathId, *subDomain->GetAlter());
        }

        for (auto shardIdx: subDomain->GetPrivateShards()) {
            db.Table<Schema::SubDomainShards>().Key(pathId.LocalPathId, shardIdx.GetLocalId()).Delete();
        }

        const auto& quotas = subDomain->GetSchemeQuotas();
        for (ui64 idx = 0; idx < Max(quotas.size(), quotas.LastKnownSize); ++idx) {
            db.Table<Schema::SubDomainSchemeQuotas>().Key(pathId.LocalPathId, idx).Delete();
        }

        for (const auto& pool : subDomain->GetStoragePools()) {
            db.Table<Schema::StoragePools>().Key(pathId.LocalPathId, pool.GetName(), pool.GetKind()).Delete();
        }

        if (RootShredManager && RootShredManager->Remove(pathId)) {
            db.Table<Schema::ShredGenerations>().Key(RootShredManager->GetGeneration()).Update<Schema::ShredGenerations::Status>(RootShredManager->GetStatus());
            db.Table<Schema::WaitingShredTenants>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
        }

        db.Table<Schema::SubDomains>().Key(pathId.LocalPathId).Delete();
        SubDomains.erase(it);
        DecrementPathDbRefCount(pathId);
    }
}

template <class Table>
void PersistSubDomainAuditSettingsImpl(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo::TMaybeAuditSettings& value) {
    using Field = typename Table::AuditSettings;
    if (value) {
        TString serialized;
        Y_ABORT_UNLESS(value->SerializeToString(&serialized));
        db.Table<Table>().Key(pathId.LocalPathId).Update(NIceDb::TUpdate<Field>(serialized));
    } else {
        db.Table<Table>().Key(pathId.LocalPathId).template UpdateToNull<Field>();
    }
}

void TSchemeShard::PersistSubDomainAuditSettings(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    PersistSubDomainAuditSettingsImpl<Schema::SubDomains>(db, pathId, subDomain.GetAuditSettings());
}

void TSchemeShard::PersistSubDomainAuditSettingsAlter(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    PersistSubDomainAuditSettingsImpl<Schema::SubDomainsAlterData>(db, pathId, subDomain.GetAuditSettings());
}

template <class Table>
void PersistSubDomainServerlessComputeResourcesModeImpl(NIceDb::TNiceDb& db, const TPathId& pathId,
                                                        const TMaybeServerlessComputeResourcesMode& value) {
    using Field = typename Table::ServerlessComputeResourcesMode;
    if (value) {
        db.Table<Table>().Key(pathId.LocalPathId).Update(NIceDb::TUpdate<Field>(*value));
    }
}

void TSchemeShard::PersistSubDomainServerlessComputeResourcesMode(NIceDb::TNiceDb& db, const TPathId& pathId,
                                                                  const TSubDomainInfo& subDomain) {
    const auto& serverlessComputeResourcesMode = subDomain.GetServerlessComputeResourcesMode();
    PersistSubDomainServerlessComputeResourcesModeImpl<Schema::SubDomains>(db, pathId, serverlessComputeResourcesMode);
}

void TSchemeShard::PersistSubDomainServerlessComputeResourcesModeAlter(NIceDb::TNiceDb& db, const TPathId& pathId,
                                                                       const TSubDomainInfo& subDomain) {
    const auto& serverlessComputeResourcesMode = subDomain.GetServerlessComputeResourcesMode();
    PersistSubDomainServerlessComputeResourcesModeImpl<Schema::SubDomainsAlterData>(db, pathId, serverlessComputeResourcesMode);
}

void TSchemeShard::PersistACL(NIceDb::TNiceDb& db, const TPathElement::TPtr path) {
    if (path->PathId.OwnerId == TabletID()) {
        db.Table<Schema::Paths>().Key(path->PathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::Paths::ACL>(path->ACL),
                NIceDb::TUpdate<Schema::Paths::ACLVersion>(path->ACLVersion));
    } else {
        db.Table<Schema::MigratedPaths>().Key(path->PathId.OwnerId, path->PathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedPaths::ACL>(path->ACL),
            NIceDb::TUpdate<Schema::MigratedPaths::ACLVersion>(path->ACLVersion));
    }
}


void TSchemeShard::PersistOwner(NIceDb::TNiceDb& db, const TPathElement::TPtr path) {
    if (path->PathId.OwnerId == TabletID()) {
        db.Table<Schema::Paths>().Key(path->PathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::Paths::Owner>(path->Owner));
    } else {
        db.Table<Schema::MigratedPaths>().Key(path->PathId.OwnerId, path->PathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedPaths::Owner>(path->Owner));
    }
}

void TSchemeShard::PersistCreateTxId(NIceDb::TNiceDb& db, const TPathId pathId, TTxId txId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::Paths>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::Paths::CreateTxId>(txId));
}

void TSchemeShard::PersistCreateStep(NIceDb::TNiceDb& db, const TPathId pathId, TStepId step) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    // CreateTxId is saved in PersistPath
    db.Table<Schema::Paths>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::Paths::StepCreated>(step));
}

void TSchemeShard::PersistSnapshotTable(NIceDb::TNiceDb& db, const TTxId snapshotId, const TPathId tableId) {
    db.Table<Schema::SnapshotTables>().Key(snapshotId, tableId.OwnerId, tableId.LocalPathId).Update();
}

void TSchemeShard::PersistSnapshotStepId(NIceDb::TNiceDb& db, const TTxId snapshotId, const TStepId stepId) {
    db.Table<Schema::SnapshotSteps>().Key(snapshotId).Update(NIceDb::TUpdate<Schema::SnapshotSteps::StepId>(stepId));
}

void TSchemeShard::PersistDropSnapshot(NIceDb::TNiceDb& db, const TTxId snapshotId, const TPathId tableId) {
    db.Table<Schema::SnapshotTables>().Key(snapshotId, tableId.OwnerId, tableId.LocalPathId).Delete();
    db.Table<Schema::SnapshotSteps>().Key(snapshotId).Delete();
}

void TSchemeShard::PersistLongLock(NIceDb::TNiceDb &db, const TTxId lockId, const TPathId pathId) {
    db.Table<Schema::LongLocks>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::LongLocks::LockId>(lockId));
}

void TSchemeShard::PersistUnLock(NIceDb::TNiceDb& db, const TPathId pathId) {
    db.Table<Schema::LongLocks>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistDropStep(NIceDb::TNiceDb& db, const TPathId pathId, TStepId step, TOperationId opId) {
    Y_ABORT_UNLESS(step, "Drop step must be valid (not 0)");
    if (pathId.OwnerId == TabletID()) {
        db.Table<Schema::Paths>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::Paths::StepDropped>(step),
                NIceDb::TUpdate<Schema::Paths::DropTxId>(opId.GetTxId()));
    } else {
        db.Table<Schema::MigratedPaths>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::MigratedPaths::StepDropped>(step),
                NIceDb::TUpdate<Schema::MigratedPaths::DropTxId>(opId.GetTxId()));
    }
}

void TSchemeShard::PersistTxState(NIceDb::TNiceDb& db, const TOperationId opId) {
    Y_ABORT_UNLESS(TxInFlight.contains(opId));
    TTxState& txState = TxInFlight.at(opId);

    Y_ABORT_UNLESS(txState.TxType != TTxState::TxInvalid);
    Y_ABORT_UNLESS(txState.State != TTxState::Invalid);
    TString extraData;
    if (txState.TxType == TTxState::TxSplitTablePartition || txState.TxType == TTxState::TxMergeTablePartition) {
        Y_ABORT_UNLESS(txState.SplitDescription, "Split Tx must have non-empty split description");
        bool serializeRes = txState.SplitDescription->SerializeToString(&extraData);
        Y_ABORT_UNLESS(serializeRes);
    } else if (txState.TxType == TTxState::TxFinalizeBuildIndex) {
        if (txState.BuildIndexOutcome) {
            bool serializeRes = txState.BuildIndexOutcome->SerializeToString(&extraData);
            Y_ABORT_UNLESS(serializeRes);
        }
    } else if (txState.TxType == TTxState::TxAlterTable) {
        TPathId pathId = txState.TargetPathId;

        Y_VERIFY_S(PathsById.contains(pathId), "Path id " << pathId << " doesn't exist");
        Y_VERIFY_S(PathsById.at(pathId)->IsTable(), "Path id " << pathId << " is not a table");
        Y_VERIFY_S(Tables.FindPtr(pathId), "Table " << pathId << " doesn't exist");

        TTableInfo::TPtr tableInfo = Tables.at(pathId);
        extraData = tableInfo->SerializeAlterExtraData();
    } else if (txState.TxType == TTxState::TxCopyTable || txState.TxType == TTxState::TxReadOnlyCopyColumnTable) {
        NKikimrSchemeOp::TGenericTxInFlyExtraData proto;
        txState.CdcPathId.ToProto(proto.MutableTxCopyTableExtraData()->MutableCdcPathId());
        if (txState.TargetPathTargetState) {
            proto.MutableTxCopyTableExtraData()->SetTargetPathTargetState(*txState.TargetPathTargetState);
        }
        bool serializeRes = proto.SerializeToString(&extraData);
        Y_ABORT_UNLESS(serializeRes);
    } else if (txState.TxType == TTxState::TxChangePathState) {
        if (txState.TargetPathTargetState) {
            NKikimrSchemeOp::TGenericTxInFlyExtraData proto;
            proto.MutableTxCopyTableExtraData()->SetTargetPathTargetState(*txState.TargetPathTargetState);
            bool serializeRes = proto.SerializeToString(&extraData);
            Y_ABORT_UNLESS(serializeRes);
        }
    }  else if (txState.TxType == TTxState::TxRotateCdcStreamAtTable) {
        NKikimrSchemeOp::TGenericTxInFlyExtraData proto;
        txState.CdcPathId.ToProto(proto.MutableTxCopyTableExtraData()->MutableCdcPathId());
        bool serializeRes = proto.SerializeToString(&extraData);
        Y_ABORT_UNLESS(serializeRes);
    } else if (txState.TxType == TTxState::TxCreateCdcStreamAtTable ||
               txState.TxType == TTxState::TxCreateCdcStreamAtTableWithInitialScan ||
               txState.TxType == TTxState::TxAlterCdcStreamAtTable ||
               txState.TxType == TTxState::TxAlterCdcStreamAtTableDropSnapshot ||
               txState.TxType == TTxState::TxDropCdcStreamAtTable ||
               txState.TxType == TTxState::TxDropCdcStreamAtTableDropSnapshot) {
        NKikimrSchemeOp::TGenericTxInFlyExtraData proto;
        txState.CdcPathId.ToProto(proto.MutableTxCopyTableExtraData()->MutableCdcPathId());
        bool serializeRes = proto.SerializeToString(&extraData);
        Y_ABORT_UNLESS(serializeRes);
    }

    db.Table<Schema::TxInFlightV2>().Key(opId.GetTxId(), opId.GetSubTxId()).Update(
                NIceDb::TUpdate<Schema::TxInFlightV2::TxType>((ui8)txState.TxType),
                NIceDb::TUpdate<Schema::TxInFlightV2::TargetPathId>(txState.TargetPathId.LocalPathId),
                NIceDb::TUpdate<Schema::TxInFlightV2::State>(txState.State),
                NIceDb::TUpdate<Schema::TxInFlightV2::MinStep>(txState.MinStep),
                NIceDb::TUpdate<Schema::TxInFlightV2::ExtraBytes>(extraData),
                NIceDb::TUpdate<Schema::TxInFlightV2::StartTime>(txState.StartTime.GetValue()),
                NIceDb::TUpdate<Schema::TxInFlightV2::TargetOwnerPathId>(txState.TargetPathId.OwnerId),
                NIceDb::TUpdate<Schema::TxInFlightV2::BuildIndexId>(txState.BuildIndexId),
                NIceDb::TUpdate<Schema::TxInFlightV2::SourceLocalPathId>(txState.SourcePathId.LocalPathId),
                NIceDb::TUpdate<Schema::TxInFlightV2::SourceOwnerId>(txState.SourcePathId.OwnerId),
                NIceDb::TUpdate<Schema::TxInFlightV2::NeedUpdateObject>(txState.NeedUpdateObject),
                NIceDb::TUpdate<Schema::TxInFlightV2::NeedSyncHive>(txState.NeedSyncHive)
                );

    for (const auto& shardOp : txState.Shards) {
        PersistUpdateTxShard(db, opId, shardOp.Idx, shardOp.Operation);
    }
}

void TSchemeShard::PersistTxMinStep(NIceDb::TNiceDb& db, const TOperationId opId, TStepId minStep) {
    db.Table<Schema::TxInFlightV2>().Key(opId.GetTxId(), opId.GetSubTxId()).Update(
                NIceDb::TUpdate<Schema::TxInFlightV2::MinStep>(minStep)
                );
}

void TSchemeShard::ChangeTxState(NIceDb::TNiceDb& db, const TOperationId opId, TTxState::ETxState newState) {
    Y_VERIFY_S(FindTx(opId),
               "Unknown TxId " << opId.GetTxId()
                               << " PartId " << opId.GetSubTxId());
    Y_ABORT_UNLESS(FindTx(opId)->State != TTxState::Invalid);

    const auto& ctx = TActivationContext::AsActorContext();

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Change state for txid " << opId << " "
                 << NKikimr::NSchemeShard::TxStateName(TxInFlight[opId].State) << " -> " << NKikimr::NSchemeShard::TxStateName(newState));

    FindTx(opId)->State = newState;
    db.Table<Schema::TxInFlightV2>().Key(opId.GetTxId(), opId.GetSubTxId()).Update(
        NIceDb::TUpdate<Schema::TxInFlightV2::State>(newState));
}

void TSchemeShard::PersistCancelTx(NIceDb::TNiceDb &db, const TOperationId opId, const TTxState &txState) {
    Y_ABORT_UNLESS(txState.TxType == TTxState::TxBackup || txState.TxType == TTxState::TxRestore);

    db.Table<Schema::TxInFlightV2>().Key(opId.GetTxId(), opId.GetSubTxId()).Update(
        NIceDb::TUpdate<Schema::TxInFlightV2::CancelBackup>(txState.Cancel));
}

void TSchemeShard::PersistTxPlanStep(NIceDb::TNiceDb &db, TOperationId opId, TStepId step) {
    db.Table<Schema::TxInFlightV2>().Key(opId.GetTxId(), opId.GetSubTxId()).Update(
        NIceDb::TUpdate<Schema::TxInFlightV2::PlanStep>(step));
}

void TSchemeShard::PersistRemoveTx(NIceDb::TNiceDb& db, const TOperationId opId, const TTxState& txState) {
    db.Table<Schema::TxInFlightV2>().Key(opId.GetTxId(), opId.GetSubTxId()).Delete();
    for (const auto& shardOp : txState.Shards) {
        PersistRemoveTxShard(db, opId, shardOp.Idx);
    }
}

void TSchemeShard::PersistTable(NIceDb::TNiceDb& db, const TPathId tableId) {
    Y_ABORT_UNLESS(Tables.contains(tableId));
    const TTableInfo::TPtr tableInfo = Tables.at(tableId);

    PersistTableAltered(db, tableId, tableInfo);
    PersistTablePartitioning(db, tableId, tableInfo);
}

void TSchemeShard::PersistChannelsBinding(NIceDb::TNiceDb& db, const TShardIdx shardId, const TChannelsBindings& bindedChannels) {
    for (ui32 channelId = 0; channelId < bindedChannels.size(); ++channelId) {
        const auto& bind = bindedChannels[channelId];
        if (IsLocalId(shardId)) {
            db.Table<Schema::ChannelsBinding>().Key(shardId.GetLocalId(), channelId).Update(
                    NIceDb::TUpdate<Schema::ChannelsBinding::PoolName>(bind.GetStoragePoolName()),
                    NIceDb::TUpdate<Schema::ChannelsBinding::Binding>(bind.SerializeAsString()));
        } else {
            db.Table<Schema::MigratedChannelsBinding>().Key(shardId.GetOwnerId(), shardId.GetLocalId(), channelId).Update(
                NIceDb::TUpdate<Schema::MigratedChannelsBinding::PoolName>(bind.GetStoragePoolName()),
                NIceDb::TUpdate<Schema::MigratedChannelsBinding::Binding>(bind.SerializeAsString()));
        }
    }
}

void TSchemeShard::PersistTablePartitioningVersion(NIceDb::TNiceDb& db, const TPathId pathId, const TTableInfo::TPtr tableInfo) {
    if (IsLocalId(pathId)) {
        db.Table<Schema::Tables>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::Tables::PartitioningVersion>(++tableInfo->PartitioningVersion));
    } else {
        db.Table<Schema::MigratedTables>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedTables::PartitioningVersion>(++tableInfo->PartitioningVersion));
    }
}

void TSchemeShard::PersistTablePartitioning(NIceDb::TNiceDb& db, const TPathId pathId, const TTableInfo::TPtr tableInfo, ui64 startIdx) {
    for (ui64 pi = startIdx; pi < tableInfo->GetPartitions().size(); ++pi) {
        const auto* partition = tableInfo->GetPartitions()[pi];
        if (tableInfo->PartitionsInShardIdxFormat) {
            db.Table<Schema::TablePartitionsByShardIdx>()
                .Key(pathId.OwnerId, pathId.LocalPathId,
                     partition->ShardIdx.GetOwnerId(), partition->ShardIdx.GetLocalId())
                .Update(
                    NIceDb::TUpdate<Schema::TablePartitionsByShardIdx::RangeEnd>(partition->EndOfRange),
                    NIceDb::TUpdate<Schema::TablePartitionsByShardIdx::LastCondErase>(partition->LastCondErase.GetValue()),
                    NIceDb::TUpdate<Schema::TablePartitionsByShardIdx::NextCondErase>(partition->NextCondErase.GetValue()));
        } else if (IsLocalId(pathId) && IsLocalId(partition->ShardIdx)) {
            db.Table<Schema::TablePartitions>().Key(pathId.LocalPathId, pi).Update(
                NIceDb::TUpdate<Schema::TablePartitions::RangeEnd>(partition->EndOfRange),
                NIceDb::TUpdate<Schema::TablePartitions::DatashardIdx>(partition->ShardIdx.GetLocalId()),
                NIceDb::TUpdate<Schema::TablePartitions::LastCondErase>(partition->LastCondErase.GetValue()),
                NIceDb::TUpdate<Schema::TablePartitions::NextCondErase>(partition->NextCondErase.GetValue()));
        } else {
            if (IsLocalId(pathId)) {
                // Incompatible change 1:
                // Store migrated shards of local tables in migrated table partitions
                // This change is incompatible with older versions because partitions
                // may no longer be in a single table and will require sorting at load time.
                BumpIncompatibleChanges(db, 1);
            }
            db.Table<Schema::MigratedTablePartitions>().Key(pathId.OwnerId, pathId.LocalPathId, pi).Update(
                NIceDb::TUpdate<Schema::MigratedTablePartitions::RangeEnd>(partition->EndOfRange),
                NIceDb::TUpdate<Schema::MigratedTablePartitions::OwnerShardIdx>(partition->ShardIdx.GetOwnerId()),
                NIceDb::TUpdate<Schema::MigratedTablePartitions::LocalShardIdx>(partition->ShardIdx.GetLocalId()),
                NIceDb::TUpdate<Schema::MigratedTablePartitions::LastCondErase>(partition->LastCondErase.GetValue()),
                NIceDb::TUpdate<Schema::MigratedTablePartitions::NextCondErase>(partition->NextCondErase.GetValue()));
        }
    }
    if (IsLocalId(pathId)) {
        db.Table<Schema::Tables>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::Tables::PartitioningVersion>(++tableInfo->PartitioningVersion));
    } else {
        db.Table<Schema::MigratedTables>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedTables::PartitioningVersion>(++tableInfo->PartitioningVersion));
    }
}

void TSchemeShard::PersistTablePartitioningDeletion(NIceDb::TNiceDb& db, const TPathId pathId, const TTableInfo::TPtr tableInfo, ui64 startIdx) {
    const auto& partitions = tableInfo->GetPartitions();
    for (ui64 pi = startIdx; pi < partitions.size(); ++pi) {
        if (tableInfo->PartitionsInShardIdxFormat) {
            const auto& partition = partitions[pi];
            db.Table<Schema::TablePartitionsByShardIdx>()
                .Key(pathId.OwnerId, pathId.LocalPathId,
                     partition->ShardIdx.GetOwnerId(), partition->ShardIdx.GetLocalId())
                .Delete();
            db.Table<Schema::TablePartitionStatsByShardIdx>()
                .Key(pathId.OwnerId, pathId.LocalPathId,
                     partition->ShardIdx.GetOwnerId(), partition->ShardIdx.GetLocalId())
                .Delete();
        } else {
            if (IsLocalId(pathId)) {
                db.Table<Schema::TablePartitions>().Key(pathId.LocalPathId, pi).Delete();
            }
            db.Table<Schema::MigratedTablePartitions>().Key(pathId.OwnerId, pathId.LocalPathId, pi).Delete();
        }
        db.Table<Schema::TablePartitionStats>().Key(pathId.OwnerId, pathId.LocalPathId, pi).Delete();
    }
}

void TSchemeShard::PersistTablePartitionCondErase(NIceDb::TNiceDb& db, const TPathId& pathId, const TTableShardInfo* partition, const TTableInfo::TPtr tableInfo) {
    const ui64 id = partition->Position;
    if (tableInfo->PartitionsInShardIdxFormat) {
        db.Table<Schema::TablePartitionsByShardIdx>()
            .Key(pathId.OwnerId, pathId.LocalPathId,
                 partition->ShardIdx.GetOwnerId(), partition->ShardIdx.GetLocalId())
            .Update(
                NIceDb::TUpdate<Schema::TablePartitionsByShardIdx::LastCondErase>(partition->LastCondErase.GetValue()),
                NIceDb::TUpdate<Schema::TablePartitionsByShardIdx::NextCondErase>(partition->NextCondErase.GetValue()));
    } else if (IsLocalId(pathId) && IsLocalId(partition->ShardIdx)) {
        db.Table<Schema::TablePartitions>().Key(pathId.LocalPathId, id).Update(
            NIceDb::TUpdate<Schema::TablePartitions::LastCondErase>(partition->LastCondErase.GetValue()),
            NIceDb::TUpdate<Schema::TablePartitions::NextCondErase>(partition->NextCondErase.GetValue()));
    } else {
        if (IsLocalId(pathId)) {
            // Incompatible change 1 (see above)
            BumpIncompatibleChanges(db, 1);
        }
        db.Table<Schema::MigratedTablePartitions>().Key(pathId.OwnerId, pathId.LocalPathId, id).Update(
            NIceDb::TUpdate<Schema::MigratedTablePartitions::LastCondErase>(partition->LastCondErase.GetValue()),
            NIceDb::TUpdate<Schema::MigratedTablePartitions::NextCondErase>(partition->NextCondErase.GetValue()));
    }
}

void TSchemeShard::PersistTablePartitioningByShardIdxDelete(NIceDb::TNiceDb& db, const TPathId pathId, const TTableInfo::TPtr tableInfo, const TVector<TShardIdx>& srcShardIdxs) {
    for (const TShardIdx& idx : srcShardIdxs) {
        db.Table<Schema::TablePartitionsByShardIdx>()
            .Key(pathId.OwnerId, pathId.LocalPathId, idx.GetOwnerId(), idx.GetLocalId())
            .Delete();
        db.Table<Schema::TablePartitionStatsByShardIdx>()
            .Key(pathId.OwnerId, pathId.LocalPathId, idx.GetOwnerId(), idx.GetLocalId())
            .Delete();
    }
    // TablePartitionStats is position-keyed: use Position for O(k) lookup.
    for (const TShardIdx& idx : srcShardIdxs) {
        const auto* p = tableInfo->GetPartitionStore().FindPtr(idx);
        Y_ABORT_UNLESS(p);
        db.Table<Schema::TablePartitionStats>()
            .Key(pathId.OwnerId, pathId.LocalPathId, p->Position)
            .Delete();
    }
}

void TSchemeShard::PersistTablePartitioningByShardIdxInsert(NIceDb::TNiceDb& db, const TPathId pathId, const TTableInfo::TPtr tableInfo, ui64 srcFirstIdx, ui64 kAdded) {
    const auto& partitions = tableInfo->GetPartitions();
    for (ui64 i = srcFirstIdx; i < srcFirstIdx + kAdded; ++i) {
        const auto* p = partitions[i];
        db.Table<Schema::TablePartitionsByShardIdx>()
            .Key(pathId.OwnerId, pathId.LocalPathId, p->ShardIdx.GetOwnerId(), p->ShardIdx.GetLocalId())
            .Update(
                NIceDb::TUpdate<Schema::TablePartitionsByShardIdx::RangeEnd>(p->EndOfRange),
                NIceDb::TUpdate<Schema::TablePartitionsByShardIdx::LastCondErase>(p->LastCondErase.GetValue()),
                NIceDb::TUpdate<Schema::TablePartitionsByShardIdx::NextCondErase>(p->NextCondErase.GetValue()));
        PersistTablePartitionStats(db, pathId, p->ShardIdx, tableInfo);
    }
}

void TSchemeShard::PersistTablePartitioningInFormat(NIceDb::TNiceDb& db, const TPathId pathId, const TTableInfo::TPtr tableInfo, bool shardIdxFormat) {
    Y_ABORT_UNLESS(tableInfo->PartitionsInShardIdxFormat != shardIdxFormat,
        "PersistTablePartitioningInFormat called when format already matches target"
    );

    const auto& partitions = tableInfo->GetPartitions();
    for (ui64 pi = 0; pi < partitions.size(); ++pi) {
        const auto* p = partitions[pi];

        if (shardIdxFormat) {
            // position -> shardidx
            db.Table<Schema::TablePartitionsByShardIdx>()
                .Key(pathId.OwnerId, pathId.LocalPathId, p->ShardIdx.GetOwnerId(), p->ShardIdx.GetLocalId())
                .Update(
                    NIceDb::TUpdate<Schema::TablePartitionsByShardIdx::RangeEnd>(p->EndOfRange),
                    NIceDb::TUpdate<Schema::TablePartitionsByShardIdx::LastCondErase>(p->LastCondErase.GetValue()),
                    NIceDb::TUpdate<Schema::TablePartitionsByShardIdx::NextCondErase>(p->NextCondErase.GetValue())
                );
            if (IsLocalId(pathId)) {
                db.Table<Schema::TablePartitions>().Key(pathId.LocalPathId, pi).Delete();
            }
            db.Table<Schema::MigratedTablePartitions>().Key(pathId.OwnerId, pathId.LocalPathId, pi).Delete();
            db.Table<Schema::TablePartitionStats>().Key(pathId.OwnerId, pathId.LocalPathId, pi).Delete();
        } else {
            // shardidx -> position
            if (IsLocalId(pathId) && IsLocalId(p->ShardIdx)) {
                db.Table<Schema::TablePartitions>().Key(pathId.LocalPathId, pi).Update(
                    NIceDb::TUpdate<Schema::TablePartitions::RangeEnd>(p->EndOfRange),
                    NIceDb::TUpdate<Schema::TablePartitions::DatashardIdx>(p->ShardIdx.GetLocalId()),
                    NIceDb::TUpdate<Schema::TablePartitions::LastCondErase>(p->LastCondErase.GetValue()),
                    NIceDb::TUpdate<Schema::TablePartitions::NextCondErase>(p->NextCondErase.GetValue())
                );
            } else {
                if (IsLocalId(pathId)) {
                    BumpIncompatibleChanges(db, 1);
                }
                db.Table<Schema::MigratedTablePartitions>().Key(pathId.OwnerId, pathId.LocalPathId, pi).Update(
                    NIceDb::TUpdate<Schema::MigratedTablePartitions::RangeEnd>(p->EndOfRange),
                    NIceDb::TUpdate<Schema::MigratedTablePartitions::OwnerShardIdx>(p->ShardIdx.GetOwnerId()),
                    NIceDb::TUpdate<Schema::MigratedTablePartitions::LocalShardIdx>(p->ShardIdx.GetLocalId()),
                    NIceDb::TUpdate<Schema::MigratedTablePartitions::LastCondErase>(p->LastCondErase.GetValue()),
                    NIceDb::TUpdate<Schema::MigratedTablePartitions::NextCondErase>(p->NextCondErase.GetValue())
                );
            }
            db.Table<Schema::TablePartitionsByShardIdx>()
                .Key(pathId.OwnerId, pathId.LocalPathId, p->ShardIdx.GetOwnerId(), p->ShardIdx.GetLocalId())
                .Delete();
            db.Table<Schema::TablePartitionStatsByShardIdx>()
                .Key(pathId.OwnerId, pathId.LocalPathId, p->ShardIdx.GetOwnerId(), p->ShardIdx.GetLocalId())
                .Delete();
        }
    }
}

TSchemeShard::EFormatSwitchStatus TSchemeShard::SwitchTablePartitionsFormat(NIceDb::TNiceDb& db, TPathId pathId, bool shardIdxFormat) {
    auto* tableInfoPtr = Tables.FindPtr(pathId);
    if (!tableInfoPtr) {
        return EFormatSwitchStatus::NotATable;
    }
    auto& tableInfo = *tableInfoPtr;
    if (tableInfo->PartitionsInShardIdxFormat == shardIdxFormat) {
        return EFormatSwitchStatus::AlreadyDone;
    }
    if (TPath::Init(pathId, this).IsUnderOperation()) {
        return EFormatSwitchStatus::Busy;
    }

    PersistTablePartitioningInFormat(db, pathId, tableInfo, shardIdxFormat);
    tableInfo->PartitionsInShardIdxFormat = shardIdxFormat;
    // Re-write stats rows in the new format (PersistAllTablePartitionStats is a
    // no-op when EnablePersistentPartitionStats is off).
    PersistAllTablePartitionStats(db, pathId, tableInfo, 0);

    auto prevFormatCounter = shardIdxFormat ? COUNTER_FORMAT_POSITION_TABLE_COUNT : COUNTER_FORMAT_SHARDIDX_TABLE_COUNT;
    auto nextFormatCounter = shardIdxFormat ? COUNTER_FORMAT_SHARDIDX_TABLE_COUNT : COUNTER_FORMAT_POSITION_TABLE_COUNT;

    TabletCounters->Simple()[prevFormatCounter].Sub(1);
    TabletCounters->Simple()[nextFormatCounter].Add(1);

    return EFormatSwitchStatus::Ok;
}

} // namespace NSchemeShard
} // namespace NKikimr
