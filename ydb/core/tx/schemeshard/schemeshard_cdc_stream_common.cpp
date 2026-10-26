#include "schemeshard_cdc_stream_common.h"

#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NCdcStreamAtTable {

bool IsIndexTypeSupportedForCdc(NKikimrSchemeOp::EIndexType type) {
    return type == NKikimrSchemeOp::EIndexTypeGlobal
        || type == NKikimrSchemeOp::EIndexTypeGlobalUnique
        || type == NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree;
}

void MaybeGuardIndexWithBaseCdcScanLock(
    NKikimrSchemeOp::TModifyScheme& tx,
    const TPath& indexPath,
    TOperationContext& context)
{
    const auto index = context.SS->Indexes.at(indexPath.Base()->PathId);
    const bool readySupportedIndex = index->State == NKikimrSchemeOp::EIndexStateReady
        && IsIndexTypeSupportedForCdc(index->Type);
    const bool idleIndex = !index->AlterData && !indexPath.IsUnderOperation();

    if (!readySupportedIndex || !idleIndex) {
        return;
    }

    const auto basePath = indexPath.Parent();

    if (basePath.IsUnderOperation()) {
        return;
    }

    const auto baseId = basePath.Base()->PathId;
    const auto lock = context.SS->LockedPaths.find(baseId);
    const auto snapshot = context.SS->TablesWithSnapshots.find(baseId);
    const bool hasLock = lock != context.SS->LockedPaths.end();
    const bool hasSnapshot = snapshot != context.SS->TablesWithSnapshots.end();

    if (!hasLock || !hasSnapshot) {
        return;
    }

    const bool matchingSnapshot = lock->second == snapshot->second
        && context.SS->SnapshotsStepIds.contains(snapshot->second);

    if (!matchingSnapshot) {
        return;
    }

    for (const auto& [name, childId] : basePath.Base()->GetChildren()) {
        const auto child = context.SS->PathsById.at(childId);
        const bool liveCdcStream = child->IsCdcStream() && !child->Dropped();
        const bool ownsLock = child->CreateTxId == lock->second;
        const bool idleStream = child->PathState == NKikimrSchemeOp::EPathStateNoChanges;

        if (!liveCdcStream || !ownsLock || !idleStream) {
            continue;
        }

        const auto stream = context.SS->CdcStreams.at(childId);
        if (stream->State == TCdcStreamInfo::EState::ECdcStreamStateScan) {
            // The index CDC operation owns only its implementation table's lock
            // and snapshot. Borrow the verified base scan guard for the index
            // metadata update, without changing or releasing the base lock.
            tx.MutableLockGuard()->SetOwnerTxId(ui64(lock->second));
            return;
        }
    }
}

void FillNotice(const TPathId& pathId, TOperationContext& context, NKikimrTxDataShard::TCreateCdcStreamNotice& notice) {
    Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
    auto path = context.SS->PathsById.at(pathId);

    Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
    auto table = context.SS->Tables.at(pathId);

    pathId.ToProto(notice.MutablePathId());
    notice.SetTableSchemaVersion(table->AlterVersion + 1);

    bool found = false;
    for (const auto& [childName, childPathId] : path->GetChildren()) {
        Y_ABORT_UNLESS(context.SS->PathsById.contains(childPathId));
        auto childPath = context.SS->PathsById.at(childPathId);

        if (!childPath->IsCdcStream() || childPath->Dropped()) {
            continue;
        }

        Y_ABORT_UNLESS(context.SS->CdcStreams.contains(childPathId));
        auto stream = context.SS->CdcStreams.at(childPathId);

        if (stream->State != TCdcStreamInfo::EState::ECdcStreamStateInvalid) {
            continue;
        }

        Y_VERIFY_S(!found, "Too many cdc streams are planned to create"
            << ": found# " << TPathId::FromProto(notice.GetStreamDescription().GetPathId())
            << ", another# " << childPathId);
        found = true;

        Y_ABORT_UNLESS(stream->AlterData);
        context.SS->DescribeCdcStream(childPathId, childName, stream->AlterData, *notice.MutableStreamDescription());

        if (stream->AlterData->State == TCdcStreamInfo::EState::ECdcStreamStateScan) {
            notice.SetSnapshotName("ChangefeedInitialScan");
        }
    }
}

void CheckWorkingDirOnPropose(const TPath::TChecker& checks, bool isTableIndex) {
    checks
        .NotUnderDomainUpgrade()
        .IsAtLocalSchemeShard()
        .IsResolved()
        .NotDeleted()
        .IsLikeDirectory()
        .NotUnderDeleting();

    if (checks && !isTableIndex) {
        checks.IsCommonSensePath();
    }
}

void CheckSrcDirOnPropose(
    const TPath::TChecker& checks,
    bool isInsideTableIndexPath,
    TTxId op)
{
    checks
        .NotEmpty()
        .NotUnderDomainUpgrade()
        .IsAtLocalSchemeShard()
        .IsResolved()
        .NotDeleted()
        .IsTable()
        .NotAsyncReplicaTable()
        .NotUnderDeleting();

    if (checks) {
        if (!isInsideTableIndexPath) {
            checks.IsCommonSensePath();
        }

        if (op != InvalidTxId) {
            checks.IsUnderTheSameOperation(op); // lock op
        } else {
            checks.NotUnderOperation();
        }
    }
}

}
