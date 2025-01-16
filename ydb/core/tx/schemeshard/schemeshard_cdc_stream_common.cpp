#include "schemeshard_cdc_stream_common.h"

#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NCdcStreamAtTable {

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
