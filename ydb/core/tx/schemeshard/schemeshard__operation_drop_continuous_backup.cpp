#include "schemeshard__operation_common.h"
#include "schemeshard__operation_drop_cdc_stream.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> CreateDropContinuousBackup(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpDropContinuousBackup);

    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);
    const auto& cbOp = tx.GetDropContinuousBackup();
    const auto& tableName = cbOp.GetTableName();

    const auto tablePath = workingDirPath.Child(tableName);
    {
        const auto checks = tablePath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .IsTable()
            .NotAsyncReplicaTable()
            .NotUnderDeleting()
            .NotUnderOperation();

        if (checks && !tablePath.IsInsideTableIndexPath()) {
            checks.IsCommonSensePath();
        }

        if (!checks) {
            return {CreateReject(opId, checks.GetStatus(), checks.GetError())};
        }
    }

    TVector<ISubOperation::TPtr> result;
    for (const auto& [childName, childPathId] : tablePath.Base()->GetChildren()) {
        if (childName.EndsWith("_continuousBackupImpl")) {
            TPath childPath = tablePath.Child(childName);
            if (!childPath.IsDeleted() && childPath.IsCdcStream()) {
                if (!context.SS->CdcStreams.contains(childPathId)) {
                    continue;
                }
                const auto& streamInfo = context.SS->CdcStreams.at(childPathId);
                // Only drop backup streams (Proto format), not user streams (Json format)
                if (streamInfo->Format != NKikimrSchemeOp::ECdcStreamFormatProto) {
                    continue;
                }
            }

            const auto checksResult = NCdc::DoDropStreamPathChecks(opId, workingDirPath, tableName, childName);
            if (std::holds_alternative<ISubOperation::TPtr>(checksResult)) {
                return {std::get<ISubOperation::TPtr>(checksResult)};
            }

            const auto [_, streamPath] = std::get<NCdc::TStreamPaths>(checksResult);

            TString errStr;
            if (!context.SS->CheckApplyIf(tx, errStr)) {
                return {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, errStr)};
            }

            if (const auto reject = NCdc::DoDropStreamChecks(opId, tablePath, InvalidTxId, context); reject) {
                return {reject};
            }

            NKikimrSchemeOp::TDropCdcStream dropCdcStreamOp;
            dropCdcStreamOp.SetTableName(tableName);
            dropCdcStreamOp.AddStreamName(childName);

            TVector<TPath> streamPaths = {streamPath};
            NCdc::DoDropStream(result, dropCdcStreamOp, opId, workingDirPath, tablePath, streamPaths, InvalidTxId, context);
        }
    }

    return result;
}

} // namespace NKikimr::NSchemeShard
