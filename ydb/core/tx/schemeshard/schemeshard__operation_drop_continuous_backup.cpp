#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include "schemeshard__operation_drop_cdc_stream.h"

#include <ydb/core/tx/schemeshard/backup/constants.h>

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> CreateDropContinuousBackup(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpDropContinuousBackup);

    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);
    const auto& cbOp = tx.GetDropContinuousBackup();
    const auto& tableName = cbOp.GetTableName();

    const auto checksResult = NCdc::DoDropStreamPathChecks(opId, workingDirPath, tableName, NBackup::CB_CDC_STREAM_NAME);
    if (std::holds_alternative<ISubOperation::TPtr>(checksResult)) {
        return {std::get<ISubOperation::TPtr>(checksResult)};
    }

    const auto [tablePath, streamPath] = std::get<NCdc::TStreamPaths>(checksResult);

    TString errStr;
    if (!context.SS->CheckApplyIf(tx, errStr)) {
        return {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, errStr)};
    }

    if (const auto reject = NCdc::DoDropStreamChecks(opId, tablePath, InvalidTxId, context); reject) {
        return {reject};
    }

    NKikimrSchemeOp::TDropCdcStream dropCdcStreamOp;
    dropCdcStreamOp.SetTableName(tableName);
    dropCdcStreamOp.SetStreamName(NBackup::CB_CDC_STREAM_NAME);

    TVector<ISubOperation::TPtr> result;

    NCdc::DoDropStream(dropCdcStreamOp, opId, workingDirPath, tablePath, streamPath, InvalidTxId, context, result);

    return result;
}

} // namespace NKikimr::NSchemeShard
