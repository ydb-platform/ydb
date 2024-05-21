#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include "schemeshard__operation_alter_cdc_stream.h"

#include <ydb/core/tx/schemeshard/backup/constants.h>

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> CreateAlterContinuousBackup(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpAlterContinuousBackup);

    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);
    const auto& cbOp = tx.GetAlterContinuousBackup();
    const auto& tableName = cbOp.GetTableName();

    const auto checksResult = NCdc::DoAlterStreamPathChecks(opId, workingDirPath, tableName, NBackup::CB_CDC_STREAM_NAME);
    if (std::holds_alternative<ISubOperation::TPtr>(checksResult)) {
        return {std::get<ISubOperation::TPtr>(checksResult)};
    }

    const auto [tablePath, streamPath] = std::get<NCdc::TStreamPaths>(checksResult);

    TString errStr;
    if (!context.SS->CheckApplyIf(tx, errStr)) {
        return {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, errStr)};
    }

    if (!context.SS->CheckLocks(tablePath.Base()->PathId, tx, errStr)) {
        return {CreateReject(opId, NKikimrScheme::StatusMultipleModifications, errStr)};
    }

    NKikimrSchemeOp::TAlterCdcStream alterCdcStreamOp;
    alterCdcStreamOp.SetTableName(tableName);
    alterCdcStreamOp.SetStreamName(NBackup::CB_CDC_STREAM_NAME);

    switch (cbOp.GetActionCase()) {
    case NKikimrSchemeOp::TAlterContinuousBackup::kStop:
        alterCdcStreamOp.MutableDisable();
        break;
    default:
        return {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, TStringBuilder()
            << "Unknown action: " << static_cast<ui32>(cbOp.GetActionCase()))};
    }

    TVector<ISubOperation::TPtr> result;

    NCdc::DoAlterStream(alterCdcStreamOp, opId, workingDirPath, tablePath, result);

    return result;
}

} // namespace NKikimr::NSchemeShard
