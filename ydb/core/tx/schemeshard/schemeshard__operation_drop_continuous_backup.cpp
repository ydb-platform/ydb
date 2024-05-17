#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include "schemeshard__operation_drop_cdc_stream.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace {

constexpr static char const* cbCdcStreamName = "continuousBackupImpl";

}

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> CreateDropContinuousBackup(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpDropContinuousBackup);

    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);
    const auto& cbOp = tx.GetDropContinuousBackup();
    const auto& tableName = cbOp.GetTableName();
    const auto tablePath = workingDirPath.Child(tableName);

    NKikimrSchemeOp::TDropCdcStream dropCdcStreamOp;
    dropCdcStreamOp.SetTableName(tableName);
    dropCdcStreamOp.SetStreamName(cbCdcStreamName);

    const auto streamPath = tablePath.Child(cbCdcStreamName);

    TVector<ISubOperation::TPtr> result;

    DoDropStream(dropCdcStreamOp, opId, workingDirPath, tablePath, streamPath, InvalidTxId, context, result);

    return result;
}

} // namespace NKikimr::NSchemeShard
