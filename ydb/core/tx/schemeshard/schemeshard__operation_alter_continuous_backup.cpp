#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include "schemeshard__operation_alter_cdc_stream.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace {

constexpr static char const* cbCdcStreamName = "continuousBackupImpl";

}

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> CreateAlterContinuousBackup(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpAlterContinuousBackup);

    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);
    const auto& cbOp = tx.GetAlterContinuousBackup();
    const auto& tableName = cbOp.GetTableName();
    const auto tablePath = workingDirPath.Child(tableName);

    NKikimrSchemeOp::TAlterCdcStream alterCdcStreamOp;
    alterCdcStreamOp.SetTableName(tableName);
    alterCdcStreamOp.SetStreamName(cbCdcStreamName);
    alterCdcStreamOp.MutableDisable();

    TVector<ISubOperation::TPtr> result;

    DoAlterStream(alterCdcStreamOp, opId, workingDirPath, tablePath, result);

    return result;
}

} // namespace NKikimr::NSchemeShard
