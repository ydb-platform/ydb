#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_utils.h"  // for TransactionTemplate

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/ydb_convert/table_description.h>

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> CreateSetConstraintInitiate(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateSetConstraintInitiate);
    Y_UNUSED(context);
    auto tablePath = NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetSetColumnConstraintsInitiate().GetTableName()});
    TString error = "CreateSetConstraintInitiate is not implemented. TablePath = '" + tablePath + "'";
    return {CreateReject(opId, NKikimrScheme::EStatus::StatusPreconditionFailed, std::move(error))};
}

} // namespace NKikimr::NSchemeShard
