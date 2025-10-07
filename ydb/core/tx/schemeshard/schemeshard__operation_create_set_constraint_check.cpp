#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_utils.h"  // for TransactionTemplate

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/ydb_convert/table_description.h>

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateSetColumnConstraintsCheck(TOperationId opId, const TTxTransaction& tx) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateSetColumnConstraintsCheck);
    TString error = "CreateSetColumnConstraintsCheck is not implemented";
    return {CreateReject(opId, NKikimrScheme::EStatus::StatusPreconditionFailed, std::move(error))};
}

} // namespace NKikimr::NSchemeShard
