#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_utils.h"  // for TransactionTemplate

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/ydb_convert/table_description.h>

namespace NKikimr {
namespace NSchemeShard {
    TVector<ISubOperation::TPtr> CreateChangeConstraint(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
        Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateChangeConstraint);
        Y_UNUSED(opId);
        Y_UNUSED(context);
        return {};
    }
} // namespace NKikimr
} // namespace NSchemeShard
