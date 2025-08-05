#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"
#include "schemeshard_path_element.h"
#include "schemeshard_utils.h"
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/ydb_convert/table_description.h>

namespace NKikimr {
namespace NSchemeShard {
    ISubOperation::TPtr CreateChangeConstraintCheck(TOperationId opId, const TTxTransaction& tx) {
        Y_UNUSED(opId);
        Y_UNUSED(tx);
        return {};
    }
} // namespace NKikimr
} // namespace NSchemeShard
