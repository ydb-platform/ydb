#include "schemeshard_export_helpers.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

namespace NKikimr::NSchemeShard {

NKikimrSchemeOp::EPathType GetPathType(const NKikimrScheme::TEvDescribeSchemeResult& describeResult) {
    return describeResult.GetPathDescription().GetSelf().GetPathType();
}

} // namespace NKikimr::NSchemeShard
