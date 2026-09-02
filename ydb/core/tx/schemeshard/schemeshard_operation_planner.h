#pragma once

#include "schemeshard_operation_plan.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/vector.h>

namespace NKikimr::NSchemeShard {

struct TOperationContext;

// Operation types that are planned before any part is constructed or proposed.
bool IsPlannedOperationType(NKikimrSchemeOp::EOperationType type);

// Plans one operation whose every transaction is ESchemeOpCreateTable. The transactions are
// the request's, after Phase Zero rewrite; nothing has been split or constructed yet.
//
// The planner owns two decompositions that used to happen elsewhere: the auto-MkDir split of a
// relative Name (formerly TOperation::SplitIntoTransactions for this type) and the per-index,
// per-impl-table, per-sequence expansion of a CopyFromTable (formerly the vector overload of
// CreateCopyTable). It emits blueprints in exactly the part order those produced, so TxPartId
// assignment is unchanged.
//
// Returns TRejectedOperation with the status Propose would have produced when the request
// cannot be planned at all: paths outside the database, a copy source that fails its checks,
// an unknown index type. Everything else is planned and left for the parts' own checks.
TOperationPlanResult PlanCreateTableOperation(
    const TVector<NKikimrSchemeOp::TModifyScheme>& transactions,
    TOperationContext& context);

} // namespace NKikimr::NSchemeShard
