#pragma once

#include "schemeshard_operation_plan.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/vector.h>

namespace NKikimr::NSchemeShard {

struct TOperationContext;
class TPath;

// Operation types that are planned before any part is constructed or proposed.
bool IsPlannedOperationType(NKikimrSchemeOp::EOperationType type);

// Plans one operation whose every transaction is of a planned type. The transactions are the
// request's, after Phase Zero rewrite; nothing has been split or constructed yet.
//
// The planner owns the decompositions that used to happen elsewhere: the auto-MkDir split of
// a relative Name, the per-index, per-impl-table, per-sequence expansion of a CopyFromTable,
// and the cascade beneath a dropped table. It emits blueprints in exactly the part order those
// produced, so TxPartId assignment is unchanged.
//
// Returns TRejectedOperation with the status Propose would have produced when the request
// cannot be planned at all. Everything else is planned and left for the parts' own checks.
TOperationPlanResult PlanOperation(
    const TVector<NKikimrSchemeOp::TModifyScheme>& transactions,
    TOperationContext& context);

// The cascade beneath an existing table -- its indexes and their impl tables, its streams and
// their topics, its sequences -- as the parts of an operation that is not planned as a whole.
// The plan's single request holds only those parts; the table itself is a reference they are
// bound to as their container.
TOperationPlanResult PlanDropTableChildren(const TPath& table, TOperationContext& context);

} // namespace NKikimr::NSchemeShard
