#pragma once

// ydb/core/kqp/opt/cbo/cbo_interesting_orderings.h
//
// Re-exports NYql::NDq interesting orderings types into NKikimr::NKqp.
// Using type aliases (rather than independent definitions) ensures that
// NKikimr::NKqp::TJoinColumn IS NYql::NDq::TJoinColumn, so values flow
// freely across the bridge where TOptimizerStatistics (NYql) and the
// KQP solver (NKikimr::NKqp) exchange join-column vectors.

#include <yql/essentials/core/cbo/cbo_interesting_orderings.h>
// Note: NYql::EJoinAlgoType and NYql::AllJoinAlgos come from
// yql_cost_function.h which is included transitively above.

namespace NKikimr::NKqp {

using EJoinAlgoType          = NYql::EJoinAlgoType;
using NYql::AllJoinAlgos;

using TJoinColumn            = NYql::NDq::TJoinColumn;
using TOrdering              = NYql::NDq::TOrdering;
using TFunctionalDependency  = NYql::NDq::TFunctionalDependency;
using TTableAliasMap         = NYql::NDq::TTableAliasMap;
using TSorting               = NYql::NDq::TSorting;
using TShuffling             = NYql::NDq::TShuffling;
using TFDStorage             = NYql::NDq::TFDStorage;
using TOrderingsStateMachine = NYql::NDq::TOrderingsStateMachine;

} // namespace NKikimr::NKqp
