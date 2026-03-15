#pragma once

// YDB-owned header for ordering-related types used by the CBO optimizer.
// The ordering types (TOrderingsStateMachine, TFDStorage, etc.) are tightly
// coupled to TOptimizerStatistics (yql/essentials/core/yql_statistics.h) whose
// fields have these exact types.  To keep assignment compatibility we import
// them as aliases rather than independent copies.
//
// The frozen YQL copy of the definitions lives in
//   yql/essentials/core/cbo/cbo_interesting_orderings.h
// These two are intentionally separate: edits here do NOT affect YQL/YT.

#include <yql/essentials/core/yql_cost_function.h>
#include <yql/essentials/core/cbo/cbo_interesting_orderings.h>

#include <util/generic/hash.h>
#include <util/generic/algorithm.h>

namespace NKikimr::NKqp {

// Non-cbo types from yql/essentials/core — forward the same aliases used
// by cbo_optimizer_new.h so both headers are self-contained.
using TJoinColumn   = NYql::NDq::TJoinColumn;
using EJoinAlgoType = NYql::EJoinAlgoType;
using NYql::AllJoinAlgos;

// Ordering types: aliases to NYql::NDq because TOptimizerStatistics carries
// fields of exactly these types (LogicalOrderings, SortingOrderings, etc.).
using TOrdering                = NYql::NDq::TOrdering;
using TFunctionalDependency    = NYql::NDq::TFunctionalDependency;
using TTableAliasMap           = NYql::NDq::TTableAliasMap;
using TSorting                 = NYql::NDq::TSorting;
using TShuffling               = NYql::NDq::TShuffling;
using TFDStorage               = NYql::NDq::TFDStorage;
using TOrderingsStateMachine   = NYql::NDq::TOrderingsStateMachine;

} // namespace NKikimr::NKqp
