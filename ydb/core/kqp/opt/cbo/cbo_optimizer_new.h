#pragma once

// ydb/core/kqp/opt/cbo/cbo_optimizer_new.h
//
// Re-exports NYql CBO optimizer types into NKikimr::NKqp.
// All optimizer interfaces (IProviderContext, IBaseOptimizerNode, IOptimizerNew, …)
// are aliased to their NYql:: counterparts so that:
//   * solver code living in NYql::NDq that uses bare IProviderContext sees the
//     same type as KQP code using NKikimr::NKqp::IProviderContext, and
//   * KQP can extend NYql::IProviderContext to add KQP-specific cost functions
//     without needing a separate vtable.

#include <yql/essentials/core/cbo/cbo_optimizer_new.h>
#include "cbo_interesting_orderings.h"
#include "kqp_statistics.h"

namespace NKikimr::NKqp {

// ── Optimizer node kind ──────────────────────────────────────────────────────
using EOptimizerNodeKind     = NYql::EOptimizerNodeKind;
using NYql::RelNodeType;
using NYql::JoinNodeType;
using IBaseOptimizerNode     = NYql::IBaseOptimizerNode;

// ── Join kind ────────────────────────────────────────────────────────────────
using EJoinKind              = NYql::EJoinKind;
// Unscoped enum values — bring them into the NKikimr::NKqp scope explicitly.
using NYql::InnerJoin;
using NYql::LeftJoin;
using NYql::RightJoin;
using NYql::OuterJoin;
using NYql::LeftOnly;
using NYql::RightOnly;
using NYql::LeftSemi;
using NYql::RightSemi;
using NYql::Cross;
using NYql::Exclusion;
using NYql::ConvertToJoinKind;
using NYql::ConvertToJoinString;

// ── Hints ────────────────────────────────────────────────────────────────────
using TCardinalityHints      = NYql::TCardinalityHints;
using TJoinAlgoHints         = NYql::TJoinAlgoHints;
using TJoinOrderHints        = NYql::TJoinOrderHints;
using TOptimizerHints        = NYql::TOptimizerHints;
using TBytesHints            = NYql::TCardinalityHints;  // alias for clarity

// ── Provider context & base ──────────────────────────────────────────────────
using IProviderContext       = NYql::IProviderContext;
using TBaseProviderContext   = NYql::TBaseProviderContext;

// ── Optimizer nodes ──────────────────────────────────────────────────────────
using TRelOptimizerNode      = NYql::TRelOptimizerNode;
using TJoinOptimizerNode     = NYql::TJoinOptimizerNode;

// ── Optimizer & factory ──────────────────────────────────────────────────────
using IOptimizerNew          = NYql::IOptimizerNew;
using TCBOSettings           = NYql::TCBOSettings;
using IOptimizerFactory      = NYql::IOptimizerFactory;

} // namespace NKikimr::NKqp
