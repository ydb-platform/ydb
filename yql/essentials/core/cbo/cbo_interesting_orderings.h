#pragma once

// Type definitions have moved to ydb/core/kqp/opt/cbo/ (owned by ydb team).
// This file provides backward-compatible aliases in the NYql::NDq namespace.
#include <ydb/core/kqp/opt/cbo/cbo_interesting_orderings.h>

namespace NYql::NDq {

using TJoinColumn          = NKikimr::NKqp::TJoinColumn;
using TOrdering            = NKikimr::NKqp::TOrdering;
using TFunctionalDependency = NKikimr::NKqp::TFunctionalDependency;
using TTableAliasMap       = NKikimr::NKqp::TTableAliasMap;
using TSorting             = NKikimr::NKqp::TSorting;
using TShuffling           = NKikimr::NKqp::TShuffling;
using TFDStorage           = NKikimr::NKqp::TFDStorage;
using TOrderingsStateMachine = NKikimr::NKqp::TOrderingsStateMachine;

} // namespace NYql::NDq
