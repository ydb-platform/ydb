#pragma once

// Type definitions have moved to ydb/core/kqp/opt/cbo/ (owned by ydb team).
// This file provides backward-compatible aliases in the NYql namespace.
#include <ydb/core/kqp/opt/cbo/cbo_optimizer_new.h>

namespace NYql {

using EOptimizerNodeKind = NKikimr::NKqp::EOptimizerNodeKind;
using IBaseOptimizerNode = NKikimr::NKqp::IBaseOptimizerNode;
using EJoinKind          = NKikimr::NKqp::EJoinKind;
using TCardinalityHints  = NKikimr::NKqp::TCardinalityHints;
using TJoinAlgoHints     = NKikimr::NKqp::TJoinAlgoHints;
using TJoinOrderHints    = NKikimr::NKqp::TJoinOrderHints;
using TOptimizerHints    = NKikimr::NKqp::TOptimizerHints;
using IProviderContext   = NKikimr::NKqp::IProviderContext;
using TBaseProviderContext = NKikimr::NKqp::TBaseProviderContext;
using TRelOptimizerNode  = NKikimr::NKqp::TRelOptimizerNode;
using TJoinOptimizerNode = NKikimr::NKqp::TJoinOptimizerNode;
using IOptimizerNew      = NKikimr::NKqp::IOptimizerNew;
using TCBOSettings       = NKikimr::NKqp::TCBOSettings;
using IOptimizerFactory  = NKikimr::NKqp::IOptimizerFactory;

} // namespace NYql
