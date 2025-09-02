#pragma once
#include "defs.h"
#include "mkql_node.h"
#include "mkql_node_visitor.h"

namespace NKikimr {
namespace NMiniKQL {

TCallableVisitFuncProvider GetLiteralPropagationOptimizationFuncProvider();
TRuntimeNode LiteralPropagationOptimization(TRuntimeNode root, const TTypeEnvironment& env, bool inPlace);

}
}
