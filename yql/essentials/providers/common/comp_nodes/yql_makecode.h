#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/ast/yql_expr.h>

namespace NKikimr::NMiniKQL {

template <NYql::TExprNode::EType Type>
IComputationNode* WrapMakeCode(TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

} // namespace NKikimr::NMiniKQL
