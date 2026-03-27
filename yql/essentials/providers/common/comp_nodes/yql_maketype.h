#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/ast/yql_expr_types.h>

namespace NKikimr::NMiniKQL {

template <NYql::ETypeAnnotationKind Kind>
IComputationNode* WrapMakeType(TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

} // namespace NKikimr::NMiniKQL
