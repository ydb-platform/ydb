#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapBlockVariantItem(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL
