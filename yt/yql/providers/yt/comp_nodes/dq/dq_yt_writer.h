#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_node.h>

namespace NYql::NDqs {

NKikimr::NMiniKQL::IComputationNode* WrapYtDqRowsWideWrite(NKikimr::NMiniKQL::TCallable& callable, const NKikimr::NMiniKQL::TComputationNodeFactoryContext& ctx);

} // NYql
