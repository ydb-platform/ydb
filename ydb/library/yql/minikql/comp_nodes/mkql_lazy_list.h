#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapLazyList(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}

