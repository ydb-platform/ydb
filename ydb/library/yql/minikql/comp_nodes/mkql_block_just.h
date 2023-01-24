#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapBlockJust(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}
