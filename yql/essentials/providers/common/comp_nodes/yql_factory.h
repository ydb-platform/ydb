#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

TComputationNodeFactory GetYqlFactory(ui32 exprCtxMutableIndex);
TComputationNodeFactory GetYqlFactory();

}
}
