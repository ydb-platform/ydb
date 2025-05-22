#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

TComputationNodeFactory GetBuiltinFactory();

TComputationNodeFactory GetCompositeWithBuiltinFactory(TVector<TComputationNodeFactory> factories);

}
}
