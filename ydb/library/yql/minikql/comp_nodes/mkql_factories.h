#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

TComputationNodeFactory GetBuiltinFactory();

TComputationNodeFactory GetCompositeWithBuiltinFactory(TVector<TComputationNodeFactory> factories);

}
}
