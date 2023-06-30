#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/mkql_node.h>

namespace NYql::NDqs {

NKikimr::NMiniKQL::IComputationNode* WrapYtDqRowsWideWrite(NKikimr::NMiniKQL::TCallable& callable, const NKikimr::NMiniKQL::TComputationNodeFactoryContext& ctx);

} // NYql
