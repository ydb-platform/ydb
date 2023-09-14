#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* TimeOrderRecover(const TComputationNodeFactoryContext& ctx,
    TRuntimeNode inputFlow,
    TRuntimeNode inputRowArg,
    TRuntimeNode rowTime,
    TRuntimeNode inputRowColumnCount,
    TRuntimeNode outOfOrderColumnIndex,
    TRuntimeNode delay,
    TRuntimeNode ahead,
    TRuntimeNode rowLimit);

} //namespace NKikimr::NMiniKQL
