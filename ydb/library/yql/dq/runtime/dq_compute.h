#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NYql::NDq {

class TDqComputeContextBase : private TNonCopyable {
public:
    virtual ~TDqComputeContextBase() = default;
};

NKikimr::NMiniKQL::TComputationNodeFactory GetDqBaseComputeFactory(const TDqComputeContextBase* computeCtx);

}
