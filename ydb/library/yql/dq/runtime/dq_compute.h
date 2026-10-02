#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NYql::NDq {

class TDqComputeContextBase : private TNonCopyable {
public:
    virtual ~TDqComputeContextBase() = default;

    ui64* InputsConsumed = nullptr;
    TInstant* StartTs = nullptr;
};

NKikimr::NMiniKQL::TComputationNodeFactory GetDqBaseComputeFactory(const TDqComputeContextBase* computeCtx);

}
