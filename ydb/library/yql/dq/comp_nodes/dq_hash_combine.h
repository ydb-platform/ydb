#pragma once

#include <functional>

#include <ydb/library/yql/dq/runtime/dq_compute.h>

namespace NKikimr::NMiniKQL {

class IComputationNode;
class TCallable;
struct TComputationNodeFactoryContext;

struct TDqHashCombineTestState {
    bool BypassActivated = false;
};

using TTestStateCallback = std::function<void(const TDqHashCombineTestState&)>;

class TDqHashCombineTestPoints {
public:
    virtual void DisableStateDehydration(const bool disable) = 0;
    virtual void DisableKeyPassthrough(const bool disable) = 0;
    virtual void SetTestStateCallback(const TTestStateCallback& callback) = 0;
};

IComputationNode* WrapDqHashCombine(TCallable& callable, const TComputationNodeFactoryContext& ctx, NYql::NDq::TDqComputeContextBase& computeCtx);
IComputationNode* WrapDqHashAggregate(TCallable& callable, const TComputationNodeFactoryContext& ctx, NYql::NDq::TDqComputeContextBase& computeCtx);

} // namespace NKikimr::NMiniKQL
