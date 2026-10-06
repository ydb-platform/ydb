#pragma once

#include <functional>
#include <optional>
#include <vector>

#include <yql/essentials/public/udf/udf_value.h>

namespace NKikimr::NMiniKQL {

class IComputationNode;
class TCallable;
struct TComputationNodeFactoryContext;

struct TDqHashCombineTestState {
    bool BypassActivated = false;
    bool FastFinalizeEnabled = false;
    size_t SpillingBucketsRead = 0;
    std::optional<double> InputRowMemoryUsageMultiplier;
};

using TTestStateCallback = std::function<void(const TDqHashCombineTestState&)>;
using TTestStateSnapshotCallback = std::function<void(std::vector<NUdf::TUnboxedValue>)>;

class TDqHashCombineTestPoints {
public:
    virtual void DisableKeyPassthrough(const bool disable) = 0;
    virtual void SetTestStateCallback(const TTestStateCallback& callback) = 0;
    virtual void SetStateSnapshotOnDestroy(const TTestStateSnapshotCallback& callback) = 0;
};

static constexpr const size_t DqAggregationPrefetchBatchSize = 10;

IComputationNode* WrapDqHashCombine(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapDqHashAggregate(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL
