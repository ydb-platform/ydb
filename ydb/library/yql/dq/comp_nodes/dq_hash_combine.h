#pragma once

#include <util/system/types.h>

#include <functional>

namespace NKikimr::NMiniKQL {

class IComputationNode;
class TCallable;
struct TComputationNodeFactoryContext;

struct TDqHashCombineTestState {
    bool BypassActivated = false;
    size_t DrainsStarted = 0;
    size_t SpillsStarted = 0;
    size_t ShrinksRequested = 0; // give-backs through the bound operator memory quota
    // what the operator saw, so that a test that expected a spill or a drain can say why it did not happen
    bool SpillingEnabled = false; // the operator has a spiller and may spill
    bool QuotaBound = false;      // an operator memory quota was bound when the operator last read the pressure
    i64 LastAvailability = 0;     // the availability it read from that quota
    size_t InputRows = 0;
};

using TTestStateCallback = std::function<void(const TDqHashCombineTestState&)>;

class TDqHashCombineTestPoints {
public:
    virtual void DisableStateDehydration(const bool disable) = 0;
    virtual void DisableKeyPassthrough(const bool disable) = 0;
    virtual void SetTestStateCallback(const TTestStateCallback& callback) = 0;
};

static constexpr const size_t DqAggregationPrefetchBatchSize = 10;

IComputationNode* WrapDqHashCombine(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapDqHashAggregate(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL
