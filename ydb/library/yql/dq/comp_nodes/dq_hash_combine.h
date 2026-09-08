#pragma once

#include <util/system/types.h>

#include <functional>

namespace NKikimr::NMiniKQL {

class IComputationNode;
class TCallable;
struct TComputationNodeFactoryContext;

struct TDqHashCombineTestState {
    // Set by the operator to its own sizeof, checked by the test against the size it was compiled with.
    // A mismatch means the operator and the test disagree about this struct, i.e. mixed build artifacts,
    // and every other field below is then meaningless.
    size_t StructSize = 0;
    size_t ProcessFetchedRows = 0; // calls that reached ProcessFetchedRow; must equal InputRows
    bool BypassActivated = false;
    size_t DrainsStarted = 0;
    size_t SpillsStarted = 0;
    size_t ShrinksRequested = 0; // give-backs through the bound operator memory quota
    // what the operator saw, so that a test that expected a spill or a drain can say why it did not happen
    bool SpillingEnabled = false; // the operator has a spiller and may spill
    bool QuotaBound = false;      // an operator memory quota was bound when the operator last read the pressure
    i64 LastAvailability = 0;     // the availability it read from that quota
    size_t InputRows = 0;
    // how the row-sampled pressure refresh actually went: a bound refresh reads the quota, an unbound one
    // falls back to the allocator heuristics
    size_t PressureChecks = 0;
    size_t BoundRefreshes = 0;
    size_t UnboundRefreshes = 0;
    size_t LastBoundRow = 0;   // InputRows at the last bound refresh
    ui64 QuotaPtrFirst = 0;    // the quota pointer the operator saw first and last, to catch it changing
    ui64 QuotaPtrLast = 0;
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
