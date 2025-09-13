#pragma once

#include <yql/essentials/public/udf/udf_counter.h>

#include <util/generic/hash.h>
#include <util/system/types.h>

#include <algorithm>

namespace NKikimr::NMiniKQL {

struct TAllocState;

struct TAllocCounter {
    void IncreaseUsage(ui64 bytes) {
        CurrentBytes += bytes;
        PeakBytes = std::max(PeakBytes, CurrentBytes); // TODO: do some lock-free magic for peak bytes
    }

    void DecreaseUsage(ui64 bytes) {
        CurrentBytes -= bytes;
    }

    void BindCounter(const NYql::NUdf::TCounter& counter) {
        Counter = counter;
    }

    void UpdateCounter() {
        Counter.Set(PeakBytes);
    }

private:
    ui64 CurrentBytes = {};
    ui64 PeakBytes = {};
    NYql::NUdf::TCounter Counter;
};

class TAllocCountersProvider;
using TAllocCounterId = uintptr_t;

class TAllocCounterGuard {
public:
    explicit TAllocCounterGuard(TAllocCountersProvider* provider, TAllocCounterId counterId);
    explicit TAllocCounterGuard(TAllocCounter* counter, TAllocState* state);
    explicit TAllocCounterGuard(TAllocCounter* counter);

    ~TAllocCounterGuard();

private:
    TAllocCounter* PrevCounter;
    TAllocState* State;
};

class TAllocCountersProvider {
public:
    using TCountersMap = THashMap<TAllocCounterId, std::unique_ptr<TAllocCounter>>;
    using TCountersMapPtr = std::shared_ptr<TCountersMap>;

    explicit TAllocCountersProvider(TCountersMapPtr storage);

    TAllocCounter* GetAllocCounter(TAllocCounterId counterId);
    TAllocCounter* GetAllocCounter(const void* ptr);

    void ReplaceCounterId(TAllocCounterId oldId, const void* newId);
    void UpdateAllCounters();

private:
    TCountersMapPtr Storage;
};

} // namespace NKikimr::NMiniKQL
