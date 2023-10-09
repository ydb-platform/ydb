#include "memory.h"
#include <util/system/guard.h>

namespace NKikimr::NOlap {

TScanMemoryCounter::TScanMemoryCounter(const TString& limitName, const ui64 memoryLimit)
    : TBase(limitName)
{
    AvailableMemory = TBase::GetValue("Available");
    MinimalMemory = TBase::GetValue("Minimal");
    DeriviativeTake = TBase::GetDeriviative("Take");
    DeriviativeFree = TBase::GetDeriviative("Free");
    AvailableMemory->Set(memoryLimit);

    DeriviativeWaitingStart = TBase::GetDeriviative("Waiting/Start");
    DeriviativeWaitingFinish = TBase::GetDeriviative("Waiting/Finish");
    CurrentInWaiting = TBase::GetValue("InWaiting");
}

void TScanMemoryLimiter::Free(const ui64 size) {
    const i64 newVal = AvailableMemory.Add(size);
    Y_ABORT_UNLESS(newVal <= AvailableMemoryLimit);
    Counters.Free(size);
    if (newVal > 0 && newVal <= (i64)size) {
        std::vector<std::shared_ptr<IMemoryAccessor>> accessors;
        {
            ::TGuard<TMutex> g(Mutex);
            while (AvailableMemory.Val() > 0 && InWaiting.size()) {
                accessors.emplace_back(InWaiting.front());
                InWaiting.pop_front();
                Counters.WaitFinish();
            }
            WaitingCounter = InWaiting.size();
        }
        for (auto&& i : accessors) {
            i->OnBufferReady();
        }
    }
}

bool TScanMemoryLimiter::HasBufferOrSubscribe(std::shared_ptr<IMemoryAccessor> accessor) {
    if (AvailableMemory.Val() > 0) {
        return true;
    } else if (!accessor) {
        return false;
    }
    ::TGuard<TMutex> g(Mutex);
    if (accessor->InWaiting()) {
        return false;
    }
    if (AvailableMemory.Val() > 0) {
        return true;
    }
    accessor->StartWaiting();
    WaitingCounter.Inc();
    InWaiting.emplace_back(accessor);
    Counters.WaitStart();
    return false;
}

void TScanMemoryLimiter::Take(const ui64 size) {
    const i64 val = AvailableMemory.Sub(size);
    Counters.Take(size);
    AFL_TRACE(NKikimrServices::OBJECTS_MONITORING)("current_memory", val)("min", MinMemory)("event", "take");
//    ::TGuard<TMutex> g(Mutex);
//    if (MinMemory > val) {
//        MinMemory = val;
//        Counters.OnMinimal(val);
//    }
}

void TScanMemoryLimiter::TGuard::FreeAll() {
    if (MemorySignals) {
        MemorySignals->RemoveBytes(Value.Val());
    }
    if (MemoryAccessor) {
        MemoryAccessor->Free(Value.Val());
    }
    Value = 0;
}

void TScanMemoryLimiter::TGuard::Free(const ui64 size) {
    if (MemoryAccessor) {
        MemoryAccessor->Free(size);
    }
    Y_ABORT_UNLESS(Value.Sub(size) >= 0);
    if (MemorySignals) {
        MemorySignals->RemoveBytes(size);
    }
}

void TScanMemoryLimiter::TGuard::Take(const ui64 size) {
    if (MemoryAccessor) {
        MemoryAccessor->Take(size);
    }
    Value.Add(size);
    if (MemorySignals) {
        MemorySignals->AddBytes(size);
    }
}

}
