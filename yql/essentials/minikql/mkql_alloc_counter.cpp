#include "mkql_alloc_counter.h"

#include "mkql_alloc.h"

namespace NKikimr::NMiniKQL {

TAllocCounterGuard::TAllocCounterGuard(TAllocCounter* counter, TAllocState* state)
    : State(state)
{
    if (State) {
        PrevCounter = State->CurrentCounter;
        State->CurrentCounter = counter;
    }
}

TAllocCounterGuard::TAllocCounterGuard(TAllocCounter* counter)
    : TAllocCounterGuard(counter, TlsAllocState)
{
}

TAllocCounterGuard::TAllocCounterGuard(TAllocCountersProvider* provider, TAllocCounterId counterId)
    : TAllocCounterGuard(provider ? provider->GetAllocCounter(counterId) : nullptr)
{
}

TAllocCounterGuard::~TAllocCounterGuard() {
    if (State) {
        State->CurrentCounter = PrevCounter;
    }
}

TAllocCountersProvider::TAllocCountersProvider(TCountersMapPtr storage)
    : Storage(storage)
{
    if (!Storage) {
        Storage = std::make_shared<TCountersMap>();
    }
}

TAllocCounter* TAllocCountersProvider::GetAllocCounter(TAllocCounterId counterId) {
    auto counterIt = Storage->find(counterId);
    if (counterIt == Storage->end()) {
        counterIt = Storage->emplace(counterId, std::make_unique<TAllocCounter>()).first;
    }
    return counterIt->second.get();
}

TAllocCounter* TAllocCountersProvider::GetAllocCounter(const void* ptr) {
    return GetAllocCounter((uintptr_t)ptr);
}

void TAllocCountersProvider::ReplaceCounterId(TAllocCounterId oldId, const void* newId) {
    auto counterIt = Storage->find(oldId);
    if (counterIt != Storage->end()) {
        Storage->emplace((uintptr_t)newId, std::move(counterIt->second));
        Storage->erase(counterIt);
    }
}

void TAllocCountersProvider::UpdateAllCounters() {
    for (auto& [_, counter] : *Storage) {
        counter->UpdateCounter();
    }
}

} // namespace NKikimr::NMiniKQL
