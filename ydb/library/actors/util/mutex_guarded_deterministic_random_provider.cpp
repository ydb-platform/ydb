#pragma once

#include <mutex>

#include <library/cpp/random/random_provider.h>

class TMutexGuardedDeterministicRandomProvider : public TDeterministicRandomProvider {
public:
    TMutexGuardedDeterministicRandomProvider(ui64 seed)
        : TDeterministicRandomProvider(seed)
    {}

    ui64 GenRand() noexcept override {
        std::lock_guard guard(mutex);
        return TDeterministicRandomProvider::GenRand();
    }

    TGUID GenGuid() noexcept override {
        std::lock_guard guard(mutex);
        return TDeterministicRandomProvider::GenGuid();
    }

    TGUID GenUuid4() noexcept override {
        std::lock_guard guard(mutex);
        return TDeterministicRandomProvider::GenUuid4();
    }

private:
    std::mutex Mutex;
};

TIntrusivePtr<IRandomProvider> CreateMutexGuardedDeterministicRandomProvider(ui64 seed) {
    return TIntrusivePtr<IRandomProvider>(new TMutexGuardedDeterministicRandomProvider(seed));
}
