#include <mutex>

#include "mutex_guarded_deterministic_random_provider.h"

class TMutexGuardedDeterministicRandomProvider : public IRandomProvider {
public:
    TMutexGuardedDeterministicRandomProvider(ui64 seed)
        : Impl(CreateDeterministicRandomProvider(seed))
    {}

    ui64 GenRand() noexcept override {
        std::lock_guard guard(Mutex);
        return Impl->GenRand();
    }

    TGUID GenGuid() noexcept override {
        std::lock_guard guard(Mutex);
        return Impl->GenGuid();
    }

    TGUID GenUuid4() noexcept override {
        std::lock_guard guard(Mutex);
        return Impl->GenUuid4();
    }

private:
    std::mutex Mutex;
    TIntrusivePtr<IRandomProvider> Impl;
};

TIntrusivePtr<IRandomProvider> CreateMutexGuardedDeterministicRandomProvider(ui64 seed) {
    return TIntrusivePtr<IRandomProvider>(new TMutexGuardedDeterministicRandomProvider(seed));
}
