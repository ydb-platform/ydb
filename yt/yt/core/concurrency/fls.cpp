#include "fls.h"

#include <library/cpp/yt/threading/fork_aware_spin_lock.h>

#include <library/cpp/yt/misc/tls.h>

#include <library/cpp/yt/memory/leaky_singleton.h>

#include <array>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

constexpr int MaxFlsSize = 256;
std::atomic<int> FlsSize;

YT_DECLARE_SPIN_LOCK(NThreading::TForkAwareSpinLock, FlsLock);
std::array<TFlsSlotDtor, MaxFlsSize> FlsDtors;

YT_DEFINE_THREAD_LOCAL(TFls*, PerThreadFls);
YT_DEFINE_THREAD_LOCAL(TFls*, CurrentFls);

int AllocateFlsSlot(TFlsSlotDtor dtor)
{
    auto guard = Guard(FlsLock);

    int index = FlsSize++;
    YT_VERIFY(index < MaxFlsSize);

    FlsDtors[index] = dtor;

    return index;
}

void DestructFlsSlot(int index, TFls::TCookie cookie)
{
    FlsDtors[index](cookie);
}

class TGlobalFlsPool
{
public:
    static TGlobalFlsPool* Get()
    {
        return LeakySingleton<TGlobalFlsPool>();
    }

    TFls* Allocate()
    {
        auto* fls = new TFls();
        Pool_.push_back(fls);
        return fls;
    }

private:
    std::vector<TFls*> Pool_;

#ifdef __clang__
    __attribute__((destructor(101))) static void Cleanup()
    {
        Get()->DoCleanup();
    }

    void DoCleanup()
    {
        while (!Pool_.empty()) {
            auto* fls = Pool_.back();
            delete fls;
            Pool_.pop_back();
        }
        // Ensure vector's memory is released.
        Pool_ = {};
    }
#endif
};

YT_PREVENT_TLS_CACHING TFls* GetPerThreadFls()
{
    auto& perThreadFls = PerThreadFls();
    if (perThreadFls) {
        // Fast path: per-thread FLS is already allocated.
        return perThreadFls;
    }

    static thread_local bool perThreadFlsDestroyed;
    if (perThreadFlsDestroyed) {
        // Per-thread FLS was created but already destroyed.
        // Last resort: allocate from a global pool.
        perThreadFls = TGlobalFlsPool::Get()->Allocate();
        return perThreadFls;
    }

    // Allocate a per-thread FLS and install cleanup handler.
    struct TPerThreadDestroyer
    {
        ~TPerThreadDestroyer()
        {
            auto& perThreadFls = PerThreadFls();
            delete perThreadFls;
            perThreadFls = nullptr;
            perThreadFlsDestroyed = true;
        }
    };
    static thread_local TPerThreadDestroyer destroyer;
    perThreadFls = new TFls();
    return perThreadFls;
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TFls::~TFls()
{
    for (int index = 0; index < std::ssize(Slots_); ++index) {
        if (auto cookie = Slots_[index]) {
            NDetail::DestructFlsSlot(index, cookie);
        }
    }
}

void TFls::Set(int index, TCookie cookie)
{
    if (index >= std::ssize(Slots_)) [[unlikely]] {
        int newSize = NDetail::FlsSize.load();
        YT_VERIFY(index < newSize);
        Slots_.resize(newSize);
    }
    Slots_[index] = cookie;
}

////////////////////////////////////////////////////////////////////////////////

TFls* SwapCurrentFls(TFls* newFls)
{
    return std::exchange(NDetail::CurrentFls(), newFls);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
