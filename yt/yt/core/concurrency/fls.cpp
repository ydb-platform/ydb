#include "fls.h"

#include <library/cpp/yt/threading/fork_aware_spin_lock.h>

#include <library/cpp/yt/misc/tls.h>

#include <util/system/sanitizers.h>

#include <array>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

constexpr int MaxFlsSize = 256;
std::atomic<int> FlsSize;

NThreading::TForkAwareSpinLock FlsLock;
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

TFls* GetPerThreadFls()
{
    auto& perThreadFls = PerThreadFls();
    if (!perThreadFls) {
        // This is only needed when some code attempts to interact with FLS outside of a fiber context.
        // Unfortunately there's no safe place to destroy this FLS upon thread shutdown.
        perThreadFls = new TFls();
        NSan::MarkAsIntentionallyLeaked(perThreadFls);
    }
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
    if (Y_UNLIKELY(index >= std::ssize(Slots_))) {
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

} // namespace NYT::NConcurrency::NDetail

