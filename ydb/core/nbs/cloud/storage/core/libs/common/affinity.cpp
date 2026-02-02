#include "affinity.h"

#include "util/system/yassert.h"

#ifdef _linux_
#include <sched.h>
#endif

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

TVector<ui8> GetAffinityCores()
{
    TVector<ui8> cores;

    cpu_set_t mask;
    int res = sched_getaffinity(0, sizeof(cpu_set_t), &mask);
    Y_DEBUG_ABORT_UNLESS(res == 0);

    size_t count = CPU_COUNT(&mask);
    ui8 num = 0;

    cores.reserve(count);

    while (cores.size() < count) {
        if (CPU_ISSET(num, &mask)) {
            cores.push_back(num);
        }
        ++num;
    }

    return cores;
}

void SetAffinityCores(const TVector<ui8>& cores)
{
    cpu_set_t mask;
    CPU_ZERO(&mask);

    for (ui8 core: cores) {
        CPU_SET(core, &mask);
    }

    int res = sched_setaffinity(0, sizeof(cpu_set_t), &mask);
    Y_DEBUG_ABORT_UNLESS(res == 0);
}

#else

TVector<ui8> GetAffinityCores()
{
    return {};
}

void SetAffinityCores(const TVector<ui8>& cores)
{
    Y_UNUSED(cores);
}

#endif

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TAffinity TAffinity::Current()
{
    return TAffinity(GetAffinityCores());
}

TAffinity::TAffinity()
{}

TAffinity::TAffinity(TVector<ui8> cores)
    : Cores(std::move(cores))
{}

void TAffinity::Set() const
{
    SetAffinityCores(Cores);
}

bool TAffinity::Empty() const
{
    return Cores.empty();
}

const TVector<ui8>& TAffinity::GetCores() const
{
    return Cores;
}

////////////////////////////////////////////////////////////////////////////////

struct TAffinityGuard::TImpl
{
private:
    bool Stacked;
    TAffinity OldAffinity;

public:
    TImpl(const TAffinity& affinity)
    {
        Stacked = false;

        if (!affinity.Empty()) {
            OldAffinity = TAffinity::Current();
            affinity.Set();
            Stacked = true;
        }
    }

    ~TImpl()
    {
        Release();
    }

    void Release()
    {
        if (Stacked) {
            OldAffinity.Set();
            Stacked = false;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TAffinityGuard::TAffinityGuard(const TAffinity& affinity)
    : Impl(std::make_unique<TImpl>(affinity))
{}

TAffinityGuard::~TAffinityGuard() = default;

void TAffinityGuard::Release()
{
    Impl->Release();
}


}   // namespace NCloud
