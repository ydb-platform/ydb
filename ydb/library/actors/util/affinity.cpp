#include "affinity.h"

#ifdef _linux_
#include <sched.h>
#endif

class TAffinity::TImpl {
#ifdef _linux_
    cpu_set_t Mask;
#endif
public:
    TImpl(size_t pid = 0) {
#ifdef _linux_
        int ar = sched_getaffinity(pid, sizeof(cpu_set_t), &Mask);
        Y_DEBUG_ABORT_UNLESS(ar == 0);
#else
        Y_UNUSED(pid);
#endif
    }

    explicit TImpl(const ui8* cpus, ui32 size) {
#ifdef _linux_
        CPU_ZERO(&Mask);
        for (ui32 i = 0; i != size; ++i) {
            if (cpus[i]) {
                CPU_SET(i, &Mask);
            }
        }
#else
        Y_UNUSED(cpus);
        Y_UNUSED(size);
#endif
    }

    void Set(size_t pid) const {
#ifdef _linux_
        int ar = sched_setaffinity(pid, sizeof(cpu_set_t), &Mask);
        Y_DEBUG_ABORT_UNLESS(ar == 0);
#else
        Y_UNUSED(pid);
#endif
    }

    operator TCpuMask() const {
        TCpuMask result;
#ifdef _linux_
        for (ui32 i = 0; i != CPU_SETSIZE; ++i) {
            result.Cpus.emplace_back(CPU_ISSET(i, &Mask));
        }
        result.RemoveTrailingZeros();
#endif
        return result;
    }

};

TAffinity::TAffinity() {
}

TAffinity::~TAffinity() {
}

TAffinity::TAffinity(const ui8* x, ui32 sz) {
    if (x && sz) {
        Impl.Reset(new TImpl(x, sz));
    }
}

TAffinity::TAffinity(const TCpuMask& mask) {
    if (!mask.IsEmpty()) {
        static_assert(sizeof(ui8) == sizeof(mask.Cpus[0]));
        const ui8* x = reinterpret_cast<const ui8*>(&mask.Cpus[0]);
        const ui32 sz = mask.Size();
        Impl.Reset(new TImpl(x, sz));
    }
}

void TAffinity::Current(size_t pid) {
    Impl.Reset(new TImpl(pid));
}

void TAffinity::Set(size_t pid) const {
    if (!!Impl) {
        Impl->Set(pid);
    }
}

bool TAffinity::Empty() const {
    return !Impl;
}

TAffinity::operator TCpuMask() const {
    if (!!Impl) {
        return *Impl;
    }
    return TCpuMask();
}
