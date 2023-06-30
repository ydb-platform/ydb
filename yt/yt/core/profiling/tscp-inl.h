#ifndef TSCP_INL_H_
#error "Direct inclusion of this file is not allowed, include tscp.h"
// For the sake of sane code completion.
#include "tscp.h"
#endif
#undef TSCP_INL_H_

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

inline TTscp TTscp::Get()
{
#if defined(__x86_64__)
    ui64 rax, rcx, rdx;
    asm volatile ( "rdtscp\n" : "=a" (rax), "=c" (rcx), "=d" (rdx) : : );
    ui64 t = (rdx << 32) + rax;
    ui64 c = rcx;
#elif defined(__arm64__) || defined(__aarch64__)
    ui64 c;
    __asm__ volatile("mrs %x0, tpidrro_el0" : "=r"(c));
    c = c & 0x07u;
    TCpuInstant t = GetCpuInstant();
#endif
    return TTscp{
        .Instant = static_cast<TCpuInstant>(t),
        .ProcessorId = static_cast<int>(c) & (MaxProcessorId - 1)
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
