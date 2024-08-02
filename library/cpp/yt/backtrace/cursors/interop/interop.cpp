#include "interop.h"

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

TFramePointerCursorContext FramePointerCursorContextFromUcontext(const ucontext_t& ucontext)
{
#if defined(_linux_) && defined(_x86_64_)
    return {
        .Rip = static_cast<ui64>(ucontext.uc_mcontext.gregs[REG_RIP]),
        .Rsp = static_cast<ui64>(ucontext.uc_mcontext.gregs[REG_RSP]),
        .Rbp = static_cast<ui64>(ucontext.uc_mcontext.gregs[REG_RBP]),
    };
#elif defined(_darwin_) && defined(_x86_64_)
    return {
        .Rip = static_cast<ui64>(ucontext.uc_mcontext->__ss.__rip),
        .Rsp = static_cast<ui64>(ucontext.uc_mcontext->__ss.__rsp),
        .Rbp = static_cast<ui64>(ucontext.uc_mcontext->__ss.__rbp),
    };
#else
    #error Unsupported platform
#endif
}

std::optional<unw_context_t> TrySynthesizeLibunwindContextFromMachineContext(
    const TContMachineContext& machineContext)
{
    unw_context_t unwindContext;
    if (unw_getcontext(&unwindContext) != 0) {
        return {};
    }

    // Some dirty hacks follow.
    struct TUnwindContextRegisters
    {
        ui64 Rax;
        ui64 Rbx;
        ui64 Rcx;
        ui64 Rdx;
        ui64 Rdi;
        ui64 Rsi;
        ui64 Rbp;
        ui64 Rsp;
        ui64 R8;
        ui64 R9;
        ui64 R10;
        ui64 R11;
        ui64 R12;
        ui64 R13;
        ui64 R14;
        ui64 R15;
        ui64 Rip;
        ui64 Rflags;
        ui64 CS;
        ui64 FS;
        ui64 GS;
    };

    struct TMachineContextRegisters
    {
        ui64 Rbx;
        ui64 Rbp;
        ui64 R12;
        ui64 R13;
        ui64 R14;
        ui64 R15;
        ui64 Rsp;
        ui64 Rip;
    };

    static_assert(sizeof(TContMachineContext) >= sizeof(TMachineContextRegisters));
    static_assert(sizeof(unw_context_t) >= sizeof(TUnwindContextRegisters));
    const auto* machineContextRegisters = reinterpret_cast<const TMachineContextRegisters*>(&machineContext);
    auto* unwindContextRegisters = reinterpret_cast<TUnwindContextRegisters*>(&unwindContext);
    #define XX(register) unwindContextRegisters->register = machineContextRegisters->register;
    XX(Rbx)
    XX(Rbp)
    XX(R12)
    XX(R13)
    XX(R14)
    XX(R15)
    XX(Rsp)
    XX(Rip)
    #undef XX
    return unwindContext;
}

TFramePointerCursorContext FramePointerCursorContextFromLibunwindCursor(
    const unw_cursor_t& cursor)
{
    TFramePointerCursorContext context{};
    auto& mutableCursor = const_cast<unw_cursor_t&>(cursor);
    YT_VERIFY(unw_get_reg(&mutableCursor, UNW_REG_IP, &context.Rip) == 0);
    YT_VERIFY(unw_get_reg(&mutableCursor, UNW_X86_64_RSP, &context.Rsp) == 0);
    YT_VERIFY(unw_get_reg(&mutableCursor, UNW_X86_64_RBP, &context.Rbp) == 0);
    return context;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace
