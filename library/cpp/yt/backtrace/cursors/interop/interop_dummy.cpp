#include "interop.h"

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

#if !defined(_win32_) && !defined(_win64_)

TFramePointerCursorContext FramePointerCursorContextFromUcontext(const ucontext_t& /*ucontext*/)
{
    return {};
}

#endif

std::optional<unw_context_t> TrySynthesizeLibunwindContextFromMachineContext(
    const TContMachineContext& /*machineContext*/)
{
    return {};
}

TFramePointerCursorContext FramePointerCursorContextFromLibunwindCursor(
    const unw_cursor_t& /*cursor*/)
{
    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace
