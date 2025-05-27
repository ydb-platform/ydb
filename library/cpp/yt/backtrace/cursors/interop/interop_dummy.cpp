#include "interop.h"

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

TFramePointerCursorContext FramePointerCursorContextFromUcontext(const ucontext_t& /*ucontext*/)
{
    return {};
}

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
