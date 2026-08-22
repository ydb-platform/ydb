#pragma once

#include <library/cpp/yt/backtrace/cursors/frame_pointer/frame_pointer_cursor.h>

#include <contrib/libs/libunwind/include/libunwind.h>

#include <util/system/context.h>

#include <optional>

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

#if !defined(_win64_) && !defined(_win32_)

TFramePointerCursorContext FramePointerCursorContextFromUcontext(const ucontext_t& ucontext);

#endif

std::optional<unw_context_t> TrySynthesizeLibunwindContextFromMachineContext(
    const TContMachineContext& machineContext);

TFramePointerCursorContext FramePointerCursorContextFromLibunwindCursor(
    const unw_cursor_t& uwCursor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace
