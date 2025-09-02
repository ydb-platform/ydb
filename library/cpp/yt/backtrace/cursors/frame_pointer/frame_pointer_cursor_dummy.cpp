#include "frame_pointer_cursor_dummy.h"

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

TFramePointerCursor::TFramePointerCursor(
    TSafeMemoryReader* /*memoryReader*/,
    const TFramePointerCursorContext& /*context*/)
{ }

bool TFramePointerCursor::IsFinished() const
{
    return true;
}

const void* TFramePointerCursor::GetCurrentIP() const
{
    return nullptr;
}

void TFramePointerCursor::MoveNext()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace
