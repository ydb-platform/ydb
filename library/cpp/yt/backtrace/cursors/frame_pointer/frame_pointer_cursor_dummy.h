#pragma once

#include <library/cpp/yt/memory/safe_memory_reader.h>

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

struct TFramePointerCursorContext
{ };

class TFramePointerCursor
{
public:
    TFramePointerCursor(
        TSafeMemoryReader* memoryReader,
        const TFramePointerCursorContext& context);

    bool IsFinished() const;
    const void* GetCurrentIP() const;
    void MoveNext();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace
