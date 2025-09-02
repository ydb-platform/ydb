#pragma once

#include <library/cpp/yt/memory/safe_memory_reader.h>

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

struct TFramePointerCursorContext
{
#ifdef _x86_64_
    ui64 Rip;
    ui64 Rsp;
    ui64 Rbp;
#endif
};

class TFramePointerCursor
{
public:
    TFramePointerCursor(
        TSafeMemoryReader* memoryReader,
        const TFramePointerCursorContext& context);

    bool IsFinished() const;
    const void* GetCurrentIP() const;
    void MoveNext();

private:
#ifdef _x86_64_
    TSafeMemoryReader* const MemoryReader_;
    bool Finished_ = false;
    bool First_ = true;

    const void* Rip_ = nullptr;
    const void* Rbp_ = nullptr;
    const void* StartRsp_ = nullptr;
#endif
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace
