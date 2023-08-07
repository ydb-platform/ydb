#pragma once

#include <library/cpp/yt/memory/safe_memory_reader.h>

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

struct TFramePointerCursorContext
{
    ui64 Rip;
    ui64 Rsp;
    ui64 Rbp;
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
    TSafeMemoryReader* MemoryReader_;
    bool Finished_ = false;
    bool First_ = true;

    const void* Rip_ = nullptr;
    const void* Rbp_ = nullptr;
    const void* StartRsp_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace
