#pragma once

#include <contrib/libs/libunwind/include/libunwind.h>

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

class TLibunwindCursor
{
public:
    TLibunwindCursor();
    explicit TLibunwindCursor(const unw_context_t& context);

    bool IsFinished() const;
    const void* GetCurrentIP() const;
    void MoveNext();

private:
    unw_context_t Context_;
    unw_cursor_t Cursor_;

    bool Finished_ = false;

    const void* CurrentIP_ = nullptr;

    void Initialize();
    void ReadCurrentIP();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace
