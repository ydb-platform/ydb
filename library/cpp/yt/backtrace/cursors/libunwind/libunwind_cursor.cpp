#include "libunwind_cursor.h"

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

TLibunwindCursor::TLibunwindCursor()
{
    if (unw_getcontext(&Context_) != 0) {
        Finished_ = true;
        return;
    }

    Initialize();
}

TLibunwindCursor::TLibunwindCursor(const unw_context_t& context)
    : Context_(context)
{
    Initialize();
}

void TLibunwindCursor::Initialize()
{
    if (unw_init_local(&Cursor_, &Context_) != 0) {
        Finished_ = true;
        return;
    }

    ReadCurrentIP();
}

bool TLibunwindCursor::IsFinished() const
{
    return Finished_;
}

const void* TLibunwindCursor::GetCurrentIP() const
{
    return CurrentIP_;
}

void TLibunwindCursor::MoveNext()
{
    if (Finished_) {
        return;
    }

    if (unw_step(&Cursor_) <= 0) {
        Finished_ = true;
        return;
    }

    ReadCurrentIP();
}

void TLibunwindCursor::ReadCurrentIP()
{
    unw_word_t ip = 0;
    if (unw_get_reg(&Cursor_, UNW_REG_IP, &ip) < 0) {
        Finished_ = true;
        return;
    }

    CurrentIP_ = reinterpret_cast<const void*>(ip);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace
