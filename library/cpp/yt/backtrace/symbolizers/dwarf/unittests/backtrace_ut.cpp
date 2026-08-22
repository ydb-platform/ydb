#include <gtest/gtest.h>

#include <gmock/gmock.h>

#include <library/cpp/yt/backtrace/backtrace.h>

#include <library/cpp/yt/backtrace/cursors/libunwind/libunwind_cursor.h>

namespace NYT::NBacktrace {
namespace {

////////////////////////////////////////////////////////////////////////////////

#ifdef SYMBOLIZED_BUILD

using ::testing::ContainsRegex;

std::string GetStackTrace()
{
    std::array<const void*, 1> buffer;
    NBacktrace::TLibunwindCursor cursor;
    auto backtrace = GetBacktrace(
        &cursor,
        TMutableRange(buffer),
        /*framesToSkip*/ 0);
    return SymbolizeBacktrace(backtrace);
}

TEST(TStackTrace, Format)
{
    ASSERT_THAT(
        GetStackTrace(),
        ContainsRegex(
            "^ 1\\. 0x[0-9a-f]+ in NYT::NBacktrace::\\(anonymous namespace\\)::GetStackTrace.* "
            "at .+/library/cpp/yt/backtrace/symbolizers/dwarf/unittests/backtrace_ut.cpp:[0-9]+\\n"));
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NBacktrace
