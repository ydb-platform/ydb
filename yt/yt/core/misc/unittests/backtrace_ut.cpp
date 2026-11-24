#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/backtrace.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

#ifdef _unix_

TEST(TBacktraceTest, Capture)
{
    auto caller = __builtin_return_address(0);
    auto backtrace = CaptureBacktrace();

    ASSERT_GT(std::ssize(backtrace), 1);
    EXPECT_EQ(backtrace[1], caller);
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
