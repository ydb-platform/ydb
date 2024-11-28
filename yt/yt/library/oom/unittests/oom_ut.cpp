#include <gtest/gtest.h>

#include <yt/yt/library/oom/oom.h>

#include <util/datetime/base.h>
#include <util/system/fs.h>
#include <util/generic/size_literals.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TEarlyOomTest, Crash)
{
    auto checkOom = [] {
        EnableEarlyOomWatchdog(TOomWatchdogOptions{
            .MemoryLimit = 0,
        });

        Sleep(TDuration::Seconds(5));
    };

    ASSERT_DEATH(checkOom(), "");

    ASSERT_TRUE(NFs::Exists("oom.pb.gz"));
}

TEST(TEarlyOomTest, NoCrash)
{
    EnableEarlyOomWatchdog(TOomWatchdogOptions{
        .MemoryLimit = 1_GB,
    });

    Sleep(TDuration::Seconds(5));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
