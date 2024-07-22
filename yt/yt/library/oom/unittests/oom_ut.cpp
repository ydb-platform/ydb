#include <gtest/gtest.h>

#include <yt/yt/library/oom/oom.h>

#include <util/datetime/base.h>
#include <util/system/fs.h>
#include <util/generic/size_literals.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(EarlyOOM, Crash)
{
    auto checkOOM = [] {
        EnableEarlyOOMWatchdog(TOOMOptions{
            .MemoryLimit = 0,
        });

        Sleep(TDuration::Seconds(5));
    };

    ASSERT_DEATH(checkOOM(), "");

    ASSERT_TRUE(NFs::Exists("oom.pb.gz"));
}

TEST(EarlyOOM, NoCrash)
{
    EnableEarlyOOMWatchdog(TOOMOptions{
        .MemoryLimit = 1_GB,
    });

    Sleep(TDuration::Seconds(5));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
