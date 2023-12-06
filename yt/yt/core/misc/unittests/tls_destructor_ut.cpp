#include <thread>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/yt/misc/tls.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

std::atomic<bool> TlsDestructorOfWasCalled = false;

struct TTlsGuard
{
    ~TTlsGuard()
    {
        TlsDestructorOfWasCalled = true;
    }
};

YT_THREAD_LOCAL(TTlsGuard) TlsGuard;

TEST(TTlsDestructorTest, DestructorIsCalled)
{
    // This is the test of compiler in fact. Check expected behaviour.

    std::thread thread{[] {
        Y_UNUSED(TlsGuard); // Important moment. Tls must be touched to be initialized and destructed later.
    }};
    thread.join();

    EXPECT_TRUE(TlsDestructorOfWasCalled);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
