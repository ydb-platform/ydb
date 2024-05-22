#include <thread>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/yt/misc/tls.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

std::atomic<bool> TlsDestructorWasCalled = false;

struct TTlsGuard
{
    ~TTlsGuard()
    {
        TlsDestructorWasCalled = true;
    }
};

YT_DEFINE_THREAD_LOCAL(TTlsGuard, TlsGuard);

TEST(TTlsDestructorTest, DestructorIsCalled)
{
    // This is the test of compiler in fact. Check expected behaviour.

    std::thread thread{[] {
        // Important moment. TLS must be touched to be initialized and destructed later.
        Y_UNUSED(TlsGuard());
    }};
    thread.join();

    EXPECT_TRUE(TlsDestructorWasCalled);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
