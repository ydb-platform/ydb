#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/proc.h>

#ifdef _unix_
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#endif

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

#ifdef _unix_

static int PipeDescriptors[2];

static void SignalHandler(int /*signum*/)
{
    Y_UNUSED(::write(PipeDescriptors[1], "got signal\n", 11));
}

TEST(TJobSignaler, Basic)
{
    ASSERT_EQ(0, ::pipe(PipeDescriptors));
    ASSERT_EQ(0, ::fcntl(PipeDescriptors[0], F_SETFL, ::fcntl(PipeDescriptors[0], F_GETFL) | O_NONBLOCK));

    int pid = ::fork();
    ASSERT_TRUE(pid >= 0);

    if (pid == 0) {
        struct sigaction new_action;
        new_action.sa_handler = SignalHandler;
        sigemptyset(&new_action.sa_mask);
        new_action.sa_flags = 0;
        ::sigaction(SIGUSR1, &new_action, nullptr);

        while (true) {
            Sleep(TDuration::MilliSeconds(100));
        }

        _exit(0);
    }

    Sleep(TDuration::MilliSeconds(100));
    SendSignal({pid}, "SIGUSR1");
    Sleep(TDuration::MilliSeconds(100));

    char buffer[256];
    ASSERT_EQ(11, ::read(PipeDescriptors[0], buffer, sizeof(buffer)));
    ASSERT_EQ(0, ::memcmp(buffer, "got signal\n", 11));

    ASSERT_EQ(0, ::kill(pid, SIGKILL));

    ASSERT_EQ(pid, ::waitpid(pid, nullptr, 0));
}

TEST(TJobSignaler, UnknownSignal)
{
    int pid = ::getpid();
    ASSERT_THROW(SendSignal({pid}, "SIGUNKNOWN"), std::exception);
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
