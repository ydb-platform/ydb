#include <yt/yt/library/process/process.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/net/connection.h>

#include <library/cpp/yt/system/handle_eintr.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

#if defined(_unix_) and not defined(_asan_enabled_)

TEST(TProcessTest, Basic)
{
    auto p = New<TSimpleProcess>("/bin/ls");
    TFuture<void> finished;

    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
}

// NB: We cannot rely on 'ls' and 'sleep' in arcadia tests.
TEST(TProcessTest, RunFromPathEnv)
{
    auto p = New<TSimpleProcess>("/bin/ls", false);
    TFuture<void> finished;

    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
}

TEST(TProcessTest, PollDuration)
{
    auto p = New<TSimpleProcess>("/bin/sleep", true, TDuration::MilliSeconds(1));
    p->AddArgument("0.1");

    auto error = WaitFor(p->Spawn());
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
}

TEST(TProcessTest, InvalidPath)
{
    auto p = New<TSimpleProcess>("/some/bad/path/binary");

    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_FALSE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_FALSE(p->IsFinished());
    EXPECT_FALSE(error.IsOK());
}

TEST(TProcessTest, StdOut)
{
    auto p = New<TSimpleProcess>("/bin/date");

    auto outStream = p->GetStdOutReader();
    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());

    auto buffer = TSharedMutableRef::Allocate(4_KB, {.InitializeStorage = false});
    auto future = outStream->Read(buffer);
    auto result = WaitFor(future);
    size_t sz = result.ValueOrThrow();
    EXPECT_TRUE(sz > 0);
}

TEST(TSimpleProcess, GetCommandLine1)
{
    auto p = New<TSimpleProcess>("/bin/bash");
    EXPECT_EQ("/bin/bash", p->GetCommandLine());
    p->AddArgument("-c");
    EXPECT_EQ("/bin/bash -c", p->GetCommandLine());
    p->AddArgument("exit 0");
    EXPECT_EQ("/bin/bash -c \"exit 0\"", p->GetCommandLine());
}

TEST(TProcessBase, GetCommandLine2)
{
    auto p = New<TSimpleProcess>("/bin/bash");
    EXPECT_EQ("/bin/bash", p->GetCommandLine());
    p->AddArgument("-c");
    EXPECT_EQ("/bin/bash -c", p->GetCommandLine());
    p->AddArgument("\"quoted\"");
    EXPECT_EQ("/bin/bash -c \"\\\"quoted\\\"\"", p->GetCommandLine());
}

TEST(TProcessTest, ProcessReturnCode0)
{
    auto p = New<TSimpleProcess>("/bin/bash");
    p->AddArgument("-c");
    p->AddArgument("exit 0");

    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
}

TEST(TProcessTest, ProcessReturnCode123)
{
    auto p = New<TSimpleProcess>("/bin/bash");
    p->AddArgument("-c");
    p->AddArgument("exit 123");

    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_EQ(EProcessErrorCode::NonZeroExitCode, error.GetCode());
    EXPECT_EQ(123, error.Attributes().Get<int>("exit_code"));
    EXPECT_TRUE(p->IsFinished());
}

TEST(TProcessTest, Params1)
{
    auto p = New<TSimpleProcess>("/bin/bash");
    p->AddArgument("-c");
    p->AddArgument("if test 3 -gt 1; then exit 7; fi");

    auto error = WaitFor(p->Spawn());
    EXPECT_FALSE(error.IsOK());
    EXPECT_TRUE(p->IsFinished());
}

TEST(TProcessTest, Params2)
{
    auto p = New<TSimpleProcess>("/bin/bash");
    p->AddArgument("-c");
    p->AddArgument("if test 1 -gt 3; then exit 7; fi");

    auto error = WaitFor(p->Spawn());
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
}

TEST(TProcessTest, InheritEnvironment)
{
    const char* name = "SPAWN_TEST_ENV_VAR";
    const char* value = "42";
    setenv(name, value, 1);

    auto p = New<TSimpleProcess>("/bin/bash");
    p->AddArgument("-c");
    p->AddArgument("if test $SPAWN_TEST_ENV_VAR = 42; then exit 7; fi");

    auto error = WaitFor(p->Spawn());
    EXPECT_FALSE(error.IsOK());
    EXPECT_TRUE(p->IsFinished());

    unsetenv(name);
}

TEST(TProcessTest, Kill)
{
    auto p = New<TSimpleProcess>("/bin/sleep");
    p->AddArgument("5");

    auto finished = p->Spawn();

    NConcurrency::TDelayedExecutor::Submit(
        BIND([&] {
            p->Kill(SIGKILL);
        }),
        TDuration::MilliSeconds(100));

    auto error = WaitFor(finished);
    EXPECT_FALSE(error.IsOK());
    EXPECT_TRUE(p->IsFinished());
}

TEST(TProcessTest, KillFinished)
{
    auto p = New<TSimpleProcess>("/bin/bash");
    p->AddArgument("-c");
    p->AddArgument("true");

    auto finished = p->Spawn();

    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK());

    p->Kill(SIGKILL);
}

TEST(TProcessTest, KillZombie)
{
    auto p = New<TSimpleProcess>("/bin/bash");
    p->AddArgument("-c");
    p->AddArgument("/bin/sleep 1; /bin/true");

    auto finished = p->Spawn();

    siginfo_t infop;
    auto res = HandleEintr(::waitid, P_PID, p->GetProcessId(), &infop, WEXITED | WNOWAIT);
    EXPECT_EQ(0, res)
        << "errno = " << errno;
    EXPECT_EQ(p->GetProcessId(), infop.si_pid);

    p->Kill(SIGKILL);
    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK())
        << ToString(error);
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
