#include <yt/yt/core/test_framework/framework.h>

#ifdef _linux_

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/net/connection.h>

#include <yt/yt/library/containers/process.h>

#include <yt/yt/library/containers/config.h>
#include <yt/yt/library/containers/porto_executor.h>
#include <yt/yt/library/containers/instance.h>

#include <util/system/platform.h>
#include <util/system/env.h>

namespace NYT::NContainers {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TPortoProcessTest
    : public ::testing::Test
{
    void SetUp() override
    {
        if (GetEnv("SKIP_PORTO_TESTS") != "") {
            GTEST_SKIP();
        }
    }
};

static TString GetUniqueName()
{
    return "yt_ut_" + ToString(TGuid::Create());
}

IPortoExecutorPtr CreatePortoExecutor()
{
    return CreatePortoExecutor(New<TPortoExecutorDynamicConfig>(), "default");
}

TEST_F(TPortoProcessTest, Basic)
{
    auto launcher = CreatePortoInstanceLauncher(
        GetUniqueName(),
        CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/ls", launcher, true);
    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
    p->GetInstance()->Destroy();
}

TEST_F(TPortoProcessTest, RunFromPathEnv)
{
    auto launcher = CreatePortoInstanceLauncher(
        GetUniqueName(),
        CreatePortoExecutor());
    auto p = New<TPortoProcess>("ls", launcher, true);
    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
    p->GetInstance()->Destroy();
}

TEST_F(TPortoProcessTest, MultiBasic)
{
    auto portoExecutor = CreatePortoExecutor();
    auto l1 = CreatePortoInstanceLauncher(GetUniqueName(), portoExecutor);
    auto l2 = CreatePortoInstanceLauncher(GetUniqueName(), portoExecutor);
    auto p1 = New<TPortoProcess>("/bin/ls", l1, true);
    auto p2 = New<TPortoProcess>("/bin/ls", l2, true);
    TFuture<void> f1;
    TFuture<void> f2;
    ASSERT_NO_THROW(f1 = p1->Spawn());
    ASSERT_NO_THROW(f2 = p2->Spawn());
    auto error = WaitFor((AllSucceeded(std::vector<TFuture<void>>{f1, f2})));
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p1->IsFinished());
    EXPECT_TRUE(p2->IsFinished());
    p1->GetInstance()->Destroy();
    p2->GetInstance()->Destroy();
}

TEST_F(TPortoProcessTest, InvalidPath)
{
    auto portoExecutor = CreatePortoExecutor();
    auto launcher = CreatePortoInstanceLauncher(
        GetUniqueName(),
        portoExecutor);
    auto p = New<TPortoProcess>("/some/bad/path/binary", launcher, true);
    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_FALSE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_FALSE(p->IsFinished());
    EXPECT_FALSE(error.IsOK());
    WaitFor(portoExecutor->DestroyContainer(launcher->GetName()))
        .ThrowOnError();
}

TEST_F(TPortoProcessTest, StdOut)
{
    auto launcher = CreatePortoInstanceLauncher(
        GetUniqueName(),
        CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/date", launcher, true);

    auto outStream = p->GetStdOutReader();
    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());

    auto buffer = TSharedMutableRef::Allocate(4_KB, {.InitializeStorage = false});
    auto future = outStream->Read(buffer);
    TErrorOr<size_t> result = WaitFor(future);
    size_t sz = result.ValueOrThrow();
    EXPECT_TRUE(sz > 0);
    p->GetInstance()->Destroy();
}

TEST_F(TPortoProcessTest, GetCommandLine)
{
    auto launcher = CreatePortoInstanceLauncher(
        GetUniqueName(),
        CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/bash", launcher, true);
    EXPECT_EQ("/bin/bash", p->GetCommandLine());
    p->AddArgument("-c");
    EXPECT_EQ("/bin/bash -c", p->GetCommandLine());
    p->AddArgument("exit 0");
    EXPECT_EQ("/bin/bash -c \"exit 0\"", p->GetCommandLine());
}

TEST_F(TPortoProcessTest, ProcessReturnCode0)
{
    auto launcher = CreatePortoInstanceLauncher(
        GetUniqueName(),
        CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/bash", launcher, true);
    p->AddArgument("-c");
    p->AddArgument("exit 0");

    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
    p->GetInstance()->Destroy();
}

TEST_F(TPortoProcessTest, ProcessReturnCode123)
{
    auto launcher = CreatePortoInstanceLauncher(
        GetUniqueName(),
        CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/bash", launcher, true);
    p->AddArgument("-c");
    p->AddArgument("exit 123");

    TFuture<void> finished;
    ASSERT_NO_THROW(finished = p->Spawn());
    ASSERT_TRUE(p->IsStarted());
    auto error = WaitFor(finished);
    EXPECT_EQ(EProcessErrorCode::NonZeroExitCode, error.GetCode());
    EXPECT_EQ(123, error.Attributes().Get<int>("exit_code"));
    EXPECT_TRUE(p->IsFinished());
    p->GetInstance()->Destroy();
}

TEST_F(TPortoProcessTest, Params1)
{
    auto launcher = CreatePortoInstanceLauncher(
        GetUniqueName(),
        CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/bash", launcher, true);
    p->AddArgument("-c");
    p->AddArgument("if test 3 -gt 1; then exit 7; fi");

    auto error = WaitFor(p->Spawn());
    EXPECT_FALSE(error.IsOK());
    EXPECT_TRUE(p->IsFinished());
    p->GetInstance()->Destroy();
}

TEST_F(TPortoProcessTest, Params2)
{
    auto launcher = CreatePortoInstanceLauncher(
        GetUniqueName(),
        CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/bash", launcher, true);
    p->AddArgument("-c");
    p->AddArgument("if test 1 -gt 3; then exit 7; fi");

    auto error = WaitFor(p->Spawn());
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
    p->GetInstance()->Destroy();
}

TEST_F(TPortoProcessTest, InheritEnvironment)
{
    const char* name = "SPAWN_TEST_ENV_VAR";
    const char* value = "42";
    setenv(name, value, 1);

    auto launcher = CreatePortoInstanceLauncher(
        GetUniqueName(),
        CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/bash", launcher, true);
    p->AddArgument("-c");
    p->AddArgument("if test $SPAWN_TEST_ENV_VAR = 42; then exit 7; fi");

    auto error = WaitFor(p->Spawn());
    EXPECT_FALSE(error.IsOK());
    EXPECT_TRUE(p->IsFinished());

    unsetenv(name);
    p->GetInstance()->Destroy();
}

TEST_F(TPortoProcessTest, Kill)
{
    auto launcher = CreatePortoInstanceLauncher(
        GetUniqueName(),
        CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/sleep", launcher, true);
    p->AddArgument("5");

    auto finished = p->Spawn();

    NConcurrency::TDelayedExecutor::Submit(
        BIND([&] () {
            p->Kill(SIGKILL);
        }),
        TDuration::MilliSeconds(100));

    auto error = WaitFor(finished);
    EXPECT_FALSE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
    p->GetInstance()->Destroy();
}

TEST_F(TPortoProcessTest, KillFinished)
{
    auto launcher = CreatePortoInstanceLauncher(
        GetUniqueName(),
        CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/bash", launcher, true);
    p->AddArgument("-c");
    p->AddArgument("true");

    auto finished = p->Spawn();

    auto error = WaitFor(finished);
    EXPECT_TRUE(error.IsOK());

    p->Kill(SIGKILL);
    p->GetInstance()->Destroy();
}

TEST_F(TPortoProcessTest, PollDuration)
{
    auto launcher = CreatePortoInstanceLauncher(
        GetUniqueName(),
        CreatePortoExecutor());
    auto p = New<TPortoProcess>("/bin/sleep", launcher, true);
    p->AddArgument("1");

    auto error = WaitFor(p->Spawn());
    EXPECT_TRUE(error.IsOK()) << ToString(error);
    EXPECT_TRUE(p->IsFinished());
    p->GetInstance()->Destroy();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NContainers

#endif
