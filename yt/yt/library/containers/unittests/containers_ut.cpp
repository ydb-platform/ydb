#include <yt/yt/core/test_framework/framework.h>

#ifdef _linux_

#include <yt/yt/library/containers/config.h>
#include <yt/yt/library/containers/porto_executor.h>
#include <yt/yt/library/containers/instance.h>

#include <util/system/platform.h>
#include <util/system/env.h>

namespace NYT::NContainers {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TContainersTest
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

TEST_F(TContainersTest, ListSubcontainers)
{
    auto executor = CreatePortoExecutor();
    auto name = GetUniqueName();

    WaitFor(executor->CreateContainer(name))
        .ThrowOnError();

    auto absoluteName = *WaitFor(executor->GetContainerProperty(name, "absolute_name"))
        .ValueOrThrow();

    auto nestedName = absoluteName + "/nested";
    WaitFor(executor->CreateContainer(nestedName))
        .ThrowOnError();

    auto withRoot = WaitFor(executor->ListSubcontainers(name, true))
        .ValueOrThrow();
    EXPECT_EQ(std::vector<TString>({absoluteName, nestedName}), withRoot);

    auto withoutRoot = WaitFor(executor->ListSubcontainers(name, false))
        .ValueOrThrow();
    EXPECT_EQ(std::vector<TString>({nestedName}), withoutRoot);

    WaitFor(executor->DestroyContainer(absoluteName))
        .ThrowOnError();
}

// See https://st.yandex-team.ru/PORTO-846.
TEST_F(TContainersTest, DISABLED_WaitContainer)
{
    auto executor = CreatePortoExecutor();
    auto name = GetUniqueName();

    WaitFor(executor->CreateContainer(name))
        .ThrowOnError();

    WaitFor(executor->SetContainerProperty(name, "command", "sleep 10"))
        .ThrowOnError();

    WaitFor(executor->StartContainer(name))
        .ThrowOnError();

    auto exitCode = WaitFor(executor->WaitContainer(name))
        .ValueOrThrow();

    EXPECT_EQ(0, exitCode);

    WaitFor(executor->DestroyContainer(name))
        .ThrowOnError();
}

TEST_F(TContainersTest, CreateFromSpec)
{
    auto executor = CreatePortoExecutor();
    auto name = GetUniqueName();

    auto spec = TRunnableContainerSpec {
        .Name = name,
        .Command = "sleep 2",
    };

    WaitFor(executor->CreateContainer(spec, /*start*/ true))
        .ThrowOnError();

    auto exitCode = WaitFor(executor->PollContainer(name))
        .ValueOrThrow();

    EXPECT_EQ(0, exitCode);

    WaitFor(executor->DestroyContainer(name))
        .ThrowOnError();
}

TEST_F(TContainersTest, ListPids)
{
    auto launcher = CreatePortoInstanceLauncher(
        GetUniqueName(),
        CreatePortoExecutor());

    auto instance = WaitFor(launcher->Launch("sleep", {"5"}, {}))
        .ValueOrThrow();

    auto pids = instance->GetPids();
    EXPECT_LT(0u, pids.size());

    instance->Destroy();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NContainers

#endif
