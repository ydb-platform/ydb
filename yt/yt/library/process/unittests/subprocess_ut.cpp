#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/library/process/subprocess.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

#if defined(_unix_) and not defined(_asan_enabled_)

TEST(TSubprocessTest, Basic)
{
    TSubprocess subprocess("/bin/bash");

    subprocess.AddArgument("-c");
    subprocess.AddArgument("true");

    auto result = subprocess.Execute();
    EXPECT_TRUE(result.Status.IsOK());
}


TEST(TSubprocessTest, PipeOutput)
{
    TSubprocess subprocess("/bin/echo");

    subprocess.AddArgument("hello");

    auto result = subprocess.Execute();
    EXPECT_TRUE(result.Status.IsOK());
    TString output(result.Output.Begin(), result.Output.End());
    EXPECT_TRUE(output == "hello\n") << output;
}

TEST(TSubprocessTest, PipeStdin)
{
    auto queue = New<TActionQueue>();

    BIND([] {
        TSubprocess subprocess("/bin/cat");
        subprocess.AddArgument("-");

        auto input = TString("TEST test TEST");
        auto inputRef = TSharedRef::FromString(input);
        auto result = subprocess.Execute(inputRef);
        EXPECT_TRUE(result.Status.IsOK());

        TString output(result.Output.Begin(), result.Output.End());
        EXPECT_EQ(input, output);
    }).AsyncVia(queue->GetInvoker()).Run().Get().ThrowOnError();
}

TEST(TSubprocessTest, PipeBigOutput)
{
    auto queue = New<TActionQueue>();

    auto result = BIND([] {
        TSubprocess subprocess("/bin/bash");

        subprocess.AddArgument("-c");
        subprocess.AddArgument("for i in `/usr/bin/seq 100000`; do echo hello; done; echo world");

        auto result = subprocess.Execute();
        return result.Status.IsOK();
    }).AsyncVia(queue->GetInvoker()).Run().Get().Value();

    EXPECT_TRUE(result);
}


TEST(TSubprocessTest, PipeBigError)
{
    auto queue = New<TActionQueue>();

    auto result = BIND([] {
        TSubprocess subprocess("/bin/bash");

        subprocess.AddArgument("-c");
        subprocess.AddArgument("for i in `/usr/bin/seq 100000`; do echo hello 1>&2; done; echo world");

        auto result = subprocess.Execute();
        return result;
    }).AsyncVia(queue->GetInvoker()).Run().Get().Value();

    EXPECT_TRUE(result.Status.IsOK());
    EXPECT_EQ(6*100000, std::ssize(result.Error));
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
