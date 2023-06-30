#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/async_stream_pipe.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TString GetString(const TSharedRef& sharedRef)
{
    return TString(sharedRef.Begin(), sharedRef.Size());
}

TEST(TAsyncStreamPipeTest, Simple)
{
    auto pipe = New<TAsyncStreamPipe>();

    {
        const auto readResult = pipe->Read();
        ASSERT_FALSE(readResult.IsSet());

        auto writeResult = pipe->Write(TSharedRef::FromString("FOO"));
        ASSERT_TRUE(readResult.IsSet());
        ASSERT_TRUE(readResult.Get().IsOK());
        ASSERT_EQ(GetString(readResult.Get().Value()), "FOO");
        ASSERT_TRUE(writeResult.IsSet());
        ASSERT_TRUE(writeResult.Get().IsOK());
    }

    {
        auto writeResult = pipe->Write(TSharedRef::FromString("BAR_BAZ"));
        EXPECT_FALSE(writeResult.IsSet());

        const auto readResult = pipe->Read();
        ASSERT_TRUE(readResult.IsSet());
        ASSERT_TRUE(readResult.Get().IsOK());
        ASSERT_EQ(GetString(readResult.Get().Value()), "BAR_BAZ");
        ASSERT_TRUE(writeResult.IsSet());
        ASSERT_TRUE(writeResult.Get().IsOK());

    }

    {
        const auto readResult = pipe->Read();
        ASSERT_FALSE(readResult.IsSet());

        const auto closed = pipe->Close();
        ASSERT_TRUE(closed.IsSet());

        ASSERT_TRUE(readResult.IsSet());
        ASSERT_TRUE(readResult.Get().IsOK());
        ASSERT_EQ(GetString(readResult.Get().Value()), "");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
