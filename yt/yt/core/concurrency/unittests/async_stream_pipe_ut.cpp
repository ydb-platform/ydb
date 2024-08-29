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
        EXPECT_FALSE(readResult.IsSet());

        auto writeResult = pipe->Write(TSharedRef::FromString("FOO"));
        EXPECT_TRUE(readResult.IsSet());
        EXPECT_TRUE(readResult.Get().IsOK());
        EXPECT_EQ(GetString(readResult.Get().Value()), "FOO");
        EXPECT_TRUE(writeResult.IsSet());
        EXPECT_TRUE(writeResult.Get().IsOK());
    }

    {
        auto writeResult = pipe->Write(TSharedRef::FromString("BAR_BAZ"));
        EXPECT_FALSE(writeResult.IsSet());

        const auto readResult = pipe->Read();
        EXPECT_TRUE(readResult.IsSet());
        EXPECT_TRUE(readResult.Get().IsOK());
        EXPECT_EQ(GetString(readResult.Get().Value()), "BAR_BAZ");
        EXPECT_TRUE(writeResult.IsSet());
        EXPECT_TRUE(writeResult.Get().IsOK());

    }

    {
        const auto readResult = pipe->Read();
        EXPECT_FALSE(readResult.IsSet());

        const auto closed = pipe->Close();
        EXPECT_TRUE(closed.IsSet());

        EXPECT_TRUE(readResult.IsSet());
        EXPECT_TRUE(readResult.Get().IsOK());
        EXPECT_EQ(GetString(readResult.Get().Value()), "");
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBoundedAsyncStreamPipeTest, Simple)
{
    auto pipe = New<TBoundedAsyncStreamPipe>(1);

    {
        const auto readResult = pipe->Read();
        EXPECT_FALSE(readResult.IsSet());

        auto writeResult = pipe->Write(TSharedRef::FromString("FOO"));
        EXPECT_TRUE(readResult.IsSet());
        EXPECT_TRUE(readResult.Get().IsOK());
        EXPECT_EQ(GetString(readResult.Get().Value()), "FOO");

        EXPECT_TRUE(writeResult.IsSet());
        EXPECT_TRUE(writeResult.Get().IsOK());
    }

    {
        auto writeResult = pipe->Write(TSharedRef::FromString("BAR"));
        EXPECT_TRUE(writeResult.IsSet());
        EXPECT_TRUE(writeResult.Get().IsOK());

        const auto readResult = pipe->Read();
        EXPECT_TRUE(readResult.IsSet());
        EXPECT_TRUE(readResult.Get().IsOK());
        EXPECT_EQ(GetString(readResult.Get().Value()), "BAR");
    }

    {
        auto writeResult1 = pipe->Write(TSharedRef::FromString("BAZ_1"));
        EXPECT_TRUE(writeResult1.IsSet());
        EXPECT_TRUE(writeResult1.Get().IsOK());

        auto writeResult2 = pipe->Write(TSharedRef::FromString("BAZ_2"));
        EXPECT_FALSE(writeResult2.IsSet());

        const auto readResult1 = pipe->Read();
        EXPECT_TRUE(readResult1.IsSet());
        EXPECT_TRUE(readResult1.Get().IsOK());
        EXPECT_EQ(GetString(readResult1.Get().Value()), "BAZ_1");

        EXPECT_TRUE(writeResult2.IsSet());
        EXPECT_TRUE(writeResult2.Get().IsOK());

        const auto readResult2 = pipe->Read();
        EXPECT_TRUE(readResult2.IsSet());
        EXPECT_TRUE(readResult2.Get().IsOK());
        EXPECT_EQ(GetString(readResult2.Get().Value()), "BAZ_2");
    }

    {
        const auto readResult1 = pipe->Read();
        EXPECT_FALSE(readResult1.IsSet());

        const auto readResult2 = pipe->Read();
        EXPECT_FALSE(readResult2.IsSet());

        auto writeResult1 = pipe->Write(TSharedRef::FromString("ABC_1"));
        EXPECT_TRUE(writeResult1.IsSet());
        EXPECT_TRUE(writeResult1.Get().IsOK());

        auto writeResult2 = pipe->Write(TSharedRef::FromString("ABC_2"));
        EXPECT_TRUE(writeResult2.IsSet());
        EXPECT_TRUE(writeResult2.Get().IsOK());

        EXPECT_TRUE(readResult1.IsSet());
        EXPECT_TRUE(readResult1.Get().IsOK());
        EXPECT_EQ(GetString(readResult1.Get().Value()), "ABC_1");

        EXPECT_TRUE(readResult2.IsSet());
        EXPECT_TRUE(readResult2.Get().IsOK());
        EXPECT_EQ(GetString(readResult2.Get().Value()), "ABC_2");
    }

    {
        const auto readResult = pipe->Read();
        EXPECT_FALSE(readResult.IsSet());

        const auto closed = pipe->Close();
        EXPECT_TRUE(closed.IsSet());

        EXPECT_TRUE(readResult.IsSet());
        EXPECT_TRUE(readResult.Get().IsOK());
        EXPECT_EQ(GetString(readResult.Get().Value()), "");
    }
}

TEST(TBoundedAsyncStreamPipeTest, AbortWaitRead)
{
    auto pipe = New<TBoundedAsyncStreamPipe>(1);

    const auto readResult1 = pipe->Read();
    EXPECT_FALSE(readResult1.IsSet());

    auto writeResult1 = pipe->Write(TSharedRef::FromString("FOO"));
    EXPECT_TRUE(writeResult1.IsSet());
    EXPECT_TRUE(writeResult1.Get().IsOK());

    EXPECT_TRUE(readResult1.IsSet());
    EXPECT_TRUE(readResult1.Get().IsOK());
    EXPECT_EQ(GetString(readResult1.Get().Value()), "FOO");

    const auto readResult2 = pipe->Read();
    EXPECT_FALSE(readResult2.IsSet());

    pipe->Abort(TError("fail"));

    EXPECT_TRUE(readResult2.IsSet());
    EXPECT_FALSE(readResult2.Get().IsOK());
    EXPECT_THROW_WITH_SUBSTRING(readResult2.Get().ThrowOnError(), "was drained");
}

TEST(TBoundedAsyncStreamPipeTest, AbortWaitWrite)
{
    auto pipe = New<TBoundedAsyncStreamPipe>(1);

    const auto readResult1 = pipe->Read();
    EXPECT_FALSE(readResult1.IsSet());

    auto writeResult1 = pipe->Write(TSharedRef::FromString("FOO"));
    EXPECT_TRUE(writeResult1.IsSet());
    EXPECT_TRUE(writeResult1.Get().IsOK());

    EXPECT_TRUE(readResult1.IsSet());
    EXPECT_TRUE(readResult1.Get().IsOK());
    EXPECT_EQ(GetString(readResult1.Get().Value()), "FOO");

    const auto writeResult2 = pipe->Write(TSharedRef::FromString("BAR"));
    EXPECT_TRUE(writeResult2.IsSet());
    EXPECT_TRUE(writeResult2.Get().IsOK());

    const auto writeResult3 = pipe->Write(TSharedRef::FromString("BAZ"));
    EXPECT_FALSE(writeResult3.IsSet());

    pipe->Abort(TError("fail"));

    EXPECT_TRUE(writeResult3.IsSet());
    EXPECT_FALSE(writeResult3.Get().IsOK());
    EXPECT_THROW_WITH_SUBSTRING(writeResult3.Get().ThrowOnError(), "was drained");

    const auto writeResult4 = pipe->Write(TSharedRef::FromString("ABC"));
    EXPECT_TRUE(writeResult4.IsSet());
    EXPECT_FALSE(writeResult4.Get().IsOK());
    EXPECT_THROW_WITH_SUBSTRING(writeResult4.Get().ThrowOnError(), "fail");

    const auto readResult2 = pipe->Read();
    EXPECT_TRUE(readResult2.IsSet());
    EXPECT_FALSE(readResult2.Get().IsOK());
    EXPECT_THROW_WITH_SUBSTRING(readResult2.Get().ThrowOnError(), "fail");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
