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
        ASSERT_TRUE(readResult.IsSet());
        EXPECT_TRUE(readResult.GetOrCrash().IsOK());
        EXPECT_EQ(GetString(readResult.GetOrCrash().Value()), "FOO");
        ASSERT_TRUE(writeResult.IsSet());
        EXPECT_TRUE(writeResult.GetOrCrash().IsOK());
    }

    {
        auto writeResult = pipe->Write(TSharedRef::FromString("BAR_BAZ"));
        EXPECT_FALSE(writeResult.IsSet());

        const auto readResult = pipe->Read();
        ASSERT_TRUE(readResult.IsSet());
        EXPECT_TRUE(readResult.GetOrCrash().IsOK());
        EXPECT_EQ(GetString(readResult.GetOrCrash().Value()), "BAR_BAZ");
        ASSERT_TRUE(writeResult.IsSet());
        EXPECT_TRUE(writeResult.GetOrCrash().IsOK());

    }

    {
        const auto readResult = pipe->Read();
        EXPECT_FALSE(readResult.IsSet());

        const auto closed = pipe->Close();
        ASSERT_TRUE(closed.IsSet());

        ASSERT_TRUE(readResult.IsSet());
        EXPECT_TRUE(readResult.GetOrCrash().IsOK());
        EXPECT_EQ(GetString(readResult.GetOrCrash().Value()), "");
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
        ASSERT_TRUE(readResult.IsSet());
        EXPECT_TRUE(readResult.GetOrCrash().IsOK());
        EXPECT_EQ(GetString(readResult.GetOrCrash().Value()), "FOO");

        ASSERT_TRUE(writeResult.IsSet());
        EXPECT_TRUE(writeResult.GetOrCrash().IsOK());
    }

    {
        auto writeResult = pipe->Write(TSharedRef::FromString("BAR"));
        ASSERT_TRUE(writeResult.IsSet());
        EXPECT_TRUE(writeResult.GetOrCrash().IsOK());

        const auto readResult = pipe->Read();
        ASSERT_TRUE(readResult.IsSet());
        EXPECT_TRUE(readResult.GetOrCrash().IsOK());
        EXPECT_EQ(GetString(readResult.GetOrCrash().Value()), "BAR");
    }

    {
        auto writeResult1 = pipe->Write(TSharedRef::FromString("BAZ_1"));
        ASSERT_TRUE(writeResult1.IsSet());
        EXPECT_TRUE(writeResult1.GetOrCrash().IsOK());

        auto writeResult2 = pipe->Write(TSharedRef::FromString("BAZ_2"));
        EXPECT_FALSE(writeResult2.IsSet());

        const auto readResult1 = pipe->Read();
        ASSERT_TRUE(readResult1.IsSet());
        EXPECT_TRUE(readResult1.GetOrCrash().IsOK());
        EXPECT_EQ(GetString(readResult1.GetOrCrash().Value()), "BAZ_1");

        ASSERT_TRUE(writeResult2.IsSet());
        EXPECT_TRUE(writeResult2.GetOrCrash().IsOK());

        const auto readResult2 = pipe->Read();
        ASSERT_TRUE(readResult2.IsSet());
        EXPECT_TRUE(readResult2.GetOrCrash().IsOK());
        EXPECT_EQ(GetString(readResult2.GetOrCrash().Value()), "BAZ_2");
    }

    {
        const auto readResult1 = pipe->Read();
        EXPECT_FALSE(readResult1.IsSet());

        const auto readResult2 = pipe->Read();
        EXPECT_FALSE(readResult2.IsSet());

        auto writeResult1 = pipe->Write(TSharedRef::FromString("ABC_1"));
        ASSERT_TRUE(writeResult1.IsSet());
        EXPECT_TRUE(writeResult1.GetOrCrash().IsOK());

        auto writeResult2 = pipe->Write(TSharedRef::FromString("ABC_2"));
        ASSERT_TRUE(writeResult2.IsSet());
        EXPECT_TRUE(writeResult2.GetOrCrash().IsOK());

        ASSERT_TRUE(readResult1.IsSet());
        EXPECT_TRUE(readResult1.GetOrCrash().IsOK());
        EXPECT_EQ(GetString(readResult1.GetOrCrash().Value()), "ABC_1");

        ASSERT_TRUE(readResult2.IsSet());
        EXPECT_TRUE(readResult2.GetOrCrash().IsOK());
        EXPECT_EQ(GetString(readResult2.GetOrCrash().Value()), "ABC_2");
    }

    {
        const auto readResult = pipe->Read();
        EXPECT_FALSE(readResult.IsSet());

        const auto closed = pipe->Close();
        ASSERT_TRUE(closed.IsSet());

        ASSERT_TRUE(readResult.IsSet());
        EXPECT_TRUE(readResult.GetOrCrash().IsOK());
        EXPECT_EQ(GetString(readResult.GetOrCrash().Value()), "");
    }
}

TEST(TAsyncStreamPipeTest, AbortReturnsOK)
{
    auto pipe = New<TAsyncStreamPipe>();

    auto abortResult = pipe->Abort(TError("fail"));
    ASSERT_TRUE(abortResult.IsSet());
    EXPECT_TRUE(abortResult.GetOrCrash().IsOK());
}

TEST(TAsyncStreamPipeTest, AbortWaitRead)
{
    auto pipe = New<TAsyncStreamPipe>();

    const auto readResult = pipe->Read();
    EXPECT_FALSE(readResult.IsSet());

    auto abortResult = pipe->Abort(TError("fail"));
    ASSERT_TRUE(abortResult.IsSet());
    EXPECT_TRUE(abortResult.GetOrCrash().IsOK());

    ASSERT_TRUE(readResult.IsSet());
    EXPECT_FALSE(readResult.GetOrCrash().IsOK());
    EXPECT_THROW_WITH_SUBSTRING(readResult.GetOrCrash().ThrowOnError(), "fail");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBoundedAsyncStreamPipeTest, AbortWaitRead)
{
    auto pipe = New<TBoundedAsyncStreamPipe>(1);

    const auto readResult1 = pipe->Read();
    EXPECT_FALSE(readResult1.IsSet());

    auto writeResult1 = pipe->Write(TSharedRef::FromString("FOO"));
    ASSERT_TRUE(writeResult1.IsSet());
    EXPECT_TRUE(writeResult1.GetOrCrash().IsOK());

    ASSERT_TRUE(readResult1.IsSet());
    EXPECT_TRUE(readResult1.GetOrCrash().IsOK());
    EXPECT_EQ(GetString(readResult1.GetOrCrash().Value()), "FOO");

    const auto readResult2 = pipe->Read();
    EXPECT_FALSE(readResult2.IsSet());

    pipe->Abort(TError("fail"));

    ASSERT_TRUE(readResult2.IsSet());
    EXPECT_FALSE(readResult2.GetOrCrash().IsOK());
    EXPECT_THROW_WITH_SUBSTRING(readResult2.GetOrCrash().ThrowOnError(), "was drained");
}

TEST(TBoundedAsyncStreamPipeTest, AbortWaitWrite)
{
    auto pipe = New<TBoundedAsyncStreamPipe>(1);

    const auto readResult1 = pipe->Read();
    EXPECT_FALSE(readResult1.IsSet());

    auto writeResult1 = pipe->Write(TSharedRef::FromString("FOO"));
    ASSERT_TRUE(writeResult1.IsSet());
    EXPECT_TRUE(writeResult1.GetOrCrash().IsOK());

    ASSERT_TRUE(readResult1.IsSet());
    EXPECT_TRUE(readResult1.GetOrCrash().IsOK());
    EXPECT_EQ(GetString(readResult1.GetOrCrash().Value()), "FOO");

    const auto writeResult2 = pipe->Write(TSharedRef::FromString("BAR"));
    ASSERT_TRUE(writeResult2.IsSet());
    EXPECT_TRUE(writeResult2.GetOrCrash().IsOK());

    const auto writeResult3 = pipe->Write(TSharedRef::FromString("BAZ"));
    EXPECT_FALSE(writeResult3.IsSet());

    pipe->Abort(TError("fail"));

    ASSERT_TRUE(writeResult3.IsSet());
    EXPECT_FALSE(writeResult3.GetOrCrash().IsOK());
    EXPECT_THROW_WITH_SUBSTRING(writeResult3.GetOrCrash().ThrowOnError(), "was drained");

    const auto writeResult4 = pipe->Write(TSharedRef::FromString("ABC"));
    ASSERT_TRUE(writeResult4.IsSet());
    EXPECT_FALSE(writeResult4.GetOrCrash().IsOK());
    EXPECT_THROW_WITH_SUBSTRING(writeResult4.GetOrCrash().ThrowOnError(), "fail");

    const auto readResult2 = pipe->Read();
    ASSERT_TRUE(readResult2.IsSet());
    EXPECT_FALSE(readResult2.GetOrCrash().IsOK());
    EXPECT_THROW_WITH_SUBSTRING(readResult2.GetOrCrash().ThrowOnError(), "fail");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
