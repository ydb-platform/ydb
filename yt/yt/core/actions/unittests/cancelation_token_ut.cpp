#include <gtest/gtest.h>

#include <yt/yt/core/actions/cancelation_token.h>

namespace NYT::NDetail {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TSimpleToken
{
    friend bool TagInvoke(TTagInvokeTag<IsCancelationRequested>, const TSimpleToken& token) noexcept
    {
        return !token.Error.IsOK();
    }

    friend const TError& TagInvoke(TTagInvokeTag<GetCancelationError>, const TSimpleToken& token)
    {
        return token.Error;
    }

    TError Error;

    TSimpleToken()
    {
        ++CtorCount;
    }

    TSimpleToken(TError error)
        : Error(std::move(error))
    { }

    TSimpleToken(const TSimpleToken& other)
        : Error(other.Error)
    {
        ++CopyCount;
    }

    TSimpleToken& operator=(const TSimpleToken&) = default;
    TSimpleToken& operator=(TSimpleToken&&) = default;

    TSimpleToken(TSimpleToken&& other)
        : Error(std::move(other.Error))
    {
        ++MoveCount;
    }

    ~TSimpleToken()
    {
        ++DtorCount;
    }

    static inline int CtorCount = 0;
    static inline int DtorCount = 0;
    static inline int CopyCount = 0;
    static inline int MoveCount = 0;
};

static_assert(CCancelationToken<TSimpleToken>);

////////////////////////////////////////////////////////////////////////////////

void ResetCounters()
{
    TSimpleToken::CtorCount = 0;
    TSimpleToken::DtorCount = 0;
    TSimpleToken::CopyCount = 0;
    TSimpleToken::MoveCount = 0;
}

TEST(TAnyTokenTest, JustWorks)
{
    ResetCounters();
    TAnyCancelationToken any{TSimpleToken{}};
    EXPECT_FALSE(IsCancelationRequested(any));
}

TEST(TAnyTokenTest, Copy)
{
    ResetCounters();
    TSimpleToken token{TError(NYT::EErrorCode::Canceled, "Boo")};

    TAnyCancelationToken any{token};
    EXPECT_EQ(TSimpleToken::CtorCount, 0);
    EXPECT_EQ(TSimpleToken::CopyCount, 1);
    EXPECT_EQ(TSimpleToken::MoveCount, 0);

    EXPECT_TRUE(IsCancelationRequested(any));

    token.Error = TError{};

    TAnyCancelationToken any1{};

    // NB: Implicit copy ctor and then move assign.
    any1 = token;

    EXPECT_EQ(TSimpleToken::CtorCount, 0);
    EXPECT_EQ(TSimpleToken::CopyCount, 2);
    EXPECT_EQ(TSimpleToken::MoveCount, 1);
    EXPECT_EQ(TSimpleToken::DtorCount, 1);
    EXPECT_FALSE(IsCancelationRequested(any1));

    any1 = any;
    EXPECT_EQ(TSimpleToken::CtorCount, 0);
    EXPECT_EQ(TSimpleToken::CopyCount, 3);
    EXPECT_EQ(TSimpleToken::MoveCount, 1);
    EXPECT_EQ(TSimpleToken::DtorCount, 2);
    EXPECT_TRUE(IsCancelationRequested(any1));
}

TEST(TAnyTokenTest, MoveSmallToken)
{
    ResetCounters();
    TSimpleToken token{TError(NYT::EErrorCode::Canceled, "Oi")};

    TAnyCancelationToken any{std::move(token)};
    EXPECT_EQ(TSimpleToken::CtorCount, 0);
    EXPECT_EQ(TSimpleToken::CopyCount, 0);
    EXPECT_EQ(TSimpleToken::MoveCount, 1);

    EXPECT_TRUE(IsCancelationRequested(any));

    token.Error = TError{};

    TAnyCancelationToken any1{};
    EXPECT_EQ(TSimpleToken::CtorCount, 0);
    EXPECT_EQ(TSimpleToken::CopyCount, 0);
    EXPECT_EQ(TSimpleToken::MoveCount, 1);
    EXPECT_EQ(TSimpleToken::DtorCount, 0);

    any1 = std::move(token);
    EXPECT_EQ(TSimpleToken::CtorCount, 0);
    EXPECT_EQ(TSimpleToken::CopyCount, 0);
    EXPECT_EQ(TSimpleToken::MoveCount, 3);
    EXPECT_EQ(TSimpleToken::DtorCount, 1);
    EXPECT_FALSE(IsCancelationRequested(any1));

    any1 = std::move(any);
    EXPECT_EQ(TSimpleToken::CtorCount, 0);
    EXPECT_EQ(TSimpleToken::CopyCount, 0);
    EXPECT_EQ(TSimpleToken::MoveCount, 4);
    EXPECT_EQ(TSimpleToken::DtorCount, 3);
    EXPECT_TRUE(IsCancelationRequested(any1));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDetail
