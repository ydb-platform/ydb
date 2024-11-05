#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/actions/callback_internal.h>

#include <yt/yt/core/misc/public.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

// White-box testpoint.
struct TFakeInvoker
{
    using TSignature = void(NDetail::TBindStateBase*);
    static void Run(NDetail::TBindStateBase*)
    { }
};

////////////////////////////////////////////////////////////////////////////////

template <
    bool CaptureTraceContext,
    class TRunnable,
    class TSignature,
    class TBoundArgs
>
struct TBindState;

// White-box injection into a #TCallback<> object for checking
// comparators and emptiness APIs. Use a #TBindState<> that is specialized
// based on a type we declared in the anonymous namespace above to remove any
// chance of colliding with another instantiation and breaking the
// one-definition-rule.
template <>
struct TBindState<true, void(), void(), void(TFakeInvoker)>
    : public NDetail::TBindStateBase
{
public:
    using TInvokerType = TFakeInvoker;
    TBindState()
        : TBindStateBase(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
            YT_CURRENT_SOURCE_LOCATION
#endif
        )
    { }
};

template <>
struct TBindState<true, void(), void(), void(TFakeInvoker, TFakeInvoker)>
    : public NDetail::TBindStateBase
{
    using TInvokerType = TFakeInvoker;
    TBindState()
        : TBindStateBase(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
            YT_CURRENT_SOURCE_LOCATION
#endif
        )
    { }
};

////////////////////////////////////////////////////////////////////////////////

template <bool CaptureTraceContext, class TRunnable, class TSignature, class TBoundArgs>
TCallback<TSignature> MakeCallback(
    TIntrusivePtr<NYT::TBindState<CaptureTraceContext, TRunnable, TSignature, TBoundArgs>>&& bindState)
{
    auto invokeFunction = &NYT::TBindState<CaptureTraceContext, TRunnable, TSignature, TBoundArgs>::TInvokerType::Run;
    return TCallback<TSignature>(std::move(bindState), invokeFunction);
}

// TODO(sandello): Implement accurate check on the number of Ref() and Unref()s.

using TFakeBindState1 = TBindState<true, void(), void(), void(TFakeInvoker)>;
using TFakeBindState2 = TBindState<true, void(), void(), void(TFakeInvoker, TFakeInvoker)>;

class TCallbackTest
    : public ::testing::Test
{
public:
    TCallbackTest()
        : FirstCallback(MakeCallback(New<TFakeBindState1>()))
        , SecondCallback(MakeCallback(New<TFakeBindState2>()))
    { }

    virtual ~TCallbackTest()
    { }

protected:
    TCallback<void()> FirstCallback;
    const TCallback<void()> SecondCallback;

    TCallback<void()> NullCallback;
};

// Ensure we can create unbound callbacks. We need this to be able to store
// them in class members that can be initialized later.
TEST_F(TCallbackTest, DefaultConstruction)
{
    TCallback<void()> c0;

    TCallback<void(int)> c1;
    TCallback<void(int, int)> c2;
    TCallback<void(int, int, int)> c3;
    TCallback<void(int, int, int, int)> c4;
    TCallback<void(int, int, int, int, int)> c5;
    TCallback<void(int, int, int, int, int, int)> c6;

    EXPECT_FALSE(c0);
    EXPECT_FALSE(c1);
    EXPECT_FALSE(c2);
    EXPECT_FALSE(c3);
    EXPECT_FALSE(c4);
    EXPECT_FALSE(c5);
    EXPECT_FALSE(c6);
}

TEST_F(TCallbackTest, IsNull)
{
    EXPECT_FALSE(NullCallback);
    EXPECT_TRUE(FirstCallback);
    EXPECT_TRUE(SecondCallback);
}

TEST_F(TCallbackTest, Move)
{
    EXPECT_TRUE(FirstCallback);

    TCallback<void()> localCallback(std::move(FirstCallback));
    TCallback<void()> anotherCallback;

    EXPECT_FALSE(FirstCallback);
    EXPECT_TRUE(localCallback);
    EXPECT_FALSE(anotherCallback);

    anotherCallback = std::move(localCallback);

    EXPECT_FALSE(FirstCallback);
    EXPECT_FALSE(localCallback);
    EXPECT_TRUE(anotherCallback);
}

TEST_F(TCallbackTest, Equals)
{
    EXPECT_EQ(FirstCallback, FirstCallback);
    EXPECT_NE(FirstCallback, SecondCallback);
    EXPECT_NE(SecondCallback, FirstCallback);

    // We should compare based on instance, not type.
    TCallback<void()> localCallback(MakeCallback(New<TFakeBindState1>()));
    TCallback<void()> anotherCallback = FirstCallback;

    EXPECT_EQ(FirstCallback, anotherCallback);
    EXPECT_NE(FirstCallback, localCallback);

    // Empty, however, is always equal to empty.
    TCallback<void()> localNullCallback;
    EXPECT_EQ(NullCallback, localNullCallback);
}

TEST_F(TCallbackTest, Reset)
{
    // Resetting should bring us back to empty.
    ASSERT_TRUE(FirstCallback);
    ASSERT_NE(FirstCallback, NullCallback);

    FirstCallback.Reset();

    EXPECT_FALSE(FirstCallback);
    EXPECT_EQ(FirstCallback, NullCallback);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
