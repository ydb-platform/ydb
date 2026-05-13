#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/containers/sentinel_optional.h>

#include <atomic>
#include <type_traits>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

using TIntOpt = TSentinelOptional<int, TValueSentinel<-1>>;

////////////////////////////////////////////////////////////////////////////////

TEST(TSentinelOptionalTest, DefaultConstructNull)
{
    TIntOpt opt;
    EXPECT_FALSE(opt.has_value());
    EXPECT_FALSE(static_cast<bool>(opt));
    EXPECT_EQ(opt, std::nullopt);
}

TEST(TSentinelOptionalTest, NulloptConstruct)
{
    TIntOpt opt(std::nullopt);
    EXPECT_FALSE(opt.has_value());
    EXPECT_EQ(opt, std::nullopt);
}

TEST(TSentinelOptionalTest, ValueConstruct)
{
    TIntOpt opt(42);
    EXPECT_TRUE(opt.has_value());
    EXPECT_TRUE(static_cast<bool>(opt));
    EXPECT_EQ(*opt, 42);
}

TEST(TSentinelOptionalTest, ZeroIsNotNull)
{
    TIntOpt opt(0);
    EXPECT_TRUE(opt.has_value());
    EXPECT_EQ(*opt, 0);
}

TEST(TSentinelOptionalTest, AssignValue)
{
    TIntOpt opt;
    opt = 7;
    EXPECT_TRUE(opt.has_value());
    EXPECT_EQ(*opt, 7);
}

TEST(TSentinelOptionalTest, AssignNullopt)
{
    TIntOpt opt(7);
    opt = std::nullopt;
    EXPECT_FALSE(opt.has_value());
}

TEST(TSentinelOptionalTest, Reset)
{
    TIntOpt opt(7);
    opt.reset();
    EXPECT_FALSE(opt.has_value());
    EXPECT_EQ(opt, std::nullopt);
}

TEST(TSentinelOptionalTest, Dereference)
{
    TIntOpt opt(99);
    EXPECT_EQ(*opt, 99);
    *opt = 100;
    EXPECT_EQ(*opt, 100);
}

TEST(TSentinelOptionalTest, ArrowOperator)
{
    TSentinelOptional<int, TValueSentinel<0>> opt(42);
    EXPECT_EQ(*opt.operator->(), 42);
}

TEST(TSentinelOptionalTest, ValueChecked)
{
    TIntOpt opt(5);
    EXPECT_EQ(opt.value(), 5);
    opt.value() = 10;
    EXPECT_EQ(opt.value(), 10);
}

TEST(TSentinelOptionalTest, ValueOr)
{
    TIntOpt opt;
    EXPECT_EQ(opt.value_or(42), 42);

    opt = 7;
    EXPECT_EQ(opt.value_or(42), 7);
}

TEST(TSentinelOptionalTest, EqualityBetweenOptionals)
{
    TIntOpt a;
    TIntOpt b;
    EXPECT_EQ(a, b);

    a = 5;
    EXPECT_NE(a, b);

    b = 5;
    EXPECT_EQ(a, b);

    b = 6;
    EXPECT_NE(a, b);
}

TEST(TSentinelOptionalTest, EqualityWithNullopt)
{
    TIntOpt opt;
    EXPECT_EQ(opt, std::nullopt);
    EXPECT_EQ(std::nullopt, opt);

    opt = 1;
    EXPECT_NE(opt, std::nullopt);
    EXPECT_NE(std::nullopt, opt);
}

TEST(TSentinelOptionalTest, CopyConstruct)
{
    TIntOpt a(3);
    TIntOpt b = a;
    EXPECT_EQ(*b, 3);
    *a = 4;
    EXPECT_EQ(*b, 3); // b is independent
}

TEST(TSentinelOptionalTest, CopyAssign)
{
    TIntOpt a(3);
    TIntOpt b;
    b = a;
    EXPECT_EQ(*b, 3);
}

TEST(TSentinelOptionalTest, ConstAccess)
{
    const TIntOpt opt(77);
    EXPECT_EQ(*opt, 77);
    EXPECT_EQ(opt.value(), 77);
    EXPECT_EQ(*opt.operator->(), 77);
}

////////////////////////////////////////////////////////////////////////////////
// Pointer sentinel.

using TPtrOpt = TSentinelOptional<int*, TValueSentinel<nullptr>>;

TEST(TSentinelOptionalTest, PointerSentinel)
{
    TPtrOpt opt;
    EXPECT_FALSE(opt.has_value());

    int x = 42;
    opt = &x;
    EXPECT_TRUE(opt.has_value());
    EXPECT_EQ(*opt, &x);
    EXPECT_EQ(**opt, 42);
}

////////////////////////////////////////////////////////////////////////////////
// Enum sentinel.

enum class EColor
{
    Red,
    Green,
    Blue,
    Unknown,
};

using TColorOpt = TSentinelOptional<EColor, TValueSentinel<EColor::Unknown>>;

TEST(TSentinelOptionalTest, EnumSentinel)
{
    TColorOpt opt;
    EXPECT_FALSE(opt.has_value());

    opt = EColor::Red;
    EXPECT_TRUE(opt.has_value());
    EXPECT_EQ(*opt, EColor::Red);

    opt.reset();
    EXPECT_FALSE(opt.has_value());
}

////////////////////////////////////////////////////////////////////////////////
// Layout and atomic guarantees.

TEST(TSentinelOptionalTest, SizeEqualsUnderlyingType)
{
    static_assert(sizeof(TIntOpt) == sizeof(int));
    static_assert(sizeof(TPtrOpt) == sizeof(int*));
    static_assert(sizeof(TColorOpt) == sizeof(EColor));
}

TEST(TSentinelOptionalTest, TriviallyCopyable)
{
    static_assert(std::is_trivially_copyable_v<TIntOpt>);
    static_assert(std::is_trivially_copyable_v<TPtrOpt>);
    static_assert(std::is_trivially_copyable_v<TColorOpt>);
}

TEST(TSentinelOptionalTest, AtomicLockFree)
{
    // std::atomic<TSentinelOptional<T, S>> must be lock-free whenever
    // std::atomic<T> is lock-free.
    if (std::atomic<int>{}.is_lock_free()) {
        EXPECT_TRUE((std::atomic<TIntOpt>{}.is_lock_free()));
    }
    if (std::atomic<int*>{}.is_lock_free()) {
        EXPECT_TRUE((std::atomic<TPtrOpt>{}.is_lock_free()));
    }
    if (std::atomic<EColor>{}.is_lock_free()) {
        EXPECT_TRUE((std::atomic<TColorOpt>{}.is_lock_free()));
    }
}

TEST(TSentinelOptionalTest, AtomicStoreLoad)
{
    std::atomic<TIntOpt> a;
    a.store(TIntOpt{});
    EXPECT_FALSE(a.load().has_value());

    a.store(TIntOpt{42});
    EXPECT_EQ(*a.load(), 42);

    a.store(std::nullopt);
    EXPECT_FALSE(a.load().has_value());
}

////////////////////////////////////////////////////////////////////////////////
// std::optional conversions.

TEST(TSentinelOptionalTest, FromStdOptionalNull)
{
    TIntOpt opt = std::optional<int>{};
    EXPECT_FALSE(opt.has_value());
    EXPECT_EQ(opt, std::nullopt);
}

TEST(TSentinelOptionalTest, FromStdOptionalValue)
{
    TIntOpt opt = std::optional<int>{42};
    EXPECT_TRUE(opt.has_value());
    EXPECT_EQ(*opt, 42);
}

TEST(TSentinelOptionalTest, ToStdOptionalNull)
{
    TIntOpt opt;
    std::optional<int> stdOpt = opt;
    EXPECT_FALSE(stdOpt.has_value());
}

TEST(TSentinelOptionalTest, ToStdOptionalValue)
{
    TIntOpt opt(7);
    std::optional<int> stdOpt = opt;
    EXPECT_TRUE(stdOpt.has_value());
    EXPECT_EQ(*stdOpt, 7);
}

TEST(TSentinelOptionalTest, RoundTripThroughStdOptional)
{
    TIntOpt original(99);
    TIntOpt roundTripped = std::optional<int>(original);
    EXPECT_TRUE(roundTripped.has_value());
    EXPECT_EQ(*roundTripped, 99);
}

TEST(TSentinelOptionalTest, RoundTripNullThroughStdOptional)
{
    TIntOpt original;
    TIntOpt roundTripped = std::optional<int>(original);
    EXPECT_FALSE(roundTripped.has_value());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
