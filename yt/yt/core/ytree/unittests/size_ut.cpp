#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/size.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NYTree {
namespace {

////////////////////////////////////////////////////////////////////////////////

#define XX(name) \
    static_assert(std::numeric_limits<TSize>::name == std::numeric_limits<i64>::name);

XX(is_signed)
XX(digits)
XX(digits10)
XX(max_digits10)
XX(is_integer)
XX(is_exact)
XX(radix)
XX(min_exponent)
XX(min_exponent10)
XX(max_exponent)
XX(max_exponent10)
XX(has_infinity)
XX(has_quiet_NaN)
XX(has_signaling_NaN)
XX(has_denorm)
XX(has_denorm_loss)
XX(is_iec559)
XX(is_bounded)
XX(is_modulo)
XX(traps)
XX(tinyness_before)
XX(round_style)

#undef XX

#define XX(name) \
    static_assert(std::numeric_limits<TSize>::name() == TSize(std::numeric_limits<i64>::name()));

XX(min)
XX(max)
XX(lowest)
XX(epsilon)
XX(round_error)
XX(infinity)
XX(quiet_NaN)
XX(signaling_NaN)
XX(denorm_min)

#undef XX

////////////////////////////////////////////////////////////////////////////////

TEST(TYTreeSizeTest, Simple)
{
    EXPECT_EQ(TSize(1), TSize(1));
    EXPECT_NE(TSize(1), TSize(2));

    EXPECT_EQ(TSize(1), 1);
    EXPECT_LE(0, TSize(1));
    EXPECT_GE(TSize(1), 0);
    EXPECT_EQ(1u, TSize(1));

    EXPECT_TRUE(TSize(1));
    EXPECT_FALSE(TSize(0));

    EXPECT_EQ(TSize(10) + 100, 110);
    EXPECT_EQ(100 + TSize(10), 110);
    EXPECT_EQ(10 * TSize(10), 100);
    EXPECT_EQ(TSize(10) * 10, 100);
    EXPECT_EQ(TSize(10) / 10, 1);
    EXPECT_EQ(10 / TSize(10), 1);
    EXPECT_EQ(10 - TSize(10), 0);
    EXPECT_EQ(TSize(10) - 10, 0);

    {
        EXPECT_EQ(TSize(10).Underlying(), 10);

        TSize a{56};
        a.Underlying() = 55;
        EXPECT_EQ(a.Underlying(), 55);

        const TSize b{58};
        static_assert(std::is_const_v<std::remove_reference_t<decltype(b.Underlying())>>);
        EXPECT_EQ(b.Underlying(), 58);
        EXPECT_EQ(b, 58);
    }
}

TEST(TYTreeSizeTest, Serialize)
{
    EXPECT_EQ(ToString(TSize(1)), TString{"1"});
    EXPECT_EQ(Format("|%v|", TSize(42)), TString{"|42|"});

    {
        TSize v;
        EXPECT_TRUE(::TryFromString("21", 2, v));
        EXPECT_EQ(TSize(21), v);
        EXPECT_FALSE(::TryFromString("", 0, v));
    }

    EXPECT_EQ(TSize(46), FromString<TSize>("46"));

    {
        TStringBuilder builder;
        Format(&builder, "%v", TSize(42));
        EXPECT_EQ(builder.Flush(), "42");
    }

    {
        TStringStream stream;
        stream << TSize(42);
        EXPECT_EQ(stream.Str(), "42");
    }
}

TEST(TYTreeSizeTest, Constexpr)
{
    static_assert(TSize(1) == TSize(1));
    static_assert(TSize(1000).Underlying() == 1000);
}

TEST(TYTreeSizeTest, Hash)
{
    for (i64 i = -1000; i < 1000; ++i) {
        EXPECT_EQ(THash<TSize>()(TSize(i)), THash<i64>()(i));
        EXPECT_EQ(std::hash<TSize>()(TSize(i)), std::hash<i64>()(i));
    }
}

i64 DeserializeString(TStringBuf value)
{
    return TSize::FromString(value).Underlying();
}

TEST(TYTreeSizeTest, FromString)
{
    EXPECT_EQ(0, DeserializeString("0"));
    EXPECT_EQ(127, DeserializeString("127"));
    EXPECT_EQ(-2, DeserializeString("-2"));

    EXPECT_EQ(1000, DeserializeString("1K"));
    EXPECT_EQ(3000000, DeserializeString("3M"));
    EXPECT_EQ(-1024, DeserializeString("-1Ki"));
    EXPECT_EQ(2000000000, DeserializeString("2G"));
    EXPECT_THROW(DeserializeString("2000000000000P"), std::exception);

    EXPECT_EQ(0, DeserializeString("0E"));
    EXPECT_EQ(1000, DeserializeString("1K"));
    EXPECT_EQ(1000'000, DeserializeString("1M"));
    EXPECT_EQ(1000'000'000, DeserializeString("1G"));
    EXPECT_EQ(1000'000'000'000, DeserializeString("1T"));
    EXPECT_EQ(1000'000'000'000'000, DeserializeString("1P"));
    EXPECT_EQ(1000'000'000'000'000'000, DeserializeString("1E"));
    EXPECT_EQ(0LL, DeserializeString("0Ei"));
    EXPECT_EQ(1024LL, DeserializeString("1Ki"));
    EXPECT_EQ(1024LL * 1024, DeserializeString("1Mi"));
    EXPECT_EQ(1024LL * 1024 * 1024, DeserializeString("1Gi"));
    EXPECT_EQ(1024LL * 1024 * 1024 * 1024, DeserializeString("1Ti"));
    EXPECT_EQ(1024LL * 1024 * 1024 * 1024 * 1024, DeserializeString("1Pi"));
    EXPECT_EQ(1024LL * 1024 * 1024 * 1024 * 1024 * 1024, DeserializeString("1Ei"));

    EXPECT_THROW(DeserializeString("G"), std::exception);
    EXPECT_THROW(DeserializeString("Gi"), std::exception);
    EXPECT_THROW(DeserializeString("1GG"), std::exception);
    EXPECT_THROW(DeserializeString("1KG"), std::exception);
    EXPECT_THROW(DeserializeString("1KiG"), std::exception);
    EXPECT_THROW(DeserializeString("1Ki2"), std::exception);
}

TEST(TYTreeSizeTest, FromStringBoundaryCases)
{
    EXPECT_EQ(std::numeric_limits<i64>::max(), DeserializeString("9223372036854775807"));
    EXPECT_EQ(std::numeric_limits<i64>::lowest(), DeserializeString("-9223372036854775808"));
    EXPECT_THROW(DeserializeString("9223372036854775808"), std::exception);
    EXPECT_THROW(DeserializeString("-9223372036854775809"), std::exception);

    EXPECT_EQ(9'223'372'036'854'775'000LL, DeserializeString("9223372036854775K"));
    EXPECT_EQ(-9'223'372'036'854'775'000LL, DeserializeString("-9223372036854775K"));
    EXPECT_THROW(DeserializeString("9223372036854776K"), std::exception);
    EXPECT_THROW(DeserializeString("-9223372036854776K"), std::exception);

    EXPECT_EQ(9'223'372'036'854'000'000LL, DeserializeString("9223372036854M"));
    EXPECT_EQ(-9'223'372'036'854'000'000LL, DeserializeString("-9223372036854M"));
    EXPECT_THROW(DeserializeString("9223372036855M"), std::exception);
    EXPECT_THROW(DeserializeString("-9223372036855M"), std::exception);

    EXPECT_EQ(9'223'372'036'000'000'000LL, DeserializeString("9223372036G"));
    EXPECT_EQ(-9'223'372'036'000'000'000LL, DeserializeString("-9223372036G"));
    EXPECT_THROW(DeserializeString("9223372037G"), std::exception);
    EXPECT_THROW(DeserializeString("-9223372037G"), std::exception);

    EXPECT_EQ(9'223'372'000'000'000'000LL, DeserializeString("9223372T"));
    EXPECT_EQ(-9'223'372'000'000'000'000LL, DeserializeString("-9223372T"));
    EXPECT_THROW(DeserializeString("9223373T"), std::exception);
    EXPECT_THROW(DeserializeString("-9223373T"), std::exception);

    EXPECT_EQ(9'223'000'000'000'000'000LL, DeserializeString("9223P"));
    EXPECT_EQ(-9'223'000'000'000'000'000LL, DeserializeString("-9223P"));
    EXPECT_THROW(DeserializeString("9224P"), std::exception);
    EXPECT_THROW(DeserializeString("-9224P"), std::exception);

    EXPECT_EQ(9'000'000'000'000'000'000LL, DeserializeString("9E"));
    EXPECT_EQ(-9'000'000'000'000'000'000LL, DeserializeString("-9E"));
    EXPECT_THROW(DeserializeString("10E"), std::exception);
    EXPECT_THROW(DeserializeString("-10E"), std::exception);
}

template <class T>
TSize ConvertTroughYsonString(T&& t, NYson::EYsonFormat format)
{
    return NYTree::ConvertTo<TSize>(NYson::ConvertToYsonString(std::forward<T>(t), format));
}

TEST(TYTreeSizeTest, ConvertFromYsonString)
{
    EXPECT_EQ(1, ConvertTroughYsonString(1, NYson::EYsonFormat::Binary));
    EXPECT_EQ(2, ConvertTroughYsonString("2", NYson::EYsonFormat::Text));
    EXPECT_EQ(3000, ConvertTroughYsonString("3K", NYson::EYsonFormat::Pretty));
    EXPECT_EQ(4 * 1024 * 1024, ConvertTroughYsonString("4Mi", NYson::EYsonFormat::Binary));
}

template <class T>
TSize ConvertTroughNode(T&& t)
{
    return NYTree::ConvertTo<TSize>(NYTree::ConvertToNode(NYson::ConvertToYsonString(std::forward<T>(t))));
}

TEST(TYTreeSizeTest, ConvertFromNode)
{
    EXPECT_EQ(1, ConvertTroughNode(1));
    EXPECT_EQ(2, ConvertTroughNode("2"));
    EXPECT_EQ(3000, ConvertTroughNode("3K"));
    EXPECT_EQ(4 * 1024 * 1024, ConvertTroughNode("4Mi"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTree
