#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/misc/source_location.h>

#include <library/cpp/yt/yson_string/convert.h>

namespace NYT::NYTree {
namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

template <class T>
void CheckEqualConversionToTextYson(const T& value, const TSourceLocation& loc = YT_CURRENT_SOURCE_LOCATION)
{
    EXPECT_EQ(ConvertToTextYsonString(value).AsStringBuf(), ConvertToYsonString(value, EYsonFormat::Text).AsStringBuf())
        << NYT::Format("At %v", loc);
}

template <class T, class U>
void CheckEqualConversionToFromTextYson(const U& value, const TSourceLocation& loc = YT_CURRENT_SOURCE_LOCATION)
{
    auto yson = ConvertToTextYsonString(value);
    EXPECT_EQ(ConvertFromTextYsonString<T>(yson), ConvertTo<T>(yson))
        << NYT::Format("At %v", loc);
}

template <class T>
void CheckEqualConversionFromTextYson(TStringBuf value, const TSourceLocation& loc = YT_CURRENT_SOURCE_LOCATION)
{
    NYson::TYsonString yson(value);
    EXPECT_EQ(ConvertTo<T>(yson), ConvertTo<T>(yson))
        << NYT::Format("At %v", loc);
}

TEST(TTextYsonConvertTest, ConvertToTextIntegrals)
{
    CheckEqualConversionToTextYson<i8>(+14);
    CheckEqualConversionToTextYson<i8>(0);
    CheckEqualConversionToTextYson<i8>(-15);
    CheckEqualConversionToTextYson<i32>(+100);
    CheckEqualConversionToTextYson<i32>(0);
    CheckEqualConversionToTextYson<i32>(-123);
    CheckEqualConversionToTextYson<i64>(+100);
    CheckEqualConversionToTextYson<i64>(0);
    CheckEqualConversionToTextYson<i64>(-123);

    CheckEqualConversionToTextYson<ui8>(+100);
    CheckEqualConversionToTextYson<ui8>(0);
    CheckEqualConversionToTextYson<ui32>(+100);
    CheckEqualConversionToTextYson<ui32>(0);
    CheckEqualConversionToTextYson<ui64>(+100);
    CheckEqualConversionToTextYson<ui64>(0);
}

TEST(TTextYsonConvertTest, ConvertToTextIntegralsLimits)
{
    CheckEqualConversionToTextYson<i64>(std::numeric_limits<i64>::max());
    CheckEqualConversionToTextYson<i64>(std::numeric_limits<i64>::min());

    CheckEqualConversionToTextYson<ui64>(std::numeric_limits<ui64>::max());
    CheckEqualConversionToTextYson<ui64>(std::numeric_limits<ui64>::min());
}

TEST(TTextYsonConvertTest, ConvertToTextFloats)
{
    CheckEqualConversionToTextYson<float>(0.0);
    CheckEqualConversionToTextYson<float>(-0.0);
    CheckEqualConversionToTextYson<float>(-7.7777);
    CheckEqualConversionToTextYson<float>(+9.243);

    CheckEqualConversionToTextYson<double>(0.0);
    CheckEqualConversionToTextYson<double>(-0.0);
    CheckEqualConversionToTextYson<double>(-7.7777);
    CheckEqualConversionToTextYson<double>(+9.243);
}

TEST(TTextYsonConvertTest, ConvertToTextFloatsSpecialValues)
{
    CheckEqualConversionToTextYson<double>(std::numeric_limits<double>::min());
    CheckEqualConversionToTextYson<double>(std::numeric_limits<double>::max());
    CheckEqualConversionToTextYson<double>(std::numeric_limits<double>::infinity());
    CheckEqualConversionToTextYson<double>(-std::numeric_limits<double>::infinity());
    CheckEqualConversionToTextYson<double>(std::numeric_limits<double>::quiet_NaN());
}

TEST(TTextYsonConvertTest, ConvertToTextOtherPrimitiveTypes)
{
    CheckEqualConversionToTextYson<bool>(true);
    CheckEqualConversionToTextYson<bool>(false);

    CheckEqualConversionToTextYson<TInstant>(TInstant::Now());
    CheckEqualConversionToTextYson<TInstant>(TInstant::Zero());
    CheckEqualConversionToTextYson<TInstant>(TInstant::FromValue(42));

    CheckEqualConversionToTextYson<TDuration>(TDuration::Zero());
    CheckEqualConversionToTextYson<TDuration>(TDuration::Seconds(2));
    CheckEqualConversionToTextYson<TDuration>(TDuration::MilliSeconds(123));
    CheckEqualConversionToTextYson<TDuration>(TDuration::MicroSeconds(12));

    CheckEqualConversionToTextYson<std::string>("Hello, world!");
    CheckEqualConversionToTextYson<std::string>("This is a so-called \"quotation marks\" test");
    CheckEqualConversionToTextYson<std::string>("This tests \r other \b hidden symbols \n");
    CheckEqualConversionToTextYson<std::string>("And this one tests special numbers numbers \x012");

    CheckEqualConversionToTextYson<TGuid>(TGuid::Create());
}

TEST(TTextYsonConvertTest, ConvertFromTextIntegrals)
{
    CheckEqualConversionToFromTextYson<i8>(+15);
    CheckEqualConversionToFromTextYson<i8>(0);
    CheckEqualConversionToFromTextYson<i8>(-15);
    CheckEqualConversionToFromTextYson<i32>(+100);
    CheckEqualConversionToFromTextYson<i32>(0);
    CheckEqualConversionToFromTextYson<i32>(-123);
    CheckEqualConversionToFromTextYson<i64>(+100);
    CheckEqualConversionToFromTextYson<i64>(0);
    CheckEqualConversionToFromTextYson<i64>(-123);

    CheckEqualConversionToFromTextYson<ui8>(+100);
    CheckEqualConversionToFromTextYson<ui8>(0);
    CheckEqualConversionToFromTextYson<ui32>(+100);
    CheckEqualConversionToFromTextYson<ui32>(0);
    CheckEqualConversionToFromTextYson<ui64>(+100);
    CheckEqualConversionToFromTextYson<ui64>(0);
}

TEST(TTextYsonConvertTest, ConvertFromTextIntegralsLimits)
{
    CheckEqualConversionToFromTextYson<i64>(std::numeric_limits<i64>::max());
    CheckEqualConversionToFromTextYson<i64>(std::numeric_limits<i64>::min());

    CheckEqualConversionToFromTextYson<ui64>(std::numeric_limits<ui64>::max());
    CheckEqualConversionToFromTextYson<ui64>(std::numeric_limits<ui64>::min());
}

TEST(TTextYsonConvertTest, ConvertFromTextFloats)
{
    CheckEqualConversionToFromTextYson<double>(0.0);
    CheckEqualConversionToFromTextYson<double>(-0.0);
    CheckEqualConversionToFromTextYson<double>(-7.7777);
    CheckEqualConversionToFromTextYson<double>(+9.243);
}

TEST(TTextYsonConvertTest, ConvertFromTextFloatsSpecialValues)
{
    CheckEqualConversionToFromTextYson<double>(std::numeric_limits<double>::min());
    CheckEqualConversionToFromTextYson<double>(std::numeric_limits<double>::max());
    CheckEqualConversionToFromTextYson<double>(std::numeric_limits<double>::infinity());
    CheckEqualConversionToFromTextYson<double>(-std::numeric_limits<double>::infinity());

    // nans do not compare.
    // CheckEqualConversionFromTextYson<double>(std::numeric_limits<double>::quiet_NaN());
}

TEST(TTextYsonConvertTest, ConvertFromTextOtherPrimitiveTypes)
{
    CheckEqualConversionToTextYson<bool>(true);
    CheckEqualConversionToTextYson<bool>(false);
    CheckEqualConversionToTextYson<bool>("true");
    CheckEqualConversionToTextYson<bool>("false");
    CheckEqualConversionToTextYson<bool>("0");
    CheckEqualConversionToTextYson<bool>("1");

    CheckEqualConversionToTextYson<TInstant>(TInstant::Now());
    CheckEqualConversionToTextYson<TInstant>(TInstant::Zero());
    CheckEqualConversionToTextYson<TInstant>(TInstant::FromValue(42));

    CheckEqualConversionToTextYson<TDuration>(TDuration::Zero());
    CheckEqualConversionToTextYson<TDuration>(TDuration::Seconds(2));
    CheckEqualConversionToTextYson<TDuration>(TDuration::MilliSeconds(123));
    CheckEqualConversionToTextYson<TDuration>(TDuration::MicroSeconds(12));

    CheckEqualConversionToTextYson<std::string>("Hello, world!");
    CheckEqualConversionToTextYson<std::string>("This is a so-called \"quotation marks\" test");
    CheckEqualConversionToTextYson<std::string>("This tests \r other \b hidden symbols \n");
    CheckEqualConversionToTextYson<std::string>("And this one tests special numbers numbers \x012");

    CheckEqualConversionToTextYson<TGuid>(TGuid::Create());
}

TEST(TTextYsonConvertTest, ConvertFromTextIntegralsTypeMissmatch)
{
    CheckEqualConversionToFromTextYson<i8>(static_cast<ui64>(+100));
    CheckEqualConversionToFromTextYson<i8>(static_cast<ui64>(0));
    CheckEqualConversionToFromTextYson<i32>(static_cast<ui64>(+100));
    CheckEqualConversionToFromTextYson<i32>(static_cast<ui64>(0));
    CheckEqualConversionToFromTextYson<i64>(static_cast<ui64>(+100));
    CheckEqualConversionToFromTextYson<i64>(static_cast<ui64>(0));
}

TEST(TTextYsonConvertTest, ConvertFromTextTypeMissmatch)
{
    CheckEqualConversionFromTextYson<bool>("%true");
    CheckEqualConversionFromTextYson<bool>("%false");
    CheckEqualConversionFromTextYson<bool>("1");
    CheckEqualConversionFromTextYson<bool>("0");
    CheckEqualConversionFromTextYson<bool>("-0");
    CheckEqualConversionFromTextYson<bool>("1u");
    CheckEqualConversionFromTextYson<bool>("0u");

    CheckEqualConversionFromTextYson<bool>(ConvertToTextYsonString("true").AsStringBuf());
    CheckEqualConversionFromTextYson<bool>(ConvertToTextYsonString("false").AsStringBuf());
    CheckEqualConversionFromTextYson<bool>(ConvertToTextYsonString("1").AsStringBuf());
    CheckEqualConversionFromTextYson<bool>(ConvertToTextYsonString("0").AsStringBuf());
}

TEST(TTextYsonConvertTest, ConvertFromTextYsonStringThrowBasicCases)
{
    auto fromPayload = [] (const auto& value) {
        return NYson::TYsonString(TString(value));
    };

    // Overflow.
    EXPECT_ANY_THROW(ConvertFromTextYsonString<i8>(fromPayload("123123123213")));
    EXPECT_ANY_THROW(ConvertTo<i8>(fromPayload("123123123213")));

    // Negative.
    EXPECT_ANY_THROW(ConvertFromTextYsonString<ui64>(fromPayload("-123")));
    EXPECT_ANY_THROW(ConvertTo<ui64>(fromPayload("-123")));

    // Non-numeric.
    EXPECT_ANY_THROW(ConvertFromTextYsonString<i64>(fromPayload("haha")));
    EXPECT_ANY_THROW(ConvertFromTextYsonString<i64>(fromPayload("123qq")));
    EXPECT_ANY_THROW(ConvertFromTextYsonString<i64>(fromPayload("-123u")));
    EXPECT_ANY_THROW(ConvertTo<i64>(fromPayload("haha")));
    EXPECT_ANY_THROW(ConvertTo<i64>(fromPayload("123qq")));
    EXPECT_ANY_THROW(ConvertTo<i64>(fromPayload("-123u")));

    // Big positive to bool
    EXPECT_ANY_THROW(ConvertFromTextYsonString<bool>(fromPayload("42")));
    EXPECT_ANY_THROW(ConvertTo<bool>(fromPayload("42")));

    // Garbage to bool
    EXPECT_ANY_THROW(ConvertFromTextYsonString<bool>(fromPayload("%falsse")));
    EXPECT_ANY_THROW(ConvertTo<bool>(fromPayload("%falsse")));

    // Wrong string to bool
    EXPECT_ANY_THROW(ConvertFromTextYsonString<bool>(fromPayload("\"True\"")));
    EXPECT_ANY_THROW(ConvertFromTextYsonString<bool>(fromPayload("\"False\"")));
    EXPECT_ANY_THROW(ConvertFromTextYsonString<bool>(fromPayload("\"1u\"")));
    EXPECT_ANY_THROW(ConvertFromTextYsonString<bool>(fromPayload("\"0u\"")));
    EXPECT_ANY_THROW(ConvertTo<bool>(fromPayload("\"True\"")));
    EXPECT_ANY_THROW(ConvertTo<bool>(fromPayload("\"False\"")));
    EXPECT_ANY_THROW(ConvertTo<bool>(fromPayload("\"1u\"")));
    EXPECT_ANY_THROW(ConvertTo<bool>(fromPayload("\"0u\"")));

    // Wrong string to string
    EXPECT_ANY_THROW(ConvertFromTextYsonString<std::string>(fromPayload("")));
    EXPECT_ANY_THROW(ConvertFromTextYsonString<std::string>(fromPayload("\"")));
    EXPECT_ANY_THROW(ConvertFromTextYsonString<std::string>(fromPayload("haha\"")));
    EXPECT_ANY_THROW(ConvertFromTextYsonString<std::string>(fromPayload("\'oops\'")));
    EXPECT_ANY_THROW(ConvertTo<std::string>(fromPayload("")));
    EXPECT_ANY_THROW(ConvertTo<std::string>(fromPayload("\"")));
    EXPECT_ANY_THROW(ConvertTo<std::string>(fromPayload("haha\"")));
    EXPECT_ANY_THROW(ConvertTo<std::string>(fromPayload("\'oops\'")));

    // Wrong literal to double
    EXPECT_ANY_THROW(ConvertFromTextYsonString<double>(fromPayload("%%")));
    EXPECT_ANY_THROW(ConvertFromTextYsonString<std::string>(fromPayload("%42inf")));
    EXPECT_ANY_THROW(ConvertFromTextYsonString<std::string>(fromPayload("%NaaN")));
    EXPECT_ANY_THROW(ConvertTo<double>(fromPayload("%%")));
    EXPECT_ANY_THROW(ConvertTo<std::string>(fromPayload("%42inf")));
    EXPECT_ANY_THROW(ConvertTo<std::string>(fromPayload("%NaaN")));
}

////////////////////////////////////////////////////////////////////////////////
} // namespace
} // namespace NYT::NYTree
