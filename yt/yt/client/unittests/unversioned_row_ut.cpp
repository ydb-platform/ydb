#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/token_writer.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <util/stream/str.h>

namespace NYT::NTableClient {
namespace {

using namespace NYson;
using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TEST(TUnversionedValue, TestConversionToYsonTokenWriter)
{
    auto convert = [] (TUnversionedValue value) {
        TStringStream stream;
        TCheckedInDebugYsonTokenWriter tokenWriter(&stream);
        UnversionedValueToYson(value, &tokenWriter);
        tokenWriter.Finish();
        return stream.Str();
    };

    {
        auto value = MakeUnversionedInt64Value(-42);
        i64 parsed = 0;
        EXPECT_NO_THROW(parsed = ConvertTo<i64>(TYsonString(convert(value))));
        EXPECT_EQ(parsed, -42);
    }
    {
        auto value = MakeUnversionedUint64Value(std::numeric_limits<ui64>::max());
        ui64 parsed = 0;
        EXPECT_NO_THROW(parsed = ConvertTo<ui64>(TYsonString(convert(value))));
        EXPECT_EQ(parsed, std::numeric_limits<ui64>::max());
    }
    {
        auto value = MakeUnversionedDoubleValue(2.718);
        double parsed = 0.0;
        EXPECT_NO_THROW(parsed = ConvertTo<double>(TYsonString(convert(value))));
        EXPECT_DOUBLE_EQ(parsed, 2.718);
    }
    {
        auto value = MakeUnversionedStringValue("boo");
        TString parsed;
        EXPECT_NO_THROW(parsed = ConvertTo<TString>(TYsonString(convert(value))));
        EXPECT_EQ(parsed, "boo");
    }
    {
        auto value = MakeUnversionedNullValue();
        TString str;
        EXPECT_NO_THROW(str = convert(value));
        EXPECT_EQ(str, "#");
    }
    {
        auto value = MakeUnversionedAnyValue("{x=y;z=<a=b>2}");
        INodePtr parsed;
        EXPECT_NO_THROW(parsed = ConvertTo<INodePtr>(TYsonString(convert(value))));
        auto expected = BuildYsonNodeFluently()
            .BeginMap()
                .Item("x").Value("y")
                .Item("z")
                    .BeginAttributes()
                        .Item("a").Value("b")
                    .EndAttributes()
                    .Value(2)
            .EndMap();
        EXPECT_TRUE(AreNodesEqual(parsed, expected))
            << "parsed: " << ConvertToYsonString(parsed, EYsonFormat::Pretty).AsStringBuf()
            << "\nexpected: " << ConvertToYsonString(expected, EYsonFormat::Pretty).AsStringBuf();
    }
}

////////////////////////////////////////////////////////////////////////////////
// Some sanity tests.
static_assert(TUnversionedValueConversionTraits<i64>::Inline, "i64 must be inline.");
static_assert(TUnversionedValueConversionTraits<i64>::Scalar, "i64 must be scalar.");
static_assert(TUnversionedValueConversionTraits<std::optional<i64>>::Inline, "i64? must be inline.");
static_assert(TUnversionedValueConversionTraits<std::optional<i64>>::Scalar, "i64? must be scalar.");
static_assert(!TUnversionedValueConversionTraits<TString>::Inline, "TString must not be inline.");
static_assert(TUnversionedValueConversionTraits<TString>::Scalar, "TString must be scalar.");
static_assert(TUnversionedValueConversionTraits<TAnnotatedValue<i64>>::Scalar, "i64 must be scalar.");
YT_DEFINE_STRONG_TYPEDEF(TStrongInt, i64)
static_assert(TUnversionedValueConversionTraits<TStrongInt>::Scalar, "TStrongInt must be scalar.");

////////////////////////////////////////////////////////////////////////////////

TEST(TMakeUnversionedOwningRow, Empty)
{
    auto row = MakeUnversionedOwningRow();
    EXPECT_EQ(0, row.GetCount());
}

template <class T>
void CheckSingleValue(T value)
{
    auto row = MakeUnversionedOwningRow(value);
    EXPECT_EQ(1, row.GetCount());
    EXPECT_EQ(0, row[0].Id);
    EXPECT_EQ(value, FromUnversionedValue<T>(row[0]));
}

TEST(TMakeUnversionedOwningRow, SingleValue)
{
    CheckSingleValue(TGuid::Create());
    CheckSingleValue(TString("hello"));
    CheckSingleValue(TStringBuf("hello"));
    CheckSingleValue(true);
    CheckSingleValue(TYsonString(TStringBuf("{a=1}")));
    CheckSingleValue(static_cast<i64>(-123));
    CheckSingleValue(static_cast<ui64>(123));
    CheckSingleValue(static_cast<i32>(-17));
    CheckSingleValue(static_cast<ui32>(17));
    CheckSingleValue(static_cast<i16>(-2342));
    CheckSingleValue(static_cast<ui16>(2342));
    CheckSingleValue(static_cast<i8>(-12));
    CheckSingleValue(static_cast<ui8>(12));
    CheckSingleValue(static_cast<double>(3.14));
    CheckSingleValue(TInstant::Now());
    CheckSingleValue(TDuration::Seconds(10));
    CheckSingleValue(TStrongInt(123));
}

TEST(TMakeUnversionedOwningRow, CharPtr)
{
    auto row = MakeUnversionedOwningRow("test");
    EXPECT_EQ(1, row.GetCount());
    EXPECT_EQ(0, row[0].Id);
    EXPECT_EQ("test", FromUnversionedValue<TString>(row[0]));
}

TEST(TMakeUnversionedOwningRow, NullValue)
{
    auto row = MakeUnversionedOwningRow(std::nullopt);
    EXPECT_EQ(1, row.GetCount());
    EXPECT_EQ(0, row[0].Id);
    EXPECT_EQ(EValueType::Null, row[0].Type);
}

TEST(TMakeUnversionedOwningRow, Tuple)
{
    auto row = MakeUnversionedOwningRow(TString("hello"), true);
    EXPECT_EQ(2, row.GetCount());
    EXPECT_EQ(0, row[0].Id);
    EXPECT_EQ("hello", FromUnversionedValue<TString>(row[0]));
    EXPECT_EQ(1, row[1].Id);
    EXPECT_EQ(true, FromUnversionedValue<bool>(row[1]));
}

TEST(TMakeUnversionedOwningRow, FromUnversionedRow)
{
    auto row = MakeUnversionedOwningRow(TString("hello"), TStringBuf("world"), 123);
    TString a;
    TStringBuf b;
    i16 c;
    FromUnversionedRow(row, &a, &b, &c);
    EXPECT_EQ("hello", a);
    EXPECT_EQ("world", b);
    EXPECT_EQ(123, c);
}

TEST(TMakeUnversionedOwningRow, TupleFromUnversionedRow)
{
    auto row = MakeUnversionedOwningRow(TString("hello"), TStringBuf("world"), 123);
    auto [a, b, c] = FromUnversionedRow<TString, TStringBuf, i16>(row);
    EXPECT_EQ("hello", a);
    EXPECT_EQ("world", b);
    EXPECT_EQ(123, c);
}

TEST(TMakeUnversionedOwningRow, ExplicitIds)
{
    auto row = MakeUnversionedOwningRow(
        TAnnotatedValue{TString("hello"), 10},
        TAnnotatedValue{TStringBuf("world"), 20});
    EXPECT_EQ(2, row.GetCount());
    EXPECT_EQ(10, row[0].Id);
    EXPECT_EQ(20, row[1].Id);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TUnversionedRowsBuilder, Empty)
{
    TUnversionedRowsBuilder builder;
    auto rows = builder.Build();
    EXPECT_EQ(0, std::ssize(rows));
}

TEST(TUnversionedRowsBuilder, SomeValues)
{
    TUnversionedRowsBuilder builder;
    builder.AddRow(1, "hello");
    builder.AddRow(2, "world");
    auto rows = builder.Build();
    EXPECT_EQ(2, std::ssize(rows));
    {
        auto [i, s] = FromUnversionedRow<int, TString>(rows[0]);
        EXPECT_EQ(1, i);
        EXPECT_EQ("hello", s);
        EXPECT_EQ(0, rows[0][0].Id);
        EXPECT_EQ(1, rows[0][1].Id);
    }
    {
        auto [i, s] = FromUnversionedRow<int, TString>(rows[1]);
        EXPECT_EQ(2, i);
        EXPECT_EQ("world", s);
        EXPECT_EQ(0, rows[1][0].Id);
        EXPECT_EQ(1, rows[1][1].Id);
    }
}

TEST(TUnversionedRowsBuilder, AnnotatedValue)
{
    TUnversionedRowsBuilder builder;
    builder.AddRow(TAnnotatedValue{1, 10}, TAnnotatedValue{"hello", 20});
    builder.AddRow(TAnnotatedValue{2, 30}, TAnnotatedValue{"world", 40});
    builder.AddRow(TAnnotatedValue{77, 1, EValueFlags::Aggregate});
    auto rows = builder.Build();
    EXPECT_EQ(3, std::ssize(rows));
    {
        auto [i, s] = FromUnversionedRow<int, TString>(rows[0]);
        EXPECT_EQ(1, i);
        EXPECT_EQ("hello", s);
        EXPECT_EQ(10, rows[0][0].Id);
        EXPECT_EQ(20, rows[0][1].Id);
    }
    {
        auto [i, s] = FromUnversionedRow<int, TString>(rows[1]);
        EXPECT_EQ(2, i);
        EXPECT_EQ("world", s);
        EXPECT_EQ(30, rows[1][0].Id);
        EXPECT_EQ(40, rows[1][1].Id);
    }
    {
        auto [i] = FromUnversionedRow<int>(rows[2]);
        EXPECT_EQ(77, i);
        EXPECT_EQ(1, rows[2][0].Id);
        EXPECT_EQ(EValueFlags::Aggregate, rows[2][0].Flags);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
