#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/yson/writer.h>
#include <yt/yt/core/yson/string_filter.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NYson {
namespace {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TString Prettify(const TString& yson)
{
    return ConvertToYsonString(TYsonString(yson), EYsonFormat::Pretty).ToString();
}

TString PrettifyYson(const TYsonString& yson)
{
    return ConvertToYsonString(yson, EYsonFormat::Pretty).ToString();
};

void Check(const TString& yson, const std::vector<TYPath>& paths, const TString& expectedYson)
{
    auto originalYsonPretty = Prettify(yson);
    auto expectedYsonPretty = Prettify(expectedYson);
    auto actualYsonPretty = PrettifyYson(FilterYsonString(paths, TYsonStringBuf(yson), /*allowNullResult*/ false));
    EXPECT_EQ(expectedYsonPretty, actualYsonPretty)
        << "Original:" << std::endl << originalYsonPretty << std::endl
        << "Filter by: " << Format("%v", paths) << std::endl
        << "Expected:" << std::endl << expectedYsonPretty << std::endl
        << "Actual:" << std::endl << actualYsonPretty << std::endl;
}

void CheckThrow(const TString& yson)
{
    EXPECT_THROW(FilterYsonString({}, TYsonStringBuf(yson)), std::exception);
}

void CheckNull(const TString& yson, const std::vector<TYPath>& paths)
{
    auto originalYsonPretty = Prettify(yson);
    auto actualYson = FilterYsonString(paths, TYsonStringBuf(yson), /*allowNullResult*/ true);
    EXPECT_FALSE(actualYson)
        << "Original:" << std::endl << originalYsonPretty << std::endl
        << "Filter by: " << Format("%v", paths) << std::endl
        << "Expected: null YSON string" << std::endl
        << "Actual:" << std::endl << PrettifyYson(actualYson) << std::endl;
}

////////////////////////////////////////////////////////////////////////////////

const std::vector<TString> ScalarYsons = {"#", "%true", "-42", "23u", "42.5", "xyz"};
const std::vector<TString> CompositeYsons = {"{foo=bar}", "[foo;bar]"};

TEST(TStringFilterTest, TestNoMatchFallback)
{
    // In a no match case attribute-stripped original YSON must be returned.

    // Cases when original YSON is scalar.
    for (const TString& yson : ScalarYsons) {
        auto ysonWithAttributes = "<foo=bar>" + yson;
        Check(yson, {}, yson);
        Check(ysonWithAttributes, {}, yson);
        Check(yson, {"/a"}, yson);
        Check(ysonWithAttributes, {"/a"}, yson);
        Check(yson, {"/0"}, yson);
        Check(ysonWithAttributes, {"/0"}, yson);
    }

    // Cases when original YSON is a map or list.
    for (const auto& [yson, emptyCollection] : std::vector<std::pair<TString, TString>>{{"{foo=bar}", "{}"}, {"[foo; bar]", "[]"}}) {
        auto ysonWithAttributes = "<foo=bar>" + yson;
        Check(yson, {}, emptyCollection);
        Check(ysonWithAttributes, {}, emptyCollection);
        Check(yson, {"/a"}, emptyCollection);
        Check(ysonWithAttributes, {"/a"}, emptyCollection);
        Check(yson, {"/42"}, emptyCollection);
        Check(ysonWithAttributes, {"/42"}, emptyCollection);
    }
}

TEST(TStringFilterTest, TestNoMatchNullString)
{
    // In a no match case attribute-stripped original YSON must be returned.

    // Cases when original YSON is scalar.
    for (const TString& yson : Concatenate(ScalarYsons, CompositeYsons)) {
        auto ysonWithAttributes = "<foo=bar>" + yson;
        CheckNull(yson, {});
        CheckNull(ysonWithAttributes, {});
        CheckNull(yson, {"/a"});
        CheckNull(ysonWithAttributes, {"/a"});
        CheckNull(yson, {"/42"});
        CheckNull(ysonWithAttributes, {"/42"});
    }
}

TEST(TStringFilterTest, TestFullMatch)
{
    // In a match case original YSON must be returned.
    for (const TString& yson : ScalarYsons) {
        auto ysonWithAttributes = "<foo=bar>" + yson;
        Check(yson, {""}, yson);
        Check(ysonWithAttributes, {""}, ysonWithAttributes);
    }

    for (const TString& yson : {"{foo=bar}", "[foo;bar]"}) {
        auto ysonWithAttributes = "<foo=bar>" + yson;
        Check(yson, {""}, yson);
        Check(ysonWithAttributes, {""}, ysonWithAttributes);
    }
}

TEST(TStringFilterTest, TestSomeManualCases)
{
    Check("{a=1;b={foo=bar};c=2}", {"/b"}, "{b={foo=bar}}");
    Check("{a=1;b={foo=bar};c=2}", {"/b/foo"}, "{b={foo=bar}}");
    Check("{a=1;b={foo=bar};c=2}", {"/b/foo/oof"}, "{}");
    Check("{a=1;b={foo=bar};c=2}", {"/b/fuu"}, "{}");
    Check("{a=1;b={foo=bar};c=2}", {"/a", "/c"}, "{a=1;c=2}");
    Check("{a=1;b={foo=bar};c=2}", {"/a", "/b"}, "{a=1;b={foo=bar}}");
    Check("{a=1;b={foo=bar};c=2}", {"/a", "/b", "/c"}, "{a=1;b={foo=bar};c=2}");
    Check("{a=1;b={foo=bar};c=2}", {"/a", "/b/foo"}, "{a=1;b={foo=bar}}");
}

TEST(TStringFilterTest, TestSomeOtherManualCases)
{
    Check(
        "[{foo=0;bar=1};{foo=2;bar=3};{foo=4;bar=5};{foo=6;bar=7};{foo=8;bar=9}]",
        {"/0/foo", "/2/bar", "/4/foo"},
        "[{foo=0};#;{bar=5};#;{foo=8}]");
    Check(
        "[{foo=0;bar=1};{foo=2;bar=3};{foo=4;bar=5};{foo=6;bar=7};{foo=8;bar=9}]",
        {"/0", "/2/bar", "/4"},
        "[{foo=0;bar=1};#;{bar=5};#;{foo=8;bar=9}]");
    Check(
        "[{foo=0;bar=1};{foo=2;bar=3};{foo=4;bar=5};{foo=6;bar=7};{foo=8;bar=9}]",
        {"/0", "/2/bar", "/4"},
        "[{foo=0;bar=1};#;{bar=5};#;{foo=8;bar=9}]");
    Check(
        "[{foo=0;bar=1};{foo=2;bar=3};{foo=4;bar=5};{foo=6;bar=7};{foo=8;bar=9}]",
        {"/1", "/3/bar"},
        "[#;{foo=2;bar=3};#;{bar=7}]");
}

TEST(TStringFilterTest, TestAttributes)
{
    // Attributes of matched subYSONs are preserved.
    Check("<foo=bar>{a=<fuu=bor>42}", {""}, "<foo=bar>{a=<fuu=bor>42}");
    // Attributes of partially taken composites are dropped.
    Check("<foo=bar>{a=<fuu=bor>42}", {"/a"}, "{a=<fuu=bor>42}");
    // Paths with attributes are ignored (at least in the current implementation).
    Check("<foo=bar>{a=<fuu=bor>42}", {"/a/@fuu"}, "{}");
}

TEST(TStringFilterTest, TestIncorrectYson)
{
    // In case of incorrect YSON we at least must not crash. We do not intend
    // to test all possible structures of incorrect YSONs here, but let us check
    // some of them.
    for (const auto& yson : {
        "{", "[", "}", "]", "{a=1;b=2", "[1;2", "1;2]", "1;2", "<><>42", "{]", "[}",
        "{foo}", "{foo=}", "foo<", "", "[>]"
    }) {
        CheckThrow(yson);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYson
