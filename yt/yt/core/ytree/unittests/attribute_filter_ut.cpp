#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/attribute_filter.h>

namespace NYT::NYTree {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TEST(TAttributeFilterTest, TestValidation)
{
    auto expectValidationError = [] (const TYPath& path) {
        TAttributeFilter filter({}, {path});
        EXPECT_THROW_WITH_SUBSTRING(filter.Normalize(), "Error validating attribute path");
    };
    expectValidationError("");
    expectValidationError("/");
    expectValidationError("/foo/");
    expectValidationError("/foo/@");
}

TEST(TAttributeFilterTest, TestNormalization)
{
    using TKeyToFilter = TAttributeFilter::TKeyToFilter;
    using TPathFilter = TAttributeFilter::TPathFilter;

    // Old-fashioned attribute key filtering; done via fast path.
    EXPECT_EQ(
        (TKeyToFilter{{"foo", TPathFilter{}}, {"bar", TPathFilter{}}}),
        TAttributeFilter({"foo", "bar"}).Normalize());

    // Repeated attribute names must be uniquified (yes, this is an actual word).
    EXPECT_EQ(
        (TKeyToFilter{{"foo", TPathFilter({})}}),
        TAttributeFilter({"foo", "foo"}).Normalize());

    // "/foo/bar" should form its own group.
    EXPECT_EQ(
        (TKeyToFilter{{"foo", TPathFilter({"/bar"})}}),
        TAttributeFilter({}, {"/foo/bar"}).Normalize());

    // "/foo/{bar,baz}" is also a single group.
    EXPECT_EQ(
        (TKeyToFilter{{"foo", TPathFilter({"/bar", "/baz"})}}),
        TAttributeFilter({}, {"/foo/bar", "/foo/baz"}).Normalize());

    // "/foo/bar/baz" is discarded by "/foo/bar".
    EXPECT_EQ(
        (TKeyToFilter{{"foo", TPathFilter({"/bar"})}}),
        TAttributeFilter({}, {"/foo/bar/baz", "/foo/bar"}).Normalize());

    // "/foo/bar" is discarded by the key "foo".
    EXPECT_EQ(
        (TKeyToFilter{{"foo", TPathFilter({""})}}),
        TAttributeFilter({"foo"}, {"/foo/bar"}).Normalize());

    // Check that integral literals are ok.
    EXPECT_EQ(
        (TKeyToFilter{{"foo", TPathFilter({"/1", "/3/bar"})}}),
        TAttributeFilter({}, {"/foo/1", "/foo/3/bar"}).Normalize());

    // Make sure foo and foobar do not discard each other.
    EXPECT_EQ(
        (TKeyToFilter{{"foo", TPathFilter({""})}, {"foobar", TPathFilter({""})}}),
        TAttributeFilter({}, {"/foo", "/foobar"}).Normalize());

    EXPECT_EQ(
        (TKeyToFilter{
            {"yp", TPathFilter({"/rex"})},
            {"yql", TPathFilter({""})},
            {"yt", TPathFilter({"/yt/core", "/yt/server/master", "/yt/server/node"})},
        }),
        TAttributeFilter(
            {"yql"}, {
                "/yp/rex",
                "/yt/yt/core",
                "/yt/yt/server/master",
                "/yt/yt/server/node",
                "/yt/yt/server/node/data_node"
            }).Normalize());

    // A tricky case requiring normalization of paths. Note that "yp" and "y\\x70" coincide
    // as YPath literals, but as strings "yp" < "yt" < "y\\x70".
    EXPECT_EQ(
        (TKeyToFilter{{"yp", TPathFilter({"/foo"})}, {"yt", TPathFilter({"/bar"})}}),
        TAttributeFilter({}, {"/yp/foo", "/y\\x70/foo/qux", "/yt/bar"}).Normalize());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
