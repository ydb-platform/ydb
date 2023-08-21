#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/table_client/composite_compare.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/yson/writer.h>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCompositeCompare, Simple)
{
    auto compare = [] (TStringBuf lhs, TStringBuf rhs) {
        return CompareCompositeValues(NYson::TYsonStringBuf(lhs), NYson::TYsonStringBuf(rhs));
    };

    EXPECT_EQ(-1, compare("-4", "42"));
    EXPECT_EQ(1, compare("42", "-4"));
    EXPECT_EQ(0, compare("-4", "-4"));

    EXPECT_EQ(-1, compare("4u", "42u"));
    EXPECT_EQ(1, compare("42u", "4u"));
    EXPECT_EQ(0, compare("4u", "4u"));

    EXPECT_EQ(-1, compare("4.0", "4.2"));
    EXPECT_EQ(1, compare("4.2", "4.0"));
    EXPECT_EQ(0, compare("4.0", "4.0"));

    EXPECT_EQ(1, compare("%nan ", "-5.0"));
    EXPECT_EQ(-1, compare("-5.0", "%nan "));
    EXPECT_EQ(0, compare("%nan ", "%nan "));

    EXPECT_EQ(-1, compare("%false", "%true"));
    EXPECT_EQ(1, compare("%true", "%false"));
    EXPECT_EQ(0, compare("%true", "%true"));

    EXPECT_EQ(1, compare("foo", "bar"));
    EXPECT_EQ(-1, compare("foo", "fooo"));
    EXPECT_EQ(0, compare("foo", "foo"));

    // Tuple<Int64,Int64> or List<Int64>
    EXPECT_EQ(-1, compare("[1; 2]", "[1; 3]"));
    EXPECT_EQ(1, compare("[1; 3]", "[1; 2]"));

    // List<Int64>
    EXPECT_EQ(1, compare("[1; 2; 3]", "[1; 2]"));
    EXPECT_EQ(-1, compare("[1; 2]", "[1; 2; 3]"));
    EXPECT_EQ(0, compare("[1; 2; 3]", "[1; 2; 3]"));

    // List<Optional<Int64>>
    EXPECT_EQ(1, compare("[1; 2; #]", "[1; 2]"));
    EXPECT_EQ(-1, compare("[1; 2]", "[1; 2; #]"));
    EXPECT_EQ(-1, compare("[1; 2; #]", "[1; 2; 3]"));
    EXPECT_EQ(1, compare("[1; 2; 3]", "[1; 2; #]"));
}

TEST(TCompositeCompare, CompositeFingerprint)
{
    auto getFarmHash = [] (TStringBuf value) {
        return CompositeFarmHash(NYson::TYsonStringBuf(value));
    };

    EXPECT_EQ(getFarmHash("-42"), GetFarmFingerprint(MakeUnversionedInt64Value(-42)));
    EXPECT_EQ(getFarmHash("100500u"), GetFarmFingerprint(MakeUnversionedUint64Value(100500)));
    EXPECT_EQ(getFarmHash("3.25"), GetFarmFingerprint(MakeUnversionedDoubleValue(3.25)));
    EXPECT_EQ(getFarmHash("%true"), GetFarmFingerprint(MakeUnversionedBooleanValue(true)));
    EXPECT_EQ(getFarmHash("%false"), GetFarmFingerprint(MakeUnversionedBooleanValue(false)));
    EXPECT_EQ(getFarmHash("#"), GetFarmFingerprint(MakeUnversionedNullValue()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
