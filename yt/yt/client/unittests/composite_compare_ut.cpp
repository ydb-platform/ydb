#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/table_client/composite_compare.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/yson/writer.h>

namespace NYT::NTableClient {
namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TCompositeCompare, Simple)
{
    auto compare = [] (TStringBuf lhs, TStringBuf rhs) {
        return CompareYsonValues(TYsonStringBuf(lhs), TYsonStringBuf(rhs));
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

    // Tuple<Int64, Int64> or List<Int64>
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
        return CompositeFarmHash(TYsonStringBuf(value));
    };

    EXPECT_EQ(getFarmHash("-42"), GetFarmFingerprint(MakeUnversionedInt64Value(-42)));
    EXPECT_EQ(getFarmHash("100500u"), GetFarmFingerprint(MakeUnversionedUint64Value(100500)));
    EXPECT_EQ(getFarmHash("3.25"), GetFarmFingerprint(MakeUnversionedDoubleValue(3.25)));
    EXPECT_EQ(getFarmHash("%true"), GetFarmFingerprint(MakeUnversionedBooleanValue(true)));
    EXPECT_EQ(getFarmHash("%false"), GetFarmFingerprint(MakeUnversionedBooleanValue(false)));
    EXPECT_EQ(getFarmHash("#"), GetFarmFingerprint(MakeUnversionedNullValue()));
}

TEST(TCompositeCompare, TruncateYsonValue)
{
    auto normalizeYson = [] (TStringBuf yson) {
        return yson.empty() ? TString(yson) : ConvertToYsonString(TYsonString(yson), EYsonFormat::Binary).ToString();
    };

    auto getTruncatedYson = [&] (TStringBuf original, i64 size) {
        auto truncatedValue = TruncateYsonValue(TYsonString(original), size);
        return truncatedValue ? truncatedValue->ToString() : "";
    };

    // When we rebuild the whole string during truncation, we should produce the correct normalized binary YSON version of the string as output.
    auto checkFullStringIdempotence = [&] (TStringBuf yson) {
        auto normalizedYson = normalizeYson(yson);
        EXPECT_EQ(normalizedYson, getTruncatedYson(yson, std::numeric_limits<i64>::max()));
        EXPECT_EQ(normalizedYson, getTruncatedYson(yson, std::ssize(normalizedYson)));
    };

    auto checkTruncatedYson = [&] (TStringBuf expectedTruncatedYson, TStringBuf originalYson, i64 size) {
        auto normalizedExpectedTruncatedYson = normalizeYson(expectedTruncatedYson);
        auto truncatedYson = getTruncatedYson(originalYson, size);

        // For easier debugging.
        EXPECT_EQ(normalizedExpectedTruncatedYson.size(), truncatedYson.size());
        EXPECT_EQ(normalizedExpectedTruncatedYson, truncatedYson);
    };

    checkFullStringIdempotence("[[5; 7]; [1; 5];]");
    checkFullStringIdempotence("[[5; 7]; [1; 5; 4; 3]; [2; 0; 0; 7]]");
    checkFullStringIdempotence("[[5; 7]; [1; 5; 4; 3; [g; r; i; t; #; k; #; n]]; [%true; [%false; 0;];]; [2; 0; 0; 7]]");
    checkFullStringIdempotence("this-string-desperately-wants-to-be-filled-with-some-funny-references-but-i-have-no-ideas");
    checkFullStringIdempotence("%true");
    checkFullStringIdempotence("#");
    checkFullStringIdempotence("\"\"");

    checkTruncatedYson("[[5; 7]; [1; 5];]", "[[5; 7]; [1; 5; 4; 3]; [2; 0; 0; 7]]", 20);
    checkTruncatedYson("[[5; 7]; [1; 5];]", "[[5; 7]; [1; 5; 4; 3]; [2; 0; 0; 7]]", 21);
    checkTruncatedYson("[[5; 7]; [1; 5];]", "[[5; 7]; [1; 5; 4; 3]; [2; 0; 0; 7]]", 22);
    // We need 3 more bytes for the next integer: 1 for the type flag, 1 for the varint, 1 for the item separator.
    checkTruncatedYson("[[5; 7]; [1; 5; 4;];]", "[[5; 7]; [1; 5; 4; 3]; [2; 0; 0; 7]]", 23);
    // The value 1543 takes up 4 extra bytes, since it is represented as 2 varint bytes.
    checkTruncatedYson("[[5; 7]; [1; 5; 4; 1543];]", "[[5; 7]; [1; 5; 4; 1543]; [2; 0; 0; 7]]", 27);

    checkTruncatedYson("[[#; 0;];]", "[[#; 0; %true; x; #]; 1;]", 10);
    // We need 2 more bytes for the boolean: 1 for the type flag which encodes the value itself, 1 for the item serpator.
    checkTruncatedYson("[[#; 0; %true];]", "[[#; 0; %true; #; x]; 1;]", 12);
    // Same for entities.
    checkTruncatedYson("[[#; 0; %true; #;];]", "[[#; 0; %true; #; x]; 1;]", 14);

    // NB: "" is actually std::nullopt returned from the function, it is just easier to visualize this way.
    checkTruncatedYson("", "abacaba", 1);
    checkTruncatedYson("", "1", 1);
    checkTruncatedYson("", "[]", 1);
    // Entity takes up only 1 byte!
    checkTruncatedYson("#", "#", 1);

    checkTruncatedYson("this-string", "this-string-desperately-wants-to-be-filled-with-some-funny-references-but-i-have-no-ideas", 14);
    checkTruncatedYson("this-string-", "this-string-desperately-wants-to-be-filled-with-some-funny-references-but-i-have-no-ideas", 15);
    checkTruncatedYson("this-string-desperatel", "this-string-desperately-wants-to-be-filled-with-some-funny-references-but-i-have-no-ideas", 25);
    checkTruncatedYson("[please; [take; [me; ha;];];]", "[please; [take; [me; haha; too; late]]]", 34);
    // The actual size of the resulting yson is only 4 bytes, but during truncation it is too hard to account for the fact that longer strings
    // take up more bytes for their length, since it is represented as a varint.
    checkTruncatedYson("aa", TString(1000, 'a'), 5);
    checkTruncatedYson("\"\"", "erase-me", 2);

    checkTruncatedYson("[[5; 7]; [1; 5; 4; 3]; [];]", "[[5; 7]; [1; 5; 4; 3]; [{hello=darkness}; 0; 0; 7]]", 10000);
    checkTruncatedYson("[[5; 7];]", "[[5; 7]; <my-name=borat>[1; 5; 4; 3]; [{greetings=xoxo}; 0; 0; 7]]", 10000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
