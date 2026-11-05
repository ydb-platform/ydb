#include <ydb/core/kqp/common/simple/mvcc_version.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(MvccVersionHeader) {
    Y_UNIT_TEST(RoundTrip) {
        const TRowVersion versions[] = {
            { 1, 0 },
            { 0, 1 },
            { 123, 456 },
            { 1, Max<ui64>() },
            { Max<ui64>() - 1, Max<ui64>() },
        };
        for (const auto& expected : versions) {
            const auto actual = ParseMvccVersion(FormatMvccVersion(expected));
            UNIT_ASSERT(actual);
            UNIT_ASSERT(*actual == expected);
        }
        UNIT_ASSERT_VALUES_EQUAL(FormatMvccVersion({ 123, 456 }), "123:456");
        UNIT_ASSERT(ParseMvccVersion("001:002") == TMaybe<TRowVersion>(TRowVersion(1, 2)));
    }

    Y_UNIT_TEST(InvalidValues) {
        const TStringBuf values[] = {
            "",
            "1",
            ":",
            "1:",
            ":1",
            "1:2:3",
            "1/2",
            "-1:2",
            "1:-2",
            "+1:2",
            "1:+2",
            " 1:2",
            "1:2 ",
            "1: 2",
            "1:2\n",
            "a:2",
            "1:0x2",
            "0:0",
            "18446744073709551616:1",
            "1:18446744073709551616",
            "18446744073709551615:0",
            "18446744073709551615:18446744073709551615",
        };
        for (const auto value : values) {
            UNIT_ASSERT_C(!ParseMvccVersion(value), value);
        }
    }
}

}   // namespace NKikimr::NKqp
