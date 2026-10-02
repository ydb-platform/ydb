#include "out.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>

struct TPet {
    TString Name;
    ui64 Age = 0;

    friend bool operator==(const TPet& lhs, const TPet& rhs) = default;
};

struct TPerson {
    TString Name;
    ui64 Age = 0;
    TVector<TPerson> Friends;
    TMaybe<TPet> Pet;

    friend bool operator==(const TPerson& lhs, const TPerson& rhs) = default;
};

namespace NYql::NReflection {

YQL_DEFINE_REFLECTING(TPet, (Name)(Age));
YQL_DEFINE_REFLECTING(TPerson, (Name)(Age)(Friends)(Pet));

} // namespace NYql::NReflection

using THashMapU32U32 = THashMap<ui32, ui32>;

YQL_DERIVE_OUT_SPEC(TPet);
YQL_DERIVE_OUT_SPEC(TPerson);
YQL_DERIVE_OUT_SPEC(THashSet<ui32>);
YQL_DERIVE_OUT_SPEC(THashMapU32U32);

Y_UNIT_TEST_SUITE(OutTests) {

Y_UNIT_TEST(Example) {
    TPerson person = {
        .Name = "John",
        .Age = 30,
        .Friends = {
            {
                .Name = "Mary",
                .Age = 25,
                .Friends = {},
                .Pet = Nothing(),
            },
            {
                .Name = "Bob",
                .Age = 40,
                .Friends = {},
                .Pet = TPet{
                    .Name = "Kitty",
                    .Age = 2,
                },
            },
        },
    };

    const TString actual = ToString(person);

    const TString expected = "{.Name = John, .Age = 30, .Friends = {{.Name = Mary, .Age = 25, .Friends = {}, .Pet = Nothing()}, {.Name = Bob, .Age = 40, .Friends = {}, .Pet = {.Name = Kitty, .Age = 2}}}, .Pet = Nothing()}";

    UNIT_ASSERT_STRINGS_EQUAL(expected, actual);
}

Y_UNIT_TEST(EmptyHashSet) {
    UNIT_ASSERT_STRINGS_EQUAL("{}", ToString(THashSet<ui32>{}));
}

Y_UNIT_TEST(TwoElementHashSet) {
    UNIT_ASSERT_STRINGS_EQUAL("{1, 2}", ToString(THashSet<ui32>{1, 2}));
}

Y_UNIT_TEST(ThreeElementHashSet) {
    UNIT_ASSERT_STRINGS_EQUAL("{1, 2, 3}", ToString(THashSet<ui32>{1, 2, 3}));
}

Y_UNIT_TEST(EmptyHashMap) {
    UNIT_ASSERT_STRINGS_EQUAL("{}", ToString(THashMap<ui32, ui32>{}));
}

Y_UNIT_TEST(TwoElementHashMap) {
    const THashMap<ui32, ui32> value = {{1, 10}, {2, 20}};
    UNIT_ASSERT_STRINGS_EQUAL("{{1, 10}, {2, 20}}", ToString(value));
}

Y_UNIT_TEST(ThreeElementHashMap) {
    const THashMap<ui32, ui32> value = {{1, 10}, {2, 20}, {3, 30}};
    UNIT_ASSERT_STRINGS_EQUAL("{{1, 10}, {2, 20}, {3, 30}}", ToString(value));
}

} // Y_UNIT_TEST_SUITE(OutTests)
