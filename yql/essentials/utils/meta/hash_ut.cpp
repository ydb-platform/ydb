#include "hash.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

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

struct TYHashOnly {
    TString Name;

    friend bool operator==(const TYHashOnly& lhs, const TYHashOnly& rhs) = default;
};

namespace NYql::NReflection {

YQL_DEFINE_REFLECTING(TPet, (Name)(Age));
YQL_DEFINE_REFLECTING(TPerson, (Name)(Age)(Friends)(Pet));

} // namespace NYql::NReflection

YQL_DERIVE_HASH(TPet);
YQL_DERIVE_HASH(TPerson);

YQL_DEFINE_HASH(, TYHashOnly, value) {
    return CombineHashes(size_t{42}, ::THash<TString>{}(value.Name));
}

YQL_DEFINE_HASH(, TVector<ui32>, value) {
    Y_UNUSED(value);
    return 42;
}

namespace {

TPerson MakePerson() {
    return {
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
}

} // namespace

Y_UNIT_TEST_SUITE(HashTests) {

Y_UNIT_TEST(NonNull) {
    const TPerson person = MakePerson();
    UNIT_ASSERT_VALUES_UNEQUAL(THash<TPerson>{}(person), 0);
}

Y_UNIT_TEST(Change) {
    const TPerson person = MakePerson();
    TPerson changed = MakePerson();
    changed.Friends[1].Pet->Name = "Doggy";

    UNIT_ASSERT_UNEQUAL(THash<TPerson>{}(person), THash<TPerson>{}(changed));
}

Y_UNIT_TEST(Maybe) {
    const TMaybe<TPet> nothing = Nothing();
    const TMaybe<TPet> pet = TPet{.Name = "Kitty", .Age = 2};

    UNIT_ASSERT_UNEQUAL(THash<TMaybe<TPet>>{}(nothing), THash<TMaybe<TPet>>{}(pet));
}

Y_UNIT_TEST(ManualArcadiaHash) {
    const TYHashOnly value{.Name = "arcadia"};
    UNIT_ASSERT_VALUES_UNEQUAL(THash<TYHashOnly>{}(value), 0);
}

Y_UNIT_TEST(ManualVectorHashTakesPriority) {
    const TVector<ui32> value = {1, 2, 3};
    UNIT_ASSERT_VALUES_EQUAL(THash<TVector<ui32>>{}(value), 42);
}

} // Y_UNIT_TEST_SUITE(HashTests)
