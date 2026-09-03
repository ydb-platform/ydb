#include "reflection.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

using namespace NYql::NJson;

namespace {

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

    friend bool operator==(const TPerson&, const TPerson&) = default;
};

} // namespace

namespace NYql::NReflection {

YQL_DEFINE_REFLECTING(TPet, (Name)(Age));
YQL_DEFINE_REFLECTING(TPerson, (Name)(Age)(Friends)(Pet));

} // namespace NYql::NReflection

namespace NYql::NJson {

JSON_DECLARE_FROM(TPet, json);
JSON_DECLARE_TO(TPet, value);
JSON_DECLARE_FROM(TPerson, json);
JSON_DECLARE_TO(TPerson, value);

YQL_DERIVE_JSON_FROM(TPet);
YQL_DERIVE_JSON_TO(TPet);
YQL_DERIVE_JSON_FROM(TPerson);
YQL_DERIVE_JSON_TO(TPerson);

} // namespace NYql::NJson

Y_UNIT_TEST_SUITE(Reflection) {

Y_UNIT_TEST(Happy) {
    const TStringBuf string1 = R"json({
        "name": "John",
        "age": 30,
        "friends": [
            {
                "name": "Mary",
                "age": 25,
                "friends": [],
                "pet": null
            },
            {
                "name": "Bob",
                "age": 40,
                "friends": [],
                "pet": {
                    "name": "Kitty",
                    "age": 2
                }
            }
        ]
    })json";

    auto person1 = FromJsonString<TPerson>(string1);
    UNIT_ASSERT_C(person1, person1.error());

    const TString string2 = ToJsonString(std::move(*person1));
    auto person2 = FromJsonString<TPerson>(string2);

    const TString string3 = ToJsonString(std::move(*person2));

    UNIT_ASSERT_NO_DIFF(string2, string3);
    UNIT_ASSERT_EQUAL(person1, person2);
}

} // Y_UNIT_TEST_SUITE(Reflection)
