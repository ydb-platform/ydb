#include <yql/essentials/minikql/udf_value_test_support/struct_type.h>
#include <yql/essentials/minikql/udf_value_test_support/test_types_equal_to.h>
#include <yql/essentials/minikql/udf_value_test_support/test_types_hash.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

namespace NYql::NUdf {
namespace {

using TKeyFirst = NTest::TStructType<
    NTest::TStructMember<"key", ui32>,
    NTest::TStructMember<"value", TString>>;
using TValueFirst = NTest::TStructType<
    NTest::TStructMember<"value", TString>,
    NTest::TStructMember<"key", ui32>>;

Y_UNIT_TEST_SUITE(TStructMemberNameTest) {
Y_UNIT_TEST(Literal) {
    constexpr NTest::TStructMemberName name("value");
    static_assert(std::string_view(name) == "value");
    static_assert(NTest::TStructMember<name, ui32>::MemberName() == "value");
    UNIT_ASSERT_VALUES_EQUAL(TStringBuf(name), "value");
    UNIT_ASSERT_VALUES_EQUAL(name.Data.back(), '\0');
}

Y_UNIT_TEST(EmptyLiteral) {
    constexpr NTest::TStructMemberName name("");
    static_assert(std::string_view(name).empty());
    static_assert(NTest::TStructMember<name, ui32>::MemberName().empty());
    UNIT_ASSERT(TStringBuf(name).empty());
    UNIT_ASSERT_VALUES_EQUAL(name.Data.back(), '\0');
}

Y_UNIT_TEST(EmbeddedNull) {
    constexpr NTest::TStructMemberName name("x\0y");
    static_assert(std::string_view(name) == std::string_view("x\0y", 3));
    UNIT_ASSERT_VALUES_EQUAL(TStringBuf(name), TStringBuf("x\0y", 3));
}

Y_UNIT_TEST(Filled) {
    constexpr NTest::TStructMemberName<4> name('x');
    static_assert(std::string_view(name) == "xxx");
    UNIT_ASSERT_VALUES_EQUAL(TStringBuf(name), "xxx");
    UNIT_ASSERT_VALUES_EQUAL(name.Data.back(), '\0');
}

Y_UNIT_TEST(EmptyFilled) {
    constexpr NTest::TStructMemberName<1> name('x');
    static_assert(std::string_view(name).empty());
    UNIT_ASSERT(TStringBuf(name).empty());
    UNIT_ASSERT_VALUES_EQUAL(name.Data.back(), '\0');
}
} // Y_UNIT_TEST_SUITE(TStructMemberNameTest)

Y_UNIT_TEST_SUITE(TStructTypeTest) {

Y_UNIT_TEST(EqualWithDifferentDeclaredOrder) {
    TKeyFirst a{{{ui32(42)}, {TString("hello")}}};
    TValueFirst b{{{TString("hello")}, {ui32(42)}}};

    UNIT_ASSERT(TTestTypeEqualTo<TKeyFirst>{}(a, b));

    TValueFirst c{{{TString("world")}, {ui32(42)}}};
    UNIT_ASSERT(!TTestTypeEqualTo<TKeyFirst>{}(a, c));
}

Y_UNIT_TEST(HashAgreesRegardlessOfDeclaredOrder) {
    TKeyFirst a{{{ui32(42)}, {TString("hello")}}};
    TValueFirst b{{{TString("hello")}, {ui32(42)}}};

    UNIT_ASSERT_VALUES_EQUAL(TTestTypeHash<TKeyFirst>{}(a), TTestTypeHash<TKeyFirst>{}(b));
    UNIT_ASSERT_VALUES_EQUAL(TTestTypeHash<TKeyFirst>{}(a), TTestTypeHash<TValueFirst>{}(a));
}

} // Y_UNIT_TEST_SUITE(TStructTypeTest)

} // namespace
} // namespace NYql::NUdf
