#include "udf_type_printer.h"

#include <yql/essentials/minikql/mkql_type_builder.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NYql::NUdf;

Y_UNIT_TEST_SUITE(TUdfTypePrinter) {
template <typename F>
void TestType(const TString& expected, F&& f) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    NKikimr::NMiniKQL::TTypeEnvironment env(alloc);
    NKikimr::NMiniKQL::TTypeBuilder typeBuilder(env);
    NKikimr::NMiniKQL::TTypeInfoHelper typeHelper;

    auto type = f(typeBuilder);
    TStringStream stream;
    TTypePrinter(typeHelper, type).Out(stream);
    UNIT_ASSERT_STRINGS_EQUAL(stream.Str(), expected);
}

Y_UNIT_TEST(TestLinear) {
    TestType("Linear<Int32>", [](const NKikimr::NMiniKQL::TTypeBuilder& typeBuilder) {
        return typeBuilder.NewLinearType(typeBuilder.NewDataType(EDataSlot::Int32), false);
    });

    TestType("DynamicLinear<Int32>", [](const NKikimr::NMiniKQL::TTypeBuilder& typeBuilder) {
        return typeBuilder.NewLinearType(typeBuilder.NewDataType(EDataSlot::Int32), true);
    });
}
} // Y_UNIT_TEST_SUITE(TUdfTypePrinter)
