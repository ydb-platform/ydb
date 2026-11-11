#include <yql/essentials/minikql/dom/yson.h>

#include <yql/essentials/minikql/dom/json.h>

#include <yql/essentials/minikql/dom/test_support/fixtures.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>

using namespace NYql;
using namespace NYql::NDom;
using namespace NKikimr;

Y_UNIT_TEST_SUITE(TYsonTests) {
Y_UNIT_TEST(TestValidate) {
    UNIT_ASSERT(IsValidYson(NTest::GetYsonDocument()));

    UNIT_ASSERT(!IsValidYson("[123}"));
    UNIT_ASSERT(!IsValidYson("[123];[456]"));
    UNIT_ASSERT(!IsValidYson(R"({"c" = "scm"])"));
    UNIT_ASSERT(!IsValidYson(""));
    UNIT_ASSERT(!IsValidYson(R"({"c";})"));
    UNIT_ASSERT(!IsValidYson(R"({# = "scm"})"));
    UNIT_ASSERT(!IsValidYson(R"({'one'= 1})"));
}

Y_UNIT_TEST(TestSerializeJsonNanInf) {
    NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    NMiniKQL::TMemoryUsageInfo memInfo("Memory");
    NMiniKQL::THolderFactory holderFactory(alloc.Ref(), memInfo, /*functionRegistry=*/nullptr);
    NMiniKQL::TDefaultValueBuilder builder(holderFactory);

    constexpr char yson[] =
        R"(
        {
            "Nan" = %nan;
            "Inf" = %inf;
            "NegInf" = %-inf
        }
        )";

    TString expected(R"({"Inf":"inf","Nan":"nan","NegInf":"-inf"})");

    const auto dom = TryParseYsonDom(yson, &builder);
    TString res = SerializeJsonDom(dom, /*skipMapEntity=*/false, /*encodeUtf8=*/true, /*writeNanAsString=*/true);

    UNIT_ASSERT_EQUAL(expected, res);
}
} // Y_UNIT_TEST_SUITE(TYsonTests)
