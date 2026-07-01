#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLFromStringTest) {
Y_UNIT_TEST_LLVM(TestFromString) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto data = NTest::ConvertValueToLiteralNode(pb, TStringBuf("abcdefg"));
    const auto pgmReturn1 = pb.StrictFromString(data, NTest::ConvertToMinikqlType<NTest::TDecimalLiteral<10, 7>>(pb));
    const auto graph1 = setup.BuildGraph(pgmReturn1);
    UNIT_ASSERT_EXCEPTION_CONTAINS(graph1->GetValue(), std::exception, R"(Terminate was called, reason(45): could not convert "abcdefg" to Decimal(10, 7))");

    const auto pgmReturn2 = pb.StrictFromString(data, NTest::ConvertToMinikqlType<ui64>(pb));
    const auto graph2 = setup.BuildGraph(pgmReturn2);
    UNIT_ASSERT_EXCEPTION_CONTAINS(graph2->GetValue(), std::exception, R"(Terminate was called, reason(37): could not convert "abcdefg" to Uint64)");
}

Y_UNIT_TEST_LLVM(TestFromStringHugePayload) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    const TString hugeXString = TString(10000, 'x');
    const TStringBuf halfHugeXString = TStringBuf(hugeXString.data(), hugeXString.size() / 2);
    const auto data = NTest::ConvertValueToLiteralNode(pb, TStringBuf(hugeXString));
    const auto pgmReturn1 = pb.StrictFromString(data, NTest::ConvertToMinikqlType<NTest::TDecimalLiteral<10, 7>>(pb));
    const auto graph1 = setup.BuildGraph(pgmReturn1);
    UNIT_ASSERT_EXCEPTION_CONTAINS(graph1->GetValue(), std::exception, TStringBuilder() << R"(Terminate was called, reason(5050): could not convert ")" << halfHugeXString << R"(" (truncated) to Decimal(10, 7))");

    const auto pgmReturn2 = pb.StrictFromString(data, NTest::ConvertToMinikqlType<ui64>(pb));
    const auto graph2 = setup.BuildGraph(pgmReturn2);
    UNIT_ASSERT_EXCEPTION_CONTAINS(graph2->GetValue(), std::exception, TStringBuilder() << R"(Terminate was called, reason(5042): could not convert ")" << halfHugeXString << R"(" (truncated) to Uint64)");
}
} // Y_UNIT_TEST_SUITE(TMiniKQLFromStringTest)

} // namespace NMiniKQL
} // namespace NKikimr
