#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLConvertTest) {
Y_UNIT_TEST_LLVM(ConvertUI8ToBool) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui8>{0, 1, 2, 3, 4});

    const auto pgmReturn = pb.Map(list, [&pb](const TRuntimeNode item) {
        return pb.Convert(item, NTest::ConvertToMinikqlType<bool>(pb));
    });
    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui8>{0, 1, 1, 1, 1});
}
} // Y_UNIT_TEST_SUITE(TMiniKQLConvertTest)

} // namespace NMiniKQL
} // namespace NKikimr
