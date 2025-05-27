#include "mkql_computation_node_ut.h"

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLConvertTest) {
    Y_UNIT_TEST_LLVM(ConvertUI8ToBool) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral(static_cast<ui8>(0U));
        const auto data1 = pb.NewDataLiteral(static_cast<ui8>(1U));
        const auto data2 = pb.NewDataLiteral(static_cast<ui8>(2U));
        const auto data3 = pb.NewDataLiteral(static_cast<ui8>(3U));
        const auto data4 = pb.NewDataLiteral(static_cast<ui8>(4U));
        const auto type = pb.NewDataType(NUdf::TDataType<ui8>::Id);
        const auto list = pb.NewList(type, {data0, data1, data2, data3, data4});

        const auto pgmReturn = pb.Map(list, [&pb](const TRuntimeNode item) {
            return pb.Convert(item, pb.NewDataType(NUdf::TDataType<bool>::Id));
        });
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();

        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui8>(), 0);
        while (iterator.Next(item)) {
            UNIT_ASSERT_VALUES_EQUAL(item.Get<ui8>(), 1);
        }
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }
} // Y_UNIT_TEST_SUITE

} // namespace NMiniKQL
} // namespace NKikimr
