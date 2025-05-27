#include "mkql_computation_node_ut.h"

namespace NKikimr::NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLIterableTest) {

    // This particular test builds the graph similar to the one
    // below, that is a part of the reproducer in YQL-19836.
    //   (return SqueezeToDict (Map (ToFlow (Iterable
    //       (lambda '() (EmptyIterator (StreamType (StructType '('"a" (DataType 'Uint64))))))
    //     )) (lambda '($9) '((Member $9 '"a") $9)))
    //     (lambda '($10) (Nth $10 '0))
    //     (lambda '($11) (Nth $11 '1))
    //     '('Many 'Hashed 'Compact))
    Y_UNIT_TEST_LLVM(TestEmptyIterable) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const TProgramBuilder::TZeroLambda lambda = [&pb]() {
            return pb.EmptyIterator(pb.NewStreamType(pb.NewStructType({
                {"a", pb.NewDataType(NUdf::EDataSlot::Uint64)}
            })));
        };
        const auto root = pb.SqueezeToHashedDict(
            pb.ToFlow(pb.Iterable(lambda)),
            /* isMany = */ true,
            [&pb](TRuntimeNode node) { return pb.Member(node, "a"); },
            [](TRuntimeNode node) { return node; },
            /* isCompact = */ true
        );
        auto graph = setup.BuildGraph(root);
        NUdf::TUnboxedValue dict = graph->GetValue();

        UNIT_ASSERT(!dict.IsSpecial());
        UNIT_ASSERT_EQUAL(dict.GetDictLength(), 0);
    }

} // Y_UNIT_TEST_SUITE(TMiniKQLIterableTest)

} // namespace NKikimr::NMiniKQL
