#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

namespace NKikimr::NMiniKQL {

Y_UNIT_TEST_SUITE(TErasedTest) {

Y_UNIT_TEST_LLVM(TestPeekErasedSuccessfulExtraction) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto value = NTest::ConvertValueToLiteralNode(pb, ui32(42));
    const auto erased = pb.AsErased(value);
    const auto pgmReturn = pb.PeekErased(erased, value.GetStaticType());

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TMaybe<ui32>{ui32(42)});
}

Y_UNIT_TEST_LLVM(TestPeekErasedUnsuccessfulExtraction) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto value = NTest::ConvertValueToLiteralNode(pb, ui32(42));
    const auto erased = pb.AsErased(value);
    // Request a different type (ui64) than the stored one (ui32).
    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto pgmReturn = pb.PeekErased(erased, ui64Type);

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TMaybe<ui64>{});
}

} // Y_UNIT_TEST_SUITE(TErasedTest)

namespace {

template <bool LLVM>
void TestTrampolinePrintCascade(i32 depth) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto unwrap = [&pb](TRuntimeNode x) {
        return pb.Unwrap(x, NTest::ConvertValueToLiteralNode(pb, TStringBuf("expected value")), "", 0, 0);
    };

    const auto input = pb.ListFromRange(NTest::ConvertValueToLiteralNode(pb, i32(0)),
                                        NTest::ConvertValueToLiteralNode(pb, i32(depth)),
                                        NTest::ConvertValueToLiteralNode(pb, i32(1)));
    const auto tail = pb.Skip(input, NTest::ConvertValueToLiteralNode(pb, ui64(1)));

    const auto erasedType = pb.AsErased(pb.NewNull()).GetStaticType();
    const auto sentinel = pb.NewEmptyOptional(pb.NewOptionalType(erasedType));
    const auto stringType = pb.NewDataType(NUdf::EDataSlot::String);
    const auto initNode = pb.NewStruct({{"next", sentinel},
                                        {"value", NTest::ConvertValueToLiteralNode(pb, "0")}});

    const auto root = pb.Fold(tail, initNode, [&](TRuntimeNode item, TRuntimeNode state) {
        return pb.NewStruct({{"next", pb.NewOptional(pb.AsErased(state))},
                             {"value", pb.ToString(item)}});
    });

    const auto nodeType = root.GetStaticType();
    const auto contType = TCallableTypeBuilder(pb.GetTypeEnvironment(), "", erasedType).Add(erasedType).Build();
    const auto thunkType = TCallableTypeBuilder(pb.GetTypeEnvironment(), "", erasedType).Build();
    const auto trampolineType = TCallableTypeBuilder(pb.GetTypeEnvironment(), "", erasedType)
                                    .Add(erasedType)
                                    .Add(contType)
                                    .Add(nodeType)
                                    .Build();

    const auto contLambda = pb.Callable(contType, [](const TArrayRef<const TRuntimeNode>& args) {
        return args[0];
    });

    const auto nextPrefix = NTest::ConvertValueToLiteralNode(pb, "->");
    const auto emptyString = NTest::ConvertValueToLiteralNode(pb, "");

    const auto printImpTrampoline = pb.AsErased(pb.Callable(trampolineType, [&](const TArrayRef<const TRuntimeNode>& implArgs) {
        const auto self = implArgs[0];
        const auto cont = implArgs[1];
        const auto node = implArgs[2];
        const auto value = pb.Member(node, "value");
        const auto erasedNext = pb.Member(node, "next");
        const auto hasNodeNext = pb.Exists(erasedNext);

        return pb.If(pb.Not(hasNodeNext),
                     pb.Apply(cont, {pb.AsErased(value)}),
                     pb.AsErased(pb.Callable(thunkType, [&](const TArrayRef<const TRuntimeNode>&) {
                         const auto trampoline = unwrap(pb.PeekErased(self, trampolineType));
                         const auto nextNode = unwrap(pb.PeekErased(unwrap(erasedNext), nodeType));
                         const auto recursed = pb.Apply(trampoline, {self, pb.Callable(contType, [&](const TArrayRef<const TRuntimeNode>& contArgs) {
                                                                         const auto nextValue = unwrap(pb.PeekErased(contArgs[0], stringType));
                                                                         const auto formatted = pb.If(pb.Greater(pb.Size(nextValue), NTest::ConvertValueToLiteralNode(pb, i32(0))),
                                                                                                      pb.Concat(nextPrefix, nextValue),
                                                                                                      emptyString);
                                                                         return pb.Apply(cont, {pb.AsErased(pb.Concat(value, formatted))});
                                                                     }), nextNode});
                         return pb.If(hasNodeNext, recursed, pb.Apply(cont, {pb.AsErased(value)}));
                     })));
    }));

    const auto rootThunk = pb.Apply(unwrap(pb.PeekErased(printImpTrampoline, trampolineType)), {printImpTrampoline, contLambda, root});

    const auto erasedResult = pb.Fold(input, rootThunk, [&](TRuntimeNode, TRuntimeNode state) {
        const auto thunkOpt = pb.PeekErased(state, thunkType);
        return pb.If(pb.Exists(thunkOpt), pb.Apply(unwrap(thunkOpt), {}), state);
    });

    const auto pgmReturn = unwrap(pb.PeekErased(erasedResult, stringType));

    const auto graph = setup.BuildGraph(pgmReturn);

    TStringBuilder expected;
    for (i32 i = depth - 1; i >= 1; i--) {
        expected << ToString(i) << "->";
    }
    expected << "0";
    AssertUnboxedValueElementEqual(graph->GetValue(), TString(expected));
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLCallableRecursionTest) {
Y_UNIT_TEST_LLVM(TrampolinePrintCascade) {
    for (size_t i = 0; i < 10; i++) {
        TestTrampolinePrintCascade<LLVM>(i);
    }
}
} // Y_UNIT_TEST_SUITE(TMiniKQLCallableRecursionTest)

} // namespace NKikimr::NMiniKQL
