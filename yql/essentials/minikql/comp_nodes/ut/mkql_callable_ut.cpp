#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

TCallableType* MakeComparatorCallableType(TProgramBuilder& pb) {
    const auto returnType = NTest::ConvertToMinikqlType<bool>(pb);
    const auto argType = NTest::ConvertToMinikqlType<i32>(pb);
    return TCallableTypeBuilder(pb.GetTypeEnvironment(), "", returnType)
        .Add(argType)
        .Build();
}

template <bool LLVM>
void TestSimpleClosure(bool WithCollect) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    constexpr i32 from = 0;
    constexpr i32 to = 3;
    constexpr i32 arg = 1;

    const auto upvalues = pb.ListFromRange(NTest::ConvertValueToLiteralNode(pb, from),
                                           NTest::ConvertValueToLiteralNode(pb, to),
                                           NTest::ConvertValueToLiteralNode(pb, i32(1)));
    const auto fnew = [&pb](const TRuntimeNode upvalue) {
        const auto closure = pb.Callable(MakeComparatorCallableType(pb),
                                         [&pb, upvalue](const TArrayRef<const TRuntimeNode>& args) {
                                             return pb.Equals(args[0], upvalue);
                                         });
        return pb.NewOptional(closure);
    };
    auto callables = pb.OrderedFlatMap(upvalues, fnew);
    if (WithCollect) {
        callables = pb.Collect(callables);
    }
    const auto pgmReturn = pb.OrderedFlatMap(callables, [&pb](const TRuntimeNode callable) {
        return pb.NewOptional(pb.Apply(callable, {NTest::ConvertValueToLiteralNode(pb, arg)}));
    });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<bool>{false, true, false});
}

template <bool LLVM>
void TestNestedClosure(bool WithCollect) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    constexpr i32 infFrom = 0;
    constexpr i32 supFrom = 2;
    constexpr i32 infTo = 3;
    constexpr i32 supTo = 5;
    constexpr i32 arg = 2;

    const auto infValues = pb.ListFromRange(NTest::ConvertValueToLiteralNode(pb, infFrom),
                                            NTest::ConvertValueToLiteralNode(pb, infTo),
                                            NTest::ConvertValueToLiteralNode(pb, i32(1)));
    const auto supValues = pb.ListFromRange(NTest::ConvertValueToLiteralNode(pb, supFrom),
                                            NTest::ConvertValueToLiteralNode(pb, supTo),
                                            NTest::ConvertValueToLiteralNode(pb, i32(1)));
    const auto boundaries = pb.Zip({infValues, supValues});

    const auto ltnew = [&pb](const TRuntimeNode sup) {
        return pb.Callable(MakeComparatorCallableType(pb),
                           [&pb, sup](const TArrayRef<const TRuntimeNode>& args) {
                               return pb.Less(args[0], sup);
                           });
    };
    const auto innew = [&pb, &ltnew](const TRuntimeNode boundaries) {
        const auto closure = pb.Callable(MakeComparatorCallableType(pb),
                                         [&pb, &ltnew, &boundaries](const TArrayRef<const TRuntimeNode>& args) {
                                             return pb.And({pb.Greater(args[0], pb.Nth(boundaries, 0)),
                                                            pb.Apply(ltnew(pb.Nth(boundaries, 1)), {args[0]})});
                                         });
        return pb.NewOptional(closure);
    };

    auto callables = pb.OrderedFlatMap(boundaries, innew);
    if (WithCollect) {
        callables = pb.Collect(callables);
    }
    const auto pgmReturn = pb.OrderedFlatMap(callables, [&pb](const TRuntimeNode callable) {
        return pb.NewOptional(pb.Apply(callable, {NTest::ConvertValueToLiteralNode(pb, arg)}));
    });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<bool>{false, true, false});
}

template <bool LLVM>
void TestChildClosure(bool WithCollect) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    constexpr i32 infFrom = 0;
    constexpr i32 supFrom = 2;
    constexpr i32 infTo = 3;
    constexpr i32 supTo = 5;
    constexpr i32 arg = 2;

    const auto infValues = pb.ListFromRange(NTest::ConvertValueToLiteralNode(pb, infFrom),
                                            NTest::ConvertValueToLiteralNode(pb, infTo),
                                            NTest::ConvertValueToLiteralNode(pb, i32(1)));
    const auto supValues = pb.ListFromRange(NTest::ConvertValueToLiteralNode(pb, supFrom),
                                            NTest::ConvertValueToLiteralNode(pb, supTo),
                                            NTest::ConvertValueToLiteralNode(pb, i32(1)));
    const auto boundaries = pb.Zip({infValues, supValues});

    const auto ltnew = [&pb](const TRuntimeNode sup) {
        return pb.Callable(MakeComparatorCallableType(pb),
                           [&pb, sup](const TArrayRef<const TRuntimeNode>& args) {
                               return pb.Less(args[0], sup);
                           });
    };
    const auto gtnew = [&pb](const TRuntimeNode inf) {
        return pb.Callable(MakeComparatorCallableType(pb),
                           [&pb, inf](const TArrayRef<const TRuntimeNode>& args) {
                               return pb.Greater(args[0], inf);
                           });
    };
    const auto innew = [&pb, &ltnew, &gtnew](const TRuntimeNode boundaries) {
        const auto closure = pb.Callable(MakeComparatorCallableType(pb),
                                         [&pb, &ltnew, &gtnew, &boundaries](const TArrayRef<const TRuntimeNode>& args) {
                                             return pb.And({pb.Apply(gtnew(pb.Nth(boundaries, 0)), {args[0]}),
                                                            pb.Apply(ltnew(pb.Nth(boundaries, 1)), {args[0]})});
                                         });
        return pb.NewOptional(closure);
    };

    auto callables = pb.OrderedFlatMap(boundaries, innew);
    if (WithCollect) {
        callables = pb.Collect(callables);
    }
    const auto pgmReturn = pb.OrderedFlatMap(callables, [&pb](const TRuntimeNode callable) {
        return pb.NewOptional(pb.Apply(callable, {NTest::ConvertValueToLiteralNode(pb, arg)}));
    });

    const auto graph = setup.BuildGraph(pgmReturn);
    // infFrom=0..infTo=3, supFrom=2..supTo=5, arg=2: (0<2&&2<2)=F, (1<2&&2<3)=T, (2<2&&2<4)=F
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<bool>{false, true, false});
}

}; // namespace

Y_UNIT_TEST_SUITE(TMiniKQLCallableTest) {
Y_UNIT_TEST_LLVM(SimpleClosureWithoutCollect) {
    TestSimpleClosure<LLVM>(false);
}

Y_UNIT_TEST_LLVM(SimpleClosureWithCollect) {
    TestSimpleClosure<LLVM>(true);
}

Y_UNIT_TEST_LLVM(NestedClosureWithoutCollect) {
    TestNestedClosure<LLVM>(false);
}

Y_UNIT_TEST_LLVM(NestedClosureWithCollect) {
    TestNestedClosure<LLVM>(true);
}

Y_UNIT_TEST_LLVM(ChildClosureWithoutCollect) {
    TestChildClosure<LLVM>(false);
}

Y_UNIT_TEST_LLVM(ChildClosureWithCollect) {
    TestChildClosure<LLVM>(true);
}
} // Y_UNIT_TEST_SUITE(TMiniKQLCallableTest)

} // namespace NMiniKQL
} // namespace NKikimr
