#include "mkql_computation_node_ut.h"

namespace NKikimr {
namespace NMiniKQL {

namespace {

TCallableType* MakeComparatorCallableType(TProgramBuilder& pb) {
    const auto returnType = pb.NewDataType(NUdf::TDataType<bool>::Id);
    const auto argType = pb.NewDataType(NUdf::TDataType<i32>::Id);
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

    const auto upvalues = pb.ListFromRange(pb.NewDataLiteral(from),
                                           pb.NewDataLiteral(to),
                                           pb.NewDataLiteral(1));
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
        return pb.NewOptional(pb.Apply(callable, {pb.NewDataLiteral(arg)}));
    });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    NUdf::TUnboxedValue item;
    for (i32 i = from; i < to; i++) {
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<bool>(), i == arg);
    }
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
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

    const auto infValues = pb.ListFromRange(pb.NewDataLiteral(infFrom),
                                            pb.NewDataLiteral(infTo),
                                            pb.NewDataLiteral(1));
    const auto supValues = pb.ListFromRange(pb.NewDataLiteral(supFrom),
                                            pb.NewDataLiteral(supTo),
                                            pb.NewDataLiteral(1));
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
        return pb.NewOptional(pb.Apply(callable, {pb.NewDataLiteral(arg)}));
    });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    NUdf::TUnboxedValue item;
    for (i32 i = infFrom, j = supFrom; i < infTo && j < supTo; i++, j++) {
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<bool>(), i < arg && arg < j);
    }
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
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

    const auto infValues = pb.ListFromRange(pb.NewDataLiteral(infFrom),
                                            pb.NewDataLiteral(infTo),
                                            pb.NewDataLiteral(1));
    const auto supValues = pb.ListFromRange(pb.NewDataLiteral(supFrom),
                                            pb.NewDataLiteral(supTo),
                                            pb.NewDataLiteral(1));
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
        return pb.NewOptional(pb.Apply(callable, {pb.NewDataLiteral(arg)}));
    });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    NUdf::TUnboxedValue item;
    for (i32 i = infFrom, j = supFrom; i < infTo && j < supTo; i++, j++) {
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<bool>(), i < arg && arg < j);
    }
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
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
