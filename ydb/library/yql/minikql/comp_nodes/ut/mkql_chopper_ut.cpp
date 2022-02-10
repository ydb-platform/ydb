#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<bool UseLLVM>
TRuntimeNode MakeStream(TSetup<UseLLVM>& setup, ui64 count = 9U) {
    TProgramBuilder& pb = *setup.PgmBuilder;

    TCallableBuilder callableBuilder(*setup.Env, "TestStream",
        pb.NewStreamType(
            pb.NewDataType(NUdf::EDataSlot::Uint64)
        )
    );

    callableBuilder.Add(pb.NewDataLiteral(count));

    return TRuntimeNode(callableBuilder.Build(), false);
}

template<bool UseLLVM>
TRuntimeNode MakeFlow(TSetup<UseLLVM>& setup, ui64 count = 9U) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    return pb.ToFlow(MakeStream<UseLLVM>(setup, count));
}

template<bool UseLLVM>
TRuntimeNode GroupWithBomb(TSetup<UseLLVM>& setup, TRuntimeNode stream) {
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto keyExtractor = [&](TRuntimeNode item) { return item; };
    const auto groupSwitch = [&](TRuntimeNode, TRuntimeNode) { return pb.NewDataLiteral<bool>(false); };

    return pb.Chopper(stream, keyExtractor, groupSwitch, [&](TRuntimeNode, TRuntimeNode group) {
        const auto bomb = pb.NewDataLiteral<NUdf::EDataSlot::String>("BOMB");
        return pb.Ensure(pb.Map(group, [&] (TRuntimeNode) { return bomb; }), pb.NewDataLiteral<bool>(false), bomb, "", 0, 0);
    });
}

template<bool UseLLVM>
TRuntimeNode Group(TSetup<UseLLVM>& setup, TRuntimeNode stream, const std::function<TRuntimeNode(TRuntimeNode, TRuntimeNode)>& groupSwitch) {
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto keyExtractor = [&](TRuntimeNode item) { return item; };

    return pb.Chopper(stream, keyExtractor, groupSwitch, [&](TRuntimeNode key, TRuntimeNode grpItem) {
        return pb.Condense(grpItem, pb.NewDataLiteral<NUdf::EDataSlot::String>("*"),
            [&] (TRuntimeNode, TRuntimeNode) { return pb.NewDataLiteral<bool>(false); },
            [&] (TRuntimeNode item, TRuntimeNode state) {
                auto res = pb.Concat(pb.ToString(key), pb.ToString(item));
                res = pb.Concat(state, res);
                return pb.Concat(res, pb.NewDataLiteral<NUdf::EDataSlot::String>("*"));
            });
    });
}

template<bool UseLLVM>
TRuntimeNode GroupGetKeysFirst(TSetup<UseLLVM>& setup, TRuntimeNode stream, const std::function<TRuntimeNode(TRuntimeNode, TRuntimeNode)>& groupSwitch) {
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto keyExtractor = [&](TRuntimeNode item) { return item; };

    const bool isFlow = stream.GetStaticType()->IsFlow();

    return pb.Chopper(stream, keyExtractor, groupSwitch, [&](TRuntimeNode key, TRuntimeNode grpItem) {
        auto list = pb.ToList(pb.AggrConcat(
            pb.NewOptional(pb.Concat(pb.ToString(key), pb.NewDataLiteral<NUdf::EDataSlot::String>(":"))),
            pb.ToOptional(pb.Collect(pb.Condense1(grpItem,
                [&] (TRuntimeNode item) { return pb.ToString(item); },
                [&] (TRuntimeNode, TRuntimeNode) { return pb.NewDataLiteral<bool>(false); },
                [&] (TRuntimeNode item, TRuntimeNode state) {
                    return pb.Concat(state, pb.ToString(item));
                }
            )))
        ));
        return isFlow ? pb.ToFlow(list) : pb.Iterator(list, {});
    });
}

template<bool UseLLVM>
TRuntimeNode GroupKeys(TSetup<UseLLVM>& setup, TRuntimeNode stream, const std::function<TRuntimeNode(TRuntimeNode, TRuntimeNode)>& groupSwitch) {
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto keyExtractor = [&](TRuntimeNode item) { return item; };

    return pb.Chopper(stream, keyExtractor, groupSwitch,
        [&](TRuntimeNode key, TRuntimeNode group) {
            return pb.Map(pb.Take(group, pb.NewDataLiteral<ui64>(1ULL)), [&](TRuntimeNode) { return pb.ToString(key); });
        }
    );
}

template<bool UseLLVM>
TRuntimeNode StreamToString(TSetup<UseLLVM>& setup, TRuntimeNode stream) {
    TProgramBuilder& pb = *setup.PgmBuilder;

    stream = pb.Condense(stream, pb.NewDataLiteral<NUdf::EDataSlot::String>("|"),
        [&] (TRuntimeNode, TRuntimeNode) { return pb.NewDataLiteral(false); },
        [&] (TRuntimeNode item, TRuntimeNode state) {
            return pb.Concat(pb.Concat(state, item), pb.NewDataLiteral<NUdf::EDataSlot::String>("|"));
        }
    );
    if (stream.GetStaticType()->IsFlow()) {
        stream = pb.FromFlow(stream);
    }
    return stream;
}

} // unnamed

Y_UNIT_TEST_SUITE(TMiniKQLChopperStreamTest) {
    Y_UNIT_TEST_LLVM(TestEmpty) {
        TSetup<LLVM> setup;

        const auto stream = GroupWithBomb(setup, MakeStream(setup, 0U));
        const auto pgm = StreamToString(setup, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|");
    }

    Y_UNIT_TEST_LLVM(TestGrouping) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto stream = MakeStream(setup);
        stream = Group(setup, stream, [&](TRuntimeNode key, TRuntimeNode item) {
            Y_UNUSED(key);
            return pb.Equals(item, pb.NewDataLiteral<ui64>(0));
        });
        const auto pgm = StreamToString(setup, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|*00*|*00*01*|*00*|*00*|*00*01*02*03*|");
    }

    Y_UNIT_TEST_LLVM(TestGroupingGetKeysFirst) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto stream = MakeStream(setup);
        stream = GroupGetKeysFirst(setup, stream, [&](TRuntimeNode key, TRuntimeNode item) {
            Y_UNUSED(key);
            return pb.Equals(item, pb.NewDataLiteral<ui64>(0));
        });
        const auto pgm = StreamToString(setup, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0:0|0:01|0:0|0:0|0:0123|");
    }

    Y_UNIT_TEST_LLVM(TestGroupingKeyNotEquals) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto stream = MakeStream(setup);
        stream = Group(setup, stream, [&](TRuntimeNode key, TRuntimeNode item) {
            return pb.NotEquals(item, key);
        });
        const auto pgm = StreamToString(setup, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|*00*00*|*11*|*00*00*00*|*11*|*22*|*33*|");
    }

    Y_UNIT_TEST_LLVM(TestGroupingWithEmptyInput) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto stream = MakeStream(setup, 0);
        stream = Group(setup, stream, [&](TRuntimeNode key, TRuntimeNode item) {
            Y_UNUSED(key);
            return pb.Equals(item, pb.NewDataLiteral<ui64>(0));
        });
        const auto pgm = StreamToString(setup, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|");
    }

    Y_UNIT_TEST_LLVM(TestSingleGroup) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto stream = MakeStream(setup);
        stream = Group(setup, stream, [&](TRuntimeNode key, TRuntimeNode item) {
            Y_UNUSED(key);
            Y_UNUSED(item);
            return pb.NewDataLiteral<bool>(false);
        });
        const auto pgm = StreamToString(setup, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|*00*00*01*00*00*00*01*02*03*|");
    }

    Y_UNIT_TEST_LLVM(TestGroupingWithYield) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto stream = MakeStream(setup);
        TSwitchInput switchInput;
        switchInput.Indicies.push_back(0);
        switchInput.InputType = stream.GetStaticType();

        stream = pb.Switch(stream,
                MakeArrayRef(&switchInput, 1),
                [&](ui32 /*index*/, TRuntimeNode item1) {
                    return Group(setup, item1, [&](TRuntimeNode key, TRuntimeNode item2) {
                        Y_UNUSED(key);
                        return pb.Equals(item2, pb.NewDataLiteral<ui64>(0));
                    });
                },
                1,
                pb.NewStreamType(pb.NewDataType(NUdf::EDataSlot::String))
                );

        const auto pgm = StreamToString(setup, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|*00*|*00*01*|*00*|*00*|*00*01*02*03*|");
    }

    Y_UNIT_TEST_LLVM(TestGroupingWithCutSubStreams) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto stream = MakeStream(setup);

        stream = GroupKeys(setup, stream, [&](TRuntimeNode key, TRuntimeNode item) {
            Y_UNUSED(key);
            return pb.Equals(item, pb.NewDataLiteral<ui64>(0));
        });

        const auto pgm = StreamToString(setup, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|0|0|0|");
    }

    Y_UNIT_TEST_LLVM(TestGroupingWithYieldAndCutSubStreams) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto stream = MakeStream(setup);
        TSwitchInput switchInput;
        switchInput.Indicies.push_back(0);
        switchInput.InputType = stream.GetStaticType();

        stream = pb.Switch(stream,
                MakeArrayRef(&switchInput, 1),
                [&](ui32 /*index*/, TRuntimeNode item1) {
                    return GroupKeys(setup, item1, [&](TRuntimeNode key, TRuntimeNode item2) {
                        Y_UNUSED(key);
                        return pb.Equals(item2, pb.NewDataLiteral<ui64>(0));
                    });
                },
                1,
                pb.NewStreamType(pb.NewDataType(NUdf::EDataSlot::String))
                );

        const auto pgm = StreamToString(setup, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|0|0|0|");
    }
}
#if !defined(MKQL_RUNTIME_VERSION) || MKQL_RUNTIME_VERSION >= 9u
Y_UNIT_TEST_SUITE(TMiniKQLChopperFlowTest) {
    Y_UNIT_TEST_LLVM(TestEmpty) {
        TSetup<LLVM> setup;

        const auto stream = GroupWithBomb(setup, MakeFlow(setup, 0U));
        const auto pgm = StreamToString(setup, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|");
    }

    Y_UNIT_TEST_LLVM(TestGrouping) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto stream = MakeFlow(setup);
        stream = Group(setup, stream, [&](TRuntimeNode key, TRuntimeNode item) {
            Y_UNUSED(key);
            return pb.Equals(item, pb.NewDataLiteral<ui64>(0));
        });
        const auto pgm = StreamToString(setup, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|*00*|*00*01*|*00*|*00*|*00*01*02*03*|");
    }

    Y_UNIT_TEST_LLVM(TestGroupingGetKeysFirst) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto stream = MakeFlow(setup);
        stream = GroupGetKeysFirst(setup, stream, [&](TRuntimeNode key, TRuntimeNode item) {
            Y_UNUSED(key);
            return pb.Equals(item, pb.NewDataLiteral<ui64>(0));
        });
        const auto pgm = StreamToString(setup, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0:0|0:01|0:0|0:0|0:0123|");
    }

    Y_UNIT_TEST_LLVM(TestGroupingKeyNotEquals) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto stream = MakeFlow(setup);
        stream = Group(setup, stream, [&](TRuntimeNode key, TRuntimeNode item) {
            return pb.NotEquals(item, key);
        });
        const auto pgm = StreamToString(setup, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|*00*00*|*11*|*00*00*00*|*11*|*22*|*33*|");
    }

    Y_UNIT_TEST_LLVM(TestGroupingWithEmptyInput) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto stream = MakeFlow(setup, 0);
        stream = Group(setup, stream, [&](TRuntimeNode key, TRuntimeNode item) {
            Y_UNUSED(key);
            return pb.Equals(item, pb.NewDataLiteral<ui64>(0));
        });
        const auto pgm = StreamToString(setup, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|");
    }

    Y_UNIT_TEST_LLVM(TestSingleGroup) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto stream = MakeFlow(setup);
        stream = Group(setup, stream, [&](TRuntimeNode key, TRuntimeNode item) {
            Y_UNUSED(key);
            Y_UNUSED(item);
            return pb.NewDataLiteral<bool>(false);
        });
        const auto pgm = StreamToString(setup, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|*00*00*01*00*00*00*01*02*03*|");
    }

    Y_UNIT_TEST_LLVM(TestGroupingWithYield) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto stream = MakeFlow(setup);
        TSwitchInput switchInput;
        switchInput.Indicies.push_back(0);
        switchInput.InputType = stream.GetStaticType();

        stream = pb.Switch(stream,
                MakeArrayRef(&switchInput, 1),
                [&](ui32 /*index*/, TRuntimeNode item1) {
                    return Group(setup, item1, [&](TRuntimeNode key, TRuntimeNode item2) {
                        Y_UNUSED(key);
                        return pb.Equals(item2, pb.NewDataLiteral<ui64>(0));
                    });
                },
                1,
                pb.NewFlowType(pb.NewDataType(NUdf::EDataSlot::String))
                );

        const auto pgm = StreamToString(setup, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|*00*|*00*01*|*00*|*00*|*00*01*02*03*|");
    }

    Y_UNIT_TEST_LLVM(TestGroupingWithCutSubStreams) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto stream = MakeFlow(setup);

        stream = GroupKeys(setup, stream, [&](TRuntimeNode key, TRuntimeNode item) {
            Y_UNUSED(key);
            return pb.Equals(item, pb.NewDataLiteral<ui64>(0));
        });

        const auto pgm = StreamToString(setup, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|0|0|0|");
    }

    Y_UNIT_TEST_LLVM(TestGroupingWithYieldAndCutSubStreams) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        auto stream = MakeFlow(setup);
        TSwitchInput switchInput;
        switchInput.Indicies.push_back(0);
        switchInput.InputType = stream.GetStaticType();

        stream = pb.Switch(stream,
                MakeArrayRef(&switchInput, 1),
                [&](ui32 /*index*/, TRuntimeNode item1) {
                    return GroupKeys(setup, item1, [&](TRuntimeNode key, TRuntimeNode item2) {
                        Y_UNUSED(key);
                        return pb.Equals(item2, pb.NewDataLiteral<ui64>(0));
                    });
                },
                1,
                pb.NewFlowType(pb.NewDataType(NUdf::EDataSlot::String))
                );

        const auto pgm = StreamToString(setup, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|0|0|0|");
    }
}
#endif
} // NMiniKQL
} // NKikimr
