#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<bool UseLLVM>
TRuntimeNode MakeStream(TSetup<UseLLVM>& setup, ui64 count = 9U) {
    TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

    TCallableBuilder callableBuilder(*setup.Env, "TestStream",
        pgmBuilder.NewStreamType(
            pgmBuilder.NewDataType(NUdf::EDataSlot::Uint64)
        )
    );

    callableBuilder.Add(pgmBuilder.NewDataLiteral(count));

    return TRuntimeNode(callableBuilder.Build(), false);
}

template<bool UseLLVM>
TRuntimeNode Group(TSetup<UseLLVM>& setup, TRuntimeNode stream, const std::function<TRuntimeNode(TRuntimeNode, TRuntimeNode)>& groupSwitch,
    const std::function<TRuntimeNode(TRuntimeNode)>& handler = {})
{
    TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

    auto keyExtractor = [&](TRuntimeNode item) {
        return item;
    };

    stream = pgmBuilder.GroupingCore(stream, groupSwitch, keyExtractor, handler);
    return pgmBuilder.FlatMap(stream, [&](TRuntimeNode grpItem) {
        return pgmBuilder.Squeeze(pgmBuilder.Nth(grpItem, 1),
            pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("*"),
            [&] (TRuntimeNode item, TRuntimeNode state) {
                auto res = pgmBuilder.Concat(pgmBuilder.ToString(pgmBuilder.Nth(grpItem, 0)), pgmBuilder.ToString(item));
                res = pgmBuilder.Concat(state, res);
                res = pgmBuilder.Concat(res, pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("*"));
                return res;
            },
            {}, {});
    });
}

template<bool UseLLVM>
TRuntimeNode GroupKeys(TSetup<UseLLVM>& setup, TRuntimeNode stream, const std::function<TRuntimeNode(TRuntimeNode, TRuntimeNode)>& groupSwitch) {
    TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

    auto keyExtractor = [&](TRuntimeNode item) {
        return item;
    };

    stream = pgmBuilder.GroupingCore(stream, groupSwitch, keyExtractor);
    return pgmBuilder.Map(stream, [&](TRuntimeNode grpItem) {
        return pgmBuilder.ToString(pgmBuilder.Nth(grpItem, 0));
    });
}

template<bool UseLLVM>
TRuntimeNode StreamToString(TSetup<UseLLVM>& setup, TRuntimeNode stream) {
    TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

    return pgmBuilder.Squeeze(stream, pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("|"), [&] (TRuntimeNode item, TRuntimeNode state) {
        return pgmBuilder.Concat(pgmBuilder.Concat(state, item), pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("|"));
    }, {}, {});
}

} // unnamed


Y_UNIT_TEST_SUITE(TMiniKQLGroupingTest) {
    Y_UNIT_TEST_LLVM(TestGrouping) {
        TSetup<LLVM> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

        auto stream = MakeStream(setup);
        stream = Group(setup, stream, [&](TRuntimeNode key, TRuntimeNode item) {
            Y_UNUSED(key);
            return pgmBuilder.Equals(item, pgmBuilder.NewDataLiteral<ui64>(0));
        });
        auto pgm = StreamToString(setup, stream);
        auto graph = setup.BuildGraph(pgm);
        auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|*00*|*00*01*|*00*|*00*|*00*01*02*03*|");
    }

    Y_UNIT_TEST_LLVM(TestGroupingKeyNotEquals) {
        TSetup<LLVM> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

        auto stream = MakeStream(setup);
        stream = Group(setup, stream, [&](TRuntimeNode key, TRuntimeNode item) {
            return pgmBuilder.NotEquals(item, key);
        });
        auto pgm = StreamToString(setup, stream);
        auto graph = setup.BuildGraph(pgm);
        auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|*00*00*|*11*|*00*00*00*|*11*|*22*|*33*|");
    }

    Y_UNIT_TEST_LLVM(TestGroupingWithEmptyInput) {
        TSetup<LLVM> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

        auto stream = MakeStream(setup, 0);
        stream = Group(setup, stream, [&](TRuntimeNode key, TRuntimeNode item) {
            Y_UNUSED(key);
            return pgmBuilder.Equals(item, pgmBuilder.NewDataLiteral<ui64>(0));
        });
        auto pgm = StreamToString(setup, stream);
        auto graph = setup.BuildGraph(pgm);
        auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|");
    }

    Y_UNIT_TEST_LLVM(TestSingleGroup) {
        TSetup<LLVM> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

        auto stream = MakeStream(setup);
        stream = Group(setup, stream, [&](TRuntimeNode key, TRuntimeNode item) {
            Y_UNUSED(key);
            Y_UNUSED(item);
            return pgmBuilder.NewDataLiteral<bool>(false);
        });
        auto pgm = StreamToString(setup, stream);
        auto graph = setup.BuildGraph(pgm);
        auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|*00*00*01*00*00*00*01*02*03*|");
    }

    Y_UNIT_TEST_LLVM(TestGroupingWithYield) {
        TSetup<LLVM> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

        auto stream = MakeStream(setup);
        TSwitchInput switchInput;
        switchInput.Indicies.push_back(0);
        switchInput.InputType = stream.GetStaticType();

        stream = pgmBuilder.Switch(stream,
                MakeArrayRef(&switchInput, 1),
                [&](ui32 /*index*/, TRuntimeNode item1) {
                    return Group(setup, item1, [&](TRuntimeNode key, TRuntimeNode item2) {
                        Y_UNUSED(key);
                        return pgmBuilder.Equals(item2, pgmBuilder.NewDataLiteral<ui64>(0));
                    });
                },
                1,
                pgmBuilder.NewStreamType(pgmBuilder.NewDataType(NUdf::EDataSlot::String))
                );

        auto pgm = StreamToString(setup, stream);
        auto graph = setup.BuildGraph(pgm);
        auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|*00*|*00*01*|*00*|*00*|*00*01*02*03*|");
    }

    Y_UNIT_TEST_LLVM(TestGroupingWithoutFetchingSubStreams) {
        TSetup<LLVM> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

        auto stream = MakeStream(setup);

        stream = GroupKeys(setup, stream, [&](TRuntimeNode key, TRuntimeNode item) {
            Y_UNUSED(key);
            return pgmBuilder.Equals(item, pgmBuilder.NewDataLiteral<ui64>(0));
        });

        auto pgm = StreamToString(setup, stream);
        auto graph = setup.BuildGraph(pgm);
        auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|0|0|0|");
    }

    Y_UNIT_TEST_LLVM(TestGroupingWithYieldAndWithoutFetchingSubStreams) {
        TSetup<LLVM> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

        auto stream = MakeStream(setup);
        TSwitchInput switchInput;
        switchInput.Indicies.push_back(0);
        switchInput.InputType = stream.GetStaticType();

        stream = pgmBuilder.Switch(stream,
                MakeArrayRef(&switchInput, 1),
                [&](ui32 /*index*/, TRuntimeNode item1) {
                    return GroupKeys(setup, item1, [&](TRuntimeNode key, TRuntimeNode item2) {
                        Y_UNUSED(key);
                        return pgmBuilder.Equals(item2, pgmBuilder.NewDataLiteral<ui64>(0));
                    });
                },
                1,
                pgmBuilder.NewStreamType(pgmBuilder.NewDataType(NUdf::EDataSlot::String))
                );

        auto pgm = StreamToString(setup, stream);
        auto graph = setup.BuildGraph(pgm);
        auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|0|0|0|");
    }

    Y_UNIT_TEST_LLVM(TestGroupingWithHandler) {
        TSetup<LLVM> setup;
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

        auto stream = MakeStream(setup);
        stream = Group(setup, stream,
            [&](TRuntimeNode key, TRuntimeNode item) {
                Y_UNUSED(key);
                return pgmBuilder.Equals(item, pgmBuilder.NewDataLiteral<ui64>(0));
            },
            [&](TRuntimeNode item) {
                return pgmBuilder.Add(pgmBuilder.Convert(item, pgmBuilder.NewDataType(NUdf::EDataSlot::Int32)), pgmBuilder.NewDataLiteral<ui64>(1));
            }
        );
        auto pgm = StreamToString(setup, stream);
        auto graph = setup.BuildGraph(pgm);
        auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|*01*|*01*02*|*01*|*01*|*01*02*03*04*|");
    }
}

} // NMiniKQL
} // NKikimr
