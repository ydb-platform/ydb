#include <yql/essentials/minikql/comp_nodes/ut/mkql_block_test_helper.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

namespace {

TType* MakeHashedGroupByReturnType(TProgramBuilder& pb, TRuntimeNode wideStream, const TVector<ui32>& keys) {
    auto items = GetWideComponents(AS_TYPE(TStreamType, wideStream.GetStaticType()));
    TVector<TType*> returnItems(Reserve(keys.size() + 1));
    for (ui32 key : keys) {
        auto* itemType = AS_TYPE(TBlockType, items[key])->GetItemType();
        returnItems.push_back(pb.NewBlockType(itemType, TBlockType::EShape::Many));
    }
    returnItems.push_back(items.back());
    return pb.NewStreamType(pb.NewMultiType(returnItems));
}

TRuntimeNode CombineHashedByKeys(TSetup<false>& setup, TRuntimeNode fuzzedWideStream, const TVector<ui32>& keys) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    return pb.BlockCombineHashed(fuzzedWideStream, /*filterColumn=*/{}, keys, /*aggs=*/{}, MakeHashedGroupByReturnType(pb, fuzzedWideStream, keys));
}

template <typename... TExpected, typename... TInputs>
void RunHashedGroupByTest(const std::tuple<TVector<TExpected>...>& expected,
                          const std::tuple<TInputs...>& input, const TVector<ui32>& keys) {
    TBlockHelper helper;
    helper.WithScopedFuzzers([&] {
        helper.RunWideStreamNode(
            expected,
            [&](TSetup<false>& setup, TRuntimeNode fuzzedWideStream) {
                return CombineHashedByKeys(setup, fuzzedWideStream, keys);
            },
            /*unordered=*/true,
            input);
    });
}

TType* MakeAggResultItemType(TProgramBuilder& pb, TArrayRef<TType* const> items, const TAggInfo& agg) {
    if (agg.Name == "count" || agg.Name == "count_all") {
        return pb.NewDataType(NUdf::EDataSlot::Uint64);
    }
    return AS_TYPE(TBlockType, items[agg.ArgsColumns.front()])->GetItemType();
}

TType* MakeCombineAllReturnType(TProgramBuilder& pb, TRuntimeNode wideStream, const TVector<TAggInfo>& aggs) {
    auto items = GetWideComponents(AS_TYPE(TStreamType, wideStream.GetStaticType()));
    TVector<TType*> returnItems(Reserve(aggs.size()));
    for (const auto& agg : aggs) {
        returnItems.push_back(MakeAggResultItemType(pb, items, agg));
    }
    return pb.NewStreamType(pb.NewMultiType(returnItems));
}

TRuntimeNode CombineAll(TSetup<false>& setup, TRuntimeNode fuzzedWideStream,
                        const TVector<TAggInfo>& aggs, std::optional<ui32> filterColumn) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    return pb.BlockCombineAll(fuzzedWideStream, filterColumn, aggs, MakeCombineAllReturnType(pb, fuzzedWideStream, aggs));
}

template <typename... TExpected, typename... TInputs>
void RunCombineAllTest(const std::tuple<TVector<TExpected>...>& expected, const std::tuple<TInputs...>& input,
                       const TVector<TAggInfo>& aggs, std::optional<ui32> filterColumn = {}) {
    TBlockHelper helper;
    helper.WithScopedFuzzers([&] {
        helper.RunWideStreamNode(
            expected,
            [&](TSetup<false>& setup, TRuntimeNode fuzzedWideStream) {
                return CombineAll(setup, fuzzedWideStream, aggs, filterColumn);
            },
            /*unordered=*/false,
            input);
    });
}

TType* MakeHashedAggReturnType(TProgramBuilder& pb, TRuntimeNode wideStream, const TVector<ui32>& keys,
                               const TVector<TAggInfo>& aggs) {
    auto items = GetWideComponents(AS_TYPE(TStreamType, wideStream.GetStaticType()));
    TVector<TType*> returnItems(Reserve(keys.size() + aggs.size() + 1));
    for (ui32 key : keys) {
        auto* itemType = AS_TYPE(TBlockType, items[key])->GetItemType();
        returnItems.push_back(pb.NewBlockType(itemType, TBlockType::EShape::Many));
    }
    for (const auto& agg : aggs) {
        returnItems.push_back(pb.NewBlockType(MakeAggResultItemType(pb, items, agg), TBlockType::EShape::Many));
    }
    returnItems.push_back(items.back());
    return pb.NewStreamType(pb.NewMultiType(returnItems));
}

TRuntimeNode CombineHashedWithAggs(TSetup<false>& setup, TRuntimeNode fuzzedWideStream, const TVector<ui32>& keys,
                                   const TVector<TAggInfo>& aggs) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    return pb.BlockCombineHashed(fuzzedWideStream, /*filterColumn=*/{}, keys, aggs, MakeHashedAggReturnType(pb, fuzzedWideStream, keys, aggs));
}

template <typename... TExpected, typename... TInputs>
void RunCombineHashedWithAggsTest(const std::tuple<TVector<TExpected>...>& expected, const std::tuple<TInputs...>& input,
                                  const TVector<ui32>& keys, const TVector<TAggInfo>& aggs) {
    TBlockHelper helper;
    helper.WithScopedFuzzers([&] {
        helper.RunWideStreamNode(
            expected,
            [&](TSetup<false>& setup, TRuntimeNode fuzzedWideStream) {
                return CombineHashedWithAggs(setup, fuzzedWideStream, keys, aggs);
            },
            /*unordered=*/true,
            input);
    });
}

TRuntimeNode MergeFinalizeHashedByKeys(TSetup<false>& setup, TRuntimeNode fuzzedWideStream, const TVector<ui32>& keys,
                                       const TVector<TAggInfo>& aggs) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    return pb.BlockMergeFinalizeHashed(fuzzedWideStream, keys, aggs, MakeHashedAggReturnType(pb, fuzzedWideStream, keys, aggs));
}

template <typename... TExpected, typename... TInputs>
void RunMergeFinalizeHashedTest(const std::tuple<TVector<TExpected>...>& expected, const std::tuple<TInputs...>& input,
                                const TVector<ui32>& keys, const TVector<TAggInfo>& aggs) {
    TBlockHelper helper;
    helper.WithScopedFuzzers([&] {
        helper.RunWideStreamNode(
            expected,
            [&](TSetup<false>& setup, TRuntimeNode fuzzedWideStream) {
                return MergeFinalizeHashedByKeys(setup, fuzzedWideStream, keys, aggs);
            },
            /*unordered=*/true,
            input);
    });
}

TType* MakeManyAggFinalItemType(TProgramBuilder& pb, TArrayRef<TType* const> items, const TAggInfo& agg) {
    if (agg.Name == "count" || agg.Name == "count_all") {
        return pb.NewDataType(NUdf::EDataSlot::Uint64);
    }
    bool isOptional;
    return UnpackOptional(AS_TYPE(TBlockType, items[agg.ArgsColumns.front()])->GetItemType(), isOptional);
}

TType* MakeManyAggReturnType(TProgramBuilder& pb, TRuntimeNode wideStream, const TVector<ui32>& keys,
                             const TVector<TAggInfo>& aggs) {
    auto items = GetWideComponents(AS_TYPE(TStreamType, wideStream.GetStaticType()));
    TVector<TType*> returnItems(Reserve(keys.size() + aggs.size() + 1));
    for (ui32 key : keys) {
        auto* itemType = AS_TYPE(TBlockType, items[key])->GetItemType();
        returnItems.push_back(pb.NewBlockType(itemType, TBlockType::EShape::Many));
    }
    for (const auto& agg : aggs) {
        returnItems.push_back(pb.NewBlockType(MakeManyAggFinalItemType(pb, items, agg), TBlockType::EShape::Many));
    }
    returnItems.push_back(items.back());
    return pb.NewStreamType(pb.NewMultiType(returnItems));
}

TRuntimeNode MergeManyFinalizeHashedByKeys(TSetup<false>& setup, TRuntimeNode fuzzedWideStream, const TVector<ui32>& keys,
                                           const TVector<TAggInfo>& aggs, ui32 streamIndexColumn,
                                           const TVector<TVector<ui32>>& streams) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    auto returnType = MakeManyAggReturnType(pb, fuzzedWideStream, keys, aggs);
    return pb.BlockMergeManyFinalizeHashed(fuzzedWideStream, keys, aggs, streamIndexColumn, streams, returnType);
}

template <typename... TExpected, typename... TInputs>
void RunMergeManyFinalizeHashedTest(const std::tuple<TVector<TExpected>...>& expected, const std::tuple<TInputs...>& input,
                                    const TVector<ui32>& keys, const TVector<TAggInfo>& aggs, ui32 streamIndexColumn,
                                    const TVector<TVector<ui32>>& streams) {
    TBlockHelper helper;
    helper.WithScopedFuzzers([&] {
        helper.RunWideStreamNode(
            expected,
            [&](TSetup<false>& setup, TRuntimeNode fuzzedWideStream) {
                return MergeManyFinalizeHashedByKeys(setup, fuzzedWideStream, keys, aggs, streamIndexColumn, streams);
            },
            /*unordered=*/true,
            input);
    });
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockAggTest) {

Y_UNIT_TEST(GroupByNullableStringKey) {
    TVector<TMaybe<TString>> key = {
        TMaybe<TString>{},
        TMaybe<TString>("truck"),
        TMaybe<TString>{},
        TMaybe<TString>("truck"),
    };

    TVector<TMaybe<TString>> expected = {
        TMaybe<TString>{},
        TMaybe<TString>("truck"),
    };

    RunHashedGroupByTest(std::make_tuple(expected), std::make_tuple(key), {0});
}

Y_UNIT_TEST(GroupByArrayScalarArrayKeys) {
    TVector<TMaybe<TString>> key0 = {
        TMaybe<TString>{},
        TMaybe<TString>("truck"),
        TMaybe<TString>{},
        TMaybe<TString>("truck"),
    };
    TVector<ui32> key2 = {10U, 10U, 20U, 20U};

    TVector<TMaybe<TString>> expectedKey0 = {
        TMaybe<TString>{},
        TMaybe<TString>("truck"),
        TMaybe<TString>{},
        TMaybe<TString>("truck"),
    };
    TVector<TString> expectedKey1 = {"berlin", "berlin", "berlin", "berlin"};
    TVector<ui32> expectedKey2 = {10U, 10U, 20U, 20U};

    RunHashedGroupByTest(std::make_tuple(expectedKey0, expectedKey1, expectedKey2),
                         std::make_tuple(key0, TString("berlin"), key2), {0, 1, 2});
}

Y_UNIT_TEST(GroupByAllScalarKeys) {
    TVector<TMaybe<TString>> expectedKey0 = {TMaybe<TString>("truck")};
    TVector<ui32> expectedKey1 = {42U};

    RunHashedGroupByTest(std::make_tuple(expectedKey0, expectedKey1),
                         std::make_tuple(TMaybe<TString>("truck"), 42U), {0, 1});
}

Y_UNIT_TEST(CombineAllCountOverNullableArray) {
    TVector<TMaybe<TString>> names = {TString("a"), TMaybe<TString>{}, TString("b")};

    RunCombineAllTest(std::make_tuple(TVector<ui64>{2}), std::make_tuple(names), {TAggInfo{.Name = "count", .ArgsColumns = {0}}});
}

Y_UNIT_TEST(CombineAllScalarInputMin) {
    RunCombineAllTest(std::make_tuple(TVector<ui32>{7}), std::make_tuple(7U), {TAggInfo{.Name = "min", .ArgsColumns = {0}}});
}

Y_UNIT_TEST(CombineAllMultipleAggsMixedTypes) {
    TVector<ui32> vals = {5U, 2U, 9U, 2U};
    TVector<TMaybe<TString>> names = {TString("x"), TMaybe<TString>{}, TString("y"), TString("x")};

    RunCombineAllTest(std::make_tuple(TVector<ui32>{2}, TVector<ui32>{9}, TVector<ui64>{3}),
                      std::make_tuple(vals, names),
                      {TAggInfo{.Name = "min", .ArgsColumns = {0}}, TAggInfo{.Name = "max", .ArgsColumns = {0}}, TAggInfo{.Name = "count", .ArgsColumns = {1}}});
}

Y_UNIT_TEST(CombineAllWithFilterColumn) {
    TVector<ui32> vals = {1U, 2U, 3U, 4U, 5U};
    TVector<bool> filter = {true, false, true, false, true};

    RunCombineAllTest(std::make_tuple(TVector<ui64>{3}, TVector<ui32>{1}),
                      std::make_tuple(vals, filter),
                      {TAggInfo{.Name = "count_all", .ArgsColumns = {}}, TAggInfo{.Name = "min", .ArgsColumns = {0}}},
                      /*filterColumn=*/1);
}

Y_UNIT_TEST(CombineAllEmptyInput) {
    TVector<ui32> vals = {};

    RunCombineAllTest(std::make_tuple(TVector<ui64>{}), std::make_tuple(vals), {TAggInfo{.Name = "count_all", .ArgsColumns = {}}});
}

Y_UNIT_TEST(CombineAllDoubleOptionalValue) {
    TVector<TMaybe<TMaybe<ui64>>> vals = {
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>(1U)),
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>()),
        TMaybe<TMaybe<ui64>>(),
    };

    RunCombineAllTest(std::make_tuple(TVector<ui64>{2}), std::make_tuple(vals), {TAggInfo{.Name = "count", .ArgsColumns = {0}}});
}

Y_UNIT_TEST(CombineAllVoidValue) {
    TVector<TMaybe<NTest::TSingularVoid>> vals = {
        TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()),
        TMaybe<NTest::TSingularVoid>(),
        TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()),
    };

    RunCombineAllTest(std::make_tuple(TVector<ui64>{2}), std::make_tuple(vals), {TAggInfo{.Name = "count", .ArgsColumns = {0}}});
}

Y_UNIT_TEST(CombineHashedCountAllByUi32Key) {
    TVector<ui32> key = {1U, 1U, 2U, 2U, 2U};

    RunCombineHashedWithAggsTest(std::make_tuple(TVector<ui32>{1U, 2U}, TVector<ui64>{2U, 3U}),
                                 std::make_tuple(key), {0}, {TAggInfo{.Name = "count_all", .ArgsColumns = {}}});
}

Y_UNIT_TEST(CombineHashedMinMaxByNullableStringKey) {
    TVector<TMaybe<TString>> key = {TMaybe<TString>{}, TString("truck"), TMaybe<TString>{}, TString("truck")};
    TVector<ui32> vals = {10U, 3U, 7U, 20U};

    RunCombineHashedWithAggsTest(
        std::make_tuple(TVector<TMaybe<TString>>{TMaybe<TString>{}, TString("truck")}, TVector<ui32>{7U, 3U}, TVector<ui32>{10U, 20U}),
        std::make_tuple(key, vals), {0}, {TAggInfo{.Name = "min", .ArgsColumns = {1}}, TAggInfo{.Name = "max", .ArgsColumns = {1}}});
}

Y_UNIT_TEST(CombineHashedTupleKeyCountNullable) {
    TVector<std::tuple<ui32, TString>> key = {{1U, "x"}, {1U, "x"}, {2U, "y"}, {2U, "y"}};
    TVector<TMaybe<ui32>> vals = {5U, TMaybe<ui32>{}, TMaybe<ui32>{}, TMaybe<ui32>{}};

    RunCombineHashedWithAggsTest(
        std::make_tuple(TVector<std::tuple<ui32, TString>>{{1U, "x"}, {2U, "y"}}, TVector<ui64>{1U, 0U}),
        std::make_tuple(key, vals), {0}, {TAggInfo{.Name = "count", .ArgsColumns = {1}}});
}

Y_UNIT_TEST(CombineHashedAllScalarKeyMultipleAggs) {
    RunCombineHashedWithAggsTest(std::make_tuple(TVector<ui32>{42U}, TVector<ui64>{1U}, TVector<ui32>{7U}),
                                 std::make_tuple(42U, 7U), {0}, {TAggInfo{.Name = "count_all", .ArgsColumns = {}}, TAggInfo{.Name = "min", .ArgsColumns = {1}}});
}

Y_UNIT_TEST(CombineHashedMixedScalarArrayKeysWithChunking) {
    TVector<ui32> key = {1U, 2U, 1U, 2U, 1U};
    TVector<ui32> vals = {10U, 20U, 30U, 40U, 50U};

    RunCombineHashedWithAggsTest(
        std::make_tuple(TVector<ui32>{1U, 2U}, TVector<TString>{"region", "region"}, TVector<ui32>{10U, 20U}, TVector<ui32>{50U, 40U}),
        std::make_tuple(key, TString("region"), vals), {0, 1}, {TAggInfo{.Name = "min", .ArgsColumns = {2}}, TAggInfo{.Name = "max", .ArgsColumns = {2}}});
}

Y_UNIT_TEST(CombineHashedDoubleOptionalKey) {
    TVector<TMaybe<TMaybe<ui64>>> key = {
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>(1U)),
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>()),
        TMaybe<TMaybe<ui64>>(),
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>(1U)),
    };

    RunCombineHashedWithAggsTest(
        std::make_tuple(TVector<TMaybe<TMaybe<ui64>>>{key[0], key[1], key[2]}, TVector<ui64>{2U, 1U, 1U}),
        std::make_tuple(key), {0}, {TAggInfo{.Name = "count_all", .ArgsColumns = {}}});
}

Y_UNIT_TEST(CombineHashedVoidKey) {
    TVector<TMaybe<NTest::TSingularVoid>> key = {
        TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()),
        TMaybe<NTest::TSingularVoid>(),
        TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()),
    };

    RunCombineHashedWithAggsTest(
        std::make_tuple(TVector<TMaybe<NTest::TSingularVoid>>{key[0], key[1]}, TVector<ui64>{2U, 1U}),
        std::make_tuple(key), {0}, {TAggInfo{.Name = "count_all", .ArgsColumns = {}}});
}

Y_UNIT_TEST(MergeFinalizeCountByUi32Key) {
    TVector<ui32> key = {1U, 1U, 2U};
    TVector<ui64> partialCounts = {2U, 3U, 7U};

    RunMergeFinalizeHashedTest(std::make_tuple(TVector<ui32>{1U, 2U}, TVector<ui64>{5U, 7U}),
                               std::make_tuple(key, partialCounts), {0}, {TAggInfo{.Name = "count", .ArgsColumns = {1}}});
}

Y_UNIT_TEST(MergeFinalizeMinMaxByNullableStringKey) {
    TVector<TMaybe<TString>> key = {TMaybe<TString>{}, TMaybe<TString>{}, TString("truck"), TString("truck")};
    TVector<ui32> vals = {10U, 7U, 3U, 20U};

    RunMergeFinalizeHashedTest(
        std::make_tuple(TVector<TMaybe<TString>>{TMaybe<TString>{}, TString("truck")}, TVector<ui32>{7U, 3U}, TVector<ui32>{10U, 20U}),
        std::make_tuple(key, vals), {0}, {TAggInfo{.Name = "min", .ArgsColumns = {1}}, TAggInfo{.Name = "max", .ArgsColumns = {1}}});
}

Y_UNIT_TEST(MergeFinalizeTupleKeyCount) {
    TVector<std::tuple<ui32, TString>> key = {{1U, "x"}, {1U, "x"}, {2U, "y"}};
    TVector<ui64> partialCounts = {1U, 1U, 0U};

    RunMergeFinalizeHashedTest(
        std::make_tuple(TVector<std::tuple<ui32, TString>>{{1U, "x"}, {2U, "y"}}, TVector<ui64>{2U, 0U}),
        std::make_tuple(key, partialCounts), {0}, {TAggInfo{.Name = "count", .ArgsColumns = {1}}});
}

Y_UNIT_TEST(MergeFinalizeAllScalarKeyMultipleAggs) {
    RunMergeFinalizeHashedTest(std::make_tuple(TVector<ui32>{42U}, TVector<ui64>{1U}, TVector<ui32>{7U}),
                               std::make_tuple(42U, ui64(1U), 7U), {0}, {TAggInfo{.Name = "count", .ArgsColumns = {1}}, TAggInfo{.Name = "min", .ArgsColumns = {2}}});
}

Y_UNIT_TEST(MergeFinalizeMixedScalarArrayKeysWithChunking) {
    TVector<ui32> key = {1U, 2U, 1U, 2U, 1U};
    TVector<ui32> vals = {10U, 20U, 30U, 40U, 50U};

    RunMergeFinalizeHashedTest(
        std::make_tuple(TVector<ui32>{1U, 2U}, TVector<TString>{"region", "region"}, TVector<ui32>{10U, 20U}, TVector<ui32>{50U, 40U}),
        std::make_tuple(key, TString("region"), vals), {0, 1}, {TAggInfo{.Name = "min", .ArgsColumns = {2}}, TAggInfo{.Name = "max", .ArgsColumns = {2}}});
}

Y_UNIT_TEST(MergeFinalizeDoubleOptionalKey) {
    TVector<TMaybe<TMaybe<ui64>>> key = {
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>(1U)),
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>()),
        TMaybe<TMaybe<ui64>>(),
    };
    TVector<ui64> partialCounts = {2U, 3U, 5U};

    RunMergeFinalizeHashedTest(
        std::make_tuple(TVector<TMaybe<TMaybe<ui64>>>{key[0], key[1], key[2]}, TVector<ui64>{2U, 3U, 5U}),
        std::make_tuple(key, partialCounts), {0}, {TAggInfo{.Name = "count", .ArgsColumns = {1}}});
}

Y_UNIT_TEST(MergeFinalizeVoidKey) {
    TVector<TMaybe<NTest::TSingularVoid>> key = {
        TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()),
        TMaybe<NTest::TSingularVoid>(),
    };
    TVector<ui64> partialCounts = {2U, 5U};

    RunMergeFinalizeHashedTest(
        std::make_tuple(TVector<TMaybe<NTest::TSingularVoid>>{key[0], key[1]}, TVector<ui64>{2U, 5U}),
        std::make_tuple(key, partialCounts), {0}, {TAggInfo{.Name = "count", .ArgsColumns = {1}}});
}

Y_UNIT_TEST(MergeManyFinalizeDisjointAggOwnershipByUi32Key) {
    TVector<ui32> key = {1U, 1U, 2U, 2U};
    TVector<ui32> streamIndex = {0U, 1U, 0U, 1U};
    TVector<TMaybe<ui64>> countState = {TMaybe<ui64>(3U), TMaybe<ui64>{}, TMaybe<ui64>(5U), TMaybe<ui64>{}};
    TVector<TMaybe<ui32>> minState = {TMaybe<ui32>{}, TMaybe<ui32>(7U), TMaybe<ui32>{}, TMaybe<ui32>(2U)};

    RunMergeManyFinalizeHashedTest(
        std::make_tuple(TVector<ui32>{1U, 2U}, TVector<ui64>{3U, 5U}, TVector<ui32>{7U, 2U}),
        std::make_tuple(key, streamIndex, countState, minState), {0},
        {TAggInfo{.Name = "count", .ArgsColumns = {2}}, TAggInfo{.Name = "min", .ArgsColumns = {3}}}, /*streamIndexColumn=*/1, {{0}, {1}});
}

Y_UNIT_TEST(MergeManyFinalizeScalarKeySingleStream) {
    TVector<TMaybe<ui64>> countState = {TMaybe<ui64>(5U)};

    RunMergeManyFinalizeHashedTest(std::make_tuple(TVector<ui32>{42U}, TVector<ui64>{5U}),
                                   std::make_tuple(42U, 0U, countState), {0}, {TAggInfo{.Name = "count", .ArgsColumns = {2}}},
                                   /*streamIndexColumn=*/1, {{0}});
}

Y_UNIT_TEST(MergeManyFinalizeMixedScalarArrayKeysWithChunking) {
    TVector<ui32> key = {1U, 1U, 2U, 2U};
    TVector<ui32> streamIndex = {0U, 1U, 0U, 1U};
    TVector<TMaybe<ui64>> countState = {TMaybe<ui64>(2U), TMaybe<ui64>{}, TMaybe<ui64>(3U), TMaybe<ui64>{}};
    TVector<TMaybe<ui32>> minState = {TMaybe<ui32>{}, TMaybe<ui32>(10U), TMaybe<ui32>{}, TMaybe<ui32>(20U)};

    RunMergeManyFinalizeHashedTest(
        std::make_tuple(TVector<ui32>{1U, 2U}, TVector<TString>{"region", "region"}, TVector<ui64>{2U, 3U}, TVector<ui32>{10U, 20U}),
        std::make_tuple(key, TString("region"), streamIndex, countState, minState), {0, 1},
        {TAggInfo{.Name = "count", .ArgsColumns = {3}}, TAggInfo{.Name = "min", .ArgsColumns = {4}}}, /*streamIndexColumn=*/2, {{0}, {1}});
}

Y_UNIT_TEST(MergeManyFinalizeDoubleOptionalKey) {
    TVector<TMaybe<TMaybe<ui64>>> key = {
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>(1U)),
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>()),
        TMaybe<TMaybe<ui64>>(),
    };
    TVector<ui32> streamIndex = {0U, 0U, 0U};
    TVector<TMaybe<ui64>> countState = {TMaybe<ui64>(2U), TMaybe<ui64>(3U), TMaybe<ui64>(5U)};

    RunMergeManyFinalizeHashedTest(
        std::make_tuple(TVector<TMaybe<TMaybe<ui64>>>{key[0], key[1], key[2]}, TVector<ui64>{2U, 3U, 5U}),
        std::make_tuple(key, streamIndex, countState), {0}, {TAggInfo{.Name = "count", .ArgsColumns = {2}}}, /*streamIndexColumn=*/1, {{0}});
}

Y_UNIT_TEST(MergeManyFinalizeVoidKey) {
    TVector<TMaybe<NTest::TSingularVoid>> key = {
        TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()),
        TMaybe<NTest::TSingularVoid>(),
    };
    TVector<ui32> streamIndex = {0U, 0U};
    TVector<TMaybe<ui64>> countState = {TMaybe<ui64>(2U), TMaybe<ui64>(5U)};

    RunMergeManyFinalizeHashedTest(
        std::make_tuple(TVector<TMaybe<NTest::TSingularVoid>>{key[0], key[1]}, TVector<ui64>{2U, 5U}),
        std::make_tuple(key, streamIndex, countState), {0}, {TAggInfo{.Name = "count", .ArgsColumns = {2}}}, /*streamIndexColumn=*/1, {{0}});
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockAggTest)

} // namespace NKikimr::NMiniKQL
