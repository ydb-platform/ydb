#include "mkql_block_test_helper.h"
#include "mkql_computation_node_ut.h"

#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

namespace {

bool IsOptionalOrNull(const TType* type) {
    return type->IsOptional() || type->IsNull() || type->IsPg();
}

TType* MakeJoinType(TProgramBuilder& pb, EJoinKind joinKind, TType* leftStreamType, const TVector<ui32>& leftKeyDrops,
                    TType* rightListType, const TVector<ui32>& rightKeyDrops) {
    const auto leftStreamItems = ValidateBlockStreamType(leftStreamType);
    const auto rightListItemType = AS_TYPE(TListType, rightListType)->GetItemType();
    const auto rightPlainStructType = AS_TYPE(TStructType, pb.ValidateBlockStructType(AS_TYPE(TStructType, rightListItemType)));

    TVector<TType*> joinReturnItems;

    const THashSet<ui32> leftKeyDropsSet(leftKeyDrops.cbegin(), leftKeyDrops.cend());
    for (size_t i = 0; i < leftStreamItems.size() - 1; i++) {
        if (leftKeyDropsSet.contains(i)) {
            continue;
        }
        joinReturnItems.push_back(pb.NewBlockType(leftStreamItems[i], TBlockType::EShape::Many));
    }

    if (joinKind != EJoinKind::LeftSemi && joinKind != EJoinKind::LeftOnly) {
        const THashSet<ui32> rightKeyDropsSet(rightKeyDrops.cbegin(), rightKeyDrops.cend());
        for (size_t i = 0; i < rightPlainStructType->GetMembersCount(); i++) {
            const auto& memberName = rightPlainStructType->GetMemberName(i);
            if (rightKeyDropsSet.contains(i) || memberName == NYql::BlockLengthColumnName) {
                continue;
            }

            auto memberType = rightPlainStructType->GetMemberType(i);
            joinReturnItems.push_back(pb.NewBlockType(
                joinKind == EJoinKind::Inner   ? memberType
                : IsOptionalOrNull(memberType) ? memberType
                                               : pb.NewOptionalType(memberType),
                TBlockType::EShape::Many));
        }
    }

    joinReturnItems.push_back(pb.NewBlockType(pb.NewDataType(NUdf::TDataType<ui64>::Id), TBlockType::EShape::Scalar));
    return pb.NewStreamType(pb.NewMultiType(joinReturnItems));
}

TRuntimeNode BuildRightBlockList(TProgramBuilder& pb, TRuntimeNode plainList) {
    TTupleType* tupleType = AS_TYPE(TTupleType, AS_TYPE(TListType, plainList.GetStaticType())->GetItemType());
    const ui32 width = tupleType->GetElementsCount();

    auto wideFlow = pb.ExpandMap(pb.ToFlow(plainList, {}), [&](TRuntimeNode tupleNode) -> TRuntimeNode::TList {
        TRuntimeNode::TList result;
        for (ui32 i = 0; i < width; ++i) {
            result.push_back(pb.Nth(tupleNode, i));
        }
        return result;
    });
    auto wideBlockStream = pb.WideToBlocks(pb.FromFlow(wideFlow));

    auto narrow = pb.NarrowMap(pb.ToFlow(wideBlockStream, {}), [&](TRuntimeNode::TList items) -> TRuntimeNode {
        std::vector<const std::pair<std::string_view, TRuntimeNode>> structItems;
        structItems.reserve(items.size());
        structItems.emplace_back(NYql::BlockLengthColumnName, items.back());
        for (ui32 i = 0; i + 1 < items.size(); ++i) {
            const auto& memberName = pb.GetTypeEnvironment().InternName(ToString(i));
            structItems.emplace_back(memberName.Str(), items[i]);
        }
        return pb.NewStruct(structItems);
    });
    return pb.Collect(narrow);
}

TRuntimeNode BuildRightBlockIndex(TProgramBuilder& pb, TRuntimeNode rightList, const TVector<ui32>& rightKeyColumns,
                                  bool rightAny, TType*& rightBlockListItemType) {
    auto rightBlockList = BuildRightBlockList(pb, rightList);
    rightBlockListItemType = AS_TYPE(TListType, rightBlockList.GetStaticType())->GetItemType();
    auto storage = pb.BlockStorage(rightBlockList, pb.NewResourceType(BlockStorageResourcePrefix));
    return pb.BlockMapJoinIndex(storage, rightBlockListItemType, rightKeyColumns, rightAny,
                                pb.NewResourceType(BlockMapJoinIndexResourcePrefix));
}

TRuntimeNode BuildRightBlockStorage(TProgramBuilder& pb, TRuntimeNode rightList, TType*& rightBlockListItemType) {
    auto rightBlockList = BuildRightBlockList(pb, rightList);
    rightBlockListItemType = AS_TYPE(TListType, rightBlockList.GetStaticType())->GetItemType();
    return pb.BlockStorage(rightBlockList, pb.NewResourceType(BlockStorageResourcePrefix));
}

template <typename... TExpected, typename... TInputs, typename TBuildRightList>
void RunMapJoinTest(const std::tuple<TVector<TExpected>...>& expected, const std::tuple<TInputs...>& leftInputs,
                    EJoinKind joinKind, const TVector<ui32>& leftKeyColumns, const TVector<ui32>& leftKeyDrops,
                    TBuildRightList&& buildRightList, const TVector<ui32>& rightKeyColumns,
                    const TVector<ui32>& rightKeyDrops, bool rightAny = false,
                    size_t iterations = TBlockHelper::ManyIterations) {
    TBlockHelper helper;
    helper.WithScopedFuzzers([&] {
        helper.RunWideStreamNode(
            expected,
            [&](TSetup<false>& setup, TRuntimeNode fuzzedWideStream) {
                TProgramBuilder& pb = *setup.PgmBuilder;
                auto rightList = buildRightList(pb);

                TType* rightBlockListItemType = nullptr;
                TRuntimeNode rightBlockIndex = (joinKind == EJoinKind::Cross)
                                                   ? BuildRightBlockStorage(pb, rightList, rightBlockListItemType)
                                                   : BuildRightBlockIndex(pb, rightList, rightKeyColumns, rightAny, rightBlockListItemType);

                auto joinReturnType = MakeJoinType(pb, joinKind, fuzzedWideStream.GetStaticType(), leftKeyDrops,
                                                   pb.NewListType(rightBlockListItemType), rightKeyDrops);
                return pb.BlockMapJoinCore(fuzzedWideStream, rightBlockIndex, rightBlockListItemType, joinKind,
                                           leftKeyColumns, leftKeyDrops, rightKeyColumns, rightKeyDrops, joinReturnType);
            },
            /*unordered=*/true,
            leftInputs);
    }, iterations);
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockMapJoinCoreTest) {

Y_UNIT_TEST(InnerJoinBasic) {
    TVector<ui64> leftKey = {10U, 20U, 40U};
    TVector<TString> leftVal = {"x", "y", "z"};

    RunMapJoinTest(
        std::make_tuple(TVector<ui64>{10U, 20U}, TVector<TString>{"x", "y"}, TVector<TString>{"a", "b"}),
        std::make_tuple(leftKey, leftVal), EJoinKind::Inner, {0}, {},
        [](TProgramBuilder& pb) {
            return NTest::ConvertValueToLiteralNode(
                pb, TVector<std::tuple<ui64, TString>>{{10U, "a"}, {20U, "b"}, {30U, "c"}});
        },
        {0}, {0});
}

Y_UNIT_TEST(LeftJoinWithMisses) {
    TVector<ui64> leftKey = {10U, 20U, 40U};
    TVector<TString> leftVal = {"x", "y", "z"};

    RunMapJoinTest(
        std::make_tuple(TVector<ui64>{10U, 20U, 20U, 40U}, TVector<TString>{"x", "y", "y", "z"},
                        TVector<TMaybe<TString>>{TString("a"), TString("b1"), TString("b2"), TMaybe<TString>{}}),
        std::make_tuple(leftKey, leftVal), EJoinKind::Left, {0}, {},
        [](TProgramBuilder& pb) {
            return NTest::ConvertValueToLiteralNode(
                pb, TVector<std::tuple<ui64, TString>>{{10U, "a"}, {20U, "b1"}, {20U, "b2"}});
        },
        {0}, {0});
}

Y_UNIT_TEST(InnerJoinMultipleRightMatchesPerKey) {
    TVector<ui64> leftKey = {10U, 20U, 40U};
    TVector<TString> leftVal = {"x", "y", "z"};

    RunMapJoinTest(
        std::make_tuple(TVector<ui64>{10U, 10U, 20U}, TVector<TString>{"x", "x", "y"},
                        TVector<TString>{"a1", "a2", "b"}),
        std::make_tuple(leftKey, leftVal), EJoinKind::Inner, {0}, {},
        [](TProgramBuilder& pb) {
            return NTest::ConvertValueToLiteralNode(
                pb, TVector<std::tuple<ui64, TString>>{{10U, "a1"}, {10U, "a2"}, {20U, "b"}});
        },
        {0}, {0});
}

Y_UNIT_TEST(InnerJoinLargeStringValuesForceOutputSlicing) {
    const TString hugeString(128, 'q');
    constexpr size_t leftSize = 2000;

    TVector<ui64> leftKey(leftSize, 1U);
    TVector<TString> leftVal(leftSize, "x");

    TVector<ui64> expectedKey(leftSize, 1U);
    TVector<TString> expectedVal(leftSize, "x");
    TVector<TString> expectedRightVal(leftSize, hugeString);

    RunMapJoinTest(
        std::make_tuple(expectedKey, expectedVal, expectedRightVal),
        std::make_tuple(leftKey, leftVal), EJoinKind::Inner, {0}, {},
        [&](TProgramBuilder& pb) {
            return NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui64, TString>>{{1U, hugeString}});
        },
        {0}, {0}, /*rightAny=*/false, /*iterations=*/5);
}

Y_UNIT_TEST(InnerJoinHugeRightSideSameKey) {
    constexpr size_t rightSize = 10000;

    TVector<ui64> leftKey = {1U};
    TVector<TString> leftVal = {"only"};

    TVector<std::tuple<ui64, TString>> rightRows;
    rightRows.reserve(rightSize);
    for (size_t i = 0; i < rightSize; ++i) {
        rightRows.emplace_back(1U, ToString(i));
    }

    TVector<ui64> expectedKey(rightSize, 1U);
    TVector<TString> expectedVal(rightSize, "only");
    TVector<TString> expectedRightVal;
    expectedRightVal.reserve(rightSize);
    for (const auto& [key, val] : rightRows) {
        expectedRightVal.push_back(val);
    }

    RunMapJoinTest(
        std::make_tuple(expectedKey, expectedVal, expectedRightVal),
        std::make_tuple(leftKey, leftVal), EJoinKind::Inner, {0}, {},
        [&](TProgramBuilder& pb) { return NTest::ConvertValueToLiteralNode(pb, rightRows); },
        {0}, {0}, /*rightAny=*/false, /*iterations=*/5);
}

Y_UNIT_TEST(InnerJoinKeyCollisionRegression) {
    // Zero key must not collide with an internal NULL sentinel value used by a previous
    // implementation of the join index.
    TVector<ui64> leftKey = {0U, 1U, 2U, 3U, 4U, 5U, 6U, 7U};
    TVector<TString> leftVal = {"v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7"};

    RunMapJoinTest(
        std::make_tuple(leftKey, leftVal, TVector<TString>{"r0", "r1", "r2", "r3", "r4", "r5", "r6", "r7"}),
        std::make_tuple(leftKey, leftVal), EJoinKind::Inner, {0}, {},
        [](TProgramBuilder& pb) {
            return NTest::ConvertValueToLiteralNode(
                pb, TVector<std::tuple<ui64, TString>>{
                        {0U, "r0"}, {1U, "r1"}, {2U, "r2"}, {3U, "r3"},
                        {4U, "r4"},
                        {5U, "r5"},
                        {6U, "r6"},
                        {7U, "r7"}});
        },
        {0}, {0});
}

Y_UNIT_TEST(InnerJoinEmptyRightSideProducesNoRows) {
    TVector<ui64> leftKey = {1U, 2U, 3U};
    TVector<TString> leftVal = {"a", "b", "c"};

    RunMapJoinTest(
        std::make_tuple(TVector<ui64>{}, TVector<TString>{}, TVector<TString>{}),
        std::make_tuple(leftKey, leftVal), EJoinKind::Inner, {0}, {},
        [](TProgramBuilder& pb) {
            return NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui64, TString>>{});
        },
        {0}, {0});
}

Y_UNIT_TEST(InnerJoinNullableTupleKeyPartialNullNeverMatches) {
    TVector<TMaybe<ui32>> leftKeyA = {TMaybe<ui32>(1U), TMaybe<ui32>{}, TMaybe<ui32>(2U), TMaybe<ui32>(1U)};
    TVector<TMaybe<TString>> leftKeyB = {TString("x"), TString("y"), TMaybe<TString>{}, TString("z")};
    TVector<ui32> leftVal = {100U, 200U, 300U, 400U};

    RunMapJoinTest(
        std::make_tuple(TVector<TMaybe<ui32>>{TMaybe<ui32>(1U)}, TVector<TMaybe<TString>>{TString("x")},
                        TVector<ui32>{100U}, TVector<TString>{"R"}),
        std::make_tuple(leftKeyA, leftKeyB, leftVal), EJoinKind::Inner, {0, 1}, {},
        [](TProgramBuilder& pb) {
            return NTest::ConvertValueToLiteralNode(
                pb, TVector<std::tuple<TMaybe<ui32>, TMaybe<TString>, TString>>{
                        {TMaybe<ui32>(1U), TString("x"), "R"}});
        },
        {0, 1}, {0, 1});
}

Y_UNIT_TEST(LeftSemiJoinKeepsOnlyMatchedRows) {
    TVector<ui64> leftKey = {10U, 20U, 40U};
    TVector<TString> leftVal = {"x", "y", "z"};

    RunMapJoinTest(
        std::make_tuple(TVector<ui64>{10U, 20U}, TVector<TString>{"x", "y"}),
        std::make_tuple(leftKey, leftVal), EJoinKind::LeftSemi, {0}, {},
        [](TProgramBuilder& pb) {
            return NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui64, TString>>{{10U, "a"}, {20U, "b"}});
        },
        {0}, {});
}

Y_UNIT_TEST(LeftOnlyJoinKeepsOnlyUnmatchedRows) {
    TVector<ui64> leftKey = {10U, 20U, 40U};
    TVector<TString> leftVal = {"x", "y", "z"};

    RunMapJoinTest(
        std::make_tuple(TVector<ui64>{40U}, TVector<TString>{"z"}),
        std::make_tuple(leftKey, leftVal), EJoinKind::LeftOnly, {0}, {},
        [](TProgramBuilder& pb) {
            return NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui64, TString>>{{10U, "a"}, {20U, "b"}});
        },
        {0}, {});
}

Y_UNIT_TEST(LeftSemiJoinExcludesNullKeyRows) {
    TVector<TMaybe<ui64>> leftKey = {TMaybe<ui64>(10U), TMaybe<ui64>{}, TMaybe<ui64>(20U)};
    TVector<TString> leftVal = {"x", "y", "z"};

    RunMapJoinTest(
        std::make_tuple(TVector<TMaybe<ui64>>{TMaybe<ui64>(10U)}, TVector<TString>{"x"}),
        std::make_tuple(leftKey, leftVal), EJoinKind::LeftSemi, {0}, {},
        [](TProgramBuilder& pb) {
            return NTest::ConvertValueToLiteralNode(
                pb, TVector<std::tuple<TMaybe<ui64>>>{{TMaybe<ui64>(10U)}, {TMaybe<ui64>{}}});
        },
        {0}, {});
}

Y_UNIT_TEST(LeftOnlyJoinIncludesNullKeyRows) {
    TVector<TMaybe<ui64>> leftKey = {TMaybe<ui64>(10U), TMaybe<ui64>{}, TMaybe<ui64>(20U)};
    TVector<TString> leftVal = {"x", "y", "z"};

    RunMapJoinTest(
        std::make_tuple(TVector<TMaybe<ui64>>{TMaybe<ui64>{}, TMaybe<ui64>(20U)}, TVector<TString>{"y", "z"}),
        std::make_tuple(leftKey, leftVal), EJoinKind::LeftOnly, {0}, {},
        [](TProgramBuilder& pb) {
            return NTest::ConvertValueToLiteralNode(
                pb, TVector<std::tuple<TMaybe<ui64>>>{{TMaybe<ui64>(10U)}, {TMaybe<ui64>{}}});
        },
        {0}, {});
}

Y_UNIT_TEST(CrossJoinProducesFullProduct) {
    TVector<TMaybe<ui32>> leftVal = {TMaybe<ui32>(1U), TMaybe<ui32>{}};

    RunMapJoinTest(
        std::make_tuple(TVector<TMaybe<ui32>>{TMaybe<ui32>(1U), TMaybe<ui32>(1U), TMaybe<ui32>{}, TMaybe<ui32>{}},
                        TVector<TMaybe<TString>>{TString("a"), TMaybe<TString>{}, TString("a"), TMaybe<TString>{}}),
        std::make_tuple(leftVal), EJoinKind::Cross, {}, {},
        [](TProgramBuilder& pb) {
            return NTest::ConvertValueToLiteralNode(
                pb, TVector<std::tuple<TMaybe<TString>>>{{TString("a")}, {TMaybe<TString>{}}});
        },
        {}, {});
}

Y_UNIT_TEST(CrossJoinLargeRightTableWithLargeStringsForceOutputSlicing) {
    const TString hugeString(128, 'q');
    constexpr size_t rightSize = 2000;

    TVector<ui64> leftVal = {1U, 2U};

    TVector<ui64> expectedLeftVal;
    TVector<TMaybe<TString>> expectedRightVal;
    expectedLeftVal.reserve(leftVal.size() * rightSize);
    expectedRightVal.reserve(leftVal.size() * rightSize);
    for (const auto& left : leftVal) {
        for (size_t i = 0; i < rightSize; ++i) {
            expectedLeftVal.push_back(left);
            expectedRightVal.push_back(hugeString);
        }
    }

    RunMapJoinTest(
        std::make_tuple(expectedLeftVal, expectedRightVal),
        std::make_tuple(leftVal), EJoinKind::Cross, {}, {},
        [&](TProgramBuilder& pb) {
            TVector<std::tuple<TString>> rightRows(rightSize, std::tuple<TString>{hugeString});
            return NTest::ConvertValueToLiteralNode(pb, rightRows);
        },
        {}, {}, /*rightAny=*/false, /*iterations=*/5);
}

Y_UNIT_TEST(InnerJoinNullableKeysAndValuesRetainAllColumns) {
    TVector<TMaybe<ui64>> leftKey = {TMaybe<ui64>(10U), TMaybe<ui64>{}, TMaybe<ui64>(20U)};
    TVector<TMaybe<ui32>> leftVal = {TMaybe<ui32>(1U), TMaybe<ui32>(2U), TMaybe<ui32>{}};

    RunMapJoinTest(
        std::make_tuple(TVector<TMaybe<ui64>>{TMaybe<ui64>(10U), TMaybe<ui64>(20U)},
                        TVector<TMaybe<ui32>>{TMaybe<ui32>(1U), TMaybe<ui32>{}},
                        TVector<TMaybe<ui64>>{TMaybe<ui64>(10U), TMaybe<ui64>(20U)},
                        TVector<TMaybe<TString>>{TString("a"), TMaybe<TString>{}}),
        std::make_tuple(leftKey, leftVal), EJoinKind::Inner, {0}, {},
        [](TProgramBuilder& pb) {
            return NTest::ConvertValueToLiteralNode(
                pb, TVector<std::tuple<TMaybe<ui64>, TMaybe<TString>>>{
                        {TMaybe<ui64>(10U), TString("a")}, {TMaybe<ui64>(20U), TMaybe<TString>{}}});
        },
        {0}, {});
}

Y_UNIT_TEST(InnerJoinTupleKeyMatchesOnBothColumns) {
    TVector<ui32> leftKeyA = {1U, 1U, 2U};
    TVector<TString> leftKeyB = {"x", "y", "x"};
    TVector<ui32> leftVal = {100U, 200U, 300U};

    RunMapJoinTest(
        std::make_tuple(TVector<ui32>{1U, 2U}, TVector<TString>{"x", "x"}, TVector<ui32>{100U, 300U},
                        TVector<TString>{"R1", "R2"}),
        std::make_tuple(leftKeyA, leftKeyB, leftVal), EJoinKind::Inner, {0, 1}, {},
        [](TProgramBuilder& pb) {
            return NTest::ConvertValueToLiteralNode(
                pb, TVector<std::tuple<ui32, TString, TString>>{{1U, "x", "R1"}, {2U, "x", "R2"}});
        },
        {0, 1}, {0, 1});
}

Y_UNIT_TEST(InnerJoinRightAnyDedupsMultipleMatches) {
    TVector<ui64> leftKey = {5U};
    TVector<TString> leftVal = {"L"};

    RunMapJoinTest(
        std::make_tuple(TVector<ui64>{5U}, TVector<TString>{"L"}, TVector<TString>{"first"}),
        std::make_tuple(leftKey, leftVal), EJoinKind::Inner, {0}, {},
        [](TProgramBuilder& pb) {
            return NTest::ConvertValueToLiteralNode(
                pb, TVector<std::tuple<ui64, TString>>{{5U, "first"}, {5U, "second"}, {5U, "third"}});
        },
        {0}, {0}, /*rightAny=*/true);
}

Y_UNIT_TEST(InnerJoinAllScalarInputs) {
    RunMapJoinTest(
        std::make_tuple(TVector<ui64>{7U}, TVector<TString>{"only"}, TVector<TString>{"match"}),
        std::make_tuple(ui64(7U), TString("only")), EJoinKind::Inner, {0}, {},
        [](TProgramBuilder& pb) {
            return NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui64, TString>>{{7U, "match"}});
        },
        {0}, {0});
}

Y_UNIT_TEST(InnerJoinMixedScalarArrayKeysWithChunking) {
    TVector<ui64> leftKey = {1U, 2U, 3U, 4U, 5U};
    TString leftLabel = "tag";

    RunMapJoinTest(
        std::make_tuple(TVector<ui64>{2U, 4U}, TVector<TString>{"tag", "tag"}, TVector<TString>{"two", "four"}),
        std::make_tuple(leftKey, leftLabel), EJoinKind::Inner, {0}, {},
        [](TProgramBuilder& pb) {
            return NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui64, TString>>{{2U, "two"}, {4U, "four"}});
        },
        {0}, {0});
}

Y_UNIT_TEST(InnerJoinDoubleOptionalKey) {
    TVector<TMaybe<TMaybe<ui64>>> leftKey = {
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>(1U)),
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>()),
        TMaybe<TMaybe<ui64>>(),
        TMaybe<TMaybe<ui64>>(TMaybe<ui64>(2U)),
    };
    TVector<ui32> leftVal = {1U, 2U, 3U, 4U};

    RunMapJoinTest(
        std::make_tuple(TVector<TMaybe<TMaybe<ui64>>>{leftKey[0], leftKey[1]}, TVector<ui32>{1U, 2U},
                        TVector<TString>{"a", "b"}),
        std::make_tuple(leftKey, leftVal), EJoinKind::Inner, {0}, {},
        [](TProgramBuilder& pb) {
            return NTest::ConvertValueToLiteralNode(
                pb, TVector<std::tuple<TMaybe<TMaybe<ui64>>, TString>>{
                        {TMaybe<TMaybe<ui64>>(TMaybe<ui64>(1U)), "a"},
                        {TMaybe<TMaybe<ui64>>(TMaybe<ui64>()), "b"},
                    });
        },
        {0}, {0});
}

Y_UNIT_TEST(InnerJoinVoidKey) {
    TVector<TMaybe<NTest::TSingularVoid>> leftKey = {
        TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()),
        TMaybe<NTest::TSingularVoid>(),
    };
    TVector<ui32> leftVal = {1U, 2U};

    RunMapJoinTest(
        std::make_tuple(TVector<TMaybe<NTest::TSingularVoid>>{leftKey[0]}, TVector<ui32>{1U}, TVector<TString>{"a"}),
        std::make_tuple(leftKey, leftVal), EJoinKind::Inner, {0}, {},
        [](TProgramBuilder& pb) {
            return NTest::ConvertValueToLiteralNode(
                pb, TVector<std::tuple<TMaybe<NTest::TSingularVoid>, TString>>{
                        {TMaybe<NTest::TSingularVoid>(NTest::TSingularVoid()), "a"},
                    });
        },
        {0}, {0});
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockMapJoinCoreTest)

} // namespace NKikimr::NMiniKQL
