#include "mkql_block_map_join_ut_utils.h"
#include "mkql_computation_node_ut.h"

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_block_grace_join_policy.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

// -------------------------------------------------------------------
[[maybe_unused]] constexpr size_t KB = 1024;
[[maybe_unused]] constexpr size_t MB = KB * KB;
[[maybe_unused]] constexpr size_t L1_CACHE_SIZE = 256 * KB;
[[maybe_unused]] constexpr size_t L2_CACHE_SIZE =   2 * MB;
[[maybe_unused]] constexpr size_t L3_CACHE_SIZE =  16 * MB;

// -------------------------------------------------------------------
#define DEFINE_TEST_POLICY(name, useExternal, algo)             \
struct name : public IBlockGraceJoinPolicy {                    \
    name() {                                                    \
        SetMaximumInitiallyFetchedData(L3_CACHE_SIZE / 2);      \
    }                                                           \
                                                                \
    bool UseExternalPayload(EJoinAlgo, size_t) const override { \
        return useExternal;                                     \
    }                                                           \
                                                                \
    EJoinAlgo PickAlgorithm(size_t, size_t) const override {    \
        return algo;                                            \
    }                                                           \
};                                                              \
                                                                \
[[maybe_unused]] name g ## name{};                              \
[[maybe_unused]] IBlockGraceJoinPolicy* gp ## name{&g ## name}

DEFINE_TEST_POLICY(TAlwaysHashJoinPolicy,               false, EJoinAlgo::HashJoin);
DEFINE_TEST_POLICY(TAlwaysInMemGraceJoinPolicy,         false, EJoinAlgo::InMemoryGraceJoin);
DEFINE_TEST_POLICY(TAlwaysExternalHashJoinPolicy,       true,  EJoinAlgo::HashJoin);
DEFINE_TEST_POLICY(TAlwaysExternalInMemGraceJoinPolicy, true,  EJoinAlgo::InMemoryGraceJoin);

[[maybe_unused]] IBlockGraceJoinPolicy* gpDefaultPolicy{nullptr};

// -------------------------------------------------------------------
// List<Tuple<...>> -> Stream<Multi<...>>
TRuntimeNode ToWideStream(TProgramBuilder& pgmBuilder, TRuntimeNode list) {
    auto wideFlow = pgmBuilder.ExpandMap(pgmBuilder.ToFlow(list),
        [&](TRuntimeNode tupleNode) -> TRuntimeNode::TList {
            TTupleType* tupleType = AS_TYPE(TTupleType, tupleNode.GetStaticType());
            TRuntimeNode::TList wide;
            wide.reserve(tupleType->GetElementsCount());
            for (size_t i = 0; i < tupleType->GetElementsCount(); i++) {
                wide.emplace_back(pgmBuilder.Nth(tupleNode, i));
            }
            return wide;
        }
    );

    return pgmBuilder.FromFlow(wideFlow);
}

// Stream<Multi<...>> -> List<Tuple<...>>
TRuntimeNode FromWideStream(TProgramBuilder& pgmBuilder, TRuntimeNode stream) {
    return pgmBuilder.Collect(pgmBuilder.NarrowMap(pgmBuilder.ToFlow(stream),
        [&](TRuntimeNode::TList items) -> TRuntimeNode {
            TVector<TRuntimeNode> tupleElements;
            tupleElements.reserve(items.size());
            for (size_t i = 0; i < items.size(); i++) {
                tupleElements.emplace_back(items[i]);
            }
            return pgmBuilder.NewTuple(tupleElements);
        })
    );
}

TRuntimeNode BuildBlockJoin(TProgramBuilder& pgmBuilder, const IBlockGraceJoinPolicy* policy, EJoinKind joinKind,
    TRuntimeNode leftList, const TVector<ui32>& leftKeyColumns, const TVector<ui32>& leftKeyDrops,
    TRuntimeNode rightList, const TVector<ui32>& rightKeyColumns, const TVector<ui32>& rightKeyDrops
) {
    const auto leftStream = ThrottleStream(pgmBuilder, ToWideStream(pgmBuilder, leftList));
    const auto rightStream = ThrottleStream(pgmBuilder, ToWideStream(pgmBuilder, rightList));

    const auto leftStreamItems = ValidateBlockStreamType(leftStream.GetStaticType());
    const auto rightStreamItems = ValidateBlockStreamType(rightStream.GetStaticType());

    TVector<TType*> joinReturnItems;

    const THashSet<ui32> leftKeyDropsSet(leftKeyDrops.cbegin(), leftKeyDrops.cend());
    for (size_t i = 0; i < leftStreamItems.size() - 1; i++) {  // Excluding block size
        if (leftKeyDropsSet.contains(i)) {
            continue;
        }
        joinReturnItems.push_back(pgmBuilder.NewBlockType(leftStreamItems[i], TBlockType::EShape::Many));
    }

    const THashSet<ui32> rightKeyDropsSet(rightKeyDrops.cbegin(), rightKeyDrops.cend());
    for (size_t i = 0; i < rightStreamItems.size() - 1; i++) {  // Excluding block size
        if (rightKeyDropsSet.contains(i)) {
            continue;
        }

        joinReturnItems.push_back(pgmBuilder.NewBlockType(rightStreamItems[i], TBlockType::EShape::Many));
    }

    joinReturnItems.push_back(pgmBuilder.NewBlockType(pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id), TBlockType::EShape::Scalar));

    TType* joinReturnType = pgmBuilder.NewStreamType(pgmBuilder.NewMultiType(joinReturnItems));
    auto joinNode = pgmBuilder.BlockGraceJoinCore(
        leftStream,
        rightStream,
        joinKind,
        leftKeyColumns,
        leftKeyDrops,
        rightKeyColumns,
        rightKeyDrops,
        false,
        joinReturnType,
        static_cast<const void*>(policy)
    );

    return FromWideStream(pgmBuilder, DethrottleStream(pgmBuilder, joinNode));
}

NUdf::TUnboxedValue DoTestBlockJoin(
    TSetup<false>& setup, const IBlockGraceJoinPolicy* policy,
    TType* leftType, NUdf::TUnboxedValue&& leftListValue, const TVector<ui32>& leftKeyColumns, const TVector<ui32>& leftKeyDrops,
    TType* rightType, NUdf::TUnboxedValue&& rightListValue, const TVector<ui32>& rightKeyColumns, const TVector<ui32>& rightKeyDrops,
    EJoinKind joinKind, size_t blockSize)
{
    TProgramBuilder& pb = *setup.PgmBuilder;

    Y_ENSURE(leftType->IsList(), "Left node has to be list");
    const auto leftItemType = AS_TYPE(TListType, leftType)->GetItemType();
    Y_ENSURE(leftItemType->IsTuple(), "List item has to be tuple");
    TType* leftBlockType = MakeBlockTupleType(pb, leftItemType, false);

    Y_ENSURE(rightType->IsList(), "Right node has to be list");
    const auto rightItemType = AS_TYPE(TListType, rightType)->GetItemType();
    Y_ENSURE(rightItemType->IsTuple(), "Right item has to be tuple");
    TType* rightBlockType = MakeBlockTupleType(pb, rightItemType, false);

    TRuntimeNode leftList = pb.Arg(pb.NewListType(leftBlockType));
    TRuntimeNode rightList = pb.Arg(pb.NewListType(rightBlockType));
    const auto joinNode = BuildBlockJoin(
        pb, policy, joinKind, leftList, leftKeyColumns, leftKeyDrops, rightList, rightKeyColumns, rightKeyDrops);

    const auto joinType = joinNode.GetStaticType();
    Y_ENSURE(joinType->IsList(), "Join result has to be list");
    const auto joinItemType = AS_TYPE(TListType, joinType)->GetItemType();
    Y_ENSURE(joinItemType->IsTuple(), "List item has to be tuple");

    const auto graph = setup.BuildGraph(joinNode, {leftList.GetNode(), rightList.GetNode()});

    auto& ctx = graph->GetContext();

    NUdf::TUnboxedValuePod leftBlockListValue, rightBlockListValue;
    leftBlockListValue = ToBlocks(ctx, blockSize, AS_TYPE(TTupleType, leftItemType)->GetElements(), std::move(leftListValue));
    rightBlockListValue = ToBlocks(ctx, blockSize, AS_TYPE(TTupleType, rightItemType)->GetElements(), std::move(rightListValue));

    graph->GetEntryPoint(0, true)->SetValue(ctx, leftBlockListValue);
    graph->GetEntryPoint(1, true)->SetValue(ctx, rightBlockListValue);
    return FromBlocks(ctx, AS_TYPE(TTupleType, joinItemType)->GetElements(), graph->GetValue());
}

void RunTestBlockJoin(
    TSetup<false>& setup, const IBlockGraceJoinPolicy* policy, EJoinKind joinKind,
    TType* expectedType, const NUdf::TUnboxedValue& expected,
    TType* leftType, NUdf::TUnboxedValue&& leftListValue, const TVector<ui32>& leftKeyColumns,
    TType* rightType, NUdf::TUnboxedValue&& rightListValue, const TVector<ui32>& rightKeyColumns,
    const TVector<ui32>& leftKeyDrops = {}, const TVector<ui32>& rightKeyDrops = {})
{
    const size_t testSize = leftListValue.GetListLength();
    // WARNING: Do not start with small block size (like 1) on large datasets, because it is so slow
    for (size_t blockSize = std::min<size_t>(64, testSize); blockSize <= testSize; blockSize <<= 1) {
        const auto got = DoTestBlockJoin(
            setup, policy,
            leftType, std::move(leftListValue), leftKeyColumns, leftKeyDrops,
            rightType, std::move(rightListValue), rightKeyColumns, rightKeyDrops,
            joinKind, blockSize
        );
        CompareResults(expectedType, expected, got);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockGraceJoinTestBasic) {
    constexpr size_t testSize = 1 << 14;
    constexpr size_t valueSize = 3;
    static const TVector<TString> threeLetterValues = GenerateValues(valueSize); // generate strings with len 3
    static const TSet<ui64> fibonacci = GenerateFibonacci(testSize); // generate n fib numbers
    static const TString hugeString(128, '1');

    Y_UNIT_TEST(TestInnerJoin) {
        for (auto policy: {gpTAlwaysInMemGraceJoinPolicy}) {
            TSetup<false> setup(GetNodeFactory());

            // 1. Make input for the "left" stream.
            TVector<ui64> leftKeyInit(testSize);
            std::iota(leftKeyInit.begin(), leftKeyInit.end(), 1);
            TVector<ui64> leftSubkeyInit;
            std::transform(leftKeyInit.cbegin(), leftKeyInit.cend(), std::back_inserter(leftSubkeyInit),
                [](const auto key) { return key * 1001; });
            TVector<TString> leftValueInit;
            std::transform(leftKeyInit.cbegin(), leftKeyInit.cend(), std::back_inserter(leftValueInit),
                [](const auto key) { return threeLetterValues[key]; });

            // 2. Make input for the "right" stream. Right stream is index one
            const TVector<ui64> rightKeyInit(fibonacci.cbegin(), fibonacci.cend());
            TVector<TString> rightValueInit;
            std::transform(rightKeyInit.cbegin(), rightKeyInit.cend(), std::back_inserter(rightValueInit),
                [](const auto key) { return std::to_string(key); });

            // 3. Make "expected" data.
            TMap<ui64, TString> rightMap;
            for (size_t i = 0; i < rightKeyInit.size(); i++) {
                rightMap[rightKeyInit[i]] = rightValueInit[i];
            }
            TVector<ui64> expectedKey;
            TVector<ui64> expectedSubkey;
            TVector<TString> expectedValue;
            TVector<TString> expectedRightValue;
            // Make join manually
            for (size_t i = 0; i < leftKeyInit.size(); i++) {
                const auto& found = rightMap.find(leftKeyInit[i]);
                if (found != rightMap.cend()) {
                    expectedKey.push_back(leftKeyInit[i]);
                    expectedSubkey.push_back(leftSubkeyInit[i]);
                    expectedValue.push_back(leftValueInit[i]);
                    expectedRightValue.push_back(found->second);
                }
            }

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                leftKeyInit, leftSubkeyInit, leftValueInit);
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                rightKeyInit, rightValueInit);
            auto [expectedType, expected] = ConvertVectorsToTuples(setup,
                expectedKey, expectedSubkey, expectedValue, expectedRightValue);

            RunTestBlockJoin(
                setup, policy, EJoinKind::Inner,
                expectedType, expected,
                leftType, std::move(leftList), {0},
                rightType, std::move(rightList), {0},
                {}, {0}
            );
        }
    }

    Y_UNIT_TEST(TestInnerJoinMulti) {
        for (auto policy: {gpDefaultPolicy, gpTAlwaysHashJoinPolicy, gpTAlwaysInMemGraceJoinPolicy}) {
            TSetup<false> setup(GetNodeFactory());

            // 1. Make input for the "left" stream.
            TVector<ui64> leftKeyInit(testSize);
            std::iota(leftKeyInit.begin(), leftKeyInit.end(), 1);
            TVector<ui64> leftSubkeyInit;
            std::transform(leftKeyInit.cbegin(), leftKeyInit.cend(), std::back_inserter(leftSubkeyInit),
                [](const auto key) { return key * 1001; });
            TVector<TString> leftValueInit;
            std::transform(leftKeyInit.cbegin(), leftKeyInit.cend(), std::back_inserter(leftValueInit),
                [](const auto key) { return threeLetterValues[key]; });

            // 2. Make input for the "right" stream.
            TVector<ui64> rightKeyInit(fibonacci.cbegin(), fibonacci.cend());
            TVector<TString> rightValueInit;
            std::transform(rightKeyInit.cbegin(), rightKeyInit.cend(), std::back_inserter(rightValueInit),
                [](const auto key) { return std::to_string(key); });

            // Add rows with the same keys
            rightKeyInit.reserve(rightKeyInit.size() * 2);
            std::copy_n(rightKeyInit.begin(), rightKeyInit.size(), std::back_inserter(rightKeyInit));
            std::transform(rightKeyInit.cbegin(), rightKeyInit.cend(), std::back_inserter(rightValueInit),
                [](const auto key) { return std::to_string(key * 1001); });

            // 3. Make "expected" data.
            TMultiMap<ui64, TString> rightMap;
            for (size_t i = 0; i < rightKeyInit.size(); i++) {
                rightMap.insert({rightKeyInit[i], rightValueInit[i]});
            }
            TVector<ui64> expectedKey;
            TVector<ui64> expectedSubkey;
            TVector<TString> expectedValue;
            TVector<TString> expectedRightValue;
            for (size_t i = 0; i < leftKeyInit.size(); i++) {
                const auto& [begin, end] = rightMap.equal_range(leftKeyInit[i]);
                for (auto it = begin; it != end; it++) {
                    expectedKey.push_back(leftKeyInit[i]);
                    expectedSubkey.push_back(leftSubkeyInit[i]);
                    expectedValue.push_back(leftValueInit[i]);
                    expectedRightValue.push_back(it->second);
                }
            }

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                leftKeyInit, leftSubkeyInit, leftValueInit);
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                rightKeyInit, rightValueInit);
            auto [expectedType, expected] = ConvertVectorsToTuples(setup,
                expectedKey, expectedSubkey, expectedValue, expectedRightValue);

            RunTestBlockJoin(
                setup, policy, EJoinKind::Inner,
                expectedType, expected,
                leftType, std::move(leftList), {0},
                rightType, std::move(rightList), {0},
                {}, {0}
            );
        }
    }

    Y_UNIT_TEST(TestKeyTuple) {
        for (auto policy: {gpDefaultPolicy, gpTAlwaysHashJoinPolicy, gpTAlwaysInMemGraceJoinPolicy}) {
            TSetup<false> setup(GetNodeFactory());

            // 1. Make input for the "left" stream.
            TVector<ui64> leftKeyInit(testSize);
            std::iota(leftKeyInit.begin(), leftKeyInit.end(), 1);
            TVector<ui64> leftSubkeyInit;
            std::transform(leftKeyInit.cbegin(), leftKeyInit.cend(), std::back_inserter(leftSubkeyInit),
                [](const auto key) { return key * 1001; });
            TVector<TString> leftValueInit;
            std::transform(leftKeyInit.cbegin(), leftKeyInit.cend(), std::back_inserter(leftValueInit),
                [](const auto key) { return threeLetterValues[key]; });

            // 2. Make input for the "right" stream.
            TVector<ui64> rightKey1Init(fibonacci.cbegin(), fibonacci.cend());
            TVector<ui64> rightKey2Init;
            std::transform(rightKey1Init.cbegin(), rightKey1Init.cend(), std::back_inserter(rightKey2Init),
                [](const auto& key) { return key * 1001; });
            TVector<TString> rightValueInit;
            std::transform(rightKey1Init.cbegin(), rightKey1Init.cend(), std::back_inserter(rightValueInit),
                [](const auto& key) { return std::to_string(key); });

            // 3. Make "expected" data.
            TMap<std::tuple<ui64, ui64>, TString> rightMap;
            for (size_t i = 0; i < rightKey1Init.size(); i++) {
                const auto key = std::make_tuple(rightKey1Init[i], rightKey2Init[i]);
                rightMap[key] = rightValueInit[i];
            }
            TVector<ui64> expectedKey;
            TVector<ui64> expectedSubkey;
            TVector<TString> expectedValue;
            TVector<TString> expectedRightValue;
            for (size_t i = 0; i < leftKeyInit.size(); i++) {
                const auto key = std::make_tuple(leftKeyInit[i], leftSubkeyInit[i]);
                const auto found = rightMap.find(key);
                if (found != rightMap.cend()) {
                    expectedKey.push_back(leftKeyInit[i]);
                    expectedSubkey.push_back(leftSubkeyInit[i]);
                    expectedValue.push_back(leftValueInit[i]);
                    expectedRightValue.push_back(found->second);
                }
            }

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                leftKeyInit, leftSubkeyInit, leftValueInit);
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                rightKey1Init, rightValueInit, rightKey2Init);
            auto [expectedType, expected] = ConvertVectorsToTuples(setup,
                expectedKey, expectedSubkey, expectedValue, expectedRightValue);

            RunTestBlockJoin(
                setup, policy, EJoinKind::Inner,
                expectedType, expected,
                leftType, std::move(leftList), {0, 1},
                rightType, std::move(rightList), {0, 2},
                {}, {0, 2}
            );
        }
    }

    Y_UNIT_TEST(TestInnerJoinOutputSlicing) {
        for (auto policy: {gpDefaultPolicy, gpTAlwaysHashJoinPolicy, gpTAlwaysInMemGraceJoinPolicy}) {
            TSetup<false> setup(GetNodeFactory());

            // 1. Make input for the "left" stream.
            TVector<ui64> leftKeyInit(testSize);
            std::fill(leftKeyInit.begin(), leftKeyInit.end(), 1);
            TVector<ui64> leftSubkeyInit;
            std::transform(leftKeyInit.cbegin(), leftKeyInit.cend(), std::back_inserter(leftSubkeyInit),
                [](const auto key) { return key * 1001; });
            TVector<TString> leftValueInit;
            std::transform(leftKeyInit.cbegin(), leftKeyInit.cend(), std::back_inserter(leftValueInit),
                [](const auto key) { return threeLetterValues[key]; });

            // 2. Make input for the "right" stream.
            // Huge string is used to make less rows fit into one block
            const TVector<ui64> rightKeyInit({1});
            TVector<TString> rightValueInit({hugeString});

            // 3. Make "expected" data.
            TMap<ui64, TString> rightMap;
            for (size_t i = 0; i < rightKeyInit.size(); i++) {
                rightMap[rightKeyInit[i]] = rightValueInit[i];
            }
            TVector<ui64> expectedKey;
            TVector<ui64> expectedSubkey;
            TVector<TString> expectedValue;
            TVector<TString> expectedRightValue;
            for (size_t i = 0; i < leftKeyInit.size(); i++) {
                const auto& found = rightMap.find(leftKeyInit[i]);
                if (found != rightMap.cend()) {
                    expectedKey.push_back(leftKeyInit[i]);
                    expectedSubkey.push_back(leftSubkeyInit[i]);
                    expectedValue.push_back(leftValueInit[i]);
                    expectedRightValue.push_back(found->second);
                }
            }

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                leftKeyInit, leftSubkeyInit, leftValueInit);
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                rightKeyInit, rightValueInit);
            auto [expectedType, expected] = ConvertVectorsToTuples(setup,
                expectedKey, expectedSubkey, expectedValue, expectedRightValue);

            RunTestBlockJoin(
                setup, policy, EJoinKind::Inner,
                expectedType, expected,
                leftType, std::move(leftList), {0},
                rightType, std::move(rightList), {0},
                {}, {0}
            );
        }
    }

    Y_UNIT_TEST(TestInnerJoinHugeIterator) {
        for (auto policy: {gpDefaultPolicy, gpTAlwaysHashJoinPolicy, gpTAlwaysInMemGraceJoinPolicy}) {
            TSetup<false> setup(GetNodeFactory());

            // 1. Make input for the "left" stream.
            TVector<ui64> leftKeyInit({1});
            TVector<ui64> leftSubkeyInit({1001});
            TVector<TString> leftValueInit({threeLetterValues[1]});

            // 2. Make input for the "right" stream.
            // Huge string is used to make less rows fit into one block
            TVector<ui64> rightKeyInit(1 << 14);
            std::fill(rightKeyInit.begin(), rightKeyInit.end(), 1);
            TVector<TString> rightValueInit;
            std::transform(rightKeyInit.cbegin(), rightKeyInit.cend(), std::back_inserter(rightValueInit),
                [](const auto& key) { return std::to_string(key); });

            // 3. Make "expected" data.
            TMultiMap<ui64, TString> rightMap;
            for (size_t i = 0; i < rightKeyInit.size(); i++) {
                rightMap.insert({rightKeyInit[i], rightValueInit[i]});
            }
            TVector<ui64> expectedKey;
            TVector<ui64> expectedSubkey;
            TVector<TString> expectedValue;
            TVector<TString> expectedRightValue;
            for (size_t i = 0; i < leftKeyInit.size(); i++) {
                const auto& [begin, end] = rightMap.equal_range(leftKeyInit[i]);
                for (auto it = begin; it != end; it++) {
                    expectedKey.push_back(leftKeyInit[i]);
                    expectedSubkey.push_back(leftSubkeyInit[i]);
                    expectedValue.push_back(leftValueInit[i]);
                    expectedRightValue.push_back(it->second);
                }
            }

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                leftKeyInit, leftSubkeyInit, leftValueInit);
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                rightKeyInit, rightValueInit);
            auto [expectedType, expected] = ConvertVectorsToTuples(setup,
                expectedKey, expectedSubkey, expectedValue, expectedRightValue);

            RunTestBlockJoin(setup, policy, EJoinKind::Inner, expectedType, expected,
                            leftType, std::move(leftList), {0},
                            rightType, std::move(rightList), {0},
                            {}, {0}
            );
        }
    }

    Y_UNIT_TEST(TestInnerJoinWideProbeTuples) { // Test that external payload storage optimization works correctly
        for (auto policy: {gpDefaultPolicy, gpTAlwaysExternalHashJoinPolicy, gpTAlwaysExternalInMemGraceJoinPolicy}) {
            TSetup<false> setup(GetNodeFactory());

            // 1. Make input for the "left" stream.
            TVector<ui64> leftKeyInit(testSize);
            std::iota(leftKeyInit.begin(), leftKeyInit.end(), 1);
            TVector<ui64> leftSubkeyInit;
            std::transform(leftKeyInit.cbegin(), leftKeyInit.cend(), std::back_inserter(leftSubkeyInit),
                [](const auto key) { return key * 1001; });
            TVector<TString> leftValueInit;
            std::transform(leftKeyInit.cbegin(), leftKeyInit.cend(), std::back_inserter(leftValueInit),
                [](const auto key) { return threeLetterValues[key]; });
            TVector<ui64> leftIntValueInit(testSize);
                std::iota(leftIntValueInit.begin(), leftIntValueInit.end(), 1);

            // 2. Make input for the "right" stream. Right stream is index one
            const TVector<ui64> rightKeyInit(fibonacci.cbegin(), fibonacci.cend());
            TVector<TString> rightValueInit;
            std::transform(rightKeyInit.cbegin(), rightKeyInit.cend(), std::back_inserter(rightValueInit),
                [](const auto key) { return std::to_string(key); });

            // 3. Make "expected" data.
            TMap<ui64, TString> rightMap;
            for (size_t i = 0; i < rightKeyInit.size(); i++) {
                rightMap[rightKeyInit[i]] = rightValueInit[i];
            }
            TVector<ui64> expectedKey;
            TVector<ui64> expectedSubkey;
            TVector<TString> expectedValue;
            TVector<ui64> expectedIntValue;
            TVector<TString> expectedRightValue;
            // Make join manually
            for (size_t i = 0; i < leftKeyInit.size(); i++) {
                const auto& found = rightMap.find(leftKeyInit[i]);
                if (found != rightMap.cend()) {
                    expectedKey.push_back(leftKeyInit[i]);
                    expectedSubkey.push_back(leftSubkeyInit[i]);
                    expectedValue.push_back(leftValueInit[i]);
                    expectedRightValue.push_back(found->second);
                    expectedIntValue.push_back(leftIntValueInit[i]);
                }
            }

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                leftKeyInit, leftSubkeyInit, leftValueInit,
                leftIntValueInit, leftIntValueInit, leftIntValueInit, leftIntValueInit, // make wide tuple with width > 64
                leftIntValueInit, leftIntValueInit, leftIntValueInit, leftIntValueInit);
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                rightKeyInit, rightValueInit);
            auto [expectedType, expected] = ConvertVectorsToTuples(setup,
                expectedKey, expectedSubkey, expectedValue,
                expectedIntValue, expectedIntValue, expectedIntValue, expectedIntValue, // make wide tuple with width > 64
                expectedIntValue, expectedIntValue, expectedIntValue, expectedIntValue,
                expectedRightValue);

            RunTestBlockJoin(
                setup, policy, EJoinKind::Inner,
                expectedType, expected,
                leftType, std::move(leftList), {0},
                rightType, std::move(rightList), {0},
                {}, {0}
            );
        }
    }

} // Y_UNIT_TEST_SUITE

Y_UNIT_TEST_SUITE(TMiniKQLBlockGraceJoinTestOptional) {
    constexpr size_t testSize = 1 << 14;
    constexpr size_t valueSize = 3;
    static const TVector<TString> threeLetterValues = GenerateValues(valueSize);
    static const TSet<ui64> fibonacci = GenerateFibonacci(testSize);

    Y_UNIT_TEST(TestInnerJoin) {
        for (auto policy: {gpDefaultPolicy, gpTAlwaysHashJoinPolicy, gpTAlwaysInMemGraceJoinPolicy}) {
            TSetup<false> setup(GetNodeFactory());

            // 1. Make input for the "left" stream.
            TVector<std::optional<ui64>> leftKeyInit(testSize);
            std::iota(leftKeyInit.begin(), leftKeyInit.end(), 1);
            TVector<ui64> leftSubkeyInit;
            std::transform(leftKeyInit.cbegin(), leftKeyInit.cend(), std::back_inserter(leftSubkeyInit),
                [](const auto key) { return *key * 1001; });
            TVector<std::optional<TString>> leftValueInit;
            std::transform(leftKeyInit.cbegin(), leftKeyInit.cend(), std::back_inserter(leftValueInit),
                [](const auto key) { return threeLetterValues[*key]; });

            // 2. Make input for the "right" stream.
            TVector<std::optional<ui64>> rightKeyInit(fibonacci.cbegin(), fibonacci.cend());
            TVector<std::optional<TString>> rightValueInit;
            std::transform(rightKeyInit.cbegin(), rightKeyInit.cend(), std::back_inserter(rightValueInit),
                [](const auto key) { return std::to_string(*key); });

            // 3. Add some NULLs
            leftKeyInit[0] = leftKeyInit[2] = std::nullopt;
            rightKeyInit[2] = rightKeyInit[3] = std::nullopt;

            leftValueInit[1] = leftValueInit[11] = leftValueInit[41] = std::nullopt;
            rightValueInit[2] = rightValueInit[12] = rightValueInit[42] = std::nullopt;

            // 4. Make "expected" data.
            TMap<ui64, std::optional<TString>> rightMap;
            for (size_t i = 0; i < rightKeyInit.size(); i++) {
                if (rightKeyInit[i].has_value()) {
                    rightMap[*rightKeyInit[i]] = rightValueInit[i];
                }
            }
            TVector<std::optional<ui64>> expectedLeftKey;
            TVector<ui64> expectedSubkey;
            TVector<std::optional<TString>> expectedValue;
            TVector<std::optional<ui64>> expectedRightKey;
            TVector<std::optional<TString>> expectedRightValue;
            for (size_t i = 0; i < leftKeyInit.size(); i++) {
                if (!leftKeyInit[i]) {
                    continue;
                }
                const auto& found = rightMap.find(*leftKeyInit[i]);
                if (found != rightMap.cend()) {
                    expectedLeftKey.push_back(leftKeyInit[i]);
                    expectedSubkey.push_back(leftSubkeyInit[i]);
                    expectedValue.push_back(leftValueInit[i]);
                    expectedRightKey.push_back(found->first);
                    expectedRightValue.push_back(found->second);
                }
            }

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                leftKeyInit, leftSubkeyInit, leftValueInit);
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                rightKeyInit, rightValueInit);
            auto [expectedType, expected] = ConvertVectorsToTuples(setup,
                expectedLeftKey, expectedSubkey, expectedValue, expectedRightKey, expectedRightValue);

            RunTestBlockJoin(
                setup, policy, EJoinKind::Inner,
                expectedType, expected,
                leftType, std::move(leftList), {0},
                rightType, std::move(rightList), {0}
            );
        }
    }

    Y_UNIT_TEST(TestKeyTuple) {
        for (auto policy: {gpDefaultPolicy, gpTAlwaysHashJoinPolicy, gpTAlwaysInMemGraceJoinPolicy}) {
            TSetup<false> setup(GetNodeFactory());

            // 1. Make input for the "left" stream.
            TVector<std::optional<ui64>> leftKey1Init(testSize);
            std::iota(leftKey1Init.begin(), leftKey1Init.end(), 1);
            TVector<std::optional<ui64>> leftKey2Init(testSize);
            std::iota(leftKey2Init.begin(), leftKey2Init.end(), 1);
            TVector<TString> leftValueInit;
            std::transform(leftKey1Init.cbegin(), leftKey1Init.cend(), std::back_inserter(leftValueInit),
                [](const auto key) { return threeLetterValues[*key]; });

            // 2. Make input for the "right" stream.
            TVector<std::optional<ui64>> rightKey1Init(fibonacci.cbegin(), fibonacci.cend());
            TVector<std::optional<ui64>> rightKey2Init(fibonacci.cbegin(), fibonacci.cend());
            TVector<TString> rightValueInit;
            std::transform(rightKey1Init.cbegin(), rightKey1Init.cend(), std::back_inserter(rightValueInit),
                [](const auto key) { return std::to_string(*key); });

            // 3. Add some NULLs
            leftKey1Init[0] = leftKey1Init[1] = std::nullopt;
            leftKey2Init[1] = std::nullopt;
            rightKey1Init[1] = rightKey1Init[2] = std::nullopt;
            rightKey2Init[2] = std::nullopt;

            // 4. Make "expected" data.
            TMap<std::tuple<ui64, ui64>, TString> rightMap;
            for (size_t i = 0; i < rightKey1Init.size(); i++) {
                if (rightKey1Init[i].has_value() && rightKey2Init[i].has_value()) {
                    const auto key = std::make_tuple(*rightKey1Init[i], *rightKey2Init[i]);
                    rightMap[key] = rightValueInit[i];
                }
            }
            TVector<std::optional<ui64>> expectedLeftKey1;
            TVector<std::optional<ui64>> expectedLeftKey2;
            TVector<TString> expectedValue;
            TVector<std::optional<ui64>> expectedRightKey1;
            TVector<std::optional<ui64>> expectedRightKey2;
            TVector<TString> expectedRightValue;
            for (size_t i = 0; i < leftKey1Init.size(); i++) {
                if (!leftKey1Init[i] || !leftKey2Init[i]) {
                    continue;
                }
                const auto key = std::make_tuple(*leftKey1Init[i], *leftKey2Init[i]);
                const auto& found = rightMap.find(key);
                if (found != rightMap.cend()) {
                    expectedLeftKey1.push_back(leftKey1Init[i]);
                    expectedLeftKey2.push_back(leftKey2Init[i]);
                    expectedValue.push_back(leftValueInit[i]);
                    expectedRightKey1.push_back(std::get<0>(found->first));
                    expectedRightKey2.push_back(std::get<1>(found->first));
                    expectedRightValue.push_back(found->second);
                }
            }

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                leftKey1Init, leftKey2Init, leftValueInit);
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                rightKey1Init, rightKey2Init, rightValueInit);
            auto [expectedType, expected] = ConvertVectorsToTuples(setup,
                expectedLeftKey1, expectedLeftKey2, expectedValue, expectedRightKey1, expectedRightKey2, expectedRightValue);

            RunTestBlockJoin(
                setup, policy, EJoinKind::Inner,
                expectedType, expected,
                leftType, std::move(leftList), {0, 1},
                rightType, std::move(rightList), {0, 1}
            );
        }
    }

} // Y_UNIT_TEST_SUITE

} // namespace NMiniKQL
} // namespace NKikimr
