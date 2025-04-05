#include "mkql_block_map_join_ut_utils.h"
#include "mkql_computation_node_ut.h"

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_block_grace_join_policy.h>
#include <yql/essentials/minikql/computation/mkql_resource_meter.h>

#include <tuple>
#include <random>

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
#define DEFINE_TEST_POLICY(name, algo)                              \
struct name : public TDefaultBlockGraceJoinPolicy {                 \
    name() = default;                                               \
                                                                    \
    EJoinAlgo PickAlgorithm(size_t, size_t) const override {        \
        return algo;                                                \
    }                                                               \
};                                                                  \
                                                                    \
[[maybe_unused]] name g ## name{};                                  \
[[maybe_unused]] IBlockGraceJoinPolicy* gp ## name{&g ## name}

DEFINE_TEST_POLICY(TAlwaysHashJoinPolicy,       EJoinAlgo::HashJoin);
DEFINE_TEST_POLICY(TAlwaysInMemGraceJoinPolicy, EJoinAlgo::InMemoryGraceJoin);

// -------------------------------------------------------------------
enum class JoinType {
    BlockMapJoin,
    BlockGraceJoin_HashJoin,
    BlockGraceJoin_InMemoryGraceJoin,
    GraceJoin
};

TString GenerateRandomString(ui64 len) {
    std::mt19937 rng(std::random_device{}());
    TString str(len, 'a');
    for (auto& c: str) {
        c = 'a' + rng() % 26;
    }
    return str;
}

// -------------------------------------------------------------------
// List<Tuple<...>> -> Flow<Multi<...>>
TRuntimeNode ToWideFlow(TProgramBuilder& pgmBuilder, TRuntimeNode list) {
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

    return wideFlow;
}

// Flow<Multi<...>> -> List<Tuple<...>>
TRuntimeNode FromWideFlow(TProgramBuilder& pgmBuilder, TRuntimeNode flow) {
    return pgmBuilder.Collect(pgmBuilder.NarrowMap(flow,
        [&](TRuntimeNode::TList items) -> TRuntimeNode {
            return pgmBuilder.NewTuple(items);
        })
    );
}

// List<Tuple<...>> -> Stream<Multi<...>>
TRuntimeNode ToWideStream(TProgramBuilder& pgmBuilder, TRuntimeNode list) {
    auto wideFlow = ToWideFlow(pgmBuilder, list);
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

// -------------------------------------------------------------------
TRuntimeNode BuildBlockJoin(TProgramBuilder& pgmBuilder, JoinType blockJoinKind,
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
    switch (blockJoinKind) {
    case JoinType::BlockMapJoin: {
        auto joinNode = pgmBuilder.BlockMapJoinCore(
            leftStream,
            rightStream,
            EJoinKind::Inner,
            leftKeyColumns,
            leftKeyDrops,
            rightKeyColumns,
            rightKeyDrops,
            false,
            joinReturnType
        );
        return FromWideStream(pgmBuilder, DethrottleStream(pgmBuilder, joinNode));
    }
    case JoinType::BlockGraceJoin_HashJoin: {
        // Set large maximum initially fetched data size to run benches with large build stream
        gpTAlwaysHashJoinPolicy->SetMaximumInitiallyFetchedData(200 * MB);
        auto joinNode = pgmBuilder.BlockGraceJoinCore(
            leftStream,
            rightStream,
            EJoinKind::Inner,
            leftKeyColumns,
            leftKeyDrops,
            rightKeyColumns,
            rightKeyDrops,
            false,
            joinReturnType,
            static_cast<const void*>(gpTAlwaysHashJoinPolicy)
        );
        return FromWideStream(pgmBuilder, DethrottleStream(pgmBuilder, joinNode));
    }
    case JoinType::BlockGraceJoin_InMemoryGraceJoin: {
        // Set large maximum initially fetched data size to run InMemoryGraceJoin
        gpTAlwaysInMemGraceJoinPolicy->SetMaximumInitiallyFetchedData(200 * MB);
        auto joinNode = pgmBuilder.BlockGraceJoinCore(
            leftStream,
            rightStream,
            EJoinKind::Inner,
            leftKeyColumns,
            leftKeyDrops,
            rightKeyColumns,
            rightKeyDrops,
            false,
            joinReturnType,
            static_cast<const void*>(gpTAlwaysInMemGraceJoinPolicy)
        );
        return FromWideStream(pgmBuilder, DethrottleStream(pgmBuilder, joinNode));
    }
    default:
        Y_UNREACHABLE();
        UNIT_ASSERT(false);
    }
}

NUdf::TUnboxedValue DoBenchBlockJoin(
    TSetup<false>& setup, JoinType blockJoinKind, ui64 maxItemSize,
    TType* leftType, NUdf::TUnboxedValue&& leftListValue, const TVector<ui32>& leftKeyColumns, const TVector<ui32>& leftKeyDrops,
    TType* rightType, NUdf::TUnboxedValue&& rightListValue, const TVector<ui32>& rightKeyColumns, const TVector<ui32>& rightKeyDrops)
{
    using std::min;
    ui64 blockSize = min(rightListValue.GetListLength(), MaxBlockSizeInBytes / maxItemSize);
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
        pb, blockJoinKind, leftList, leftKeyColumns, leftKeyDrops, rightList, rightKeyColumns, rightKeyDrops);

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

void RunBenchBlockJoin(
    TSetup<false>& setup, JoinType blockJoinKind, ui64 maxItemSize,
    TType* leftType, NUdf::TUnboxedValue&& leftListValue, const TVector<ui32>& leftKeyColumns,
    TType* rightType, NUdf::TUnboxedValue&& rightListValue, const TVector<ui32>& rightKeyColumns,
    const TVector<ui32>& leftKeyDrops = {}, const TVector<ui32>& rightKeyDrops = {})
{
    const auto got = DoBenchBlockJoin(
        setup, blockJoinKind, maxItemSize,
        leftType, std::move(leftListValue), leftKeyColumns, leftKeyDrops,
        rightType, std::move(rightListValue), rightKeyColumns, rightKeyDrops);
    auto gotItems = ConvertListToVector(got);
    UNIT_ASSERT(gotItems.size() > 0);
}

// -------------------------------------------------------------------
TRuntimeNode BuildGraceJoin(TProgramBuilder& pgmBuilder,
    TRuntimeNode leftList, const TVector<ui32>& leftKeyColumns, const TVector<ui32>& leftKeyDrops,
    TRuntimeNode rightList, const TVector<ui32>& rightKeyColumns, const TVector<ui32>& rightKeyDrops
) {
    const auto leftFlow = ToWideFlow(pgmBuilder, leftList);
    const auto rightFlow = ToWideFlow(pgmBuilder, rightList);

    const auto leftFlowItems = GetWideComponents(AS_TYPE(TFlowType, leftFlow));
    const auto rightFlowItems = GetWideComponents(AS_TYPE(TFlowType, rightFlow));

    TVector<TType*> joinReturnItems;

    ui32 counter = 0;
    TVector<ui32> leftRenames;
    const THashSet<ui32> leftKeyDropsSet(leftKeyDrops.cbegin(), leftKeyDrops.cend());
    for (size_t i = 0; i < leftFlowItems.size(); i++) {
        if (leftKeyDropsSet.contains(i)) {
            continue;
        }
        leftRenames.push_back(i);
        leftRenames.push_back(counter);
        counter++;
        joinReturnItems.push_back(pgmBuilder.NewFlowType(leftFlowItems[i]));
    }

    TVector<ui32> rightRenames;
    const THashSet<ui32> rightKeyDropsSet(rightKeyDrops.cbegin(), rightKeyDrops.cend());
    for (size_t i = 0; i < rightFlowItems.size(); i++) {
        if (rightKeyDropsSet.contains(i)) {
            continue;
        }
        rightRenames.push_back(i);
        rightRenames.push_back(counter);
        counter++;
        joinReturnItems.push_back(pgmBuilder.NewFlowType(rightFlowItems[i]));
    }

    TType* joinReturnType = pgmBuilder.NewFlowType(pgmBuilder.NewMultiType(joinReturnItems));
    auto joinNode = pgmBuilder.GraceJoin(
        leftFlow,
        rightFlow,
        EJoinKind::Inner,
        leftKeyColumns,
        rightKeyColumns,
        leftRenames,
        rightRenames,
        joinReturnType
    );
    return FromWideFlow(pgmBuilder, joinNode);
}

NUdf::TUnboxedValue DoBenchGraceJoin(
    TSetup<false>& setup,
    TType* leftType, NUdf::TUnboxedValue&& leftListValue, const TVector<ui32>& leftKeyColumns, const TVector<ui32>& leftKeyDrops,
    TType* rightType, NUdf::TUnboxedValue&& rightListValue, const TVector<ui32>& rightKeyColumns, const TVector<ui32>& rightKeyDrops)
{
    TProgramBuilder& pb = *setup.PgmBuilder;

    Y_ENSURE(leftType->IsList(), "Left node has to be list");
    const auto leftItemType = AS_TYPE(TListType, leftType)->GetItemType();
    Y_ENSURE(leftItemType->IsTuple(), "List item has to be tuple");

    Y_ENSURE(rightType->IsList(), "Right node has to be list");
    const auto rightItemType = AS_TYPE(TListType, rightType)->GetItemType();
    Y_ENSURE(rightItemType->IsTuple(), "Right item has to be tuple");

    TRuntimeNode leftList = pb.Arg(pb.NewListType(leftItemType));
    TRuntimeNode rightList = pb.Arg(pb.NewListType(rightItemType));
    const auto joinNode = BuildGraceJoin(
        pb, leftList, leftKeyColumns, leftKeyDrops, rightList, rightKeyColumns, rightKeyDrops);

    const auto joinType = joinNode.GetStaticType();
    Y_ENSURE(joinType->IsList(), "Join result has to be list");
    const auto joinItemType = AS_TYPE(TListType, joinType)->GetItemType();
    Y_ENSURE(joinItemType->IsTuple(), "List item has to be tuple");

    const auto graph = setup.BuildGraph(joinNode, {leftList.GetNode(), rightList.GetNode()});
    auto& ctx = graph->GetContext();

    graph->GetEntryPoint(0, true)->SetValue(ctx, std::move(leftListValue));
    graph->GetEntryPoint(1, true)->SetValue(ctx, std::move(rightListValue));
    return graph->GetValue();
}

void RunBenchGraceJoin(
    TSetup<false>& setup,
    TType* leftType, NUdf::TUnboxedValue&& leftListValue, const TVector<ui32>& leftKeyColumns,
    TType* rightType, NUdf::TUnboxedValue&& rightListValue, const TVector<ui32>& rightKeyColumns,
    const TVector<ui32>& leftKeyDrops = {}, const TVector<ui32>& rightKeyDrops = {})
{
    const auto got = DoBenchGraceJoin(
        setup,
        leftType, std::move(leftListValue), leftKeyColumns, leftKeyDrops,
        rightType, std::move(rightListValue), rightKeyColumns, rightKeyDrops);
    auto gotItems = ConvertListToVector(got);
    UNIT_ASSERT(gotItems.size() > 0);
}


// -------------------------------------------------------------------
/**
 * https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf
 * 
 * Typical characteristics of data from the TPC-H dataset:
 *  - Number of columns: 3 - 16
 *  - Number of rows: 5, 25, 10_000 * SF - 6_000_000 * SF, where SF -- scale factor
 *  - Types of columns: integer (i64), decimal, date, fixed / variable size text (length: 10 - 199, mostly, length <= 32)
 * 
 * Typical characteristics of Join operations from the TPC-H queries:
 *  - Ratio of the sizes of the left and right streams: 1/4, 1/10, 1/15, 1/40, 1/80, 1/400
 *  - Join selectivity: 1-2%, 3-5%, 5-10%, <= 20% in most cases
 */

/**
 * Query #1:
 *  - Selectivity: 5%       <-- Main query characteristic
 *  - Cols 1 and 2 are keys
 *  - Left row max size: 80
 *  - Right row max size: 56
 *  - Left size: 600_000, right size: 60_000
 */
std::pair<
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>, TVector<TString>>,
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>>
>
Query1GenerateData(ui64& datasetSize) {
    std::mt19937 rng(std::random_device{}());
    const ui64 leftRowsCount = 600'000;
    const ui64 rightRowsCount = 60'000;

    datasetSize = leftRowsCount * 32 + rightRowsCount * 24;

    TVector<ui64> leftCol1(leftRowsCount);
    std::iota(leftCol1.begin(), leftCol1.end(), 0);
    TVector<ui64> leftCol2(leftRowsCount);
    std::iota(leftCol2.begin(), leftCol2.end(), 0);
    TVector<ui64> leftCol3(leftRowsCount);
    std::iota(leftCol3.begin(), leftCol3.end(), 0);
    TVector<ui64> leftCol4(leftRowsCount);
    std::iota(leftCol4.begin(), leftCol4.end(), 0);
    TVector<TString> leftCol5(leftRowsCount);
    for (auto& s: leftCol5) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    TVector<TString> leftCol6(leftRowsCount);
    for (auto& s: leftCol6) {
        auto sz = 6 + rng() % 10;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    for (size_t i = 0; i < leftRowsCount / 5; i++) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % leftRowsCount;
        std::swap(leftCol1[lhs], leftCol1[rhs]);
    }

    TVector<ui64> rightCol1(rightRowsCount);
    std::iota(rightCol1.begin(), rightCol1.end(), leftRowsCount);
    TVector<ui64> rightCol2(rightRowsCount);
    std::iota(rightCol2.begin(), rightCol2.end(), leftRowsCount);
    TVector<ui64> rightCol3(rightRowsCount);
    std::iota(rightCol3.begin(), rightCol3.end(), leftRowsCount);
    TVector<TString> rightCol4(rightRowsCount);
    for (auto& s: rightCol4) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    for (size_t i = 0; i < rightRowsCount / 5; i++) {
        size_t lhs = rng() % rightRowsCount;
        size_t rhs = rng() % rightRowsCount;
        std::swap(rightCol1[lhs], rightCol1[rhs]);
    }

    // Make 5% to match
    for (size_t i = 0; i < leftRowsCount / 20; ++i) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % rightRowsCount;
        leftCol1[lhs] = rightCol1[rhs];
        leftCol2[lhs] = rightCol2[rhs];
    }

    return {
        std::make_tuple(std::move(leftCol1), std::move(leftCol2), std::move(leftCol3), std::move(leftCol4), std::move(leftCol5), std::move(leftCol6)),
        std::make_tuple(std::move(rightCol1), std::move(rightCol2), std::move(rightCol3), std::move(rightCol4))
    };
}

/**
 * Query #2:
 *  - Selectivity: 20%       <-- Main query characteristic
 *  - Cols 1 and 2 are keys
 *  - Left row max size: 80
 *  - Right row max size: 56
 *  - Left size: 600_000, right size: 60_000
 */
std::pair<
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>, TVector<TString>>,
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>>
>
Query2GenerateData(ui64& datasetSize) {
    std::mt19937 rng(std::random_device{}());
    const ui64 leftRowsCount = 600'000;
    const ui64 rightRowsCount = 60'000;

    datasetSize = leftRowsCount * 32 + rightRowsCount * 24;

    TVector<ui64> leftCol1(leftRowsCount);
    std::iota(leftCol1.begin(), leftCol1.end(), 0);
    TVector<ui64> leftCol2(leftRowsCount);
    std::iota(leftCol2.begin(), leftCol2.end(), 0);
    TVector<ui64> leftCol3(leftRowsCount);
    std::iota(leftCol3.begin(), leftCol3.end(), 0);
    TVector<ui64> leftCol4(leftRowsCount);
    std::iota(leftCol4.begin(), leftCol4.end(), 0);
    TVector<TString> leftCol5(leftRowsCount);
    for (auto& s: leftCol5) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    TVector<TString> leftCol6(leftRowsCount);
    for (auto& s: leftCol6) {
        auto sz = 6 + rng() % 10;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    for (size_t i = 0; i < leftRowsCount / 5; i++) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % leftRowsCount;
        std::swap(leftCol1[lhs], leftCol1[rhs]);
    }

    TVector<ui64> rightCol1(rightRowsCount);
    std::iota(rightCol1.begin(), rightCol1.end(), leftRowsCount);
    TVector<ui64> rightCol2(rightRowsCount);
    std::iota(rightCol2.begin(), rightCol2.end(), leftRowsCount);
    TVector<ui64> rightCol3(rightRowsCount);
    std::iota(rightCol3.begin(), rightCol3.end(), leftRowsCount);
    TVector<TString> rightCol4(rightRowsCount);
    for (auto& s: rightCol4) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    for (size_t i = 0; i < rightRowsCount / 5; i++) {
        size_t lhs = rng() % rightRowsCount;
        size_t rhs = rng() % rightRowsCount;
        std::swap(rightCol1[lhs], rightCol1[rhs]);
    }

    // Make 20% to match
    for (size_t i = 0; i < leftRowsCount / 5; ++i) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % rightRowsCount;
        leftCol1[lhs] = rightCol1[rhs];
        leftCol2[lhs] = rightCol2[rhs];
    }

    return {
        std::make_tuple(std::move(leftCol1), std::move(leftCol2), std::move(leftCol3), std::move(leftCol4), std::move(leftCol5), std::move(leftCol6)),
        std::make_tuple(std::move(rightCol1), std::move(rightCol2), std::move(rightCol3), std::move(rightCol4))
    };
}

/**
 * Query #3:
 *  - Selectivity: 50%       <-- Main query characteristic
 *  - Cols 1 and 2 are keys
 *  - Left row max size: 80
 *  - Right row max size: 56
 *  - Left size: 600_000, right size: 60_000
 */
std::pair<
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>, TVector<TString>>,
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>>
>
Query3GenerateData(ui64& datasetSize) {
    std::mt19937 rng(std::random_device{}());
    const ui64 leftRowsCount = 600'000;
    const ui64 rightRowsCount = 60'000;

    datasetSize = leftRowsCount * 32 + rightRowsCount * 24;

    TVector<ui64> leftCol1(leftRowsCount);
    std::iota(leftCol1.begin(), leftCol1.end(), 0);
    TVector<ui64> leftCol2(leftRowsCount);
    std::iota(leftCol2.begin(), leftCol2.end(), 0);
    TVector<ui64> leftCol3(leftRowsCount);
    std::iota(leftCol3.begin(), leftCol3.end(), 0);
    TVector<ui64> leftCol4(leftRowsCount);
    std::iota(leftCol4.begin(), leftCol4.end(), 0);
    TVector<TString> leftCol5(leftRowsCount);
    for (auto& s: leftCol5) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    TVector<TString> leftCol6(leftRowsCount);
    for (auto& s: leftCol6) {
        auto sz = 6 + rng() % 10;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    for (size_t i = 0; i < leftRowsCount / 5; i++) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % leftRowsCount;
        std::swap(leftCol1[lhs], leftCol1[rhs]);
    }

    TVector<ui64> rightCol1(rightRowsCount);
    std::iota(rightCol1.begin(), rightCol1.end(), leftRowsCount);
    TVector<ui64> rightCol2(rightRowsCount);
    std::iota(rightCol2.begin(), rightCol2.end(), leftRowsCount);
    TVector<ui64> rightCol3(rightRowsCount);
    std::iota(rightCol3.begin(), rightCol3.end(), leftRowsCount);
    TVector<TString> rightCol4(rightRowsCount);
    for (auto& s: rightCol4) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    for (size_t i = 0; i < rightRowsCount / 5; i++) {
        size_t lhs = rng() % rightRowsCount;
        size_t rhs = rng() % rightRowsCount;
        std::swap(rightCol1[lhs], rightCol1[rhs]);
    }

    // Make 50% to match
    for (size_t i = 0; i < leftRowsCount / 2; ++i) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % rightRowsCount;
        leftCol1[lhs] = rightCol1[rhs];
        leftCol2[lhs] = rightCol2[rhs];
    }

    return {
        std::make_tuple(std::move(leftCol1), std::move(leftCol2), std::move(leftCol3), std::move(leftCol4), std::move(leftCol5), std::move(leftCol6)),
        std::make_tuple(std::move(rightCol1), std::move(rightCol2), std::move(rightCol3), std::move(rightCol4))
    };
}

/**
 * Query #4:
 *  - Selectivity: 70%       <-- Main query characteristic
 *  - Cols 1 and 2 are keys
 *  - Left row max size: 80
 *  - Right row max size: 56
 *  - Left size: 600_000, right size: 60_000
 */
std::pair<
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>, TVector<TString>>,
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>>
>
Query4GenerateData(ui64& datasetSize) {
    std::mt19937 rng(std::random_device{}());
    const ui64 leftRowsCount = 600'000;
    const ui64 rightRowsCount = 60'000;

    datasetSize = leftRowsCount * 32 + rightRowsCount * 24;

    TVector<ui64> leftCol1(leftRowsCount);
    std::iota(leftCol1.begin(), leftCol1.end(), 0);
    TVector<ui64> leftCol2(leftRowsCount);
    std::iota(leftCol2.begin(), leftCol2.end(), 0);
    TVector<ui64> leftCol3(leftRowsCount);
    std::iota(leftCol3.begin(), leftCol3.end(), 0);
    TVector<ui64> leftCol4(leftRowsCount);
    std::iota(leftCol4.begin(), leftCol4.end(), 0);
    TVector<TString> leftCol5(leftRowsCount);
    for (auto& s: leftCol5) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    TVector<TString> leftCol6(leftRowsCount);
    for (auto& s: leftCol6) {
        auto sz = 6 + rng() % 10;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    for (size_t i = 0; i < leftRowsCount / 5; i++) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % leftRowsCount;
        std::swap(leftCol1[lhs], leftCol1[rhs]);
    }

    TVector<ui64> rightCol1(rightRowsCount);
    std::iota(rightCol1.begin(), rightCol1.end(), leftRowsCount);
    TVector<ui64> rightCol2(rightRowsCount);
    std::iota(rightCol2.begin(), rightCol2.end(), leftRowsCount);
    TVector<ui64> rightCol3(rightRowsCount);
    std::iota(rightCol3.begin(), rightCol3.end(), leftRowsCount);
    TVector<TString> rightCol4(rightRowsCount);
    for (auto& s: rightCol4) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    for (size_t i = 0; i < rightRowsCount / 5; i++) {
        size_t lhs = rng() % rightRowsCount;
        size_t rhs = rng() % rightRowsCount;
        std::swap(rightCol1[lhs], rightCol1[rhs]);
    }

    // Make 70% to match
    for (size_t i = 0; i < leftRowsCount * 7 / 10; ++i) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % rightRowsCount;
        leftCol1[lhs] = rightCol1[rhs];
        leftCol2[lhs] = rightCol2[rhs];
    }

    return {
        std::make_tuple(std::move(leftCol1), std::move(leftCol2), std::move(leftCol3), std::move(leftCol4), std::move(leftCol5), std::move(leftCol6)),
        std::make_tuple(std::move(rightCol1), std::move(rightCol2), std::move(rightCol3), std::move(rightCol4))
    };
}

/**
 * Query #5:
 *  - Selectivity: 100%       <-- Main query characteristic, modeling foreign key join
 *  - Col 1 is key
 *  - Left row max size: 80
 *  - Right row max size: 56
 *  - Left size: 600_000, right size: 60_000
 */
std::pair<
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>, TVector<TString>>,
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>>
>
Query5GenerateData(ui64& datasetSize) {
    std::mt19937 rng(std::random_device{}());
    const ui64 leftRowsCount = 600'000;
    const ui64 rightRowsCount = 60'000;

    datasetSize = leftRowsCount * 32 + rightRowsCount * 24;

    TVector<ui64> leftCol1(leftRowsCount);
    std::iota(leftCol1.begin(), leftCol1.end(), 0);
    TVector<ui64> leftCol2(leftRowsCount);
    std::iota(leftCol2.begin(), leftCol2.end(), 0);
    TVector<ui64> leftCol3(leftRowsCount);
    std::iota(leftCol3.begin(), leftCol3.end(), 0);
    TVector<ui64> leftCol4(leftRowsCount);
    std::iota(leftCol4.begin(), leftCol4.end(), 0);
    TVector<TString> leftCol5(leftRowsCount);
    for (auto& s: leftCol5) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    TVector<TString> leftCol6(leftRowsCount);
    for (auto& s: leftCol6) {
        auto sz = 6 + rng() % 10;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }

    TVector<ui64> rightCol1(rightRowsCount);
    std::iota(rightCol1.begin(), rightCol1.end(), 0);
    TVector<ui64> rightCol2(rightRowsCount);
    std::iota(rightCol2.begin(), rightCol2.end(), leftRowsCount);
    TVector<ui64> rightCol3(rightRowsCount);
    std::iota(rightCol3.begin(), rightCol3.end(), leftRowsCount);
    TVector<TString> rightCol4(rightRowsCount);
    for (auto& s: rightCol4) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }

    // Make 100% to match
    for (size_t i = 0; i < leftRowsCount; ++i) {
        size_t rhs = rng() % rightRowsCount;
        leftCol1[i] = rightCol1[rhs];
    }

    return {
        std::make_tuple(std::move(leftCol1), std::move(leftCol2), std::move(leftCol3), std::move(leftCol4), std::move(leftCol5), std::move(leftCol6)),
        std::make_tuple(std::move(rightCol1), std::move(rightCol2), std::move(rightCol3), std::move(rightCol4))
    };
}

/**
 * Query #6:
 *  - Selectivity: 10%               <-- Main query characteristic
 *  - Cols 1 and 2 are keys
 *  - Left row max size: 152         <-- Main query characteristic, wide probe tuples
 *  - Right row max size: 56
 *  - Left size: 600_000, right size: 60_000
 */
std::pair<
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>, TVector<TString>>,
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>>
>
Query6GenerateData(ui64& datasetSize) {
    std::mt19937 rng(std::random_device{}());
    const ui64 leftRowsCount = 600'000;
    const ui64 rightRowsCount = 60'000;

    datasetSize = leftRowsCount * 104 + rightRowsCount * 24;

    TVector<ui64> leftCol1(leftRowsCount);
    std::iota(leftCol1.begin(), leftCol1.end(), 0);
    TVector<ui64> leftCol2(leftRowsCount);
    std::iota(leftCol2.begin(), leftCol2.end(), 0);
    TVector<ui64> leftCol3(leftRowsCount);
    std::iota(leftCol3.begin(), leftCol3.end(), 0);
    TVector<ui64> leftCol4(leftRowsCount);
    std::iota(leftCol4.begin(), leftCol4.end(), 0);
    TVector<ui64> leftCol5(leftRowsCount);
    std::iota(leftCol5.begin(), leftCol5.end(), 0);
    TVector<ui64> leftCol6(leftRowsCount);
    std::iota(leftCol6.begin(), leftCol6.end(), 0);
    TVector<ui64> leftCol7(leftRowsCount);
    std::iota(leftCol7.begin(), leftCol7.end(), 0);
    TVector<ui64> leftCol8(leftRowsCount);
    std::iota(leftCol8.begin(), leftCol8.end(), 0);
    TVector<ui64> leftCol9(leftRowsCount);
    std::iota(leftCol9.begin(), leftCol9.end(), 0);
    TVector<ui64> leftCol10(leftRowsCount);
    std::iota(leftCol10.begin(), leftCol10.end(), 0);
    TVector<ui64> leftCol11(leftRowsCount);
    std::iota(leftCol11.begin(), leftCol11.end(), 0);
    TVector<ui64> leftCol12(leftRowsCount);
    std::iota(leftCol12.begin(), leftCol12.end(), 0);
    TVector<ui64> leftCol13(leftRowsCount);
    std::iota(leftCol13.begin(), leftCol13.end(), 0);
    TVector<TString> leftCol14(leftRowsCount);
    for (auto& s: leftCol14) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    TVector<TString> leftCol15(leftRowsCount);
    for (auto& s: leftCol15) {
        auto sz = 6 + rng() % 10;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    for (size_t i = 0; i < leftRowsCount / 5; i++) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % leftRowsCount;
        std::swap(leftCol1[lhs], leftCol1[rhs]);
    }

    TVector<ui64> rightCol1(rightRowsCount);
    std::iota(rightCol1.begin(), rightCol1.end(), leftRowsCount);
    TVector<ui64> rightCol2(rightRowsCount);
    std::iota(rightCol2.begin(), rightCol2.end(), leftRowsCount);
    TVector<ui64> rightCol3(rightRowsCount);
    std::iota(rightCol3.begin(), rightCol3.end(), leftRowsCount);
    TVector<TString> rightCol4(rightRowsCount);
    for (auto& s: rightCol4) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    for (size_t i = 0; i < rightRowsCount / 5; i++) {
        size_t lhs = rng() % rightRowsCount;
        size_t rhs = rng() % rightRowsCount;
        std::swap(rightCol1[lhs], rightCol1[rhs]);
    }

    // Make 10% to match
    for (size_t i = 0; i < leftRowsCount / 10; ++i) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % rightRowsCount;
        leftCol1[lhs] = rightCol1[rhs];
        leftCol2[lhs] = rightCol2[rhs];
    }

    return {
        std::make_tuple(std::move(leftCol1), std::move(leftCol2), std::move(leftCol3), std::move(leftCol4), std::move(leftCol5), std::move(leftCol6), std::move(leftCol7), std::move(leftCol8), std::move(leftCol9), std::move(leftCol10), std::move(leftCol11), std::move(leftCol12), std::move(leftCol13), std::move(leftCol14), std::move(leftCol15)),
        std::make_tuple(std::move(rightCol1), std::move(rightCol2), std::move(rightCol3), std::move(rightCol4))
    };
}

/**
 * Query #7:
 *  - Selectivity: 50%               <-- Main query characteristic
 *  - Cols 1 and 2 are keys
 *  - Left row max size: 152         <-- Main query characteristic, wide probe tuples
 *  - Right row max size: 56
 *  - Left size: 600_000, right size: 60_000
 */
std::pair<
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>, TVector<TString>>,
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>>
>
Query7GenerateData(ui64& datasetSize) {
    std::mt19937 rng(std::random_device{}());
    const ui64 leftRowsCount = 600'000;
    const ui64 rightRowsCount = 60'000;

    datasetSize = leftRowsCount * 104 + rightRowsCount * 24;

    TVector<ui64> leftCol1(leftRowsCount);
    std::iota(leftCol1.begin(), leftCol1.end(), 0);
    TVector<ui64> leftCol2(leftRowsCount);
    std::iota(leftCol2.begin(), leftCol2.end(), 0);
    TVector<ui64> leftCol3(leftRowsCount);
    std::iota(leftCol3.begin(), leftCol3.end(), 0);
    TVector<ui64> leftCol4(leftRowsCount);
    std::iota(leftCol4.begin(), leftCol4.end(), 0);
    TVector<ui64> leftCol5(leftRowsCount);
    std::iota(leftCol5.begin(), leftCol5.end(), 0);
    TVector<ui64> leftCol6(leftRowsCount);
    std::iota(leftCol6.begin(), leftCol6.end(), 0);
    TVector<ui64> leftCol7(leftRowsCount);
    std::iota(leftCol7.begin(), leftCol7.end(), 0);
    TVector<ui64> leftCol8(leftRowsCount);
    std::iota(leftCol8.begin(), leftCol8.end(), 0);
    TVector<ui64> leftCol9(leftRowsCount);
    std::iota(leftCol9.begin(), leftCol9.end(), 0);
    TVector<ui64> leftCol10(leftRowsCount);
    std::iota(leftCol10.begin(), leftCol10.end(), 0);
    TVector<ui64> leftCol11(leftRowsCount);
    std::iota(leftCol11.begin(), leftCol11.end(), 0);
    TVector<ui64> leftCol12(leftRowsCount);
    std::iota(leftCol12.begin(), leftCol12.end(), 0);
    TVector<ui64> leftCol13(leftRowsCount);
    std::iota(leftCol13.begin(), leftCol13.end(), 0);
    TVector<TString> leftCol14(leftRowsCount);
    for (auto& s: leftCol14) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    TVector<TString> leftCol15(leftRowsCount);
    for (auto& s: leftCol15) {
        auto sz = 6 + rng() % 10;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    for (size_t i = 0; i < leftRowsCount / 5; i++) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % leftRowsCount;
        std::swap(leftCol1[lhs], leftCol1[rhs]);
    }

    TVector<ui64> rightCol1(rightRowsCount);
    std::iota(rightCol1.begin(), rightCol1.end(), leftRowsCount);
    TVector<ui64> rightCol2(rightRowsCount);
    std::iota(rightCol2.begin(), rightCol2.end(), leftRowsCount);
    TVector<ui64> rightCol3(rightRowsCount);
    std::iota(rightCol3.begin(), rightCol3.end(), leftRowsCount);
    TVector<TString> rightCol4(rightRowsCount);
    for (auto& s: rightCol4) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    for (size_t i = 0; i < rightRowsCount / 5; i++) {
        size_t lhs = rng() % rightRowsCount;
        size_t rhs = rng() % rightRowsCount;
        std::swap(rightCol1[lhs], rightCol1[rhs]);
    }

    // Make 50% to match
    for (size_t i = 0; i < leftRowsCount / 2; ++i) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % rightRowsCount;
        leftCol1[lhs] = rightCol1[rhs];
        leftCol2[lhs] = rightCol2[rhs];
    }

    return {
        std::make_tuple(std::move(leftCol1), std::move(leftCol2), std::move(leftCol3), std::move(leftCol4), std::move(leftCol5), std::move(leftCol6), std::move(leftCol7), std::move(leftCol8), std::move(leftCol9), std::move(leftCol10), std::move(leftCol11), std::move(leftCol12), std::move(leftCol13), std::move(leftCol14), std::move(leftCol15)),
        std::make_tuple(std::move(rightCol1), std::move(rightCol2), std::move(rightCol3), std::move(rightCol4))
    };
}

/**
 * Query #8:
 *  - Selectivity: 10%
 *  - Cols 1 and 2 are keys
 *  - Left row max size: 56
 *  - Right row max size: 152         <-- Main query characteristic, wide build tuples
 *  - Left size: 400_000, right size: 20_000
 */
std::pair<
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>>,
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>, TVector<TString>>
>
Query8GenerateData(ui64& datasetSize) {
    std::mt19937 rng(std::random_device{}());
    const ui64 leftRowsCount = 400'000;
    const ui64 rightRowsCount = 20'000;

    datasetSize = leftRowsCount * 24 + rightRowsCount * 104;

    TVector<ui64> leftCol1(leftRowsCount);
    std::iota(leftCol1.begin(), leftCol1.end(), leftRowsCount);
    TVector<ui64> leftCol2(leftRowsCount);
    std::iota(leftCol2.begin(), leftCol2.end(), leftRowsCount);
    TVector<ui64> leftCol3(leftRowsCount);
    std::iota(leftCol3.begin(), leftCol3.end(), leftRowsCount);
    TVector<TString> leftCol4(leftRowsCount);
    for (auto& s: leftCol4) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    for (size_t i = 0; i < leftRowsCount / 5; i++) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % leftRowsCount;
        std::swap(leftCol1[lhs], leftCol1[rhs]);
    }

    TVector<ui64> rightCol1(rightRowsCount);
    std::iota(rightCol1.begin(), rightCol1.end(), 0);
    TVector<ui64> rightCol2(rightRowsCount);
    std::iota(rightCol2.begin(), rightCol2.end(), 0);
    TVector<ui64> rightCol3(rightRowsCount);
    std::iota(rightCol3.begin(), rightCol3.end(), 0);
    TVector<ui64> rightCol4(rightRowsCount);
    std::iota(rightCol4.begin(), rightCol4.end(), 0);
    TVector<ui64> rightCol5(rightRowsCount);
    std::iota(rightCol5.begin(), rightCol5.end(), 0);
    TVector<ui64> rightCol6(rightRowsCount);
    std::iota(rightCol6.begin(), rightCol6.end(), 0);
    TVector<ui64> rightCol7(rightRowsCount);
    std::iota(rightCol7.begin(), rightCol7.end(), 0);
    TVector<ui64> rightCol8(rightRowsCount);
    std::iota(rightCol8.begin(), rightCol8.end(), 0);
    TVector<ui64> rightCol9(rightRowsCount);
    std::iota(rightCol9.begin(), rightCol9.end(), 0);
    TVector<ui64> rightCol10(rightRowsCount);
    std::iota(rightCol10.begin(), rightCol10.end(), 0);
    TVector<ui64> rightCol11(rightRowsCount);
    std::iota(rightCol11.begin(), rightCol11.end(), 0);
    TVector<ui64> rightCol12(rightRowsCount);
    std::iota(rightCol12.begin(), rightCol12.end(), 0);
    TVector<ui64> rightCol13(rightRowsCount);
    std::iota(rightCol13.begin(), rightCol13.end(), 0);
    TVector<TString> rightCol14(rightRowsCount);
    for (auto& s: rightCol14) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    TVector<TString> rightCol15(rightRowsCount);
    for (auto& s: rightCol15) {
        auto sz = 6 + rng() % 10;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    for (size_t i = 0; i < rightRowsCount / 5; i++) {
        size_t lhs = rng() % rightRowsCount;
        size_t rhs = rng() % rightRowsCount;
        std::swap(rightCol1[lhs], rightCol1[rhs]);
    }

    // Make 10% to match
    for (size_t i = 0; i < leftRowsCount / 10; ++i) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % rightRowsCount;
        leftCol1[lhs] = rightCol1[rhs];
        leftCol2[lhs] = rightCol2[rhs];
    }

    return {
        std::make_tuple(std::move(leftCol1), std::move(leftCol2), std::move(leftCol3), std::move(leftCol4)),
        std::make_tuple(std::move(rightCol1), std::move(rightCol2), std::move(rightCol3), std::move(rightCol4), std::move(rightCol5), std::move(rightCol6), std::move(rightCol7), std::move(rightCol8), std::move(rightCol9), std::move(rightCol10), std::move(rightCol11), std::move(rightCol12), std::move(rightCol13), std::move(rightCol14), std::move(rightCol15))
    };
}

/**
 * Query #9:
 *  - Selectivity: 10%
 *  - Col 1 is key
 *  - Left row max size: 32
 *  - Right row max size: 32
 *  - Left size: 40_000, right size: 40_000   <-- Main query characteristic
 */
std::pair<
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>>,
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>>
>
Query9GenerateData(ui64& datasetSize) {
    std::mt19937 rng(std::random_device{}());
    const ui64 leftRowsCount = 40'000;
    const ui64 rightRowsCount = 40'000;

    datasetSize = leftRowsCount * 32 + rightRowsCount * 32;

    TVector<ui64> leftCol1(leftRowsCount);
    std::iota(leftCol1.begin(), leftCol1.end(), 0);
    TVector<ui64> leftCol2(leftRowsCount);
    std::iota(leftCol2.begin(), leftCol2.end(), 0);
    TVector<ui64> leftCol3(leftRowsCount);
    std::iota(leftCol3.begin(), leftCol3.end(), 0);
    TVector<ui64> leftCol4(leftRowsCount);
    std::iota(leftCol4.begin(), leftCol4.end(), 0);
    for (size_t i = 0; i < leftRowsCount / 5; i++) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % leftRowsCount;
        std::swap(leftCol1[lhs], leftCol1[rhs]);
    }

    TVector<ui64> rightCol1(rightRowsCount);
    std::iota(rightCol1.begin(), rightCol1.end(), leftRowsCount);
    TVector<ui64> rightCol2(rightRowsCount);
    std::iota(rightCol2.begin(), rightCol2.end(), leftRowsCount);
    TVector<ui64> rightCol3(rightRowsCount);
    std::iota(rightCol3.begin(), rightCol3.end(), leftRowsCount);
    TVector<ui64> rightCol4(rightRowsCount);
    std::iota(rightCol4.begin(), rightCol4.end(), leftRowsCount);
    for (size_t i = 0; i < rightRowsCount / 5; i++) {
        size_t lhs = rng() % rightRowsCount;
        size_t rhs = rng() % rightRowsCount;
        std::swap(rightCol1[lhs], rightCol1[rhs]);
    }

    // Make 10% to match
    for (size_t i = 0; i < leftRowsCount / 10; ++i) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % rightRowsCount;
        leftCol1[lhs] = rightCol1[rhs];
        leftCol2[lhs] = rightCol2[rhs];
    }

    return {
        std::make_tuple(std::move(leftCol1), std::move(leftCol2), std::move(leftCol3), std::move(leftCol4)),
        std::make_tuple(std::move(rightCol1), std::move(rightCol2), std::move(rightCol3), std::move(rightCol4))
    };
}

/**
 * Query #10:
 *  - Selectivity: 10%
 *  - Col 1 is key
 *  - Left row max size: 32
 *  - Right row max size: 32
 *  - Left size: 1_200_000, right size: 40_000   <-- Main query characteristic
 */
std::pair<
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>>,
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>>
>
Query10GenerateData(ui64& datasetSize) {
    std::mt19937 rng(std::random_device{}());
    const ui64 leftRowsCount = 1'200'000;
    const ui64 rightRowsCount = 40'000;

    datasetSize = leftRowsCount * 32 + rightRowsCount * 32;

    TVector<ui64> leftCol1(leftRowsCount);
    std::iota(leftCol1.begin(), leftCol1.end(), 0);
    TVector<ui64> leftCol2(leftRowsCount);
    std::iota(leftCol2.begin(), leftCol2.end(), 0);
    TVector<ui64> leftCol3(leftRowsCount);
    std::iota(leftCol3.begin(), leftCol3.end(), 0);
    TVector<ui64> leftCol4(leftRowsCount);
    std::iota(leftCol4.begin(), leftCol4.end(), 0);
    for (size_t i = 0; i < leftRowsCount / 5; i++) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % leftRowsCount;
        std::swap(leftCol1[lhs], leftCol1[rhs]);
    }

    TVector<ui64> rightCol1(rightRowsCount);
    std::iota(rightCol1.begin(), rightCol1.end(), leftRowsCount);
    TVector<ui64> rightCol2(rightRowsCount);
    std::iota(rightCol2.begin(), rightCol2.end(), leftRowsCount);
    TVector<ui64> rightCol3(rightRowsCount);
    std::iota(rightCol3.begin(), rightCol3.end(), leftRowsCount);
    TVector<ui64> rightCol4(rightRowsCount);
    std::iota(rightCol4.begin(), rightCol4.end(), leftRowsCount);
    for (size_t i = 0; i < rightRowsCount / 5; i++) {
        size_t lhs = rng() % rightRowsCount;
        size_t rhs = rng() % rightRowsCount;
        std::swap(rightCol1[lhs], rightCol1[rhs]);
    }

    // Make 10% to match
    for (size_t i = 0; i < leftRowsCount / 10; ++i) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % rightRowsCount;
        leftCol1[lhs] = rightCol1[rhs];
        leftCol2[lhs] = rightCol2[rhs];
    }

    return {
        std::make_tuple(std::move(leftCol1), std::move(leftCol2), std::move(leftCol3), std::move(leftCol4)),
        std::make_tuple(std::move(rightCol1), std::move(rightCol2), std::move(rightCol3), std::move(rightCol4))
    };
}

/**
 * Query #11:
 *  - Selectivity: 10%                         <-- Main query characteristic
 *  - Cols 1 and 2 are keys
 *  - Left row max size: 80
 *  - Right row max size: 56
 *  - Left size: 800_000, right size: 300_000  <-- Main query characteristic, grace join oriented sizes
 */
std::pair<
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>, TVector<TString>>,
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>>
>
Query11GenerateData(ui64& datasetSize) {
    std::mt19937 rng(std::random_device{}());
    const ui64 leftRowsCount = 800'000;
    const ui64 rightRowsCount = 300'000;

    datasetSize = leftRowsCount * 32 + rightRowsCount * 24;

    TVector<ui64> leftCol1(leftRowsCount);
    std::iota(leftCol1.begin(), leftCol1.end(), 0);
    TVector<ui64> leftCol2(leftRowsCount);
    std::iota(leftCol2.begin(), leftCol2.end(), 0);
    TVector<ui64> leftCol3(leftRowsCount);
    std::iota(leftCol3.begin(), leftCol3.end(), 0);
    TVector<ui64> leftCol4(leftRowsCount);
    std::iota(leftCol4.begin(), leftCol4.end(), 0);
    TVector<TString> leftCol5(leftRowsCount);
    for (auto& s: leftCol5) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    TVector<TString> leftCol6(leftRowsCount);
    for (auto& s: leftCol6) {
        auto sz = 6 + rng() % 10;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    for (size_t i = 0; i < leftRowsCount / 5; i++) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % leftRowsCount;
        std::swap(leftCol1[lhs], leftCol1[rhs]);
    }

    TVector<ui64> rightCol1(rightRowsCount);
    std::iota(rightCol1.begin(), rightCol1.end(), leftRowsCount);
    TVector<ui64> rightCol2(rightRowsCount);
    std::iota(rightCol2.begin(), rightCol2.end(), leftRowsCount);
    TVector<ui64> rightCol3(rightRowsCount);
    std::iota(rightCol3.begin(), rightCol3.end(), leftRowsCount);
    TVector<TString> rightCol4(rightRowsCount);
    for (auto& s: rightCol4) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    for (size_t i = 0; i < rightRowsCount / 5; i++) {
        size_t lhs = rng() % rightRowsCount;
        size_t rhs = rng() % rightRowsCount;
        std::swap(rightCol1[lhs], rightCol1[rhs]);
    }

    // Make 10% to match
    for (size_t i = 0; i < leftRowsCount / 10; ++i) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % rightRowsCount;
        leftCol1[lhs] = rightCol1[rhs];
        leftCol2[lhs] = rightCol2[rhs];
    }

    return {
        std::make_tuple(std::move(leftCol1), std::move(leftCol2), std::move(leftCol3), std::move(leftCol4), std::move(leftCol5), std::move(leftCol6)),
        std::make_tuple(std::move(rightCol1), std::move(rightCol2), std::move(rightCol3), std::move(rightCol4))
    };
}

/**
 * Query #12:
 *  - Selectivity: 50%                          <-- Main query characteristic
 *  - Cols 1 and 2 are keys
 *  - Left row max size: 80
 *  - Right row max size: 56
 *  - Left size: 800_000, right size: 300_000   <-- Main query characteristic, grace join oriented sizes
 */
std::pair<
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>, TVector<TString>>,
std::tuple<TVector<ui64>, TVector<ui64>, TVector<ui64>, TVector<TString>>
>
Query12GenerateData(ui64& datasetSize) {
    std::mt19937 rng(std::random_device{}());
    const ui64 leftRowsCount = 800'000;
    const ui64 rightRowsCount = 300'000;

    datasetSize = leftRowsCount * 32 + rightRowsCount * 24;

    TVector<ui64> leftCol1(leftRowsCount);
    std::iota(leftCol1.begin(), leftCol1.end(), 0);
    TVector<ui64> leftCol2(leftRowsCount);
    std::iota(leftCol2.begin(), leftCol2.end(), 0);
    TVector<ui64> leftCol3(leftRowsCount);
    std::iota(leftCol3.begin(), leftCol3.end(), 0);
    TVector<ui64> leftCol4(leftRowsCount);
    std::iota(leftCol4.begin(), leftCol4.end(), 0);
    TVector<TString> leftCol5(leftRowsCount);
    for (auto& s: leftCol5) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    TVector<TString> leftCol6(leftRowsCount);
    for (auto& s: leftCol6) {
        auto sz = 6 + rng() % 10;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    for (size_t i = 0; i < leftRowsCount / 5; i++) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % leftRowsCount;
        std::swap(leftCol1[lhs], leftCol1[rhs]);
    }

    TVector<ui64> rightCol1(rightRowsCount);
    std::iota(rightCol1.begin(), rightCol1.end(), leftRowsCount);
    TVector<ui64> rightCol2(rightRowsCount);
    std::iota(rightCol2.begin(), rightCol2.end(), leftRowsCount);
    TVector<ui64> rightCol3(rightRowsCount);
    std::iota(rightCol3.begin(), rightCol3.end(), leftRowsCount);
    TVector<TString> rightCol4(rightRowsCount);
    for (auto& s: rightCol4) {
        auto sz = 8 + rng() % 24;
        datasetSize += sz;
        s = GenerateRandomString(sz);
    }
    for (size_t i = 0; i < rightRowsCount / 5; i++) {
        size_t lhs = rng() % rightRowsCount;
        size_t rhs = rng() % rightRowsCount;
        std::swap(rightCol1[lhs], rightCol1[rhs]);
    }

    // Make 50% to match
    for (size_t i = 0; i < leftRowsCount / 2; ++i) {
        size_t lhs = rng() % leftRowsCount;
        size_t rhs = rng() % rightRowsCount;
        leftCol1[lhs] = rightCol1[rhs];
        leftCol2[lhs] = rightCol2[rhs];
    }

    return {
        std::make_tuple(std::move(leftCol1), std::move(leftCol2), std::move(leftCol3), std::move(leftCol4), std::move(leftCol5), std::move(leftCol6)),
        std::make_tuple(std::move(rightCol1), std::move(rightCol2), std::move(rightCol3), std::move(rightCol4))
    };
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLJoinBenchmarks) {

    Y_UNIT_TEST(BenchQ1) {
        ui64 datasetSize = 0;
        Cerr << ">>> Benchmark Q1:" << Endl;
        auto [lhs, rhs] = Query1GenerateData(datasetSize);

        for (auto type: {JoinType::BlockGraceJoin_HashJoin,
                         JoinType::BlockGraceJoin_InMemoryGraceJoin,
                         JoinType::BlockMapJoin,
                         JoinType::GraceJoin})
        {
            TSetup<false> setup(GetNodeFactory());

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                std::get<0>(lhs), std::get<1>(lhs), std::get<2>(lhs), std::get<3>(lhs), std::get<4>(lhs), std::get<5>(lhs));
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                std::get<0>(rhs), std::get<1>(rhs), std::get<2>(rhs), std::get<3>(rhs));

            if (type == JoinType::GraceJoin) {
                RunBenchGraceJoin(
                    setup,
                    leftType, std::move(leftList), {0, 1},
                    rightType, std::move(rightList), {0, 1},
                    {}, {0, 1}
                );
            } else {
                RunBenchBlockJoin(
                    setup, type, 32,
                    leftType, std::move(leftList), {0, 1},
                    rightType, std::move(rightList), {0, 1},
                    {}, {0, 1}
                );
            }
        }

        Cerr << globalResourceMeter.GetFullLog(datasetSize) << Endl;
    } // Y_UNIT_TEST(BenchQ1)

    Y_UNIT_TEST(BenchQ2) {
        ui64 datasetSize = 0;
        Cerr << ">>> Benchmark Q2:" << Endl;
        auto [lhs, rhs] = Query2GenerateData(datasetSize);

        for (auto type: {JoinType::BlockGraceJoin_HashJoin,
                         JoinType::BlockGraceJoin_InMemoryGraceJoin,
                         JoinType::BlockMapJoin,
                         JoinType::GraceJoin})
        {
            TSetup<false> setup(GetNodeFactory());

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                std::get<0>(lhs), std::get<1>(lhs), std::get<2>(lhs), std::get<3>(lhs), std::get<4>(lhs), std::get<5>(lhs));
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                std::get<0>(rhs), std::get<1>(rhs), std::get<2>(rhs), std::get<3>(rhs));

            if (type == JoinType::GraceJoin) {
                RunBenchGraceJoin(
                    setup,
                    leftType, std::move(leftList), {0, 1},
                    rightType, std::move(rightList), {0, 1},
                    {}, {0, 1}
                );
            } else {
                RunBenchBlockJoin(
                    setup, type, 32,
                    leftType, std::move(leftList), {0, 1},
                    rightType, std::move(rightList), {0, 1},
                    {}, {0, 1}
                );
            }
        }

        Cerr << globalResourceMeter.GetFullLog(datasetSize) << Endl;
    } // Y_UNIT_TEST(BenchQ2)

    Y_UNIT_TEST(BenchQ3) {
        ui64 datasetSize = 0;
        Cerr << ">>> Benchmark Q3:" << Endl;
        auto [lhs, rhs] = Query3GenerateData(datasetSize);

        for (auto type: {JoinType::BlockGraceJoin_HashJoin,
                         JoinType::BlockGraceJoin_InMemoryGraceJoin,
                         JoinType::BlockMapJoin,
                         JoinType::GraceJoin})
        {
            TSetup<false> setup(GetNodeFactory());

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                std::get<0>(lhs), std::get<1>(lhs), std::get<2>(lhs), std::get<3>(lhs), std::get<4>(lhs), std::get<5>(lhs));
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                std::get<0>(rhs), std::get<1>(rhs), std::get<2>(rhs), std::get<3>(rhs));

            if (type == JoinType::GraceJoin) {
                RunBenchGraceJoin(
                    setup,
                    leftType, std::move(leftList), {0, 1},
                    rightType, std::move(rightList), {0, 1},
                    {}, {0, 1}
                );
            } else {
                RunBenchBlockJoin(
                    setup, type, 32,
                    leftType, std::move(leftList), {0, 1},
                    rightType, std::move(rightList), {0, 1},
                    {}, {0, 1}
                );
            }
        }

        Cerr << globalResourceMeter.GetFullLog(datasetSize) << Endl;
    } // Y_UNIT_TEST(BenchQ3)

    Y_UNIT_TEST(BenchQ4) {
        ui64 datasetSize = 0;
        Cerr << ">>> Benchmark Q4:" << Endl;
        auto [lhs, rhs] = Query4GenerateData(datasetSize);

        for (auto type: {JoinType::BlockGraceJoin_HashJoin,
                         JoinType::BlockGraceJoin_InMemoryGraceJoin,
                         JoinType::BlockMapJoin,
                         JoinType::GraceJoin})
        {
            TSetup<false> setup(GetNodeFactory());

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                std::get<0>(lhs), std::get<1>(lhs), std::get<2>(lhs), std::get<3>(lhs), std::get<4>(lhs), std::get<5>(lhs));
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                std::get<0>(rhs), std::get<1>(rhs), std::get<2>(rhs), std::get<3>(rhs));

            if (type == JoinType::GraceJoin) {
                RunBenchGraceJoin(
                    setup,
                    leftType, std::move(leftList), {0, 1},
                    rightType, std::move(rightList), {0, 1},
                    {}, {0, 1}
                );
            } else {
                RunBenchBlockJoin(
                    setup, type, 32,
                    leftType, std::move(leftList), {0, 1},
                    rightType, std::move(rightList), {0, 1},
                    {}, {0, 1}
                );
            }
        }

        Cerr << globalResourceMeter.GetFullLog(datasetSize) << Endl;
    } // Y_UNIT_TEST(BenchQ4)

    Y_UNIT_TEST(BenchQ5) {
        ui64 datasetSize = 0;
        Cerr << ">>> Benchmark Q5:" << Endl;
        auto [lhs, rhs] = Query5GenerateData(datasetSize);

        for (auto type: {JoinType::BlockGraceJoin_HashJoin,
                         JoinType::BlockGraceJoin_InMemoryGraceJoin,
                         JoinType::BlockMapJoin,
                         JoinType::GraceJoin})
        {
            TSetup<false> setup(GetNodeFactory());

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                std::get<0>(lhs), std::get<1>(lhs), std::get<2>(lhs), std::get<3>(lhs), std::get<4>(lhs), std::get<5>(lhs));
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                std::get<0>(rhs), std::get<1>(rhs), std::get<2>(rhs), std::get<3>(rhs));

            if (type == JoinType::GraceJoin) {
                RunBenchGraceJoin(
                    setup,
                    leftType, std::move(leftList), {0},
                    rightType, std::move(rightList), {0},
                    {}, {0}
                );
            } else {
                RunBenchBlockJoin(
                    setup, type, 32,
                    leftType, std::move(leftList), {0},
                    rightType, std::move(rightList), {0},
                    {}, {0}
                );
            }
        }

        Cerr << globalResourceMeter.GetFullLog(datasetSize) << Endl;
    } // Y_UNIT_TEST(BenchQ5)

    Y_UNIT_TEST(BenchQ6) {
        ui64 datasetSize = 0;
        Cerr << ">>> Benchmark Q6:" << Endl;
        auto [lhs, rhs] = Query6GenerateData(datasetSize);

        for (auto type: {JoinType::BlockGraceJoin_HashJoin,
                         JoinType::BlockGraceJoin_InMemoryGraceJoin,
                         JoinType::BlockMapJoin,
                         JoinType::GraceJoin})
        {
            TSetup<false> setup(GetNodeFactory());

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                std::get<0>(lhs), std::get<1>(lhs), std::get<2>(lhs), std::get<3>(lhs), std::get<4>(lhs),
                std::get<5>(lhs), std::get<6>(lhs), std::get<7>(lhs), std::get<8>(lhs), std::get<9>(lhs),
                std::get<10>(lhs), std::get<11>(lhs), std::get<12>(lhs), std::get<13>(lhs), std::get<14>(lhs));
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                std::get<0>(rhs), std::get<1>(rhs), std::get<2>(rhs), std::get<3>(rhs));

            if (type == JoinType::GraceJoin) {
                RunBenchGraceJoin(
                    setup,
                    leftType, std::move(leftList), {0, 1},
                    rightType, std::move(rightList), {0, 1},
                    {}, {0, 1}
                );
            } else {
                RunBenchBlockJoin(
                    setup, type, 32,
                    leftType, std::move(leftList), {0, 1},
                    rightType, std::move(rightList), {0, 1},
                    {}, {0, 1}
                );
            }
        }

        Cerr << globalResourceMeter.GetFullLog(datasetSize) << Endl;
    } // Y_UNIT_TEST(BenchQ6)

    Y_UNIT_TEST(BenchQ7) {
        ui64 datasetSize = 0;
        Cerr << ">>> Benchmark Q7:" << Endl;
        auto [lhs, rhs] = Query7GenerateData(datasetSize);

        for (auto type: {JoinType::BlockGraceJoin_HashJoin,
                         JoinType::BlockGraceJoin_InMemoryGraceJoin,
                         JoinType::BlockMapJoin,
                         JoinType::GraceJoin})
        {
            TSetup<false> setup(GetNodeFactory());

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                std::get<0>(lhs), std::get<1>(lhs), std::get<2>(lhs), std::get<3>(lhs), std::get<4>(lhs),
                std::get<5>(lhs), std::get<6>(lhs), std::get<7>(lhs), std::get<8>(lhs), std::get<9>(lhs),
                std::get<10>(lhs), std::get<11>(lhs), std::get<12>(lhs), std::get<13>(lhs), std::get<14>(lhs));
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                std::get<0>(rhs), std::get<1>(rhs), std::get<2>(rhs), std::get<3>(rhs));

            if (type == JoinType::GraceJoin) {
                RunBenchGraceJoin(
                    setup,
                    leftType, std::move(leftList), {0, 1},
                    rightType, std::move(rightList), {0, 1},
                    {}, {0, 1}
                );
            } else {
                RunBenchBlockJoin(
                    setup, type, 32,
                    leftType, std::move(leftList), {0, 1},
                    rightType, std::move(rightList), {0, 1},
                    {}, {0, 1}
                );
            }
        }

        Cerr << globalResourceMeter.GetFullLog(datasetSize) << Endl;
    } // Y_UNIT_TEST(BenchQ7)

    Y_UNIT_TEST(BenchQ8) {
        ui64 datasetSize = 0;
        Cerr << ">>> Benchmark Q8:" << Endl;
        auto [lhs, rhs] = Query8GenerateData(datasetSize);

        for (auto type: {JoinType::BlockGraceJoin_HashJoin,
                         JoinType::BlockGraceJoin_InMemoryGraceJoin,
                         JoinType::BlockMapJoin,
                         JoinType::GraceJoin})
        {
            TSetup<false> setup(GetNodeFactory());

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                std::get<0>(lhs), std::get<1>(lhs), std::get<2>(lhs), std::get<3>(lhs));
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                std::get<0>(rhs), std::get<1>(rhs), std::get<2>(rhs), std::get<3>(rhs), std::get<4>(rhs),
                std::get<5>(rhs), std::get<6>(rhs), std::get<7>(rhs), std::get<8>(rhs), std::get<9>(rhs),
                std::get<10>(rhs), std::get<11>(rhs), std::get<12>(rhs), std::get<13>(rhs), std::get<14>(rhs));

            if (type == JoinType::GraceJoin) {
                RunBenchGraceJoin(
                    setup,
                    leftType, std::move(leftList), {0, 1},
                    rightType, std::move(rightList), {0, 1},
                    {}, {0, 1}
                );
            } else {
                RunBenchBlockJoin(
                    setup, type, 32,
                    leftType, std::move(leftList), {0, 1},
                    rightType, std::move(rightList), {0, 1},
                    {}, {0, 1}
                );
            }
        }

        Cerr << globalResourceMeter.GetFullLog(datasetSize) << Endl;
    } // Y_UNIT_TEST(BenchQ8)

    Y_UNIT_TEST(BenchQ9) {
        ui64 datasetSize = 0;
        Cerr << ">>> Benchmark Q9:" << Endl;
        auto [lhs, rhs] = Query9GenerateData(datasetSize);

        for (auto type: {JoinType::BlockGraceJoin_HashJoin,
                         JoinType::BlockGraceJoin_InMemoryGraceJoin,
                         JoinType::BlockMapJoin,
                         JoinType::GraceJoin})
        {
            TSetup<false> setup(GetNodeFactory());

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                std::get<0>(lhs), std::get<1>(lhs), std::get<2>(lhs), std::get<3>(lhs));
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                std::get<0>(rhs), std::get<1>(rhs), std::get<2>(rhs), std::get<3>(rhs));

            if (type == JoinType::GraceJoin) {
                RunBenchGraceJoin(
                    setup,
                    leftType, std::move(leftList), {0},
                    rightType, std::move(rightList), {0},
                    {}, {0}
                );
            } else {
                RunBenchBlockJoin(
                    setup, type, 8,
                    leftType, std::move(leftList), {0},
                    rightType, std::move(rightList), {0},
                    {}, {0}
                );
            }
        }

        Cerr << globalResourceMeter.GetFullLog(datasetSize) << Endl;
    } // Y_UNIT_TEST(BenchQ9)

    Y_UNIT_TEST(BenchQ10) {
        ui64 datasetSize = 0;
        Cerr << ">>> Benchmark Q10:" << Endl;
        auto [lhs, rhs] = Query10GenerateData(datasetSize);

        for (auto type: {JoinType::BlockGraceJoin_HashJoin,
                         JoinType::BlockGraceJoin_InMemoryGraceJoin,
                         JoinType::BlockMapJoin,
                         JoinType::GraceJoin})
        {
            TSetup<false> setup(GetNodeFactory());

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                std::get<0>(lhs), std::get<1>(lhs), std::get<2>(lhs), std::get<3>(lhs));
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                std::get<0>(rhs), std::get<1>(rhs), std::get<2>(rhs), std::get<3>(rhs));

            if (type == JoinType::GraceJoin) {
                RunBenchGraceJoin(
                    setup,
                    leftType, std::move(leftList), {0},
                    rightType, std::move(rightList), {0},
                    {}, {0}
                );
            } else {
                RunBenchBlockJoin(
                    setup, type, 8,
                    leftType, std::move(leftList), {0},
                    rightType, std::move(rightList), {0},
                    {}, {0}
                );
            }
        }

        Cerr << globalResourceMeter.GetFullLog(datasetSize) << Endl;
    } // Y_UNIT_TEST(BenchQ10)

    Y_UNIT_TEST(BenchQ11) {
        ui64 datasetSize = 0;
        Cerr << ">>> Benchmark Q11:" << Endl;
        auto [lhs, rhs] = Query11GenerateData(datasetSize);

        for (auto type: {JoinType::BlockGraceJoin_HashJoin,
                         JoinType::BlockGraceJoin_InMemoryGraceJoin,
                         JoinType::BlockMapJoin,
                         JoinType::GraceJoin})
        {
            TSetup<false> setup(GetNodeFactory());

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                std::get<0>(lhs), std::get<1>(lhs), std::get<2>(lhs), std::get<3>(lhs), std::get<4>(lhs), std::get<5>(lhs));
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                std::get<0>(rhs), std::get<1>(rhs), std::get<2>(rhs), std::get<3>(rhs));

            if (type == JoinType::GraceJoin) {
                RunBenchGraceJoin(
                    setup,
                    leftType, std::move(leftList), {0, 1},
                    rightType, std::move(rightList), {0, 1},
                    {}, {0, 1}
                );
            } else {
                RunBenchBlockJoin(
                    setup, type, 32,
                    leftType, std::move(leftList), {0, 1},
                    rightType, std::move(rightList), {0, 1},
                    {}, {0, 1}
                );
            }
        }

        Cerr << globalResourceMeter.GetFullLog(datasetSize) << Endl;
    } // Y_UNIT_TEST(BenchQ11)

    Y_UNIT_TEST(BenchQ12) {
        ui64 datasetSize = 0;
        Cerr << ">>> Benchmark Q12:" << Endl;
        auto [lhs, rhs] = Query12GenerateData(datasetSize);

        for (auto type: {JoinType::BlockGraceJoin_HashJoin,
                         JoinType::BlockGraceJoin_InMemoryGraceJoin,
                         JoinType::BlockMapJoin,
                         JoinType::GraceJoin})
        {
            TSetup<false> setup(GetNodeFactory());

            auto [leftType, leftList] = ConvertVectorsToTuples(setup,
                std::get<0>(lhs), std::get<1>(lhs), std::get<2>(lhs), std::get<3>(lhs), std::get<4>(lhs), std::get<5>(lhs));
            auto [rightType, rightList] = ConvertVectorsToTuples(setup,
                std::get<0>(rhs), std::get<1>(rhs), std::get<2>(rhs), std::get<3>(rhs));

            if (type == JoinType::GraceJoin) {
                RunBenchGraceJoin(
                    setup,
                    leftType, std::move(leftList), {0, 1},
                    rightType, std::move(rightList), {0, 1},
                    {}, {0, 1}
                );
            } else {
                RunBenchBlockJoin(
                    setup, type, 32,
                    leftType, std::move(leftList), {0, 1},
                    rightType, std::move(rightList), {0, 1},
                    {}, {0, 1}
                );
            }
        }

        Cerr << globalResourceMeter.GetFullLog(datasetSize) << Endl;
    } // Y_UNIT_TEST(BenchQ12)

} // Y_UNIT_TEST_SUITE

} // namespace NMiniKQL
} // namespace NKikimr
