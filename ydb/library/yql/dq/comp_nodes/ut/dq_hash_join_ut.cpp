#include "utils/dq_factories.h"
#include "utils/dq_setup.h"
#include "utils/utils.h"
#include <type_utils.h>
#include <ydb/core/kqp/tools/join_perf/construct_join_graph.h>
#include <ydb/library/yql/dq/comp_nodes/dq_block_hash_join.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

namespace {
struct TJoinTestData {
    TJoinTestData() {
        Setup->Alloc.SetLimit(1);
        Setup->Alloc.Ref().SetIncreaseMemoryLimitCallback([&](ui64 limit, ui64 required) {
            auto newLimit = std::max(required, limit);
            Setup->Alloc.SetLimit(newLimit);
        });
    }

    auto MakeHardLimitIncreaseMemCallback(ui64 hardLimit) {
        return [hardLimit, this](ui64 limit, ui64 required) {
            auto newLimit = std::min(std::max(limit, required), hardLimit);
            Cout << std::format("hard limit: {}, limit: {}, required: {}, alloc limit {} -> {}, TotalAllocated_: {}",
                                hardLimit, limit, required, Setup->Alloc.GetLimit(), newLimit,
                                Setup->Alloc.GetAllocated())
                 << Endl;
            Setup->Alloc.SetLimit(std::min(std::max(limit, required), hardLimit));
        };
    }

    void SetHardLimitIncreaseMemCallback(ui64 hardLimit) {
        Setup->Alloc.SetMaximumLimitValueReached(true);
        Cout << std::format("finished sides prep. allocated: {}, used: {}, new limit: {}", Setup->Alloc.GetAllocated(),
                            Setup->Alloc.GetUsed(), hardLimit)
             << Endl;

        Setup->Alloc.Ref().SetIncreaseMemoryLimitCallback(MakeHardLimitIncreaseMemCallback(hardLimit));
    }

    std::unique_ptr<TDqSetup<false, true>> Setup = std::make_unique<TDqSetup<false, true>>();
    EJoinKind Kind;
    TypeAndValue Left;
    TVector<ui32> LeftKeyColmns = {0};
    TypeAndValue Right;
    TVector<ui32> RightKeyColmns = {0};
    TDqUserRenames Renames = {{0, EJoinSide::kLeft}, {1, EJoinSide::kLeft}, {0, EJoinSide::kRight},
                              {1, EJoinSide::kRight}};
    TypeAndValue Result;
    std::optional<ui64> JoinMemoryConstraint = std::nullopt;
    int BlockSize = 128;
    bool SliceBlocks = false;
};

void FilterRenamesForSemiAndOnlyJoins(TJoinTestData& td) {
    std::erase_if(td.Renames, [&](const auto& indexAndSide) {
        return RightSemiOrOnly(td.Kind) && indexAndSide.Side == EJoinSide::kLeft ||
               LeftSemiOrOnly(td.Kind) && indexAndSide.Side == EJoinSide::kRight;
    });
}

TJoinTestData BasicInnerJoinTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys = {1, 2, 3, 4, 5};
    TVector<TString> leftValues = {"a1", "b1", "c1", "d1", "e1"};

    TVector<ui64> rightKeys = {2, 3, 4, 5, 6};
    TVector<TString> rightValues = {"b2", "c2", "d2", "e2", "f2"};

    TVector<ui64> expectedKeysLeft = {2, 3, 4, 5};
    TVector<TString> expectedValuesLeft = {"b1", "c1", "d1", "e1"};
    TVector<ui64> expectedKeysRight = {2, 3, 4, 5};
    TVector<TString> expectedValuesRight = {"b2", "c2", "d2", "e2"};

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result =
        ConvertVectorsToTuples(setup, expectedKeysLeft, expectedValuesLeft, expectedKeysRight, expectedValuesRight);
    td.Kind = EJoinKind::Inner;
    return td;
}

TJoinTestData EmptyInnerJoinTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> emptyKeys = {};
    TVector<TString> emptyValues = {};

    td.Left = ConvertVectorsToTuples(setup, emptyKeys, emptyValues);
    td.Right = ConvertVectorsToTuples(setup, emptyKeys, emptyValues);
    td.Result = ConvertVectorsToTuples(setup, emptyKeys, emptyValues, emptyKeys, emptyValues);

    td.Kind = EJoinKind::Inner;
    return td;
}

TJoinTestData CrossJoinTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys = {1, 1, 1, 1, 1};
    TVector<TString> leftValues = {"a1", "b1", "c1", "d1", "e1"};

    TVector<ui64> rightKeys = {1, 1, 1, 1, 1};
    TVector<TString> rightValues = {"a2", "b2", "c2", "d2", "e2"};

    TVector<ui64> expectedKeysLeft(25, ui64{1});

    TVector<TString> expectedValuesLeft = {"a1", "b1", "c1", "d1", "e1", "a1", "b1", "c1", "d1", "e1", "a1", "b1", "c1",
                                           "d1", "e1", "a1", "b1", "c1", "d1", "e1", "a1", "b1", "c1", "d1", "e1"};
    TVector<ui64> expectedKeysRight(25, ui64{1});

    TVector<TString> expectedValuesRight = {"a2", "a2", "a2", "a2", "a2", "b2", "b2", "b2", "b2",
                                            "b2", "c2", "c2", "c2", "c2", "c2", "d2", "d2", "d2",
                                            "d2", "d2", "e2", "e2", "e2", "e2", "e2"};

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result =
        ConvertVectorsToTuples(setup, expectedKeysLeft, expectedValuesLeft, expectedKeysRight, expectedValuesRight);
    td.Kind = EJoinKind::Inner;
    return td;
}

TJoinTestData MixedKeysInnerTestData() {

    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys = {1, 1, 2, 3, 4};
    TVector<TString> leftValues = {"a1", "b1", "c1", "d1", "e1"};

    TVector<ui64> rightKeys = {1, 2, 2, 4, 5};
    TVector<TString> rightValues = {"a2", "b2", "c2", "d2", "e2"};

    TVector<ui64> expectedKeysLeft = {1, 1, 2, 2, 4};
    TVector<TString> expectedValuesLeft = {
        "a1",
        "b1",
        "c1",
        "c1",
        "e1",
    };
    TVector<ui64> expectedKeysRight = {1, 1, 2, 2, 4};
    TVector<TString> expectedValuesRight = {
        "a2",
        "a2",
        "b2",
        "c2",
        "d2",
    };

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result =
        ConvertVectorsToTuples(setup, expectedKeysLeft, expectedValuesLeft, expectedKeysRight, expectedValuesRight);
    td.Kind = EJoinKind::Inner;
    return td;
}

TJoinTestData EmptyLeftInnerTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys;
    TVector<TString> leftValues;

    TVector<ui64> rightKeys = {1, 2, 3};
    TVector<TString> rightValues = {"x", "y", "z"};

    TVector<ui64> expectedKeysLeft;
    TVector<TString> expectedValuesLeft;
    TVector<ui64> expectedKeysRight;
    TVector<TString> expectedValuesRight;

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result =
        ConvertVectorsToTuples(setup, expectedKeysLeft, expectedValuesLeft, expectedKeysRight, expectedValuesRight);
    td.Kind = EJoinKind::Inner;
    return td;
}

TJoinTestData EmptyRightInnerTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys = {1, 2, 3};
    TVector<TString> leftValues = {"a", "b", "c"};

    TVector<ui64> rightKeys;
    TVector<TString> rightValues;

    TVector<ui64> expectedKeysLeft;
    TVector<TString> expectedValuesLeft;
    TVector<ui64> expectedKeysRight;
    TVector<TString> expectedValuesRight;

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result =
        ConvertVectorsToTuples(setup, expectedKeysLeft, expectedValuesLeft, expectedKeysRight, expectedValuesRight);
    td.Kind = EJoinKind::Inner;
    return td;
}

[[maybe_unused]] TJoinTestData LeftJoinTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys = {1, 2, 3};
    TVector<TString> leftValues = {"a", "b", "c"};

    TVector<ui64> rightKeys;
    TVector<TString> rightValues;

    TVector<ui64> expectedKeysLeft = leftKeys;
    TVector<TString> expectedValuesLeft = leftValues;
    TVector<std::optional<ui64>> expectedKeysRight(3, std::nullopt);
    TVector<std::optional<TString>> expectedValuesRight(3, std::nullopt);

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result =
        ConvertVectorsToTuples(setup, expectedKeysLeft, expectedValuesLeft, expectedKeysRight, expectedValuesRight);

    td.Kind = EJoinKind::Left;
    return td;
}

[[maybe_unused]] TJoinTestData LeftJoinWithMatchesTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys = {1, 2, 3, 4, 5};
    TVector<TString> leftValues = {"a1", "b1", "c1", "d1", "e1"};

    TVector<ui64> rightKeys = {2, 3, 4, 5, 6};
    TVector<TString> rightValues = {"b2", "c2", "d2", "e2", "f2"};

    TVector<ui64> expectedKeysLeft = {1, 2, 3, 4, 5};
    TVector<TString> expectedValuesLeft = {"a1", "b1", "c1", "d1", "e1"};
    TVector<std::optional<ui64>> expectedKeysRight = {std::nullopt, 2, 3, 4, 5};
    TVector<std::optional<TString>> expectedValuesRight = {std::nullopt, "b2", "c2", "d2", "e2"};

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result =
        ConvertVectorsToTuples(setup, expectedKeysLeft, expectedValuesLeft, expectedKeysRight, expectedValuesRight);

    td.Kind = EJoinKind::Left;
    return td;
}

[[maybe_unused]] TJoinTestData LeftJoinSpillingTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;

    TVector<ui64> leftKeys = {1, 2, 3, 4, 5};
    TVector<ui64> leftValues = {13, 14, 15, 16, 17};

    constexpr int rightSize = 200000;
    TVector<ui64> rightKeys(rightSize);
    TVector<ui64> rightValues(rightSize);
    for (int index = 0; index < rightSize; ++index) {
        rightKeys[index] = 2 * index + 3;
        rightValues[index] = index;
    }

    TVector<ui64> expectedKeysLeft = {1, 2, 3, 4, 5};
    TVector<ui64> expectedValuesLeft = {13, 14, 15, 16, 17};
    TVector<std::optional<ui64>> expectedKeysRight = {std::nullopt, std::nullopt, 3, std::nullopt, 5};
    TVector<std::optional<ui64>> expectedValuesRight = {std::nullopt, std::nullopt, 0, std::nullopt, 1};

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result =
        ConvertVectorsToTuples(setup, expectedKeysLeft, expectedValuesLeft, expectedKeysRight, expectedValuesRight);

    constexpr int packedTupleSize = 2 * 8 + 5;
    constexpr ui64 joinMemory = packedTupleSize * (0.5 * rightSize);
    td.JoinMemoryConstraint = joinMemory;
    td.Kind = EJoinKind::Left;
    return td;
}

[[maybe_unused]] TJoinTestData LeftJoinSpillingMultiKeyTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;

    TVector<ui64> leftCol0 = {100, 200, 300, 400, 500};                              
    TVector<std::optional<ui64>> leftCol1 = {10, std::nullopt, 30, std::nullopt, 50};
    TVector<ui64> leftCol2 = {1, 2, 3, 4, 5};                                        
    TVector<ui64> leftCol3 = {11, 22, 33, 44, 55};                                   
    TVector<ui64> leftCol4 = {1000, 2000, 3000, 4000, 5000};                         

    constexpr int rightSize = 200000;
    TVector<ui64> rightCol0(rightSize);
    TVector<ui64> rightCol1(rightSize);
    for (int i = 0; i < rightSize; ++i) {
        rightCol0[i] = 2 * i + 3;
        rightCol1[i] = 20 * i + 33;
    }
    // expected: left row 2 matches, rest dont
    TVector<ui64> expLeftCol0 = {100, 200, 300, 400, 500};
    TVector<std::optional<ui64>> expLeftCol1 = {10, std::nullopt, 30, std::nullopt, 50};
    TVector<ui64> expLeftCol2 = {1, 2, 3, 4, 5};
    TVector<ui64> expLeftCol3 = {11, 22, 33, 44, 55};
    TVector<ui64> expLeftCol4 = {1000, 2000, 3000, 4000, 5000};
    TVector<std::optional<ui64>> expRightCol0 = {std::nullopt, std::nullopt, 3, std::nullopt, std::nullopt};
    TVector<std::optional<ui64>> expRightCol1 = {std::nullopt, std::nullopt, 33, std::nullopt, std::nullopt};

    td.Left = ConvertVectorsToTuples(setup, leftCol0, leftCol1, leftCol2, leftCol3, leftCol4);
    td.Right = ConvertVectorsToTuples(setup, rightCol0, rightCol1);
    td.Result = ConvertVectorsToTuples(setup, expLeftCol0, expLeftCol1, expLeftCol2, expLeftCol3, expLeftCol4,
                                       expRightCol0, expRightCol1);

    td.LeftKeyColmns = {2, 3};
    td.RightKeyColmns = {0, 1};
    td.Renames = {{0, EJoinSide::kLeft}, {1, EJoinSide::kLeft}, {2, EJoinSide::kLeft},
                  {3, EJoinSide::kLeft}, {4, EJoinSide::kLeft},
                  {0, EJoinSide::kRight}, {1, EJoinSide::kRight}};

    constexpr int packedTupleSize = 2 * 8 + 5;
    constexpr ui64 joinMemory = packedTupleSize * (0.5 * rightSize);
    td.JoinMemoryConstraint = joinMemory;
    td.Kind = EJoinKind::Left;
    return td;
}

[[maybe_unused]] TJoinTestData LeftJoinSpillingTwoKeysTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;

    TVector<ui64> leftKey1 = {1, 2, 3, 4, 5};
    TVector<ui64> leftKey2 = {10, 20, 30, 40, 50};

    constexpr int rightSize = 200000;
    TVector<ui64> rightKey1(rightSize);
    TVector<ui64> rightKey2(rightSize);
    for (int i = 0; i < rightSize; ++i) {
        rightKey1[i] = 2 * i + 3;
        rightKey2[i] = 20 * i + 30;
    }

    TVector<ui64> expLeftKey1 = {1, 2, 3, 4, 5};
    TVector<ui64> expLeftKey2 = {10, 20, 30, 40, 50};
    TVector<std::optional<ui64>> expRightKey1 = {std::nullopt, std::nullopt, 3, std::nullopt, 5};
    TVector<std::optional<ui64>> expRightKey2 = {std::nullopt, std::nullopt, 30, std::nullopt, 50};

    td.Left = ConvertVectorsToTuples(setup, leftKey1, leftKey2);
    td.Right = ConvertVectorsToTuples(setup, rightKey1, rightKey2);
    td.Result = ConvertVectorsToTuples(setup, expLeftKey1, expLeftKey2, expRightKey1, expRightKey2);

    td.LeftKeyColmns = {0, 1};
    td.RightKeyColmns = {0, 1};
    td.Renames = {{0, EJoinSide::kLeft}, {1, EJoinSide::kLeft},
                  {0, EJoinSide::kRight}, {1, EJoinSide::kRight}};

    constexpr int packedTupleSize = 2 * 8 + 5;
    constexpr ui64 joinMemory = packedTupleSize * (0.5 * rightSize);
    td.JoinMemoryConstraint = joinMemory;
    td.Kind = EJoinKind::Left;
    return td;
}

[[maybe_unused]] TJoinTestData LargeBothSidesInnerSpillingTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;

    constexpr int leftSize = 200000;
    constexpr int rightSize = 200000;
    constexpr int keyOffset = 100000;

    TVector<ui64> leftCol0(leftSize);
    TVector<ui64> leftCol1(leftSize);
    for (int i = 0; i < leftSize; ++i) {
        leftCol0[i] = i;
        leftCol1[i] = i * 10;
    }

    TVector<ui64> rightCol0(rightSize);
    TVector<std::optional<ui64>> rightCol1(rightSize);
    for (int i = 0; i < rightSize; ++i) {
        rightCol0[i] = i + keyOffset;
        rightCol1[i] = (i % 5 == 0) ? std::nullopt : std::optional<ui64>(i * 7);
    }

    constexpr int matchCount = leftSize - keyOffset;
    TVector<ui64> expLeftCol0(matchCount);
    TVector<ui64> expLeftCol1(matchCount);
    TVector<ui64> expRightCol0(matchCount);
    TVector<std::optional<ui64>> expRightCol1(matchCount);
    for (int i = 0; i < matchCount; ++i) {
        int leftKey = i + keyOffset;
        int rightIdx = i;
        expLeftCol0[i] = leftKey;
        expLeftCol1[i] = leftKey * 10;
        expRightCol0[i] = leftKey;
        expRightCol1[i] = (rightIdx % 5 == 0) ? std::nullopt : std::optional<ui64>(rightIdx * 7);
    }

    td.Left = ConvertVectorsToTuples(setup, leftCol0, leftCol1);
    td.Right = ConvertVectorsToTuples(setup, rightCol0, rightCol1);
    td.Result = ConvertVectorsToTuples(setup, expLeftCol0, expLeftCol1, expRightCol0, expRightCol1);

    td.LeftKeyColmns = {0};
    td.RightKeyColmns = {0};
    td.Renames = {{0, EJoinSide::kLeft}, {1, EJoinSide::kLeft},
                  {0, EJoinSide::kRight}, {1, EJoinSide::kRight}};

    constexpr int packedTupleSize = 2 * 8 + 5;
    constexpr ui64 joinMemory = packedTupleSize * static_cast<ui64>(0.3 * rightSize);
    td.JoinMemoryConstraint = joinMemory;
    td.Kind = EJoinKind::Inner;
    return td;
}

[[maybe_unused]] TJoinTestData LargeBothSidesLeftSpillingTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;

    constexpr int leftSize = 200000;
    constexpr int rightSize = 200000;
    constexpr int keyOffset = 100000;

    TVector<ui64> leftCol0(leftSize);
    TVector<ui64> leftCol1(leftSize);
    TVector<std::optional<ui64>> leftCol2(leftSize);
    for (int i = 0; i < leftSize; ++i) {
        leftCol0[i] = i;
        leftCol1[i] = i * 3;
        leftCol2[i] = (i % 4 == 0) ? std::nullopt : std::optional<ui64>(i * 100);
    }

    TVector<ui64> rightCol0(rightSize);
    TVector<ui64> rightCol1(rightSize);
    for (int i = 0; i < rightSize; ++i) {
        rightCol0[i] = i + keyOffset;
        rightCol1[i] = (i + keyOffset) * 3;
    }

    TVector<ui64> expLeftCol0(leftSize);
    TVector<ui64> expLeftCol1(leftSize);
    TVector<std::optional<ui64>> expLeftCol2(leftSize);
    TVector<std::optional<ui64>> expRightCol0(leftSize);
    TVector<std::optional<ui64>> expRightCol1(leftSize);
    for (int i = 0; i < leftSize; ++i) {
        expLeftCol0[i] = i;
        expLeftCol1[i] = i * 3;
        expLeftCol2[i] = (i % 4 == 0) ? std::nullopt : std::optional<ui64>(i * 100);
        if (i >= keyOffset) {
            expRightCol0[i] = static_cast<ui64>(i);
            expRightCol1[i] = static_cast<ui64>(i * 3);
        }
    }

    td.Left = ConvertVectorsToTuples(setup, leftCol0, leftCol1, leftCol2);
    td.Right = ConvertVectorsToTuples(setup, rightCol0, rightCol1);
    td.Result = ConvertVectorsToTuples(setup, expLeftCol0, expLeftCol1, expLeftCol2,
                                       expRightCol0, expRightCol1);

    td.LeftKeyColmns = {0, 1};
    td.RightKeyColmns = {0, 1};
    td.Renames = {{0, EJoinSide::kLeft}, {1, EJoinSide::kLeft}, {2, EJoinSide::kLeft},
                  {0, EJoinSide::kRight}, {1, EJoinSide::kRight}};

    constexpr int packedTupleSize = 2 * 8 + 5;
    constexpr ui64 joinMemory = packedTupleSize * static_cast<ui64>(0.3 * rightSize);
    td.JoinMemoryConstraint = joinMemory;
    td.Kind = EJoinKind::Left;
    return td;
}

[[maybe_unused]] TJoinTestData SlicedBlocksInnerSpillingTestData() {
    auto td = LargeBothSidesInnerSpillingTestData();
    td.SliceBlocks = true;
    return td;
}

[[maybe_unused]] TJoinTestData SlicedBlocksLeftSpillingTestData() {
    auto td = LargeBothSidesLeftSpillingTestData();
    td.SliceBlocks = true;
    return td;
}

[[maybe_unused]] TJoinTestData FullBehavesAsLeftIfRightEmptyTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys = {1, 2, 3};
    TVector<TString> leftValues = {"a", "b", "c"};

    TVector<ui64> rightKeys;
    TVector<TString> rightValues;

    TVector<ui64> expectedKeysLeft = leftKeys;
    TVector<TString> expectedValuesLeft = leftValues;
    TVector<std::optional<ui64>> expectedKeysRight(3, std::nullopt);
    TVector<std::optional<TString>> expectedValuesRight(3, std::nullopt);

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result =
        ConvertVectorsToTuples(setup, expectedKeysLeft, expectedValuesLeft, expectedKeysRight, expectedValuesRight);
    td.Kind = EJoinKind::Full;
    return td;
}

[[maybe_unused]] TJoinTestData RightJoinTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys;
    TVector<TString> leftValues;

    TVector<ui64> rightKeys = {1, 2, 3};
    TVector<TString> rightValues = {"a", "b", "c"};

    TVector<std::optional<ui64>> expectedKeysLeft(3, std::nullopt);
    TVector<std::optional<TString>> expectedValuesLeft(3, std::nullopt);
    TVector<ui64> expectedKeysRight = rightKeys;
    TVector<TString> expectedValuesRight = rightValues;

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result =
        ConvertVectorsToTuples(setup, expectedKeysLeft, expectedValuesLeft, expectedKeysRight, expectedValuesRight);
    td.Kind = EJoinKind::Right;
    return td;
}

[[maybe_unused]] TJoinTestData FullBehavesAsRightIfLeftEmptyTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys;
    TVector<TString> leftValues;

    TVector<ui64> rightKeys = {1, 2, 3};
    TVector<TString> rightValues = {"a", "b", "c"};

    TVector<std::optional<ui64>> expectedKeysLeft(3, std::nullopt);
    TVector<std::optional<TString>> expectedValuesLeft(3, std::nullopt);
    TVector<ui64> expectedKeysRight = rightKeys;
    TVector<TString> expectedValuesRight = rightValues;

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result =
        ConvertVectorsToTuples(setup, expectedKeysLeft, expectedValuesLeft, expectedKeysRight, expectedValuesRight);
    td.Kind = EJoinKind::Full;
    return td;
}

[[maybe_unused]] TJoinTestData FullTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys = {2, 3, 4};
    TVector<TString> leftValues = {"b1", "c1", "d1"};

    TVector<ui64> rightKeys = {1, 2, 3};
    TVector<TString> rightValues = {"a2", "b2", "c2"};

    TVector<std::optional<ui64>> expectedKeysLeft = {std::nullopt, 2, 3, 4};
    TVector<std::optional<TString>> expectedValuesLeft = {std::nullopt, "b1", "c1", "d1"};
    TVector<std::optional<ui64>> expectedKeysRight = {1, 2, 3, std::nullopt};
    TVector<std::optional<TString>> expectedValuesRight = {"a2", "b2", "c2", std::nullopt};

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result =
        ConvertVectorsToTuples(setup, expectedKeysLeft, expectedValuesLeft, expectedKeysRight, expectedValuesRight);
    td.Kind = EJoinKind::Full;
    return td;
}

[[maybe_unused]] TJoinTestData ExclusionTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys = {2, 3, 4};
    TVector<TString> leftValues = {"b1", "c1", "d1"};

    TVector<ui64> rightKeys = {1, 2, 3};
    TVector<TString> rightValues = {"a2", "b2", "c2"};

    TVector<std::optional<ui64>> expectedKeysLeft = {std::nullopt, 4};
    TVector<std::optional<TString>> expectedValuesLeft = {std::nullopt, "d1"};
    TVector<std::optional<ui64>> expectedKeysRight = {1, std::nullopt};
    TVector<std::optional<TString>> expectedValuesRight = {"a2", std::nullopt};

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result =
        ConvertVectorsToTuples(setup, expectedKeysLeft, expectedValuesLeft, expectedKeysRight, expectedValuesRight);
    td.Kind = EJoinKind::Exclusion;
    return td;
}

[[maybe_unused]] TJoinTestData LeftSemiTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys = {2, 3, 4};
    TVector<TString> leftValues = {"b1", "c1", "d1"};

    TVector<ui64> rightKeys = {1, 2, 3};
    TVector<TString> rightValues = {"a2", "b2", "c2"};

    TVector<ui64> expectedKeys = {2, 3};
    TVector<TString> expectedValues = {"b1", "c1"};

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result = ConvertVectorsToTuples(setup, expectedKeys, expectedValues);
    td.Kind = EJoinKind::LeftSemi;
    td.Renames = TDqUserRenames{{0, EJoinSide::kLeft}, {1, EJoinSide::kLeft} };
    return td;
}

[[maybe_unused]] TJoinTestData RightSemiTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys = {2, 3, 4};
    TVector<TString> leftValues = {"b1", "c1", "d1"};

    TVector<ui64> rightKeys = {1, 2, 3};
    TVector<TString> rightValues = {"a2", "b2", "c2"};

    TVector<ui64> expectedKeys = {2, 3};
    TVector<TString> expectedValues = {"b2", "c2"};

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result = ConvertVectorsToTuples(setup, expectedKeys, expectedValues);
    td.Kind = EJoinKind::RightSemi;
    td.Renames = TDqUserRenames{{0, EJoinSide::kRight}, {1, EJoinSide::kRight} };
    return td;
}

[[maybe_unused]] TJoinTestData LeftOnlyTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys = {2, 3, 4};
    TVector<TString> leftValues = {"b1", "c1", "d1"};

    TVector<ui64> rightKeys = {1, 2, 3};
    TVector<TString> rightValues = {"a2", "b2", "c2"};

    TVector<ui64> expectedKeys = {4};
    TVector<TString> expectedValues = {"d1"};

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result = ConvertVectorsToTuples(setup, expectedKeys, expectedValues);
    td.Kind = EJoinKind::LeftOnly;
    td.Renames = TDqUserRenames{{0, EJoinSide::kLeft}, {1, EJoinSide::kLeft} };

    return td;
}

[[maybe_unused]] TJoinTestData RightOnlyTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys = {2, 3, 4};
    TVector<TString> leftValues = {"b1", "c1", "d1"};

    TVector<ui64> rightKeys = {1, 2, 3};
    TVector<TString> rightValues = {"a2", "b2", "c2"};

    TVector<ui64> expectedKeys = {1};
    TVector<TString> expectedValues = {"a2"};

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result = ConvertVectorsToTuples(setup, expectedKeys, expectedValues);
    td.Kind = EJoinKind::RightOnly;
    td.Renames = TDqUserRenames{{0, EJoinSide::kRight}, {1, EJoinSide::kRight} };
    return td;
}

TJoinTestData InnerJoinRenamesTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys = {1, 2, 3, 4, 5};
    TVector<TString> leftValues = {"a1", "b1", "c1", "d1", "e1"};

    TVector<ui64> rightKeys = {2, 3, 4, 5, 6};
    TVector<TString> rightValues = {"b2", "c2", "d2", "e2", "f2"};

    TVector<ui64> expectedKeysLeft = {2, 3, 4, 5};
    TVector<TString> expectedValuesLeft = {"b1", "c1", "d1", "e1"};
    TVector<ui64> expectedKeysRight = {2, 3, 4, 5};
    TVector<TString> expectedValuesRight = {"b2", "c2", "d2", "e2"};
    td.Renames = {{1, EJoinSide::kRight}, {1, EJoinSide::kLeft}, {0, EJoinSide::kRight}, {0, EJoinSide::kLeft},
                  {0, EJoinSide::kLeft}};
    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result = ConvertVectorsToTuples(setup, expectedValuesRight, expectedValuesLeft, expectedKeysRight,
                                       expectedKeysLeft, expectedKeysLeft);
    td.Kind = EJoinKind::Inner;
    return td;
}

TJoinTestData SpillingTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;

    TVector<ui64> leftKeys = {1, 2, 3, 4, 5};
    TVector<ui64> leftValues = {13, 14, 15, 16, 17};
    constexpr int rightSize = 200000;
    TVector<ui64> rightKeys(rightSize);
    TVector<ui64> rightValues(rightSize);
    for (int index = 0; index < rightSize; ++index) {
        rightKeys[index] = 2 * index + 3;
        rightValues[index] = index;
    }

    TVector<ui64> expectedKeysLeft = {3, 5};
    TVector<ui64> expectedValuesLeft = {15, 17};
    TVector<ui64> expectedKeysRight = {3, 5};
    TVector<ui64> expectedValuesRight = {0, 1};
    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result =
        ConvertVectorsToTuples(setup, expectedKeysLeft, expectedValuesLeft, expectedKeysRight, expectedValuesRight);

    constexpr int packedTupleSize = 2 * 8 + 5;
    constexpr ui64 joinMemory = packedTupleSize * (0.5 * rightSize);
    [[maybe_unused]] constexpr ui64 rightSizeBytes = rightSize * packedTupleSize;
    td.JoinMemoryConstraint = joinMemory;
    td.Kind = EJoinKind::Inner;
    return td;
}

TJoinTestData SmallStringsTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;

    TVector<TString> leftKeys{"foobarbazfoobarbazfoobarbaz", "foobarbazfoobarbazfoobarbazfoo", "foobarbazfoobarbazfoobarbazfoobar"};
    TVector<TString> leftValues{"a1", "b1", "c1"};

    TVector<TString> rightKeys{"foobarbazfoobarbazfoobarbaz", "foobarbazfoobarbazfoobarbazfoo", "foobarbazfoobarbazfoobarbazfoobar"};
    TVector<TString> rightValues{"a2", "b2", "c2"};

    TVector<TString> expectedKeysLeft = leftKeys;
    TVector<TString> expectedValuesLeft = leftValues;
    TVector<TString> expectedKeysRight = rightKeys;
    TVector<TString> expectedValuesRight = rightValues;
    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result =
        ConvertVectorsToTuples(setup, expectedKeysLeft, expectedValuesLeft, expectedKeysRight, expectedValuesRight);

    td.Kind = EJoinKind::Inner;
    return td;
}

TJoinTestData BigStringsTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;

    constexpr int leftSize = 2000;
    TVector<ui64> leftKeys(leftSize);
    TVector<TString> leftValues(leftSize);
    for (int index = 0; index < leftSize; ++index) {
        leftKeys[index] = 2 * index + 3;
        leftValues[index] = TString(std::min(400 + index / 1000, index+1), static_cast<char>((index+1)%10+'a'));
    }
    constexpr int rightSize = 200;
    TVector<ui64> rightKeys(rightSize);
    TVector<TString> rightValues(rightSize);
    for (int index = 0; index < rightSize; ++index) {
        rightKeys[index] = 2 * index + 3;
        rightValues[index] = TString(std::min(400 + index / 1000, index+1), static_cast<char>((index+1)%10+'a'));
    }

    TVector<ui64> expectedKeysLeft = rightKeys;
    TVector<TString> expectedValuesLeft = rightValues;
    TVector<ui64> expectedKeysRight = rightKeys;
    TVector<TString> expectedValuesRight = rightValues;
    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result =
        ConvertVectorsToTuples(setup, expectedKeysLeft, expectedValuesLeft, expectedKeysRight, expectedValuesRight);

    td.Kind = EJoinKind::Inner;
    return td;
}

void Test(TJoinTestData testData, bool blockJoin, bool withSpiller = true) {
    FilterRenamesForSemiAndOnlyJoins(testData);
    TJoinDescription descr;
    descr.CustomRenames = testData.Renames;
    descr.Setup = testData.Setup.get();
    descr.LeftSource.KeyColumnIndexes = testData.LeftKeyColmns;
    descr.LeftSource.ColumnTypes =
        AS_TYPE(TTupleType, AS_TYPE(TListType, testData.Left.Type)->GetItemType())->GetElements();
    descr.LeftSource.ValuesList = testData.Left.Value;
    descr.RightSource.KeyColumnIndexes = testData.RightKeyColmns;
    descr.RightSource.ColumnTypes =
        AS_TYPE(TTupleType, AS_TYPE(TListType, testData.Right.Type)->GetItemType())->GetElements();
    descr.RightSource.ValuesList = testData.Right.Value;
    descr.BlockSize = testData.BlockSize;
    descr.SliceBlocks = testData.SliceBlocks;
    if (testData.JoinMemoryConstraint){
        descr.Setup->Alloc.Ref().ForcefullySetMemoryYellowZone(true);
    } else {
        descr.Setup->Alloc.Ref().ForcefullySetMemoryYellowZone(false);
    }
    THolder<IComputationGraph> got = ConstructJoinGraphStream(
        testData.Kind, blockJoin ? ETestedJoinAlgo::kBlockHash : ETestedJoinAlgo::kScalarHash, descr, withSpiller);
    if (testData.JoinMemoryConstraint) {
        testData.SetHardLimitIncreaseMemCallback(*testData.JoinMemoryConstraint + 3000_MB +
                                                 testData.Setup->Alloc.GetUsed());
    }

    if (blockJoin) {
        CompareListAndBlockStreamIgnoringOrder(testData.Result, *got);
    } else {
        CompareListAndStreamIgnoringOrder(testData.Result, *got);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TDqHashJoinBasicTest) {
    Y_UNIT_TEST_TWIN(TestBasicPassthrough, BlockJoin) {
        Test(BasicInnerJoinTestData(), BlockJoin);
    }

    Y_UNIT_TEST_TWIN(TestCrossPassthrough, BlockJoin) {
        Test(CrossJoinTestData(), BlockJoin);
    }

    Y_UNIT_TEST_TWIN(TestMixedKeysPassthrough, BlockJoin) {
        Test(MixedKeysInnerTestData(), BlockJoin);
    }

    Y_UNIT_TEST_TWIN(TestEmptyFlows, BlockJoin) {
        Test(EmptyInnerJoinTestData(), BlockJoin);
    }

    Y_UNIT_TEST_TWIN(TestEmptyLeft, BlockJoin) {
        Test(EmptyLeftInnerTestData(), BlockJoin);
    }

    Y_UNIT_TEST_TWIN(TestEmptyRight, BlockJoin) {
        Test(EmptyRightInnerTestData(), BlockJoin);
    }

    Y_UNIT_TEST(TestLeftKind) {
        Test(LeftJoinTestData(), true);
    }

    Y_UNIT_TEST(TestLeftJoinWithMatches) {
        Test(LeftJoinWithMatchesTestData(), true);
    }

    Y_UNIT_TEST(TestLeftJoinSpilling) {
        Test(LeftJoinSpillingTestData(), true);
    }

    Y_UNIT_TEST(TestLeftJoinSpillingTwoKeys) {
        Test(LeftJoinSpillingTwoKeysTestData(), true);
    }

    Y_UNIT_TEST(TestLeftJoinSpillingMultiKey) {
        Test(LeftJoinSpillingMultiKeyTestData(), true);
    }

    Y_UNIT_TEST(TestLargeBothSidesInnerSpilling) {
        Test(LargeBothSidesInnerSpillingTestData(), true);
    }

    Y_UNIT_TEST(TestLargeBothSidesLeftSpilling) {
        Test(LargeBothSidesLeftSpillingTestData(), true);
    }

    Y_UNIT_TEST(TestSlicedBlocksInnerSpilling) {
        Test(SlicedBlocksInnerSpillingTestData(), true);
    }

    Y_UNIT_TEST(TestSlicedBlocksLeftSpilling) {
        Test(SlicedBlocksLeftSpillingTestData(), true);
    }

    // Y_UNIT_TEST_TWIN(FullBehavesAsLeftIfRightEmpty, BlockJoin) {
    //     Test(FullBehavesAsLeftIfRightEmptyTestData(), BlockJoin);
    // }

    // Y_UNIT_TEST_TWIN(TestRightKind, BlockJoin) {
    //     Test(RightJoinTestData(), BlockJoin);
    // }

    // Y_UNIT_TEST_TWIN(TestFullKindBehavesAsRightIfLeftIsEmpty, BlockJoin) {
    //     Test(FullBehavesAsRightIfLeftEmptyTestData(), BlockJoin);
    // }

    // Y_UNIT_TEST_TWIN(TestFullKind, BlockJoin) {
    //     Test(FullTestData(), BlockJoin);
    // }

    // Y_UNIT_TEST_TWIN(TestExclusionKind, BlockJoin) {
    //     Test(ExclusionTestData(), BlockJoin);
    // }

    Y_UNIT_TEST(TestLeftSemiKind) {
        Test(LeftSemiTestData(),true);
    }

    // Y_UNIT_TEST_TWIN(TestRightSemiKind, BlockJoin) {
    //     Test(RightSemiTestData(), BlockJoin);
    // }

    Y_UNIT_TEST(TestLeftOnlyKind) {
        Test(LeftOnlyTestData(), true);
    }

    // Y_UNIT_TEST_TWIN(TestRightOnlyKind, BlockJoin) {
    //     Test(RightOnlyTestData(), BlockJoin);
    // }
    Y_UNIT_TEST_TWIN(TestInnerRenamesKind, BlockJoin) {
        Test(InnerJoinRenamesTestData(), BlockJoin);
    }

    Y_UNIT_TEST(TestBlockSpilling) { 
        Test(SpillingTestData(), true);
    }

    Y_UNIT_TEST(TestBlockJoinWithoutSpilling) {
        Test(BasicInnerJoinTestData(), true, false);
    }
    Y_UNIT_TEST(TestBigStrings) { 
        Test(BigStringsTestData(), true);
    }
    Y_UNIT_TEST(TestSmallStrings) { 
        Test(SmallStringsTestData(), true);
    }
}
} // namespace NKikimr::NMiniKQL
