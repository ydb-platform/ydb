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
    std::unique_ptr<TDqSetup<false>> Setup = std::make_unique<TDqSetup<false>>();
    EJoinKind Kind;
    TypeAndValue Left;
    TVector<ui32> LeftKeyColmns = {0};
    TypeAndValue Right;
    TVector<ui32> RightKeyColmns = {0};
    TDqUserRenames Renames = {{0, EJoinSide::kLeft}, {1, EJoinSide::kLeft}, {0, EJoinSide::kRight},
                              {1, EJoinSide::kRight}};
    TypeAndValue Result;
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

    TVector<std::optional<ui64>> expectedKeys = {2, 3};
    TVector<std::optional<TString>> expectedValues = {"b1", "c1"};

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result = ConvertVectorsToTuples(setup, expectedKeys, expectedValues);
    td.Kind = EJoinKind::LeftSemi;
    return td;
}

[[maybe_unused]] TJoinTestData RightSemiTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys = {2, 3, 4};
    TVector<TString> leftValues = {"b1", "c1", "d1"};

    TVector<ui64> rightKeys = {1, 2, 3};
    TVector<TString> rightValues = {"a2", "b2", "c2"};

    TVector<std::optional<ui64>> expectedKeys = {2, 3};
    TVector<std::optional<TString>> expectedValues = {"b2", "c2"};

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result = ConvertVectorsToTuples(setup, expectedKeys, expectedValues);
    td.Kind = EJoinKind::RightSemi;
    return td;
}

[[maybe_unused]] TJoinTestData LeftOnlyTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys = {2, 3, 4};
    TVector<TString> leftValues = {"b1", "c1", "d1"};

    TVector<ui64> rightKeys = {1, 2, 3};
    TVector<TString> rightValues = {"a2", "b2", "c2"};

    TVector<std::optional<ui64>> expectedKeys = {4};
    TVector<std::optional<TString>> expectedValues = {"d1"};

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result = ConvertVectorsToTuples(setup, expectedKeys, expectedValues);
    td.Kind = EJoinKind::LeftOnly;
    return td;
}

[[maybe_unused]] TJoinTestData RightOnlyTestData() {
    TJoinTestData td;
    auto& setup = *td.Setup;
    TVector<ui64> leftKeys = {2, 3, 4};
    TVector<TString> leftValues = {"b1", "c1", "d1"};

    TVector<ui64> rightKeys = {1, 2, 3};
    TVector<TString> rightValues = {"a2", "b2", "c2"};

    TVector<std::optional<ui64>> expectedKeys = {1};
    TVector<std::optional<TString>> expectedValues = {"a2"};

    td.Left = ConvertVectorsToTuples(setup, leftKeys, leftValues);
    td.Right = ConvertVectorsToTuples(setup, rightKeys, rightValues);
    td.Result = ConvertVectorsToTuples(setup, expectedKeys, expectedValues);
    td.Kind = EJoinKind::RightOnly;
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

void Test(TJoinTestData testData, bool blockJoin) {
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

    THolder<IComputationGraph> got = ConstructJoinGraphStream(
        testData.Kind, blockJoin ? ETestedJoinAlgo::kBlockHash : ETestedJoinAlgo::kScalarHash, descr);
    // FromWideStream
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

    // Y_UNIT_TEST_TWIN(TestLeftKind, BlockJoin) {
    //     Test(LeftJoinTestData(), BlockJoin);
    // }

    // Y_UNIT_TEST_TWIN(FullBehaves, BlockJoin) {
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

    // Y_UNIT_TEST_TWIN(TestLeftSemiKind, BlockJoin) {
    //     Test(LeftSemiTestData(), BlockJoin);
    // }

    // Y_UNIT_TEST_TWIN(TestRightSemiKind, BlockJoin) {
    //     Test(RightSemiTestData(), BlockJoin);
    // }

    // Y_UNIT_TEST_TWIN(TestLeftOnlyKind, BlockJoin) {
    //     Test(LeftOnlyTestData(), BlockJoin);
    // }

    // Y_UNIT_TEST_TWIN(TestRightOnlyKind, BlockJoin) {
    //     Test(RightOnlyTestData(), BlockJoin);
    // }
    Y_UNIT_TEST_TWIN(TestInnerRenamesKind, BlockJoin) {
        Test(InnerJoinRenamesTestData(), BlockJoin);
    }
}
} // namespace NKikimr::NMiniKQL