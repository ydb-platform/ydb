#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/minikql/comp_nodes/mkql_block_coalesce.h>

#include <yql/essentials/minikql/comp_nodes/mkql_block_coalesce_blending_helper.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>
#include <yql/essentials/ast/yql_expr_builder.h>
#include <yql/essentials/public/udf/arrow/memory_pool.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/arrow/arrow_util.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_block_test_helper.h>

#include <arrow/compute/exec_internal.h>

namespace NKikimr::NMiniKQL {

namespace {

#define UNIT_TEST_WITH_INTEGER(TestName)                                          \
    template <typename TTestType>                                                 \
    void TestName##Execute(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED); \
    Y_UNIT_TEST(TestName##_i8) {                                                  \
        TestName##Execute<i8>(ut_context);                                        \
    }                                                                             \
    Y_UNIT_TEST(TestName##_ui8) {                                                 \
        TestName##Execute<ui8>(ut_context);                                       \
    }                                                                             \
    Y_UNIT_TEST(TestName##_i16) {                                                 \
        TestName##Execute<i16>(ut_context);                                       \
    }                                                                             \
    Y_UNIT_TEST(TestName##_ui16) {                                                \
        TestName##Execute<ui16>(ut_context);                                      \
    }                                                                             \
    Y_UNIT_TEST(TestName##_i32) {                                                 \
        TestName##Execute<i32>(ut_context);                                       \
    }                                                                             \
    Y_UNIT_TEST(TestName##_ui32) {                                                \
        TestName##Execute<ui32>(ut_context);                                      \
    }                                                                             \
    Y_UNIT_TEST(TestName##_i64) {                                                 \
        TestName##Execute<i64>(ut_context);                                       \
    }                                                                             \
    Y_UNIT_TEST(TestName##_ui64) {                                                \
        TestName##Execute<ui64>(ut_context);                                      \
    }                                                                             \
    Y_UNIT_TEST(TestName##_float) {                                               \
        TestName##Execute<float>(ut_context);                                     \
    }                                                                             \
    Y_UNIT_TEST(TestName##_double) {                                              \
        TestName##Execute<double>(ut_context);                                    \
    }                                                                             \
    Y_UNIT_TEST(TestName##_TDate) {                                               \
        TestName##Execute<NYql::NUdf::TDate>(ut_context);                         \
    }                                                                             \
    Y_UNIT_TEST(TestName##_TDatetime) {                                           \
        TestName##Execute<NYql::NUdf::TDatetime>(ut_context);                     \
    }                                                                             \
    Y_UNIT_TEST(TestName##_TTimestamp) {                                          \
        TestName##Execute<NYql::NUdf::TTimestamp>(ut_context);                    \
    }                                                                             \
    Y_UNIT_TEST(TestName##_TInterval) {                                           \
        TestName##Execute<NYql::NUdf::TInterval>(ut_context);                     \
    }                                                                             \
    Y_UNIT_TEST(TestName##_TDate32) {                                             \
        TestName##Execute<NYql::NUdf::TDate32>(ut_context);                       \
    }                                                                             \
    Y_UNIT_TEST(TestName##_TDatetime64) {                                         \
        TestName##Execute<NYql::NUdf::TDatetime64>(ut_context);                   \
    }                                                                             \
    Y_UNIT_TEST(TestName##_TTimestamp64) {                                        \
        TestName##Execute<NYql::NUdf::TTimestamp64>(ut_context);                  \
    }                                                                             \
    Y_UNIT_TEST(TestName##_TInterval64) {                                         \
        TestName##Execute<NYql::NUdf::TInterval64>(ut_context);                   \
    }                                                                             \
                                                                                  \
    template <typename TTestType>                                                 \
    void TestName##Execute(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED)

template <typename T>
arrow::Datum GenerateArray(TTypeInfoHelper& typeInfoHelper, TType* type, std::vector<TMaybe<T>>& array, size_t offset) {
    auto rightArrayBuilder = MakeArrayBuilder(typeInfoHelper, type, *NYql::NUdf::GetYqlMemoryPool(), array.size() + offset, nullptr);
    for (size_t i = 0; i < offset; i++) {
        if (array[0]) {
            rightArrayBuilder->Add(TBlockItem(array[0].GetRef()));
        } else {
            rightArrayBuilder->Add(TBlockItem());
        }
    }

    for (size_t i = 0; i < array.size(); i++) {
        if (array[i]) {
            rightArrayBuilder->Add(TBlockItem(array[i].GetRef()));
        } else {
            rightArrayBuilder->Add(TBlockItem());
        }
    };

    arrow::Datum resultArray = rightArrayBuilder->Build(/*finish=*/true);
    resultArray.array()->offset += offset;
    resultArray.array()->null_count = arrow::kUnknownNullCount;
    resultArray.array()->length -= offset;
    return resultArray;
}

template <typename T, typename U, typename V>
void TestCoalesceKernel(T left, U right, V expected) {
    TBlockHelper().TestKernel(left, right, expected,
                              [](TSetup<false>& setup, TRuntimeNode left, TRuntimeNode right) {
                                  return setup.PgmBuilder->BlockCoalesce(left, right);
                              });
}

enum class ERightOperandType {
    SCALAR,
    ARRAY,
    OPTIONAL_ARRAY,
    OPTIONAL_SCALAR
};

template <typename T>
using InputOptionalVector =
    std::vector<TMaybe<typename NUdf::TDataType<T>::TLayout>>;

std::unique_ptr<IArrowKernelComputationNode> GetArrowKernel(IComputationGraph* graph) {
    std::vector<std::unique_ptr<IArrowKernelComputationNode>> allKernels;
    for (auto node : graph->GetNodes()) {
        auto kernelNode = node->PrepareArrowKernelComputationNode(graph->GetContext());
        if (!kernelNode) {
            continue;
        }
        allKernels.push_back(std::move(kernelNode));
    }
    UNIT_ASSERT_EQUAL(allKernels.size(), 1u);
    return std::move(allKernels[0]);
}

template <typename T, ERightOperandType rightTypeShape = ERightOperandType::ARRAY>
void TestBlockCoalesceForVector(InputOptionalVector<T> left,
                                InputOptionalVector<T> right,
                                InputOptionalVector<T> expected,
                                size_t leftOffset,
                                size_t rightOffset,
                                bool resetNullBitmapWhenAllNotNull = false) {
    using TLayout = typename NUdf::TDataType<T>::TLayout;
    TSetup<false> setup;
    NYql::TExprContext exprCtx;
    auto* rightType = setup.PgmBuilder->NewDataType(NUdf::TDataType<T>::Id);
    auto* leftType = setup.PgmBuilder->NewOptionalType(rightType);
    if (rightTypeShape == ERightOperandType::OPTIONAL_ARRAY || rightTypeShape == ERightOperandType::OPTIONAL_SCALAR) {
        // Make both operands optional.
        rightType = leftType;
    }
    TTypeInfoHelper typeInfoHelper;
    arrow::Datum leftOperand = GenerateArray(typeInfoHelper, leftType, left, leftOffset);

    arrow::compute::ExecContext execCtx;
    arrow::compute::KernelContext ctx(&execCtx);

    arrow::Datum rightOperand;
    if constexpr (rightTypeShape == ERightOperandType::SCALAR) {
        rightOperand = MakeScalarDatum<TLayout>(right[0].GetRef());
    } else if constexpr (rightTypeShape == ERightOperandType::OPTIONAL_SCALAR) {
        if (right[0]) {
            rightOperand = MakeScalarDatum<TLayout>(right[0].GetRef());
        } else {
            rightOperand = MakeScalarDatum<TLayout>(0);
            rightOperand.scalar()->is_valid = false;
        }
    } else {
        rightOperand = GenerateArray(typeInfoHelper, rightType, right, rightOffset);
    }
    // Reset bitmap that responses for nullability of arrow::ArrayData.
    // If all elements are not null then we have two options:
    // 1. All bitmask elements are set to 1.
    // 2. There is no bitmask at all.
    // So we want to test both variants via |resetNullBitmapWhenAllNotNull| flag.
    if (rightOperand.is_array() && resetNullBitmapWhenAllNotNull && rightOperand.array()->GetNullCount() == 0) {
        rightOperand.array()->buffers[0] = nullptr;
    }
    auto bi = arrow::compute::detail::ExecBatchIterator::Make({leftOperand, rightOperand}, 1000).ValueOrDie();
    arrow::compute::ExecBatch batch;
    UNIT_ASSERT(bi->Next(&batch));
    arrow::Datum out;
    // This graph will never be executed. We need it only to extrace coalesce arrow kernel.
    auto graph = setup.BuildGraph(
        setup.PgmBuilder->BlockCoalesce(
            setup.PgmBuilder->Arg(setup.PgmBuilder->NewBlockType(leftType, TBlockType::EShape::Many)),
            setup.PgmBuilder->Arg(setup.PgmBuilder->NewBlockType(rightType, TBlockType::EShape::Many))));
    auto kernel = GetArrowKernel(graph.Get());
    // kernel is exectly coalesce kernel.
    Y_ENSURE(kernel->GetArrowKernel().exec(&ctx, batch, &out).ok());
    arrow::Datum expectedArrowArray = GenerateArray(typeInfoHelper, rightType, expected, 0);
    UNIT_ASSERT_EQUAL_C(out, expectedArrowArray, "Expected : " << expectedArrowArray.make_array()->ToString() << "\n but got : " << out.make_array()->ToString());
}

template <typename T, ERightOperandType rightType = ERightOperandType::ARRAY>
void TestBlockCoalesce(InputOptionalVector<T> left,
                       InputOptionalVector<T> right,
                       InputOptionalVector<T> expected) {
    // First test different offsets.
    for (size_t leftOffset = 0; leftOffset < 10; leftOffset++) {
        for (size_t rightOffset = 0; rightOffset < 10; rightOffset++) {
            TestBlockCoalesceForVector<T, rightType>(left, right, expected, leftOffset, rightOffset);
        }
    }

    // Second test different sizes.
    // Also test only small subset of offsets to prevent a combinatorial explosion.
    while (left.size() > 1 || right.size() > 1 || expected.size() > 1) {
        if (left.size() > 1) {
            left.pop_back();
        }
        if (right.size() > 1) {
            right.pop_back();
        }
        if (expected.size() > 1) {
            expected.pop_back();
        }
        for (size_t leftOffset = 0; leftOffset < 2; leftOffset++) {
            for (size_t rightOffset = 0; rightOffset < 2; rightOffset++) {
                TestBlockCoalesceForVector<T, rightType>(left, right, expected, leftOffset, rightOffset);
                TestBlockCoalesceForVector<T, rightType>(left, right, expected, leftOffset, rightOffset, /*resetNullBitmapWhenAllNotNull=*/true);
            }
        }
    }
}

void BlockCoalesceGraphTest(size_t length, size_t offset) {
    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto ui32Type = pb.NewDataType(NUdf::TDataType<ui32>::Id);
    const auto optui32Type = pb.NewOptionalType(ui32Type);

    const auto inputTupleType = pb.NewTupleType({ui32Type, optui32Type});
    const auto outputTupleType = pb.NewTupleType({ui32Type});

    TRuntimeNode::TList right;
    TVector<bool> isNull;

    const auto drng = CreateDeterministicRandomProvider(1);
    std::vector<ui32> rightValues;
    for (size_t i = 0; i < length; i++) {
        const ui32 randomValue = drng->GenRand();
        const auto maybeNull = (randomValue % 2 == 0)
                                   ? pb.NewOptional(pb.NewDataLiteral<ui32>(randomValue / 2))
                                   : pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);

        const auto inputTuple = pb.NewTuple(inputTupleType, {
                                                                pb.NewDataLiteral<ui32>(i),
                                                                maybeNull,
                                                            });

        right.push_back(inputTuple);
        rightValues.push_back(randomValue / 2);
        isNull.push_back((randomValue % 2) != 0);
    }

    const auto list = pb.NewList(inputTupleType, std::move(right));

    auto node = pb.ToFlow(list);
    node = pb.ExpandMap(node, [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {
            pb.Nth(item, 0),
            pb.Nth(item, 1),
        };
    });

    node = pb.WideToBlocks(pb.FromFlow(node));
    if (offset > 0) {
        node = pb.WideSkipBlocks(node, pb.NewDataLiteral<ui64>(offset));
    }
    node = pb.WideMap(node, [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
        Y_ENSURE(items.size() == 3);
        return {
            pb.BlockCoalesce(items[1], items[0]),
            items[2]};
    });

    node = pb.ToFlow(pb.WideFromBlocks(node));
    node = pb.NarrowMap(node, [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return pb.NewTuple(outputTupleType, {items[0]});
    });

    const auto pgmReturn = pb.Collect(node);
    const auto graph = setup.BuildGraph(pgmReturn);

    const auto iterator = graph->GetValue().GetListIterator();
    for (size_t i = 0; i < length; i++) {
        if (i < offset) {
            continue;
        }
        NUdf::TUnboxedValue outputTuple;
        UNIT_ASSERT(iterator.Next(outputTuple));
        if (isNull[i]) {
            UNIT_ASSERT_EQUAL(outputTuple.GetElement(0).Get<ui32>(), i);
        } else {
            UNIT_ASSERT_EQUAL(outputTuple.GetElement(0).Get<ui32>(), rightValues[i]);
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockCoalesceTest) {

Y_UNIT_TEST(CoalesceGraphTest) {
    for (auto offset : {0, 7, 8, 11,6}) {
        BlockCoalesceGraphTest(32, offset);
    }
}

UNIT_TEST_WITH_INTEGER(KernelRightIsNotNullArray) {
    auto max = std::numeric_limits<typename NUdf::TDataType<TTestType>::TLayout>::max();
    auto min = std::numeric_limits<typename NUdf::TDataType<TTestType>::TLayout>::min();
    TestBlockCoalesce<TTestType, ERightOperandType::ARRAY>({Nothing(), 2, 3, Nothing(), 5, 6, 7, max, 9, Nothing(), 11, 12, 13, Nothing(), Nothing(), Nothing(), min, Nothing()},
                                                           {101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118},
                                                           {101, 2, 3, 104, 5, 6, 7, max, 9, 110, 11, 12, 13, 114, 115, 116, min, 118});
}

UNIT_TEST_WITH_INTEGER(KernelRightIsScalar) {
    auto max = std::numeric_limits<typename NUdf::TDataType<TTestType>::TLayout>::max();
    auto min = std::numeric_limits<typename NUdf::TDataType<TTestType>::TLayout>::min();

    TestBlockCoalesce<TTestType, ERightOperandType::SCALAR>({Nothing(), 2, 3, Nothing(), 5, 6, 7, max, 9, Nothing(), 11, 12, 13, Nothing(), Nothing(), Nothing(), min, Nothing()},
                                                            {77},
                                                            {77, 2, 3, 77, 5, 6, 7, max, 9, 77, 11, 12, 13, 77, 77, 77, min, 77});
}

UNIT_TEST_WITH_INTEGER(KernelRightIsOptionalArray) {
    auto max = std::numeric_limits<typename NUdf::TDataType<TTestType>::TLayout>::max();
    auto min = std::numeric_limits<typename NUdf::TDataType<TTestType>::TLayout>::min();

    TestBlockCoalesce<TTestType, ERightOperandType::OPTIONAL_ARRAY>({Nothing(), 2, 3, Nothing(), 5, 6, 7, max, 9, Nothing(), 11, 12, 13, Nothing(), Nothing(), Nothing(), min, Nothing()},
                                                                    {101, 102, Nothing(), 104, Nothing(), 106, 107, 108, 109, 110, 111, 112, 113, 114, Nothing(), 116, 117, 118},
                                                                    {101, 2, 3, 104, 5, 6, 7, max, 9, 110, 11, 12, 13, 114, Nothing(), 116, min, 118});
}

UNIT_TEST_WITH_INTEGER(KernelRightIsOptionalInvalidScalar) {
    auto max = std::numeric_limits<typename NUdf::TDataType<TTestType>::TLayout>::max();
    auto min = std::numeric_limits<typename NUdf::TDataType<TTestType>::TLayout>::min();

    TestBlockCoalesce<TTestType, ERightOperandType::OPTIONAL_SCALAR>({Nothing(), 2, 3, Nothing(), 5, 6, 7, max, 9, Nothing(), 11, 12, 13, Nothing(), Nothing(), Nothing(), min, Nothing()},
                                                                     {Nothing()},
                                                                     {Nothing(), 2, 3, Nothing(), 5, 6, 7, max, 9, Nothing(), 11, 12, 13, Nothing(), Nothing(), Nothing(), min, Nothing()});
}

UNIT_TEST_WITH_INTEGER(KernelRightIsOptionalValidScalar) {
    auto max = std::numeric_limits<typename NUdf::TDataType<TTestType>::TLayout>::max();
    auto min = std::numeric_limits<typename NUdf::TDataType<TTestType>::TLayout>::min();

    TestBlockCoalesce<TTestType, ERightOperandType::OPTIONAL_SCALAR>({Nothing(), 2, 3, Nothing(), 5, 6, 7, max, 9, Nothing(), 11, 12, 13, Nothing(), Nothing(), Nothing(), min, Nothing()},
                                                                     {77},
                                                                     {77, 2, 3, 77, 5, 6, 7, max, 9, 77, 11, 12, 13, 77, 77, 77, min, 77});
}

Y_UNIT_TEST(OptionalScalar) {
    TestCoalesceKernel(TMaybe<i32>{16}, 5, 16);
    TestCoalesceKernel(TMaybe<i32>(), 4, 4);
    TestCoalesceKernel(TMaybe<i32>(18), TMaybe<i32>(3), TMaybe<i32>(18));
    TestCoalesceKernel(TMaybe<i32>(), TMaybe<i32>(2), TMaybe<i32>(2));
}

Y_UNIT_TEST(Tuple) {
    using TTuple = std::tuple<ui32, ui64, bool>;
    TestCoalesceKernel(TMaybe<TTuple>({16, 13, false}), TMaybe<TTuple>({15, 11, true}), TMaybe<TTuple>({16, 13, false}));
    TestCoalesceKernel(TMaybe<TTuple>(), TMaybe<TTuple>({15, 11, true}), TMaybe<TTuple>({15, 11, true}));
    TestCoalesceKernel(TMaybe<TTuple>(), TTuple{15, 11, true}, TTuple{15, 11, true});
}

Y_UNIT_TEST(TestVectorAndScalar) {
    {
        std::vector<TMaybe<ui32>> left = {1, 2, Nothing(), 4};
        std::vector<ui32> right = {11, 22, 33, 44};
        std::vector<ui32> expected = {1, 2, 33, 4};
        TestCoalesceKernel(left, right, expected);
    }
    {
        std::vector<TMaybe<ui32>> left = {1, 2, Nothing(), 4};
        ui32 right = 333;
        std::vector<ui32> expected = {1, 2, 333, 4};
        TestCoalesceKernel(left, right, expected);
    }
    {
        TMaybe<ui32> left = 1;
        std::vector<ui32> right = {1111, 2222};
        std::vector<ui32> expected = {1, 1};
        TestCoalesceKernel(left, right, expected);
    }
    {
        TMaybe<ui32> left = Nothing();
        std::vector<ui32> right = {1111, 2222};
        std::vector<ui32> expected = {1111, 2222};
        TestCoalesceKernel(left, right, expected);
    }
}

Y_UNIT_TEST(ExternalOptionalScalar) {
    using TDoubleMaybe = TMaybe<TMaybe<i32>>;
    using TSingleMaybe = TMaybe<i32>;

    TestCoalesceKernel(TDoubleMaybe{TSingleMaybe{25}}, TSingleMaybe{1}, TSingleMaybe{25});
    TestCoalesceKernel(TDoubleMaybe(TSingleMaybe()), TSingleMaybe(9), TSingleMaybe());
    TestCoalesceKernel(TDoubleMaybe(), TSingleMaybe(8), TSingleMaybe(8));
    TestCoalesceKernel(TDoubleMaybe(TSingleMaybe(33)), TDoubleMaybe(TSingleMaybe(7)), TDoubleMaybe(TSingleMaybe(33)));
    TestCoalesceKernel(TDoubleMaybe(), TDoubleMaybe(TSingleMaybe(6)), TDoubleMaybe(TSingleMaybe(6)));
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockCoalesceTest)
} // namespace NKikimr::NMiniKQL
