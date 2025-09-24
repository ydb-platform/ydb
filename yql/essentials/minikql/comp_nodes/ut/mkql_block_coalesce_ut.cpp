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
    TBlockHelper().TestKernelFuzzied(left, right, expected,
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

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockCoalesceTest) {

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

// Test for String type
Y_UNIT_TEST(TestStringType) {
    // Test with mixed null/non-null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<TString>>{Nothing(), TString("hello"), TString("world")},
        std::vector<TString>{"default1", "default2", "default3"},
        std::vector<TString>{"default1", "hello", "world"});

    // Test with scalar right operand
    TestCoalesceKernel(
        std::vector<TMaybe<TString>>{Nothing(), TString("hello"), TString("world")},
        TString("default"),
        std::vector<TString>{"default", "hello", "world"});

    // Test with all non-null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<TString>>{TString("a"), TString("b"), TString("c")},
        std::vector<TString>{"default1", "default2", "default3"},
        std::vector<TString>{"a", "b", "c"});

    // Test with all null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<TString>>{Nothing(), Nothing(), Nothing()},
        std::vector<TString>{"default1", "default2", "default3"},
        std::vector<TString>{"default1", "default2", "default3"});

    // Test with both operands optional
    TestCoalesceKernel(
        std::vector<TMaybe<TString>>{Nothing(), TString("hello"), Nothing()},
        std::vector<TMaybe<TString>>{TString("default1"), Nothing(), Nothing()},
        std::vector<TMaybe<TString>>{TString("default1"), TString("hello"), Nothing()});

    // Test with scalar left operand and vector right operand
    TestCoalesceKernel(
        TMaybe<TString>{TString("constant")},
        std::vector<TString>{"a", "b", "c"},
        std::vector<TString>{"constant", "constant", "constant"});
}

// Test for Boolean type
Y_UNIT_TEST(TestBooleanType) {
    // Test with mixed null/non-null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<bool>>{Nothing(), true, false},
        std::vector<bool>{false, false, false},
        std::vector<bool>{false, true, false});

    // Test with scalar right operand
    TestCoalesceKernel(
        std::vector<TMaybe<bool>>{Nothing(), true, false},
        true,
        std::vector<bool>{true, true, false});

    // Test with all non-null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<bool>>{true, false, true},
        std::vector<bool>{false, true, false},
        std::vector<bool>{true, false, true});

    // Test with all null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<bool>>{Nothing(), Nothing(), Nothing()},
        std::vector<bool>{false, true, false},
        std::vector<bool>{false, true, false});

    // Test with both operands optional
    TestCoalesceKernel(
        std::vector<TMaybe<bool>>{Nothing(), true, Nothing()},
        std::vector<TMaybe<bool>>{true, Nothing(), false},
        std::vector<TMaybe<bool>>{true, true, false});

    // Test with scalar left operand and vector right operand
    TestCoalesceKernel(
        TMaybe<bool>{true},
        std::vector<bool>{false, false, false},
        std::vector<bool>{true, true, true});
}

// Test for Tagged types
Y_UNIT_TEST(TestTaggedType) {
    using TaggedIntType = TTagged<ui32, TTag::A>;

    // Test with mixed null/non-null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<TaggedIntType>>{Nothing(), TaggedIntType{1}, TaggedIntType{2}},
        std::vector<TaggedIntType>{TaggedIntType{10}, TaggedIntType{20}, TaggedIntType{30}},
        std::vector<TaggedIntType>{TaggedIntType{10}, TaggedIntType{1}, TaggedIntType{2}});

    // Test with scalar right operand
    TestCoalesceKernel(
        std::vector<TMaybe<TaggedIntType>>{Nothing(), TaggedIntType{1}, TaggedIntType{2}},
        TaggedIntType{99},
        std::vector<TaggedIntType>{TaggedIntType{99}, TaggedIntType{1}, TaggedIntType{2}});

    // Test with all non-null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<TaggedIntType>>{TaggedIntType{1}, TaggedIntType{2}, TaggedIntType{3}},
        std::vector<TaggedIntType>{TaggedIntType{10}, TaggedIntType{20}, TaggedIntType{30}},
        std::vector<TaggedIntType>{TaggedIntType{1}, TaggedIntType{2}, TaggedIntType{3}});

    // Test with all null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<TaggedIntType>>{Nothing(), Nothing(), Nothing()},
        std::vector<TaggedIntType>{TaggedIntType{10}, TaggedIntType{20}, TaggedIntType{30}},
        std::vector<TaggedIntType>{TaggedIntType{10}, TaggedIntType{20}, TaggedIntType{30}});

    // Test with optional tagged types - mixed null/non-null
    using OptTaggedIntType = TMaybe<TaggedIntType>;
    TestCoalesceKernel(
        std::vector<TMaybe<OptTaggedIntType>>{Nothing(), OptTaggedIntType{TaggedIntType{1}}, OptTaggedIntType{TaggedIntType{2}}},
        std::vector<OptTaggedIntType>{OptTaggedIntType{TaggedIntType{10}}, OptTaggedIntType{TaggedIntType{20}}, OptTaggedIntType{TaggedIntType{30}}},
        std::vector<OptTaggedIntType>{OptTaggedIntType{TaggedIntType{10}}, OptTaggedIntType{TaggedIntType{1}}, OptTaggedIntType{TaggedIntType{2}}});

    // Test with optional tagged types - all non-null
    TestCoalesceKernel(
        std::vector<TMaybe<OptTaggedIntType>>{OptTaggedIntType{TaggedIntType{1}}, OptTaggedIntType{TaggedIntType{2}}, OptTaggedIntType{TaggedIntType{3}}},
        std::vector<OptTaggedIntType>{OptTaggedIntType{TaggedIntType{10}}, OptTaggedIntType{TaggedIntType{20}}, OptTaggedIntType{TaggedIntType{30}}},
        std::vector<OptTaggedIntType>{OptTaggedIntType{TaggedIntType{1}}, OptTaggedIntType{TaggedIntType{2}}, OptTaggedIntType{TaggedIntType{3}}});

    // Test with optional tagged types - all null
    TestCoalesceKernel(
        std::vector<TMaybe<OptTaggedIntType>>{Nothing(), Nothing(), Nothing()},
        std::vector<OptTaggedIntType>{OptTaggedIntType{TaggedIntType{10}}, OptTaggedIntType{TaggedIntType{20}}, OptTaggedIntType{TaggedIntType{30}}},
        std::vector<OptTaggedIntType>{OptTaggedIntType{TaggedIntType{10}}, OptTaggedIntType{TaggedIntType{20}}, OptTaggedIntType{TaggedIntType{30}}});

    // Test with both operands optional
    TestCoalesceKernel(
        std::vector<TMaybe<TaggedIntType>>{Nothing(), TaggedIntType{5}, Nothing()},
        std::vector<TMaybe<TaggedIntType>>{TaggedIntType{50}, Nothing(), Nothing()},
        std::vector<TMaybe<TaggedIntType>>{TaggedIntType{50}, TaggedIntType{5}, Nothing()});

    // Test with scalar left operand and vector right operand
    TestCoalesceKernel(
        TMaybe<TaggedIntType>{TaggedIntType{42}},
        std::vector<TaggedIntType>{TaggedIntType{1}, TaggedIntType{2}, TaggedIntType{3}},
        std::vector<TaggedIntType>{TaggedIntType{42}, TaggedIntType{42}, TaggedIntType{42}});
}

Y_UNIT_TEST(TestSingularNullType) {
    // Test with mixed null/non-null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<TSingularNull>>{Nothing(), TSingularNull{}, TSingularNull{}},
        std::vector<TSingularNull>{TSingularNull{}, TSingularNull{}, TSingularNull{}},
        std::vector<TSingularNull>{TSingularNull{}, TSingularNull{}, TSingularNull{}});

    // Test with scalar right operand
    TestCoalesceKernel(
        std::vector<TMaybe<TSingularNull>>{Nothing(), TSingularNull{}, TSingularNull{}},
        TSingularNull{},
        std::vector<TSingularNull>{TSingularNull{}, TSingularNull{}, TSingularNull{}});

    // Test with all non-null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<TSingularNull>>{TSingularNull{}, TSingularNull{}, TSingularNull{}},
        std::vector<TSingularNull>{TSingularNull{}, TSingularNull{}, TSingularNull{}},
        std::vector<TSingularNull>{TSingularNull{}, TSingularNull{}, TSingularNull{}});

    // Test with all null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<TSingularNull>>{Nothing(), Nothing(), Nothing()},
        std::vector<TSingularNull>{TSingularNull{}, TSingularNull{}, TSingularNull{}},
        std::vector<TSingularNull>{TSingularNull{}, TSingularNull{}, TSingularNull{}});

    // Test with both operands optional
    TestCoalesceKernel(
        std::vector<TMaybe<TSingularNull>>{Nothing(), TSingularNull{}, Nothing()},
        std::vector<TMaybe<TSingularNull>>{TSingularNull{}, Nothing(), Nothing()},
        std::vector<TMaybe<TSingularNull>>{TSingularNull{}, TSingularNull{}, Nothing()});

    // Test with scalar left operand and vector right operand
    TestCoalesceKernel(
        TMaybe<TSingularNull>{TSingularNull{}},
        std::vector<TSingularNull>{TSingularNull{}, TSingularNull{}, TSingularNull{}},
        std::vector<TSingularNull>{TSingularNull{}, TSingularNull{}, TSingularNull{}});
}

// Test for PgInt type
Y_UNIT_TEST(TestPgIntType) {
    // Test with mixed null/non-null left operands

    TestCoalesceKernel(
        std::vector<TPgInt>{TPgInt(), TPgInt{1}, TPgInt{3}},
        std::vector<TPgInt>{TPgInt{10}, TPgInt{20}, TPgInt{30}},
        std::vector<TPgInt>{TPgInt{10}, TPgInt{1}, TPgInt{3}});

    TestCoalesceKernel(
        std::vector<TMaybe<TPgInt>>{Nothing(), TPgInt{1}, TPgInt{3}},
        std::vector<TPgInt>{TPgInt{10}, TPgInt{20}, TPgInt{30}},
        std::vector<TPgInt>{TPgInt{10}, TPgInt{1}, TPgInt{3}});

    // Test with scalar right operand
    TestCoalesceKernel(
        std::vector<TMaybe<TPgInt>>{Nothing(), TPgInt{1}, TPgInt{3}},
        TPgInt{99},
        std::vector<TPgInt>{TPgInt{99}, TPgInt{1}, TPgInt{3}});

    // Test with all non-null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<TPgInt>>{TPgInt{1}, TPgInt{2}, TPgInt{3}},
        std::vector<TPgInt>{TPgInt{10}, TPgInt{20}, TPgInt{30}},
        std::vector<TPgInt>{TPgInt{1}, TPgInt{2}, TPgInt{3}});

    // Test with all null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<TPgInt>>{Nothing(), Nothing(), Nothing()},
        std::vector<TPgInt>{TPgInt{10}, TPgInt{20}, TPgInt{30}},
        std::vector<TPgInt>{TPgInt{10}, TPgInt{20}, TPgInt{30}});

    // Test with both operands optional
    TestCoalesceKernel(
        std::vector<TMaybe<TPgInt>>{Nothing(), TPgInt{5}, Nothing()},
        std::vector<TMaybe<TPgInt>>{TPgInt{50}, Nothing(), Nothing()},
        std::vector<TMaybe<TPgInt>>{TPgInt{50}, TPgInt{5}, Nothing()});

    // Test with scalar left operand and vector right operand
    TestCoalesceKernel(
        TMaybe<TPgInt>{TPgInt{42}},
        std::vector<TPgInt>{TPgInt{1}, TPgInt{2}, TPgInt{3}},
        std::vector<TPgInt>{TPgInt{42}, TPgInt{42}, TPgInt{42}});
}

// Test for Double optional objects
Y_UNIT_TEST(TestDoubleOptionalType) {
    using DoubleOptInt = TMaybe<TMaybe<ui32>>;

    // Test with mixed null/non-null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<DoubleOptInt>>{Nothing(), DoubleOptInt{TMaybe<ui32>{1}}, DoubleOptInt{TMaybe<ui32>{3}}},
        std::vector<DoubleOptInt>{DoubleOptInt{TMaybe<ui32>{10}}, DoubleOptInt{TMaybe<ui32>{20}}, DoubleOptInt{TMaybe<ui32>{30}}},
        std::vector<DoubleOptInt>{DoubleOptInt{TMaybe<ui32>{10}}, DoubleOptInt{TMaybe<ui32>{1}}, DoubleOptInt{TMaybe<ui32>{3}}});

    // Test with scalar right operand
    TestCoalesceKernel(
        std::vector<TMaybe<DoubleOptInt>>{Nothing(), DoubleOptInt{TMaybe<ui32>{1}}, DoubleOptInt{TMaybe<ui32>{3}}},
        DoubleOptInt{TMaybe<ui32>{99}},
        std::vector<DoubleOptInt>{DoubleOptInt{TMaybe<ui32>{99}}, DoubleOptInt{TMaybe<ui32>{1}}, DoubleOptInt{TMaybe<ui32>{3}}});

    // Test with all non-null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<DoubleOptInt>>{DoubleOptInt{TMaybe<ui32>{1}}, DoubleOptInt{TMaybe<ui32>{2}}, DoubleOptInt{TMaybe<ui32>{3}}},
        std::vector<DoubleOptInt>{DoubleOptInt{TMaybe<ui32>{10}}, DoubleOptInt{TMaybe<ui32>{20}}, DoubleOptInt{TMaybe<ui32>{30}}},
        std::vector<DoubleOptInt>{DoubleOptInt{TMaybe<ui32>{1}}, DoubleOptInt{TMaybe<ui32>{2}}, DoubleOptInt{TMaybe<ui32>{3}}});

    // Test with all null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<DoubleOptInt>>{Nothing(), Nothing(), Nothing()},
        std::vector<DoubleOptInt>{DoubleOptInt{TMaybe<ui32>{10}}, DoubleOptInt{TMaybe<ui32>{20}}, DoubleOptInt{TMaybe<ui32>{30}}},
        std::vector<DoubleOptInt>{DoubleOptInt{TMaybe<ui32>{10}}, DoubleOptInt{TMaybe<ui32>{20}}, DoubleOptInt{TMaybe<ui32>{30}}});

    // Test with inner nulls
    TestCoalesceKernel(
        std::vector<TMaybe<DoubleOptInt>>{DoubleOptInt{Nothing()}, DoubleOptInt{TMaybe<ui32>{2}}, DoubleOptInt{Nothing()}},
        std::vector<DoubleOptInt>{DoubleOptInt{TMaybe<ui32>{10}}, DoubleOptInt{TMaybe<ui32>{20}}, DoubleOptInt{TMaybe<ui32>{30}}},
        std::vector<DoubleOptInt>{DoubleOptInt{Nothing()}, DoubleOptInt{TMaybe<ui32>{2}}, DoubleOptInt{Nothing()}});

    // Test with both operands optional
    TestCoalesceKernel(
        std::vector<TMaybe<DoubleOptInt>>{Nothing(), DoubleOptInt{TMaybe<ui32>{5}}, Nothing()},
        std::vector<TMaybe<DoubleOptInt>>{DoubleOptInt{TMaybe<ui32>{50}}, Nothing(), DoubleOptInt{Nothing()}},
        std::vector<TMaybe<DoubleOptInt>>{DoubleOptInt{TMaybe<ui32>{50}}, DoubleOptInt{TMaybe<ui32>{5}}, DoubleOptInt{Nothing()}});

    // Test with scalar left operand and vector right operand
    TestCoalesceKernel(
        TMaybe<DoubleOptInt>{DoubleOptInt{TMaybe<ui32>{42}}},
        std::vector<DoubleOptInt>{DoubleOptInt{TMaybe<ui32>{1}}, DoubleOptInt{TMaybe<ui32>{2}}, DoubleOptInt{TMaybe<ui32>{3}}},
        std::vector<DoubleOptInt>{DoubleOptInt{TMaybe<ui32>{42}}, DoubleOptInt{TMaybe<ui32>{42}}, DoubleOptInt{TMaybe<ui32>{42}}});
}

// Test for Optional objects of singular types
Y_UNIT_TEST(TestOptionalSingularType) {
    using OptVoid = TMaybe<TSingularVoid>;

    // Test with mixed null/non-null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<OptVoid>>{Nothing(), OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}},
        std::vector<OptVoid>{OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}},
        std::vector<OptVoid>{OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}});

    // Test with scalar right operand
    TestCoalesceKernel(
        std::vector<TMaybe<OptVoid>>{Nothing(), OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}},
        OptVoid{TSingularVoid{}},
        std::vector<OptVoid>{OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}});

    // Test with all non-null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<OptVoid>>{OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}},
        std::vector<OptVoid>{OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}},
        std::vector<OptVoid>{OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}});

    // Test with all null left operands
    TestCoalesceKernel(
        std::vector<TMaybe<OptVoid>>{Nothing(), Nothing(), Nothing()},
        std::vector<OptVoid>{OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}},
        std::vector<OptVoid>{OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}});

    // Test with inner nulls
    TestCoalesceKernel(
        std::vector<TMaybe<OptVoid>>{OptVoid{Nothing()}, OptVoid{TSingularVoid{}}, OptVoid{Nothing()}},
        std::vector<OptVoid>{OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}},
        std::vector<OptVoid>{OptVoid{Nothing()}, OptVoid{TSingularVoid{}}, OptVoid{Nothing()}});

    // Test with both operands optional
    TestCoalesceKernel(
        std::vector<TMaybe<OptVoid>>{Nothing(), OptVoid{TSingularVoid{}}, Nothing()},
        std::vector<TMaybe<OptVoid>>{OptVoid{TSingularVoid{}}, Nothing(), Nothing()},
        std::vector<TMaybe<OptVoid>>{OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}, Nothing()});

    // Test with scalar left operand and vector right operand
    TestCoalesceKernel(
        TMaybe<OptVoid>{OptVoid{TSingularVoid{}}},
        std::vector<OptVoid>{OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}},
        std::vector<OptVoid>{OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}, OptVoid{TSingularVoid{}}});
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockCoalesceTest)
} // namespace NKikimr::NMiniKQL
