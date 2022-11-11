#include <array>
#include <memory>
#include <vector>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/exec.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/arrow_kernels/ut_common.h>
#include "custom_registry.h"
#include "program.h"
#include "arrow_helpers.h"

using namespace NKikimr::NArrow;
using namespace NKikimr::NSsa;
using NKikimr::NKernels::NumVecToArray;

namespace NKikimr::NSsa {

size_t FilterTest(std::vector<std::shared_ptr<arrow::Array>> args, EOperation op1, EOperation op2) {
    auto schema = std::make_shared<arrow::Schema>(std::vector{
                                                    std::make_shared<arrow::Field>("x", args.at(0)->type()),
                                                    std::make_shared<arrow::Field>("y", args.at(1)->type()),
                                                    std::make_shared<arrow::Field>("z", args.at(2)->type())});
    auto batch = arrow::RecordBatch::Make(schema, 3, std::vector{args.at(0), args.at(1), args.at(2)});
    UNIT_ASSERT(batch->ValidateFull().ok());

    auto step = std::make_shared<TProgramStep>();
    step->Assignes = {TAssign("res1", op1, {"x", "y"}), TAssign("res2", op2, {"res1", "z"})};
    step->Filters = {"res2"};
    step->Projection = {"res1", "res2"};
    UNIT_ASSERT(ApplyProgram(batch, {step}, GetCustomExecContext()).ok());
    UNIT_ASSERT(batch->ValidateFull().ok());
    UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 2);
    return batch->num_rows();
}

size_t FilterTestUnary(std::vector<std::shared_ptr<arrow::Array>> args, EOperation op1, EOperation op2) {
    auto schema = std::make_shared<arrow::Schema>(std::vector{
                                                    std::make_shared<arrow::Field>("x", args.at(0)->type()),
                                                    std::make_shared<arrow::Field>("z", args.at(1)->type())});
    auto batch = arrow::RecordBatch::Make(schema, 3, std::vector{args.at(0), args.at(1)});
    UNIT_ASSERT(batch->ValidateFull().ok());

    auto step = std::make_shared<TProgramStep>();
    step->Assignes = {TAssign("res1", op1, {"x"}), TAssign("res2", op2, {"res1", "z"})};
    step->Filters = {"res2"};
    step->Projection = {"res1", "res2"};
    UNIT_ASSERT(ApplyProgram(batch, {step}, GetCustomExecContext()).ok());
    UNIT_ASSERT(batch->ValidateFull().ok());
    UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 2);
    return batch->num_rows();
}

void SumGroupBy(bool nullable, ui32 numKeys = 1) {
    auto schema = std::make_shared<arrow::Schema>(std::vector{
                                                std::make_shared<arrow::Field>("x", arrow::int16()),
                                                std::make_shared<arrow::Field>("y", arrow::uint32())});
    auto batch = arrow::RecordBatch::Make(schema, 4, std::vector{NumVecToArray(arrow::int16(), {-1, 1, 1, -1}),
                                                                    NumVecToArray(arrow::uint32(), {1, 2, 2, 1})});
    UNIT_ASSERT(batch->ValidateFull().ok());

    auto step = std::make_shared<TProgramStep>();
    step->GroupBy = {
        TAggregateAssign("sum_x", EAggregate::Sum, {"x"}),
        TAggregateAssign("sum_y", EAggregate::Sum, {"y"})
    };
    step->GroupByKeys.push_back("y");
    if (numKeys == 2) {
        step->GroupByKeys.push_back("x");
    }
    step->NullableGroupByKeys = nullable;

    UNIT_ASSERT(ApplyProgram(batch, {step}, GetCustomExecContext()).ok());
    UNIT_ASSERT(batch->ValidateFull().ok());
    UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), numKeys + 2);
    UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 2);
    UNIT_ASSERT_EQUAL(batch->column(0)->type_id(), arrow::Type::INT64);
    UNIT_ASSERT_EQUAL(batch->column(1)->type_id(), arrow::Type::UINT64);
    UNIT_ASSERT_EQUAL(batch->column(2)->type_id(), arrow::Type::UINT32);
    if (numKeys == 2) {
        UNIT_ASSERT_EQUAL(batch->column(3)->type_id(), arrow::Type::INT16);
    }

    ui32 row0 = 0;
    ui32 row1 = 1;
    if (static_cast<arrow::Int32Array&>(*batch->column(2)).Value(0) == 2) {
        std::swap(row0, row1);
    }
    UNIT_ASSERT_VALUES_EQUAL(static_cast<arrow::Int64Array&>(*batch->column(0)).Value(row0), -2);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<arrow::Int64Array&>(*batch->column(0)).Value(row1), 2);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<arrow::UInt64Array&>(*batch->column(1)).Value(row0), 2);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<arrow::UInt64Array&>(*batch->column(1)).Value(row1), 4);
}

}

Y_UNIT_TEST_SUITE(ProgramStep) {
    Y_UNIT_TEST(Round0) {
        for (auto eop : {EOperation::Round, EOperation::RoundBankers, EOperation::RoundToExp2}) {
            auto x = NumVecToArray(arrow::float64(), {32.3, 12.5, 34.7});
            auto z = arrow::compute::CallFunction(GetFunctionName(eop), {x}, GetCustomExecContext());
            UNIT_ASSERT(FilterTestUnary({x, z->make_array()}, eop, EOperation::Equal) == 3);
        }
    }

    Y_UNIT_TEST(Round1) {
        for (auto eop : {EOperation::Ceil, EOperation::Floor, EOperation::Trunc}) {
            auto x = NumVecToArray(arrow::float64(), {32.3, 12.5, 34.7});
            auto z = arrow::compute::CallFunction(GetFunctionName(eop), {x});
            UNIT_ASSERT(FilterTestUnary({x, z->make_array()}, eop, EOperation::Equal) == 3);
        }
    }

    Y_UNIT_TEST(Filter) {
        auto x = NumVecToArray(arrow::int32(), {10, 34, 8});
        auto y = NumVecToArray(arrow::uint32(), {10, 34, 8});
        auto z = NumVecToArray(arrow::int64(), {33, 70, 12});
        UNIT_ASSERT(FilterTest({x, y, z}, EOperation::Add, EOperation::Less) == 2);
    }

    Y_UNIT_TEST(Add) {
        auto x = NumVecToArray(arrow::int32(), {10, 34, 8});
        auto y = NumVecToArray(arrow::int32(), {32, 12, 4});
        auto z = arrow::compute::CallFunction("add", {x, y});
        UNIT_ASSERT(FilterTest({x, y, z->make_array()}, EOperation::Add, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Substract) {
        auto x = NumVecToArray(arrow::int32(), {10, 34, 8});
        auto y = NumVecToArray(arrow::int32(), {32, 12, 4});
        auto z = arrow::compute::CallFunction("subtract", {x, y});
        UNIT_ASSERT(FilterTest({x, y, z->make_array()}, EOperation::Subtract, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Multiply) {
        auto x = NumVecToArray(arrow::int32(), {10, 34, 8});
        auto y = NumVecToArray(arrow::int32(), {32, 12, 4});
        auto z = arrow::compute::CallFunction("multiply", {x, y});
        UNIT_ASSERT(FilterTest({x, y, z->make_array()}, EOperation::Multiply, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Divide) {
        auto x = NumVecToArray(arrow::int32(), {10, 34, 8});
        auto y = NumVecToArray(arrow::int32(), {32, 12, 4});
        auto z = arrow::compute::CallFunction("divide", {x, y});
        UNIT_ASSERT(FilterTest({x, y, z->make_array()}, EOperation::Divide, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Gcd) {
        auto x = NumVecToArray(arrow::int32(), {64, 16, 8});
        auto y = NumVecToArray(arrow::int32(), {32, 12, 4});
        auto z = arrow::compute::CallFunction("gcd", {x, y}, GetCustomExecContext());
        UNIT_ASSERT(FilterTest({x, y, z->make_array()}, EOperation::Gcd, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Lcm) {
        auto x = NumVecToArray(arrow::int32(), {64, 16, 8});
        auto y = NumVecToArray(arrow::int32(), {32, 12, 4});
        auto z = arrow::compute::CallFunction("lcm", {x, y}, GetCustomExecContext());
        UNIT_ASSERT(FilterTest({x, y, z->make_array()}, EOperation::Lcm, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Mod) {
        auto x = NumVecToArray(arrow::int32(), {64, 16, 8});
        auto y = NumVecToArray(arrow::int32(), {3, 5, 2});
        auto z = arrow::compute::CallFunction("mod", {x, y}, GetCustomExecContext());
        UNIT_ASSERT(FilterTest({x, y, z->make_array()}, EOperation::Modulo, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(ModOrZero) {
        auto x = NumVecToArray(arrow::int32(), {64, 16, 8});
        auto y = NumVecToArray(arrow::int32(), {3, 5, 0});
        auto z = arrow::compute::CallFunction("modOrZero", {x, y}, GetCustomExecContext());
        UNIT_ASSERT(FilterTest({x, y, z->make_array()}, EOperation::ModuloOrZero, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Abs) {
        auto x = NumVecToArray(arrow::int32(), {-64, -16, 8});
        auto z = arrow::compute::CallFunction("abs", {x});
        UNIT_ASSERT(FilterTestUnary({x, z->make_array()}, EOperation::Abs, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Negate) {
        auto x = NumVecToArray(arrow::int32(), {-64, -16, 8});
        auto z = arrow::compute::CallFunction("negate", {x});
        UNIT_ASSERT(FilterTestUnary({x, z->make_array()}, EOperation::Negate, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Compares) {
        for (auto eop : {EOperation::Equal, EOperation::Less, EOperation::Greater, EOperation::GreaterEqual,
                         EOperation::LessEqual, EOperation::NotEqual}) {
            auto x = NumVecToArray(arrow::int32(), {64, 5, 1});
            auto y = NumVecToArray(arrow::int32(), {64, 1, 5});
            auto z = arrow::compute::CallFunction(GetFunctionName(eop), {x, y});
            UNIT_ASSERT(FilterTest({x, y, z->make_array()}, eop, EOperation::Equal) == 3);
        }
    }

    Y_UNIT_TEST(Logic0) {
        for (auto eop : {EOperation::And, EOperation::Or, EOperation::Xor}) {
            auto x = BoolVecToArray({true, false, false});
            auto y = BoolVecToArray({true, true, false});
            auto z = arrow::compute::CallFunction(GetFunctionName(eop), {x, y});
            UNIT_ASSERT(FilterTest({x, y, z->make_array()}, eop, EOperation::Equal) == 3);
        }
    }

    Y_UNIT_TEST(Logic1) {
        auto x = BoolVecToArray({true, false, false});
        auto z = arrow::compute::CallFunction("invert", {x});
        UNIT_ASSERT(FilterTestUnary({x, z->make_array()}, EOperation::Invert, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(ScalarTest) {
        auto schema = std::make_shared<arrow::Schema>(std::vector{
                                                    std::make_shared<arrow::Field>("x", arrow::int64()),
                                                    std::make_shared<arrow::Field>("filter", arrow::boolean())});
        auto batch = arrow::RecordBatch::Make(schema, 4, std::vector{NumVecToArray(arrow::int64(), {64, 5, 1, 43}),
                                                                     BoolVecToArray({true, false, false, true})});
        UNIT_ASSERT(batch->ValidateFull().ok());

        auto step = std::make_shared<TProgramStep>();
        step->Assignes = {TAssign("y", 56), TAssign("res", EOperation::Add, {"x", "y"})};
        step->Filters = {"filter"};
        step->Projection = {"res", "filter"};
        UNIT_ASSERT(ApplyProgram(batch, {step}, GetCustomExecContext()).ok());
        UNIT_ASSERT(batch->ValidateFull().ok());
        UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 2);
        UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 2);
    }

    Y_UNIT_TEST(Projection) {
        auto schema = std::make_shared<arrow::Schema>(std::vector{
                                                    std::make_shared<arrow::Field>("x", arrow::int64()),
                                                    std::make_shared<arrow::Field>("y", arrow::boolean())});
        auto batch = arrow::RecordBatch::Make(schema, 4, std::vector{NumVecToArray(arrow::int64(), {64, 5, 1, 43}),
                                                                     BoolVecToArray({true, false, false, true})});
        UNIT_ASSERT(batch->ValidateFull().ok());

        auto step = std::make_shared<TProgramStep>();
        step->Projection = {"x"};
        UNIT_ASSERT(ApplyProgram(batch, {step}, GetCustomExecContext()).ok());
        UNIT_ASSERT(batch->ValidateFull().ok());
        UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 1);
        UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 4);
    }

    Y_UNIT_TEST(MinMax) {
        auto tsType = arrow::timestamp(arrow::TimeUnit::MICRO);


        auto schema = std::make_shared<arrow::Schema>(std::vector{
                                                    std::make_shared<arrow::Field>("x", arrow::int16()),
                                                    std::make_shared<arrow::Field>("y", tsType)});
        auto batch = arrow::RecordBatch::Make(schema, 4, std::vector{NumVecToArray(arrow::int16(), {1, 0, -1, 2}),
                                                                     NumVecToArray(tsType, {1, 4, 2, 3})});
        UNIT_ASSERT(batch->ValidateFull().ok());

        auto step = std::make_shared<TProgramStep>();
        step->GroupBy = {
            TAggregateAssign("min_x", EAggregate::Min, {"x"}),
            TAggregateAssign("max_y", EAggregate::Max, {"y"})
        };
        UNIT_ASSERT(ApplyProgram(batch, {step}, GetCustomExecContext()).ok());
        UNIT_ASSERT(batch->ValidateFull().ok());
        UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 2);
        UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
        UNIT_ASSERT_EQUAL(batch->column(0)->type_id(), arrow::Type::INT16);
        UNIT_ASSERT_EQUAL(batch->column(1)->type_id(), arrow::Type::TIMESTAMP);

        UNIT_ASSERT_VALUES_EQUAL(static_cast<arrow::Int16Array&>(*batch->column(0)).Value(0), -1);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<arrow::TimestampArray&>(*batch->column(1)).Value(0), 4);
    }

    Y_UNIT_TEST(Sum) {
        auto schema = std::make_shared<arrow::Schema>(std::vector{
                                                    std::make_shared<arrow::Field>("x", arrow::int16()),
                                                    std::make_shared<arrow::Field>("y", arrow::uint32())});
        auto batch = arrow::RecordBatch::Make(schema, 4, std::vector{NumVecToArray(arrow::int16(), {-1, 0, 1, 2}),
                                                                     NumVecToArray(arrow::uint32(), {1, 2, 3, 4})});
        UNIT_ASSERT(batch->ValidateFull().ok());

        auto step = std::make_shared<TProgramStep>();
        step->GroupBy = {
            TAggregateAssign("sum_x", EAggregate::Sum, {"x"}),
            TAggregateAssign("sum_y", EAggregate::Sum, {"y"})
        };
        UNIT_ASSERT(ApplyProgram(batch, {step}, GetCustomExecContext()).ok());
        UNIT_ASSERT(batch->ValidateFull().ok());
        UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 2);
        UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
        UNIT_ASSERT_EQUAL(batch->column(0)->type_id(), arrow::Type::INT64);
        UNIT_ASSERT_EQUAL(batch->column(1)->type_id(), arrow::Type::UINT64);

        UNIT_ASSERT_VALUES_EQUAL(static_cast<arrow::Int64Array&>(*batch->column(0)).Value(0), 2);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<arrow::UInt64Array&>(*batch->column(1)).Value(0), 10);
    }

    Y_UNIT_TEST(SumGroupBy) {
        SumGroupBy(true);
        SumGroupBy(true, 2);
    }

    Y_UNIT_TEST(SumGroupByNotNull) {
        SumGroupBy(false);
        SumGroupBy(false, 2);
    }
}
