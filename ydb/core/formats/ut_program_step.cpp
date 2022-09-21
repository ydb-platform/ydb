#include <array>
#include <memory>
#include <vector>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/exec.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>
#include <library/cpp/testing/unittest/registar.h>
#include "custom_registry.h"
#include "program.h"
#include "arrow_helpers.h"

namespace NKikimr::NArrow {

size_t FilterTest(std::vector<std::shared_ptr<arrow::Array>> args, EOperation frst, EOperation scnd) {
    auto schema = std::make_shared<arrow::Schema>(std::vector{
                                                    std::make_shared<arrow::Field>("x", args.at(0)->type()),
                                                    std::make_shared<arrow::Field>("y", args.at(1)->type()),
                                                    std::make_shared<arrow::Field>("z", args.at(2)->type())});
    auto rBatch = arrow::RecordBatch::Make(schema, 3, std::vector{args.at(0), args.at(1), args.at(2)});

    auto ps = std::make_shared<TProgramStep>();
    ps->Assignes = {TAssign("res1", frst, {"x", "y"}), TAssign("res2", scnd, {"res1", "z"})};
    ps->Filters = {"res2"};
    ps->Projection = {"res1", "res2"};
    UNIT_ASSERT(ApplyProgram(rBatch, {ps}, GetCustomExecContext()).ok());
    UNIT_ASSERT(rBatch->ValidateFull().ok());
    UNIT_ASSERT(rBatch->num_columns() == 2);
    return rBatch->num_rows();
}

size_t FilterTestUnary(std::vector<std::shared_ptr<arrow::Array>> args, EOperation frst, EOperation scnd) {
    auto schema = std::make_shared<arrow::Schema>(std::vector{
                                                    std::make_shared<arrow::Field>("x", args.at(0)->type()),
                                                    std::make_shared<arrow::Field>("z", args.at(1)->type())});
    auto rBatch = arrow::RecordBatch::Make(schema, 3, std::vector{args.at(0), args.at(1)});

    auto ps = std::make_shared<TProgramStep>();
    ps->Assignes = {TAssign("res1", frst, {"x"}), TAssign("res2", scnd, {"res1", "z"})};
    ps->Filters = {"res2"};
    ps->Projection = {"res1", "res2"};
    UNIT_ASSERT(ApplyProgram(rBatch, {ps}, GetCustomExecContext()).ok());
    UNIT_ASSERT(rBatch->ValidateFull().ok());
    UNIT_ASSERT(rBatch->num_columns() == 2);
    return rBatch->num_rows();
}


Y_UNIT_TEST_SUITE(ProgramStepTest) {
    Y_UNIT_TEST(ProgramStepRound0) {
        for (auto eop : {EOperation::Round, EOperation::RoundBankers, EOperation::RoundToExp2}) {
            auto x = NumVecToArray(arrow::float64(), {32.3, 12.5, 34.7});
            auto z = arrow::compute::CallFunction(GetFunctionName(eop), {x}, GetCustomExecContext());
            UNIT_ASSERT(FilterTestUnary({x, z->make_array()}, eop, EOperation::Equal) == 3);
        }
    }

    Y_UNIT_TEST(ProgramStepRound1) {
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

    Y_UNIT_TEST(ProgramStepAdd) {
        auto x = NumVecToArray(arrow::int32(), {10, 34, 8});
        auto y = NumVecToArray(arrow::int32(), {32, 12, 4});
        auto z = arrow::compute::CallFunction("add", {x, y});
        UNIT_ASSERT(FilterTest({x, y, z->make_array()}, EOperation::Add, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(ProgramStepSubstract) {
        auto x = NumVecToArray(arrow::int32(), {10, 34, 8});
        auto y = NumVecToArray(arrow::int32(), {32, 12, 4});
        auto z = arrow::compute::CallFunction("subtract", {x, y});
        UNIT_ASSERT(FilterTest({x, y, z->make_array()}, EOperation::Subtract, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(ProgramStepMultiply) {
        auto x = NumVecToArray(arrow::int32(), {10, 34, 8});
        auto y = NumVecToArray(arrow::int32(), {32, 12, 4});
        auto z = arrow::compute::CallFunction("multiply", {x, y});
        UNIT_ASSERT(FilterTest({x, y, z->make_array()}, EOperation::Multiply, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(ProgramStepDivide) {
        auto x = NumVecToArray(arrow::int32(), {10, 34, 8});
        auto y = NumVecToArray(arrow::int32(), {32, 12, 4});
        auto z = arrow::compute::CallFunction("divide", {x, y});
        UNIT_ASSERT(FilterTest({x, y, z->make_array()}, EOperation::Divide, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(ProgramStepGcd) {
        auto x = NumVecToArray(arrow::int32(), {64, 16, 8});
        auto y = NumVecToArray(arrow::int32(), {32, 12, 4});
        auto z = arrow::compute::CallFunction("gcd", {x, y}, GetCustomExecContext());
        UNIT_ASSERT(FilterTest({x, y, z->make_array()}, EOperation::Gcd, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(ProgramStepLcm) {
        auto x = NumVecToArray(arrow::int32(), {64, 16, 8});
        auto y = NumVecToArray(arrow::int32(), {32, 12, 4});
        auto z = arrow::compute::CallFunction("lcm", {x, y}, GetCustomExecContext());
        UNIT_ASSERT(FilterTest({x, y, z->make_array()}, EOperation::Lcm, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(ProgramStepMod) {
        auto x = NumVecToArray(arrow::int32(), {64, 16, 8});
        auto y = NumVecToArray(arrow::int32(), {3, 5, 2});
        auto z = arrow::compute::CallFunction("mod", {x, y}, GetCustomExecContext());
        UNIT_ASSERT(FilterTest({x, y, z->make_array()}, EOperation::Modulo, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(ProgramStepModOrZero) {
        auto x = NumVecToArray(arrow::int32(), {64, 16, 8});
        auto y = NumVecToArray(arrow::int32(), {3, 5, 0});
        auto z = arrow::compute::CallFunction("modOrZero", {x, y}, GetCustomExecContext());
        UNIT_ASSERT(FilterTest({x, y, z->make_array()}, EOperation::ModuloOrZero, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(ProgramStepAbs) {
        auto x = NumVecToArray(arrow::int32(), {-64, -16, 8});
        auto z = arrow::compute::CallFunction("abs", {x});
        UNIT_ASSERT(FilterTestUnary({x, z->make_array()}, EOperation::Abs, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(ProgramStepNegate) {
        auto x = NumVecToArray(arrow::int32(), {-64, -16, 8});
        auto z = arrow::compute::CallFunction("negate", {x});
        UNIT_ASSERT(FilterTestUnary({x, z->make_array()}, EOperation::Negate, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(ProgramStepCompares) {
        for (auto eop : {EOperation::Equal, EOperation::Less, EOperation::Greater, EOperation::GreaterEqual,
                         EOperation::LessEqual, EOperation::NotEqual}) {
            auto x = NumVecToArray(arrow::int32(), {64, 5, 1});
            auto y = NumVecToArray(arrow::int32(), {64, 1, 5});
            auto z = arrow::compute::CallFunction(GetFunctionName(eop), {x, y});
            UNIT_ASSERT(FilterTest({x, y, z->make_array()}, eop, EOperation::Equal) == 3);
        }
    }

    Y_UNIT_TEST(ProgramStepLogic0) {
        for (auto eop : {EOperation::And, EOperation::Or, EOperation::Xor}) {
            auto x = BoolVecToArray({true, false, false});
            auto y = BoolVecToArray({true, true, false});
            auto z = arrow::compute::CallFunction(GetFunctionName(eop), {x, y});
            UNIT_ASSERT(FilterTest({x, y, z->make_array()}, eop, EOperation::Equal) == 3);
        }
    }

    Y_UNIT_TEST(ProgramStepLogic1) {
        auto x = BoolVecToArray({true, false, false});
        auto z = arrow::compute::CallFunction("invert", {x});
        UNIT_ASSERT(FilterTestUnary({x, z->make_array()}, EOperation::Invert, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(ProgramStepScalarTest) {
        auto schema = std::make_shared<arrow::Schema>(std::vector{
                                                    std::make_shared<arrow::Field>("x", arrow::int64()),
                                                    std::make_shared<arrow::Field>("filter", arrow::boolean())});
        auto rBatch = arrow::RecordBatch::Make(schema, 4, std::vector{NumVecToArray(arrow::int64(), {64, 5, 1, 43}),
                                                                      BoolVecToArray({true, false, false, true})});
        auto ps = std::make_shared<TProgramStep>();
        ps->Assignes = {TAssign("y", 56), TAssign("res", EOperation::Add, {"x", "y"})};
        ps->Filters = {"filter"};
        ps->Projection = {"res", "filter"};
        UNIT_ASSERT(ApplyProgram(rBatch, {ps}, GetCustomExecContext()).ok());
        UNIT_ASSERT(rBatch->ValidateFull().ok());
        UNIT_ASSERT(rBatch->num_columns() == 2);
        UNIT_ASSERT(rBatch->num_rows() == 2);
    }

    Y_UNIT_TEST(ProgramStepEmptyFilter) {
        auto schema = std::make_shared<arrow::Schema>(std::vector{
                                                    std::make_shared<arrow::Field>("x", arrow::int64()),
                                                    std::make_shared<arrow::Field>("filter", arrow::boolean())});
        auto rBatch = arrow::RecordBatch::Make(schema, 4, std::vector{NumVecToArray(arrow::int64(), {64, 5, 1, 43}),
                                                                      BoolVecToArray({true, false, false, true})});
        auto ps = std::make_shared<TProgramStep>();
        ps->Assignes = {TAssign("y", 56), TAssign("res", EOperation::Add, {"x", "y"})};
        ps->Filters = {};
        ps->Projection = {"res", "filter"};
        UNIT_ASSERT(ApplyProgram(rBatch, {ps}, GetCustomExecContext()).ok());
        UNIT_ASSERT(rBatch->ValidateFull().ok());
        UNIT_ASSERT(rBatch->num_columns() == 2);
        UNIT_ASSERT(rBatch->num_rows() == 4);
    }
}

}
