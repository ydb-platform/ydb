#include <array>
#include <memory>
#include <vector>

#include <ydb/core/formats/arrow/custom_registry.h>
#include <ydb/core/formats/arrow/program.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/library/arrow_kernels/ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/exec.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

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
    auto res1Info = TColumnInfo::Generated(3, "res1");
    auto res2Info = TColumnInfo::Generated(3, "res2");
    auto xInfo = TColumnInfo::Original(0, "x");
    auto yInfo = TColumnInfo::Original(1, "y");
    auto zInfo = TColumnInfo::Original(2, "z");
    step->AddAssigne(TAssign(res1Info, op1, {xInfo, yInfo}));
    step->AddAssigne(TAssign(res2Info, op2, {res1Info, zInfo}));
    step->AddFilter(res2Info);
    step->AddProjection(res1Info);
    step->AddProjection(res2Info);
    UNIT_ASSERT(ApplyProgram(batch, TProgram({step}), GetCustomExecContext()).ok());
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
    auto res1Info = TColumnInfo::Generated(3, "res1");
    auto res2Info = TColumnInfo::Generated(3, "res2");
    auto xInfo = TColumnInfo::Original(0, "x");
    auto zInfo = TColumnInfo::Original(1, "z");

    step->AddAssigne(TAssign(res1Info, op1, {xInfo}));
    step->AddAssigne(TAssign(res2Info, op2, {res1Info, zInfo}));
    step->AddFilter(res2Info);
    step->AddProjection(res1Info);
    step->AddProjection(res2Info);
    auto status = ApplyProgram(batch, TProgram({step}), GetCustomExecContext());
    if (!status.ok()) {
        Cerr << status.ToString() << "\n";
    }
    UNIT_ASSERT(status.ok());
    UNIT_ASSERT(batch->ValidateFull().ok());
    UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 2);
    return batch->num_rows();
}

std::vector<bool> LikeTest(const std::vector<std::string>& data,
                           EOperation op, const std::string& pattern,
                           std::shared_ptr<arrow::DataType> type = arrow::utf8(), bool ignoreCase = false)
{
    auto schema = std::make_shared<arrow::Schema>(std::vector{
                                                std::make_shared<arrow::Field>("x", type)});
    std::shared_ptr<arrow::RecordBatch> batch;
    if (type->id() == arrow::utf8()->id()) {
        arrow::StringBuilder sb;
        sb.AppendValues(data).ok();
        batch = arrow::RecordBatch::Make(schema, data.size(), {*sb.Finish()});
    } else if (type->id() == arrow::binary()->id()) {
        arrow::BinaryBuilder sb;
        sb.AppendValues(data).ok();
        batch = arrow::RecordBatch::Make(schema, data.size(), {*sb.Finish()});
    }
    UNIT_ASSERT(batch->ValidateFull().ok());

    auto step = std::make_shared<TProgramStep>();

    auto resInfo = TColumnInfo::Generated(1, "res");
    auto xInfo = TColumnInfo::Original(0, "x");

    step->AddAssigne(TAssign(resInfo, op, {xInfo}, std::make_shared<arrow::compute::MatchSubstringOptions>(pattern, ignoreCase)));
    step->AddProjection(resInfo);
    auto status = ApplyProgram(batch, TProgram({step}), GetCustomExecContext());
    if (!status.ok()) {
        Cerr << status.ToString() << "\n";
    }
    UNIT_ASSERT(status.ok());
    UNIT_ASSERT(batch->ValidateFull().ok());
    UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 1);

    auto& resColumn = static_cast<const arrow::BooleanArray&>(*batch->GetColumnByName("res"));
    std::vector<bool> vec;
    for (int i = 0; i < resColumn.length(); ++i) {
        UNIT_ASSERT(!resColumn.IsNull(i)); // TODO
        vec.push_back(resColumn.Value(i));
    }
    return vec;
}

enum class ETest {
    DEFAULT,
    EMPTY,
    ONE_VALUE
};

struct TSumData {
    static std::shared_ptr<arrow::RecordBatch> Data(ETest test,
                                                    std::shared_ptr<arrow::Schema>& schema,
                                                    bool nullable)
    {
        std::optional<double> null;
        if (nullable) {
            null = 0;
        }

        if (test == ETest::DEFAULT) {
            return arrow::RecordBatch::Make(schema, 4, std::vector{NumVecToArray(arrow::int16(), {-1, 0, 0, -1}, null),
                                                                   NumVecToArray(arrow::uint32(), {1, 0, 0, 1}, null)});
        } else if (test == ETest::EMPTY) {
            return arrow::RecordBatch::Make(schema, 0, std::vector{NumVecToArray(arrow::int16(), {}),
                                                                   NumVecToArray(arrow::uint32(), {})});
        } else if (test == ETest::ONE_VALUE) {
            return arrow::RecordBatch::Make(schema, 1, std::vector{NumVecToArray(arrow::int16(), {1}),
                                                                   NumVecToArray(arrow::uint32(), {0}, null)});
        }
        return {};
    }

    static void CheckResult(ETest test, const std::shared_ptr<arrow::RecordBatch>& batch, ui32 numKeys, bool nullable) {
        UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), numKeys + 2);
        UNIT_ASSERT_EQUAL(batch->column(0)->type_id(), arrow::Type::INT64);
        UNIT_ASSERT_EQUAL(batch->column(1)->type_id(), arrow::Type::UINT64);
        UNIT_ASSERT_EQUAL(batch->column(2)->type_id(), arrow::Type::INT16);
        if (numKeys == 2) {
            UNIT_ASSERT_EQUAL(batch->column(3)->type_id(), arrow::Type::UINT32);
        }

        if (test == ETest::EMPTY) {
            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 0);
            return;
        }

        auto& aggX = static_cast<arrow::Int64Array&>(*batch->column(0));
        auto& aggY = static_cast<arrow::UInt64Array&>(*batch->column(1));
        auto& colX = static_cast<arrow::Int16Array&>(*batch->column(2));

        if (test == ETest::ONE_VALUE) {
            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);

            UNIT_ASSERT_VALUES_EQUAL(aggX.Value(0), 1);
            if (nullable) {
                UNIT_ASSERT(aggY.IsNull(0));
            } else {
                UNIT_ASSERT(!aggY.IsNull(0));
                UNIT_ASSERT_VALUES_EQUAL(aggY.Value(0), 0);
            }
            return;
        }

        UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 2);

        for (ui32 row = 0; row < 2; ++row) {
            if (colX.IsNull(row)) {
                UNIT_ASSERT(aggX.IsNull(row));
                UNIT_ASSERT(aggY.IsNull(row));
            } else {
                UNIT_ASSERT(!aggX.IsNull(row));
                UNIT_ASSERT(!aggY.IsNull(row));
                if (colX.Value(row) == 0) {
                    UNIT_ASSERT_VALUES_EQUAL(aggX.Value(row), 0);
                    UNIT_ASSERT_VALUES_EQUAL(aggY.Value(row), 0);
                } else if (colX.Value(row) == -1) {
                    UNIT_ASSERT_VALUES_EQUAL(aggX.Value(row), -2);
                    UNIT_ASSERT_VALUES_EQUAL(aggY.Value(row), 2);
                } else {
                    UNIT_ASSERT(false);
                }
            }
        }
    }
};

struct TMinMaxSomeData {
    static std::shared_ptr<arrow::RecordBatch> Data(ETest /*test*/,
                                                    std::shared_ptr<arrow::Schema>& schema,
                                                    bool nullable)
    {
        std::optional<double> null;
        if (nullable) {
            null = 0;
        }

        return arrow::RecordBatch::Make(schema, 1, std::vector{NumVecToArray(arrow::int16(), {1}),
                                                               NumVecToArray(arrow::uint32(), {0}, null)});
    }

    static void CheckResult(ETest /*test*/, const std::shared_ptr<arrow::RecordBatch>& batch, ui32 numKeys,
                            bool nullable) {
        UNIT_ASSERT_VALUES_EQUAL(numKeys, 1);

        UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), numKeys + 2);
        UNIT_ASSERT_EQUAL(batch->column(0)->type_id(), arrow::Type::INT16);
        UNIT_ASSERT_EQUAL(batch->column(1)->type_id(), arrow::Type::UINT32);
        UNIT_ASSERT_EQUAL(batch->column(2)->type_id(), arrow::Type::INT16);

        auto& aggX = static_cast<arrow::Int16Array&>(*batch->column(0));
        auto& aggY = static_cast<arrow::UInt32Array&>(*batch->column(1));
        auto& colX = static_cast<arrow::Int16Array&>(*batch->column(2));

        UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);

        UNIT_ASSERT_VALUES_EQUAL(colX.Value(0), 1);
        UNIT_ASSERT_VALUES_EQUAL(aggX.Value(0), 1);
        if (nullable) {
            UNIT_ASSERT(aggY.IsNull(0));
        } else {
            UNIT_ASSERT(!aggY.IsNull(0));
            UNIT_ASSERT_VALUES_EQUAL(aggY.Value(0), 0);
        }
        return;
    }
};

void GroupByXY(bool nullable, ui32 numKeys, ETest test = ETest::DEFAULT,
               EAggregate aggFunc = EAggregate::Sum) {
    auto schema = std::make_shared<arrow::Schema>(std::vector{
                                                std::make_shared<arrow::Field>("x", arrow::int16()),
                                                std::make_shared<arrow::Field>("y", arrow::uint32())});

    std::shared_ptr<arrow::RecordBatch> batch;
    switch (aggFunc) {
        case EAggregate::Sum:
            batch = TSumData::Data(test, schema, nullable);
            break;
        case EAggregate::Min:
        case EAggregate::Max:
        case EAggregate::Some:
            batch = TMinMaxSomeData::Data(test, schema, nullable);
            break;
        default:
            break;
    }
    UNIT_ASSERT(batch);
    auto status = batch->ValidateFull();
    if (!status.ok()) {
        Cerr << status.ToString() << "\n";
    }
    UNIT_ASSERT(status.ok());

    auto step = std::make_shared<TProgramStep>();

    auto xInfo = TColumnInfo::Original(0, "x");
    auto yInfo = TColumnInfo::Original(1, "y");

    auto aggXInfo = TColumnInfo::Generated(2, "agg_x");
    auto aggYInfo = TColumnInfo::Generated(3, "agg_y");

    step->AddGroupBy(TAggregateAssign(aggXInfo, aggFunc, xInfo));
    step->AddGroupBy(TAggregateAssign(aggYInfo, aggFunc, yInfo));
    step->AddGroupByKeys(xInfo);
    if (numKeys == 2) {
        step->AddGroupByKeys(yInfo);
    }

    status = ApplyProgram(batch, TProgram({step}), GetCustomExecContext());
    if (!status.ok()) {
        Cerr << status.ToString() << "\n";
    }
    UNIT_ASSERT(status.ok());

    status = batch->ValidateFull();
    if (!status.ok()) {
        Cerr << status.ToString() << "\n";
    }
    UNIT_ASSERT(status.ok());

    switch (aggFunc) {
        case EAggregate::Sum:
            TSumData::CheckResult(test, batch, numKeys, nullable);
            break;
        case EAggregate::Min:
        case EAggregate::Max:
        case EAggregate::Some:
            TMinMaxSomeData::CheckResult(test, batch, numKeys, nullable);
            break;
        default:
            break;
    }
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

    Y_UNIT_TEST(StartsWith) {
        for (auto type : {arrow::utf8() /*, arrow::binary()*/}) {
            std::vector<bool> res = LikeTest({"aa", "abaaba", "baa", ""}, EOperation::StartsWith, "aa", type);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(res[0], true);
            UNIT_ASSERT_VALUES_EQUAL(res[1], false);
            UNIT_ASSERT_VALUES_EQUAL(res[2], false);
            UNIT_ASSERT_VALUES_EQUAL(res[3], false);
        }
    }

    Y_UNIT_TEST(EndsWith) {
        for (auto type : {arrow::utf8() /*, arrow::binary()*/}) {
            std::vector<bool> res = LikeTest({"aa", "abaaba", "baa", ""}, EOperation::EndsWith, "aa", type);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(res[0], true);
            UNIT_ASSERT_VALUES_EQUAL(res[1], false);
            UNIT_ASSERT_VALUES_EQUAL(res[2], true);
            UNIT_ASSERT_VALUES_EQUAL(res[3], false);
        }
    }

    Y_UNIT_TEST(MatchSubstring) {
        for (auto type : {arrow::utf8() /*, arrow::binary()*/}) {
            std::vector<bool> res = LikeTest({"aa", "abaaba", "baa", ""}, EOperation::MatchSubstring, "aa", type);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(res[0], true);
            UNIT_ASSERT_VALUES_EQUAL(res[1], true);
            UNIT_ASSERT_VALUES_EQUAL(res[2], true);
            UNIT_ASSERT_VALUES_EQUAL(res[3], false);
        }
    }

    Y_UNIT_TEST(StartsWithIgnoreCase) {
        for (auto type : {arrow::utf8() /*, arrow::binary()*/}) {
            std::vector<bool> res = LikeTest({"Aa", "abAaba", "baA", ""}, EOperation::StartsWith, "aA", type, true);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(res[0], true);
            UNIT_ASSERT_VALUES_EQUAL(res[1], false);
            UNIT_ASSERT_VALUES_EQUAL(res[2], false);
            UNIT_ASSERT_VALUES_EQUAL(res[3], false);
        }
    }

    Y_UNIT_TEST(EndsWithIgnoreCase) {
        for (auto type : {arrow::utf8() /*, arrow::binary()*/}) {
            std::vector<bool> res = LikeTest({"Aa", "abAaba", "baA", ""}, EOperation::EndsWith, "aA", type, true);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(res[0], true);
            UNIT_ASSERT_VALUES_EQUAL(res[1], false);
            UNIT_ASSERT_VALUES_EQUAL(res[2], true);
            UNIT_ASSERT_VALUES_EQUAL(res[3], false);
        }
    }

    Y_UNIT_TEST(MatchSubstringIgnoreCase) {
        for (auto type : {arrow::utf8() /*, arrow::binary()*/}) {
            std::vector<bool> res = LikeTest({"Aa", "abAaba", "baA", ""}, EOperation::MatchSubstring, "aA", type, true);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(res[0], true);
            UNIT_ASSERT_VALUES_EQUAL(res[1], true);
            UNIT_ASSERT_VALUES_EQUAL(res[2], true);
            UNIT_ASSERT_VALUES_EQUAL(res[3], false);
        }
    }

    Y_UNIT_TEST(ScalarTest) {
        auto schema = std::make_shared<arrow::Schema>(std::vector{
                                                    std::make_shared<arrow::Field>("x", arrow::int64()),
                                                    std::make_shared<arrow::Field>("filter", arrow::boolean())});
        auto batch = arrow::RecordBatch::Make(schema, 4, std::vector{NumVecToArray(arrow::int64(), {64, 5, 1, 43}),
                                                                     BoolVecToArray({true, false, false, true})});
        UNIT_ASSERT(batch->ValidateFull().ok());

        auto step = std::make_shared<TProgramStep>();

        auto xInfo = TColumnInfo::Original(0, "x");
        auto yInfo = TColumnInfo::Generated(1, "y");

        auto filterInfo = TColumnInfo::Generated(2, "filter");
        auto resInfo = TColumnInfo::Generated(3, "res");

        step->AddAssigne(TAssign(yInfo, 56));
        step->AddAssigne(TAssign(resInfo, EOperation::Add, {xInfo, yInfo}));
        step->AddFilter(filterInfo);
        step->AddProjection(filterInfo);
        step->AddProjection(resInfo);
        UNIT_ASSERT(ApplyProgram(batch, TProgram({step}), GetCustomExecContext()).ok());
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

        auto xInfo = TColumnInfo::Original(0, "x");

        auto step = std::make_shared<TProgramStep>();
        step->AddProjection(xInfo);
        UNIT_ASSERT(ApplyProgram(batch, TProgram({step}), GetCustomExecContext()).ok());
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

        auto minXInfo = TColumnInfo::Generated(2, "min_x");
        auto maxYInfo = TColumnInfo::Generated(3, "max_y");
        auto xInfo = TColumnInfo::Original(0, "x");
        auto yInfo = TColumnInfo::Original(1, "y");

        step->AddGroupBy(TAggregateAssign(minXInfo, EAggregate::Min, {xInfo}));
        step->AddGroupBy(TAggregateAssign(maxYInfo, EAggregate::Max, {yInfo}));
        UNIT_ASSERT(ApplyProgram(batch, TProgram({step}), GetCustomExecContext()).ok());
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

        auto sumXInfo = TColumnInfo::Generated(2, "sum_x");
        auto sumYInfo = TColumnInfo::Generated(3, "sum_y");
        auto xInfo = TColumnInfo::Original(0, "x");
        auto yInfo = TColumnInfo::Original(1, "y");

        step->AddGroupBy(TAggregateAssign(sumXInfo, EAggregate::Sum, {xInfo}));
        step->AddGroupBy(TAggregateAssign(sumYInfo, EAggregate::Sum, {yInfo}));
        UNIT_ASSERT(ApplyProgram(batch, TProgram({step}), GetCustomExecContext()).ok());
        UNIT_ASSERT(batch->ValidateFull().ok());
        UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 2);
        UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
        UNIT_ASSERT_EQUAL(batch->column(0)->type_id(), arrow::Type::INT64);
        UNIT_ASSERT_EQUAL(batch->column(1)->type_id(), arrow::Type::UINT64);

        UNIT_ASSERT_VALUES_EQUAL(static_cast<arrow::Int64Array&>(*batch->column(0)).Value(0), 2);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<arrow::UInt64Array&>(*batch->column(1)).Value(0), 10);
    }

    Y_UNIT_TEST(SumGroupBy) {
        GroupByXY(true, 1);
        GroupByXY(true, 2);

        GroupByXY(true, 1, ETest::EMPTY);
        GroupByXY(true, 2, ETest::EMPTY);

        GroupByXY(true, 1, ETest::ONE_VALUE);
    }

    Y_UNIT_TEST(SumGroupByNotNull) {
        GroupByXY(false, 1);
        GroupByXY(false, 2);

        GroupByXY(false, 1, ETest::EMPTY);
        GroupByXY(false, 2, ETest::EMPTY);

        GroupByXY(false, 1, ETest::ONE_VALUE);
    }

    Y_UNIT_TEST(MinMaxSomeGroupBy) {
        GroupByXY(true, 1, ETest::ONE_VALUE, EAggregate::Min);
        GroupByXY(true, 1, ETest::ONE_VALUE, EAggregate::Max);
        GroupByXY(true, 1, ETest::ONE_VALUE, EAggregate::Some);
    }

    Y_UNIT_TEST(MinMaxSomeGroupByNotNull) {
        GroupByXY(false, 1, ETest::ONE_VALUE, EAggregate::Min);
        GroupByXY(false, 1, ETest::ONE_VALUE, EAggregate::Max);
        GroupByXY(false, 1, ETest::ONE_VALUE, EAggregate::Some);
    }
}
