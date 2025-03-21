#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/program/aggr_keys.h>
#include <ydb/core/formats/arrow/program/assign_const.h>
#include <ydb/core/formats/arrow/program/assign_internal.h>
#include <ydb/core/formats/arrow/program/collection.h>
#include <ydb/core/formats/arrow/program/custom_registry.h>
#include <ydb/core/formats/arrow/program/execution.h>
#include <ydb/core/formats/arrow/program/filter.h>
#include <ydb/core/formats/arrow/program/functions.h>
#include <ydb/core/formats/arrow/program/graph_execute.h>
#include <ydb/core/formats/arrow/program/graph_optimization.h>
#include <ydb/core/formats/arrow/program/projection.h>
#include <ydb/core/formats/arrow/program/stream_logic.h>

#include <ydb/library/arrow_kernels/operations.h>
#include <ydb/library/arrow_kernels/ut_common.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/exec.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>
#include <library/cpp/testing/unittest/registar.h>

#include <array>
#include <memory>
#include <vector>

using namespace NKikimr::NArrow;
using NKikimr::NKernels::NumVecToArray;
using EOperation = NKikimr::NKernels::EOperation;
using EAggregate = NKikimr::NArrow::NSSA::NAggregation::EAggregate;
using namespace NKikimr::NArrow::NSSA;
using namespace NKikimr::NArrow::NSSA::NGraph;

enum class ETest {
    DEFAULT,
    EMPTY,
    ONE_VALUE
};

size_t FilterTest(const std::vector<std::shared_ptr<arrow::Array>>& args, const EOperation op1, const EOperation op2) {
    auto schema = std::make_shared<arrow::Schema>(std::vector{ std::make_shared<arrow::Field>("x", args.at(0)->type()),
        std::make_shared<arrow::Field>("y", args.at(1)->type()), std::make_shared<arrow::Field>("z", args.at(2)->type()) });
    TSchemaColumnResolver resolver(schema);
    NOptimization::TGraph::TBuilder builder(resolver);
    builder.Add(TCalculationProcessor::Build(TColumnChainInfo::BuildVector({1, 2}), TColumnChainInfo(4), std::make_shared<TSimpleFunction>(op1)).DetachResult());
    builder.Add(TCalculationProcessor::Build(TColumnChainInfo::BuildVector({4, 3}), TColumnChainInfo(5), std::make_shared<TSimpleFunction>(op2)).DetachResult());
    builder.Add(std::make_shared<TFilterProcessor>(TColumnChainInfo(5)));
    builder.Add(std::make_shared<TProjectionProcessor>(TColumnChainInfo::BuildVector({ 4, 5 })));
    auto chain = builder.Finish().DetachResult();
    auto sds = std::make_shared<TSimpleDataSource>();

    ui32 idx = 1;
    for (auto&& i : args) {
        sds->AddBlob(idx, "", i);
        ++idx;
    }

    chain->Apply(sds, sds->GetResources()).Validate();
    AFL_VERIFY(sds->GetResources()->GetColumnsCount() == 2)("count", sds->GetResources()->GetColumnsCount());
    return sds->GetResources()->GetRecordsCountVerified();
}

size_t FilterTestUnary(std::vector<std::shared_ptr<arrow::Array>> args, const EOperation op1, const EOperation op2) {
    auto schema = std::make_shared<arrow::Schema>(
        std::vector{ std::make_shared<arrow::Field>("x", args.at(0)->type()), std::make_shared<arrow::Field>("z", args.at(1)->type()) });
    TSchemaColumnResolver resolver(schema);

    auto sds = std::make_shared<TSimpleDataSource>();
    ui32 idx = 1;
    for (auto&& i : args) {
        sds->AddBlob(idx, "", i);
        ++idx;
    }

    NOptimization::TGraph::TBuilder builder(resolver);
    builder.Add(TCalculationProcessor::Build(TColumnChainInfo::BuildVector({1}), TColumnChainInfo(4), std::make_shared<TSimpleFunction>(op1)).DetachResult());
    builder.Add(TCalculationProcessor::Build(TColumnChainInfo::BuildVector({2, 4}), TColumnChainInfo(5), std::make_shared<TSimpleFunction>(op2)).DetachResult());
    builder.Add(std::make_shared<TFilterProcessor>(TColumnChainInfo(5)));
    builder.Add(std::make_shared<TProjectionProcessor>(TColumnChainInfo::BuildVector({ 4, 5 })));
    auto chain = builder.Finish().DetachResult();
    chain->Apply(sds, sds->GetResources()).Validate();
    UNIT_ASSERT_VALUES_EQUAL(sds->GetResources()->GetColumnsCount(), 2);
    return sds->GetResources()->GetRecordsCountVerified();
}

std::vector<bool> LikeTest(const std::vector<std::string>& data, EOperation op, const std::string& pattern,
    std::shared_ptr<arrow::DataType> type = arrow::utf8(), bool ignoreCase = false) {
    auto schema = std::make_shared<arrow::Schema>(std::vector{ std::make_shared<arrow::Field>("x", type) });
    std::shared_ptr<arrow::RecordBatch> batch;
    if (type->id() == arrow::utf8()->id()) {
        arrow::StringBuilder sb;
        sb.AppendValues(data).ok();
        batch = arrow::RecordBatch::Make(schema, data.size(), { *sb.Finish() });
    } else if (type->id() == arrow::binary()->id()) {
        arrow::BinaryBuilder sb;
        sb.AppendValues(data).ok();
        batch = arrow::RecordBatch::Make(schema, data.size(), { *sb.Finish() });
    }
    UNIT_ASSERT(batch->ValidateFull().ok());

    TSchemaColumnResolver resolver(schema);

    NOptimization::TGraph::TBuilder builder(resolver);
    builder.Add(TCalculationProcessor::Build(TColumnChainInfo::BuildVector({1}), TColumnChainInfo(2), 
        std::make_shared<TSimpleFunction>(op, std::make_shared<arrow::compute::MatchSubstringOptions>(pattern, ignoreCase))).DetachResult());
    builder.Add(std::make_shared<TProjectionProcessor>(TColumnChainInfo::BuildVector({ 2 })));
    auto chain = builder.Finish().DetachResult();

    auto sds = std::make_shared<TSimpleDataSource>();
    ui32 idx = 1;
    for (auto&& i : batch->columns()) {
        sds->AddBlob(idx, "", i);
        ++idx;
    }

    chain->Apply(sds, sds->GetResources()).Validate();
    UNIT_ASSERT_VALUES_EQUAL(sds->GetResources()->GetColumnsCount(), 1);
    auto arr = sds->GetResources()->GetAccessorVerified(2)->GetChunkedArray();
    AFL_VERIFY(arr->type()->id() == arrow::boolean()->id());
    std::vector<bool> vec;
    for (auto&& i : arr->chunks()) {
        auto& resColumn = static_cast<const arrow::BooleanArray&>(*i);
        for (int i = 0; i < resColumn.length(); ++i) {
            UNIT_ASSERT(!resColumn.IsNull(i));
            vec.push_back(resColumn.Value(i));
        }
    }
    return vec;
}

struct TSumData {
    static std::shared_ptr<arrow::RecordBatch> Data(ETest test, std::shared_ptr<arrow::Schema>& schema, bool nullable) {
        std::optional<double> null;
        if (nullable) {
            null = 0;
        }

        if (test == ETest::DEFAULT) {
            return arrow::RecordBatch::Make(schema, 4,
                std::vector{ NumVecToArray(arrow::int16(), { -1, 0, 0, -1 }, null), NumVecToArray(arrow::uint32(), { 1, 0, 0, 1 }, null) });
        } else if (test == ETest::EMPTY) {
            return arrow::RecordBatch::Make(schema, 0, std::vector{ NumVecToArray(arrow::int16(), {}), NumVecToArray(arrow::uint32(), {}) });
        } else if (test == ETest::ONE_VALUE) {
            return arrow::RecordBatch::Make(
                schema, 1, std::vector{ NumVecToArray(arrow::int16(), { 1 }), NumVecToArray(arrow::uint32(), { 0 }, null) });
        }
        return {};
    }

    static void CheckResult(ETest test, const std::shared_ptr<TAccessorsCollection>& batch, ui32 numKeys, bool nullable) {
        AFL_VERIFY(batch->GetColumnsCount() == numKeys + 2);
        auto aggXOriginal = batch->GetArrayVerified(3);
        auto aggYOriginal = batch->GetArrayVerified(4);
        auto colXOriginal = batch->GetArrayVerified(1);
        auto colYOriginal = (numKeys == 2) ? batch->GetArrayVerified(2) : nullptr;

        UNIT_ASSERT_EQUAL(aggXOriginal->type_id(), arrow::Type::INT64);
        UNIT_ASSERT_EQUAL(aggYOriginal->type_id(), arrow::Type::UINT64);
        UNIT_ASSERT_EQUAL(colXOriginal->type_id(), arrow::Type::INT16);
        if (numKeys == 2) {
            UNIT_ASSERT_EQUAL(colYOriginal->type_id(), arrow::Type::UINT32);
        }

        if (test == ETest::EMPTY) {
            UNIT_ASSERT_VALUES_EQUAL(batch->GetRecordsCountVerified(), 0);
            return;
        }

        auto& aggX = static_cast<arrow::Int64Array&>(*aggXOriginal);
        auto& aggY = static_cast<arrow::UInt64Array&>(*aggYOriginal);
        auto& colX = static_cast<arrow::Int16Array&>(*colXOriginal);

        if (test == ETest::ONE_VALUE) {
            UNIT_ASSERT_VALUES_EQUAL(batch->GetRecordsCountVerified(), 1);

            UNIT_ASSERT_VALUES_EQUAL(aggX.Value(0), 1);
            if (nullable) {
                UNIT_ASSERT(aggY.IsNull(0));
            } else {
                UNIT_ASSERT(!aggY.IsNull(0));
                UNIT_ASSERT_VALUES_EQUAL(aggY.Value(0), 0);
            }
            return;
        }

        UNIT_ASSERT_VALUES_EQUAL(batch->GetRecordsCountVerified(), 2);

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
    static std::shared_ptr<arrow::RecordBatch> Data(ETest /*test*/, std::shared_ptr<arrow::Schema>& schema, bool nullable) {
        std::optional<double> null;
        if (nullable) {
            null = 0;
        }

        return arrow::RecordBatch::Make(
            schema, 1, std::vector{ NumVecToArray(arrow::int16(), { 1 }), NumVecToArray(arrow::uint32(), { 0 }, null) });
    }

    static void CheckResult(ETest /*test*/, const std::shared_ptr<TAccessorsCollection>& batch, ui32 numKeys, bool nullable) {
        UNIT_ASSERT_VALUES_EQUAL(numKeys, 1);
        auto aggXOriginal = batch->GetArrayVerified(3);
        auto aggYOriginal = batch->GetArrayVerified(4);
        auto colXOriginal = batch->GetArrayVerified(1);

        UNIT_ASSERT_VALUES_EQUAL(batch->GetColumnsCount(), numKeys + 2);
        UNIT_ASSERT_EQUAL(aggXOriginal->type_id(), arrow::Type::INT16);
        UNIT_ASSERT_EQUAL(aggYOriginal->type_id(), arrow::Type::UINT32);
        UNIT_ASSERT_EQUAL(colXOriginal->type_id(), arrow::Type::INT16);

        auto& aggX = static_cast<arrow::Int16Array&>(*aggXOriginal);
        auto& aggY = static_cast<arrow::UInt32Array&>(*aggYOriginal);
        auto& colX = static_cast<arrow::Int16Array&>(*colXOriginal);

        UNIT_ASSERT_VALUES_EQUAL(batch->GetRecordsCountVerified(), 1);

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

void GroupByXY(bool nullable, ui32 numKeys, ETest test = ETest::DEFAULT, EAggregate aggFunc = EAggregate::Sum) {
    auto schema = std::make_shared<arrow::Schema>(
        std::vector{ std::make_shared<arrow::Field>("x", arrow::int16()), std::make_shared<arrow::Field>("y", arrow::uint32()) });

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

    TSchemaColumnResolver resolver(schema);

    NOptimization::TGraph::TBuilder builder(resolver);
    NAggregation::TWithKeysAggregationProcessor::TBuilder aggrBuilder;
    aggrBuilder.AddGroupBy(TColumnChainInfo(1), TColumnChainInfo(3), aggFunc);
    aggrBuilder.AddGroupBy(TColumnChainInfo(2), TColumnChainInfo(4), aggFunc);
    aggrBuilder.AddKey(TColumnChainInfo(1));
    if (numKeys == 2) {
        aggrBuilder.AddKey(TColumnChainInfo(2));
    }
    builder.Add(aggrBuilder.Finish().DetachResult());
    if (numKeys == 2) {
        builder.Add(std::make_shared<TProjectionProcessor>(TColumnChainInfo::BuildVector({ 1, 2, 3, 4 })));
    } else {
        builder.Add(std::make_shared<TProjectionProcessor>(TColumnChainInfo::BuildVector({ 1, 3, 4 })));
    }
    auto chain = builder.Finish().DetachResult();

    auto sds = std::make_shared<TSimpleDataSource>();
    ui32 idx = 1;
    for (auto&& i : batch->columns()) {
        sds->AddBlob(idx, "", i);
        ++idx;
    }

    chain->Apply(sds, sds->GetResources()).Validate();

    switch (aggFunc) {
        case EAggregate::Sum:
            TSumData::CheckResult(test, sds->GetResources(), numKeys, nullable);
            break;
        case EAggregate::Min:
        case EAggregate::Max:
        case EAggregate::Some:
            TMinMaxSomeData::CheckResult(test, sds->GetResources(), numKeys, nullable);
            break;
        default:
            break;
    }
}

Y_UNIT_TEST_SUITE(ProgramStep) {
    Y_UNIT_TEST(Round0) {
        for (auto eop : { EOperation::Round, EOperation::RoundBankers, EOperation::RoundToExp2 }) {
            auto x = NumVecToArray(arrow::float64(), { 32.3, 12.5, 34.7 });
            auto z = arrow::compute::CallFunction(TSimpleFunction::GetFunctionName(eop), { x }, GetCustomExecContext());
            UNIT_ASSERT(FilterTestUnary({ x, z->make_array() }, eop, EOperation::Equal) == 3);
        }
    }

    Y_UNIT_TEST(Round1) {
        for (auto eop : { EOperation::Ceil, EOperation::Floor, EOperation::Trunc }) {
            auto x = NumVecToArray(arrow::float64(), { 32.3, 12.5, 34.7 });
            auto z = arrow::compute::CallFunction(TSimpleFunction::GetFunctionName(eop), { x });
            UNIT_ASSERT(FilterTestUnary({ x, z->make_array() }, eop, EOperation::Equal) == 3);
        }
    }

    Y_UNIT_TEST(Filter) {
        auto x = NumVecToArray(arrow::int32(), { 10, 34, 8 });
        auto y = NumVecToArray(arrow::uint32(), { 10, 34, 8 });
        auto z = NumVecToArray(arrow::int64(), { 33, 70, 12 });
        UNIT_ASSERT(FilterTest({ x, y, z }, EOperation::Add, EOperation::Less) == 2);
    }

    Y_UNIT_TEST(Add) {
        auto x = NumVecToArray(arrow::int32(), { 10, 34, 8 });
        auto y = NumVecToArray(arrow::int32(), { 32, 12, 4 });
        auto z = arrow::compute::CallFunction("add", { x, y });
        UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, EOperation::Add, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Substract) {
        auto x = NumVecToArray(arrow::int32(), { 10, 34, 8 });
        auto y = NumVecToArray(arrow::int32(), { 32, 12, 4 });
        auto z = arrow::compute::CallFunction("subtract", { x, y });
        UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, EOperation::Subtract, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Multiply) {
        auto x = NumVecToArray(arrow::int32(), { 10, 34, 8 });
        auto y = NumVecToArray(arrow::int32(), { 32, 12, 4 });
        auto z = arrow::compute::CallFunction("multiply", { x, y });
        UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, EOperation::Multiply, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Divide) {
        auto x = NumVecToArray(arrow::int32(), { 10, 34, 8 });
        auto y = NumVecToArray(arrow::int32(), { 32, 12, 4 });
        auto z = arrow::compute::CallFunction("divide", { x, y });
        UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, EOperation::Divide, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Gcd) {
        auto x = NumVecToArray(arrow::int32(), { 64, 16, 8 });
        auto y = NumVecToArray(arrow::int32(), { 32, 12, 4 });
        auto z = arrow::compute::CallFunction("gcd", { x, y }, GetCustomExecContext());
        UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, EOperation::Gcd, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Lcm) {
        auto x = NumVecToArray(arrow::int32(), { 64, 16, 8 });
        auto y = NumVecToArray(arrow::int32(), { 32, 12, 4 });
        auto z = arrow::compute::CallFunction("lcm", { x, y }, GetCustomExecContext());
        UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, EOperation::Lcm, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Mod) {
        auto x = NumVecToArray(arrow::int32(), { 64, 16, 8 });
        auto y = NumVecToArray(arrow::int32(), { 3, 5, 2 });
        auto z = arrow::compute::CallFunction("mod", { x, y }, GetCustomExecContext());
        UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, EOperation::Modulo, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(ModOrZero) {
        auto x = NumVecToArray(arrow::int32(), { 64, 16, 8 });
        auto y = NumVecToArray(arrow::int32(), { 3, 5, 0 });
        auto z = arrow::compute::CallFunction("modOrZero", { x, y }, GetCustomExecContext());
        UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, EOperation::ModuloOrZero, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Abs) {
        auto x = NumVecToArray(arrow::int32(), { -64, -16, 8 });
        auto z = arrow::compute::CallFunction("abs", { x });
        UNIT_ASSERT(FilterTestUnary({ x, z->make_array() }, EOperation::Abs, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Negate) {
        auto x = NumVecToArray(arrow::int32(), { -64, -16, 8 });
        auto z = arrow::compute::CallFunction("negate", { x });
        UNIT_ASSERT(FilterTestUnary({ x, z->make_array() }, EOperation::Negate, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(Compares) {
        for (auto eop : { EOperation::Equal, EOperation::Less, EOperation::Greater, EOperation::GreaterEqual, EOperation::LessEqual,
                 EOperation::NotEqual }) {
            auto x = NumVecToArray(arrow::int32(), { 64, 5, 1 });
            auto y = NumVecToArray(arrow::int32(), { 64, 1, 5 });
            auto z = arrow::compute::CallFunction(TSimpleFunction::GetFunctionName(eop), { x, y });
            UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, eop, EOperation::Equal) == 3);
        }
    }

    Y_UNIT_TEST(Logic0) {
        for (auto eop : { EOperation::And, EOperation::Or, EOperation::Xor }) {
            auto x = BoolVecToArray({ true, false, false });
            auto y = BoolVecToArray({ true, true, false });
            auto z = arrow::compute::CallFunction(TSimpleFunction::GetFunctionName(eop), { x, y });
            UNIT_ASSERT(FilterTest({ x, y, z->make_array() }, eop, EOperation::Equal) == 3);
        }
    }

    Y_UNIT_TEST(Logic1) {
        auto x = BoolVecToArray({ true, false, false });
        auto z = arrow::compute::CallFunction("invert", { x });
        UNIT_ASSERT(FilterTestUnary({ x, z->make_array() }, EOperation::Invert, EOperation::Equal) == 3);
    }

    Y_UNIT_TEST(StartsWith) {
        for (auto type : { arrow::utf8() /*, arrow::binary()*/ }) {
            std::vector<bool> res = LikeTest({ "aa", "abaaba", "baa", "" }, EOperation::StartsWith, "aa", type);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(res[0], true);
            UNIT_ASSERT_VALUES_EQUAL(res[1], false);
            UNIT_ASSERT_VALUES_EQUAL(res[2], false);
            UNIT_ASSERT_VALUES_EQUAL(res[3], false);
        }
    }

    Y_UNIT_TEST(EndsWith) {
        for (auto type : { arrow::utf8() /*, arrow::binary()*/ }) {
            std::vector<bool> res = LikeTest({ "aa", "abaaba", "baa", "" }, EOperation::EndsWith, "aa", type);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(res[0], true);
            UNIT_ASSERT_VALUES_EQUAL(res[1], false);
            UNIT_ASSERT_VALUES_EQUAL(res[2], true);
            UNIT_ASSERT_VALUES_EQUAL(res[3], false);
        }
    }

    Y_UNIT_TEST(MatchSubstring) {
        for (auto type : { arrow::utf8() /*, arrow::binary()*/ }) {
            std::vector<bool> res = LikeTest({ "aa", "abaaba", "baa", "" }, EOperation::MatchSubstring, "aa", type);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(res[0], true);
            UNIT_ASSERT_VALUES_EQUAL(res[1], true);
            UNIT_ASSERT_VALUES_EQUAL(res[2], true);
            UNIT_ASSERT_VALUES_EQUAL(res[3], false);
        }
    }

    Y_UNIT_TEST(StartsWithIgnoreCase) {
        for (auto type : { arrow::utf8() /*, arrow::binary()*/ }) {
            std::vector<bool> res = LikeTest({ "Aa", "abAaba", "baA", "" }, EOperation::StartsWith, "aA", type, true);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(res[0], true);
            UNIT_ASSERT_VALUES_EQUAL(res[1], false);
            UNIT_ASSERT_VALUES_EQUAL(res[2], false);
            UNIT_ASSERT_VALUES_EQUAL(res[3], false);
        }
    }

    Y_UNIT_TEST(EndsWithIgnoreCase) {
        for (auto type : { arrow::utf8() /*, arrow::binary()*/ }) {
            std::vector<bool> res = LikeTest({ "Aa", "abAaba", "baA", "" }, EOperation::EndsWith, "aA", type, true);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(res[0], true);
            UNIT_ASSERT_VALUES_EQUAL(res[1], false);
            UNIT_ASSERT_VALUES_EQUAL(res[2], true);
            UNIT_ASSERT_VALUES_EQUAL(res[3], false);
        }
    }

    Y_UNIT_TEST(MatchSubstringIgnoreCase) {
        for (auto type : { arrow::utf8() /*, arrow::binary()*/ }) {
            std::vector<bool> res = LikeTest({ "Aa", "abAaba", "baA", "" }, EOperation::MatchSubstring, "aA", type, true);
            UNIT_ASSERT_VALUES_EQUAL(res.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(res[0], true);
            UNIT_ASSERT_VALUES_EQUAL(res[1], true);
            UNIT_ASSERT_VALUES_EQUAL(res[2], true);
            UNIT_ASSERT_VALUES_EQUAL(res[3], false);
        }
    }

    Y_UNIT_TEST(ScalarTest) {
        auto schema = std::make_shared<arrow::Schema>(
            std::vector{ std::make_shared<arrow::Field>("x", arrow::int64()), std::make_shared<arrow::Field>("filter", arrow::boolean()) });
        auto batch = arrow::RecordBatch::Make(
            schema, 4, std::vector{ NumVecToArray(arrow::int64(), { 64, 5, 1, 43 }), BoolVecToArray({ true, false, false, true }) });
        UNIT_ASSERT(batch->ValidateFull().ok());

        TSchemaColumnResolver resolver(schema);
        NOptimization::TGraph::TBuilder builder(resolver);
        builder.Add(std::make_shared<TConstProcessor>(std::make_shared<arrow::Int64Scalar>(56), 3));
        builder.Add(TCalculationProcessor::Build(TColumnChainInfo::BuildVector({1, 3}), TColumnChainInfo(4), std::make_shared<TSimpleFunction>(EOperation::Add)).DetachResult());
        builder.Add(std::make_shared<TFilterProcessor>(TColumnChainInfo(2)));
        builder.Add(std::make_shared<TProjectionProcessor>(TColumnChainInfo::BuildVector({ 2, 4 })));
        auto chain = builder.Finish().DetachResult();

        auto sds = std::make_shared<TSimpleDataSource>();
        ui32 idx = 1;
        for (auto&& i : batch->columns()) {
            sds->AddBlob(idx, "", i);
            ++idx;
        }
        chain->Apply(sds, sds->GetResources()).Validate();

        AFL_VERIFY(sds->GetResources()->GetColumnsCount() == 2);
        AFL_VERIFY(sds->GetResources()->GetRecordsCountVerified() == 2);
    }

    Y_UNIT_TEST(TestValueFromNull) {
        arrow::UInt32Builder sb;
        sb.AppendNulls(10).ok();
        auto arr = std::dynamic_pointer_cast<arrow::UInt32Array>(*sb.Finish());
        AFL_VERIFY(arr->Value(0) == 0)("val", arr->Value(0));
    }

    Y_UNIT_TEST(MergeFilterSimple) {
        std::vector<std::string> data = { "aa", "aaa", "aaaa", "bbbbb" };
        arrow::StringBuilder sb;
        sb.AppendValues(data).ok();

        auto schema = std::make_shared<arrow::Schema>(
            std::vector{ std::make_shared<arrow::Field>("int", arrow::int64()), std::make_shared<arrow::Field>("string", arrow::utf8()) });
        auto batch = arrow::RecordBatch::Make(schema, 4, std::vector{ NumVecToArray(arrow::int64(), { 64, 5, 1, 43 }), *sb.Finish() });
        UNIT_ASSERT(batch->ValidateFull().ok());

        TSchemaColumnResolver resolver(schema);
        NOptimization::TGraph::TBuilder builder(resolver);
        builder.Add(std::make_shared<TConstProcessor>(std::make_shared<arrow::Int64Scalar>(56), 3));
        builder.Add(std::make_shared<TConstProcessor>(std::make_shared<arrow::Int64Scalar>(0), 4));

        builder.Add(std::make_shared<TConstProcessor>(std::make_shared<arrow::StringScalar>("abc"), 10));
        {
            auto proc = TCalculationProcessor::Build(TColumnChainInfo::BuildVector({2, 10}), TColumnChainInfo(10001), 
                std::make_shared<TSimpleFunction>(EOperation::Add), std::make_shared<TGetJsonPath>()).DetachResult();
            builder.Add(proc);
        }
        {
            auto proc = TCalculationProcessor::Build(TColumnChainInfo::BuildVector({10001}), TColumnChainInfo(1001), std::make_shared<TSimpleFunction>(EOperation::MatchSubstring)).DetachResult();
            proc->SetYqlOperationId((ui32)NYql::TKernelRequestBuilder::EBinaryOp::StringContains);
            builder.Add(proc);
        }
        {
            auto proc = TCalculationProcessor::Build(TColumnChainInfo::BuildVector({1001, 4}), TColumnChainInfo(1101), std::make_shared<TSimpleFunction>(EOperation::Add)).DetachResult();
            proc->SetYqlOperationId((ui32)NYql::TKernelRequestBuilder::EBinaryOp::Coalesce);
            builder.Add(proc);
        }
        {
            auto proc = TCalculationProcessor::Build(TColumnChainInfo::BuildVector({2}), TColumnChainInfo(1002), std::make_shared<TSimpleFunction>(EOperation::StartsWith)).DetachResult();
            proc->SetYqlOperationId((ui32)NYql::TKernelRequestBuilder::EBinaryOp::StartsWith);
            builder.Add(proc);
        }
        {
            auto proc = TCalculationProcessor::Build(TColumnChainInfo::BuildVector({1002, 4}), TColumnChainInfo(1102), std::make_shared<TSimpleFunction>(EOperation::Add)).DetachResult();
            proc->SetYqlOperationId((ui32)NYql::TKernelRequestBuilder::EBinaryOp::Coalesce);
            builder.Add(proc);
        }
        {
            auto proc = TCalculationProcessor::Build(TColumnChainInfo::BuildVector({1, 3}), TColumnChainInfo(1003), std::make_shared<TSimpleFunction>(EOperation::Equal)).DetachResult();
            proc->SetYqlOperationId((ui32)NYql::TKernelRequestBuilder::EBinaryOp::Equals);
            builder.Add(proc);
        }
        {
            auto proc = TCalculationProcessor::Build(TColumnChainInfo::BuildVector({1003, 4}), TColumnChainInfo(1103), std::make_shared<TSimpleFunction>(EOperation::Add)).DetachResult();
            proc->SetYqlOperationId((ui32)NYql::TKernelRequestBuilder::EBinaryOp::Coalesce);
            builder.Add(proc);
        }

        builder.Add(std::make_shared<TFilterProcessor>(1101));
        builder.Add(std::make_shared<TFilterProcessor>(1102));
        builder.Add(std::make_shared<TFilterProcessor>(1103));
        builder.Add(std::make_shared<TProjectionProcessor>(TColumnChainInfo::BuildVector({ 1, 2 })));
        auto chain = builder.Finish().DetachResult();
        Cerr << chain->DebugDOT() << Endl;
        AFL_VERIFY(chain->DebugStats() == "[TOTAL:Const:2;Calculation:4;Projection:1;Filter:1;FetchOriginalData:2;AssembleOriginalData:3;CheckIndexData:1;CheckHeaderData:1;StreamLogic:1;];SUB:[FetchOriginalData:1;AssembleOriginalData:1;CheckHeaderData:1;];")("debug", chain->DebugStats());
    }

    Y_UNIT_TEST(Projection) {
        auto schema = std::make_shared<arrow::Schema>(
            std::vector{ std::make_shared<arrow::Field>("x", arrow::int64()), std::make_shared<arrow::Field>("y", arrow::boolean()) });
        auto batch = arrow::RecordBatch::Make(
            schema, 4, std::vector{ NumVecToArray(arrow::int64(), { 64, 5, 1, 43 }), BoolVecToArray({ true, false, false, true }) });
        UNIT_ASSERT(batch->ValidateFull().ok());

        TSchemaColumnResolver resolver(schema);
        NOptimization::TGraph::TBuilder builder(resolver);
        builder.Add(std::make_shared<TProjectionProcessor>(TColumnChainInfo::BuildVector({ 1 })));
        auto chain = builder.Finish().DetachResult();

        auto sds = std::make_shared<TSimpleDataSource>();
        ui32 idx = 1;
        for (auto&& i : batch->columns()) {
            sds->AddBlob(idx, "", i);
            ++idx;
        }
        chain->Apply(sds, sds->GetResources()).Validate();

        UNIT_ASSERT_VALUES_EQUAL(sds->GetResources()->GetColumnsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sds->GetResources()->GetRecordsCountVerified(), 4);
    }

    Y_UNIT_TEST(MinMax) {
        auto tsType = arrow::timestamp(arrow::TimeUnit::MICRO);

        auto schema = std::make_shared<arrow::Schema>(
            std::vector{ std::make_shared<arrow::Field>("x", arrow::int16()), std::make_shared<arrow::Field>("y", tsType) });
        auto batch = arrow::RecordBatch::Make(
            schema, 4, std::vector{ NumVecToArray(arrow::int16(), { 1, 0, -1, 2 }), NumVecToArray(tsType, { 1, 4, 2, 3 }) });
        UNIT_ASSERT(batch->ValidateFull().ok());

        TSchemaColumnResolver resolver(schema);
        NOptimization::TGraph::TBuilder builder(resolver);
        NAggregation::TWithKeysAggregationProcessor::TBuilder aggrBuilder;
        builder.Add(TCalculationProcessor::Build(TColumnChainInfo::BuildVector({1}), TColumnChainInfo(3), std::make_shared<NAggregation::TAggregateFunction>(EAggregate::Min)).DetachResult());
        builder.Add(TCalculationProcessor::Build(TColumnChainInfo::BuildVector({2}), TColumnChainInfo(4), std::make_shared<NAggregation::TAggregateFunction>(EAggregate::Max)).DetachResult());
        builder.Add(std::make_shared<TProjectionProcessor>(TColumnChainInfo::BuildVector({ 3, 4 })));
        auto chain = builder.Finish().DetachResult();
        auto sds = std::make_shared<TSimpleDataSource>();
        ui32 idx = 1;
        for (auto&& i : batch->columns()) {
            sds->AddBlob(idx, "", i);
            ++idx;
        }
        chain->Apply(sds, sds->GetResources()).Validate();
        UNIT_ASSERT_VALUES_EQUAL(sds->GetResources()->GetColumnsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sds->GetResources()->GetRecordsCountVerified(), 1);
        UNIT_ASSERT_EQUAL(sds->GetResources()->GetAccessorVerified(3)->GetDataType()->id(), arrow::Type::INT16);
        UNIT_ASSERT_EQUAL(sds->GetResources()->GetAccessorVerified(4)->GetDataType()->id(), arrow::Type::TIMESTAMP);

        UNIT_ASSERT_EQUAL(static_pointer_cast<arrow::Int16Scalar>(sds->GetResources()->GetAccessorVerified(3)->GetScalar(0))->value, -1);
        UNIT_ASSERT_EQUAL(static_pointer_cast<arrow::TimestampScalar>(sds->GetResources()->GetAccessorVerified(4)->GetScalar(0))->value, 4);
    }

    Y_UNIT_TEST(Sum) {
        auto schema = std::make_shared<arrow::Schema>(
            std::vector{ std::make_shared<arrow::Field>("x", arrow::int16()), std::make_shared<arrow::Field>("y", arrow::uint32()) });
        auto batch = arrow::RecordBatch::Make(
            schema, 4, std::vector{ NumVecToArray(arrow::int16(), { -1, 0, 1, 2 }), NumVecToArray(arrow::uint32(), { 1, 2, 3, 4 }) });
        UNIT_ASSERT(batch->ValidateFull().ok());

        TSchemaColumnResolver resolver(schema);
        NOptimization::TGraph::TBuilder builder(resolver);
        builder.Add(TCalculationProcessor::Build(TColumnChainInfo::BuildVector({1}), TColumnChainInfo(3), std::make_shared<NAggregation::TAggregateFunction>(EAggregate::Sum)).DetachResult());
        builder.Add(TCalculationProcessor::Build(TColumnChainInfo::BuildVector({2}), TColumnChainInfo(4), std::make_shared<NAggregation::TAggregateFunction>(EAggregate::Sum)).DetachResult());
        builder.Add(std::make_shared<TProjectionProcessor>(TColumnChainInfo::BuildVector({ 3, 4 })));
        auto chain = builder.Finish().DetachResult();

        auto sds = std::make_shared<TSimpleDataSource>();
        ui32 idx = 1;
        for (auto&& i : batch->columns()) {
            sds->AddBlob(idx, "", i);
            ++idx;
        }

        chain->Apply(sds, sds->GetResources()).Validate();

        UNIT_ASSERT_VALUES_EQUAL(sds->GetResources()->GetColumnsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sds->GetResources()->GetRecordsCountVerified(), 1);
        UNIT_ASSERT_EQUAL(sds->GetResources()->GetAccessorVerified(3)->GetDataType()->id(), arrow::Type::INT64);
        UNIT_ASSERT_EQUAL(sds->GetResources()->GetAccessorVerified(4)->GetDataType()->id(), arrow::Type::UINT64);

        UNIT_ASSERT_EQUAL(static_pointer_cast<arrow::Int64Scalar>(sds->GetResources()->GetAccessorVerified(3)->GetScalar(0))->value, 2);
        UNIT_ASSERT_EQUAL(static_pointer_cast<arrow::UInt64Scalar>(sds->GetResources()->GetAccessorVerified(4)->GetScalar(0))->value, 10);
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
