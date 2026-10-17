#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/dictionary/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/program/collection.h>
#include <ydb/core/formats/arrow/program/functions.h>

#include <ydb/library/arrow_kernels/ut_common.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/registry.h>
#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <memory>
#include <optional>

namespace NKikimr::NArrow::NSSA {
namespace {

using NKikimr::NKernels::NumVecToArray;
using NKikimr::NKernels::BoolVecToArray;
using NKikimr::NKernels::StringVecToArray;
using NKikimr::NKernels::UInt8VecToArray;

std::shared_ptr<NAccessor::IChunkedArray> IndexInDictionary(const std::shared_ptr<arrow::Array>& valueSet) {
    const auto values = NumVecToArray(arrow::int32(), { 2, 3 });
    const auto positions = UInt8VecToArray({ 0, 1, std::nullopt, 0 });

    TAccessorsCollection resources(positions->length());
    resources.AddVerified(1, std::make_shared<NAccessor::TDictionaryArray>(values, positions), false);
    const auto function = std::dynamic_pointer_cast<arrow::compute::ScalarFunction>(
        *arrow::compute::GetFunctionRegistry()->GetFunction("index_in"));
    UNIT_ASSERT(function);
    TKernelFunction kernel(function, std::make_shared<arrow::compute::SetLookupOptions>(arrow::Datum(valueSet)));
    return kernel.Call(TExecFunctionContext(TColumnChainInfo::BuildVector({ 1 })), resources)
        .DetachResult()
        .GetAccessorVerified();
}

std::shared_ptr<arrow::compute::ScalarFunction> MakeSliceUnsafeFunction() {
    auto function = std::make_shared<arrow::compute::ScalarFunction>("slice_unsafe", arrow::compute::Arity::Unary(), nullptr);
    arrow::compute::ScalarKernel kernel;
    kernel.signature = arrow::compute::KernelSignature::Make({ arrow::uint32() }, arrow::uint32());
    kernel.exec = [](arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch, arrow::Datum* result) {
        const auto* input = batch.values[0].array()->GetValues<ui32>(1);
        const auto& resultArray = *result->array();
        // This is incorrect for chunked arrays, must account for offset in buffer,
        // otherwise consecutive chunks will overwrite each other's output.
        auto* output = reinterpret_cast<ui32*>(resultArray.buffers[1]->mutable_data());
        std::copy_n(input, batch.length, output);
        return arrow::Status::OK();
    };
    TStatusValidator::Validate(function->AddKernel(std::move(kernel)));
    return function;
}

}

Y_UNIT_TEST_SUITE(Functions) {
    Y_UNIT_TEST(ExpandsDictionaryForNullPreservingIndexIn) {
        const auto result = IndexInDictionary(NumVecToArray(arrow::int32(), { 1, 2 }));

        // Preserving dictionary encoding for null-preserving kernels is a future optimization.
        UNIT_ASSERT(std::dynamic_pointer_cast<NAccessor::TTrivialArray>(result));
        UNIT_ASSERT_VALUES_EQUAL(result->GetNullsCount(), 2);
        UNIT_ASSERT(result->GetChunkedArray()->chunk(0)->Equals(*NumVecToArray(arrow::int32(), { 1, 0, 0, 1 }, 0)));
    }

    Y_UNIT_TEST(ExpandsDictionaryForNonNullPreservingIndexIn) {
        const auto result = IndexInDictionary(NumVecToArray(arrow::int32(), { 1, 2, 0 }, 0));

        UNIT_ASSERT(std::dynamic_pointer_cast<NAccessor::TTrivialArray>(result));
        UNIT_ASSERT(result->GetChunkedArray()->chunk(0)->Equals(*NumVecToArray(arrow::int32(), { 1, 0, 2, 1 }, 0)));
    }

    Y_UNIT_TEST(KernelCallMapsDictionaryValues) {
        const auto values = StringVecToArray({ "Ada", std::nullopt, "Bobby" });
        // Index 1 explicitly references a null dictionary value.
        const auto positions = UInt8VecToArray({ 0, 1, std::nullopt, 2, 0 });

        TAccessorsCollection resources(positions->length());
        resources.AddVerified(1, std::make_shared<NAccessor::TDictionaryArray>(values, positions), false);
        const auto function = std::dynamic_pointer_cast<arrow::compute::ScalarFunction>(
            *arrow::compute::GetFunctionRegistry()->GetFunction("match_substring"));
        UNIT_ASSERT(function);
        TKernelFunction kernel(function, std::make_shared<arrow::compute::MatchSubstringOptions>("a", true));
        auto result = kernel.Call(TExecFunctionContext(TColumnChainInfo::BuildVector({ 1 })), resources).DetachResult();

        // Preserving dictionary encoding for null-preserving kernels is a future optimization.
        UNIT_ASSERT(std::dynamic_pointer_cast<NAccessor::TTrivialArray>(result.GetAccessorVerified()));
        UNIT_ASSERT(result.GetAccessorVerified()->GetChunkedArray()->chunk(0)->Equals(
            *BoolVecToArray({true, std::nullopt, std::nullopt, false, true})));
    }

    Y_UNIT_TEST(KernelCallExpandsDictionaryWithNullPositions) {
        const auto values = StringVecToArray({ "Ada", "Bobby" });
        const auto positions = UInt8VecToArray({ 0, std::nullopt, 1 });

        TAccessorsCollection resources(positions->length());
        resources.AddVerified(1, std::make_shared<NAccessor::TDictionaryArray>(values, positions), false);
        resources.AddConstantVerified(2, std::make_shared<arrow::StringScalar>("fallback"));
        const auto function = std::dynamic_pointer_cast<arrow::compute::ScalarFunction>(
            *arrow::compute::GetFunctionRegistry()->GetFunction("coalesce"));
        UNIT_ASSERT(function);
        TKernelFunction kernel(function);
        auto result = kernel.Call(TExecFunctionContext(TColumnChainInfo::BuildVector({ 1, 2 })), resources).DetachResult();

        UNIT_ASSERT(std::dynamic_pointer_cast<NAccessor::TTrivialArray>(result.GetAccessorVerified()));
        UNIT_ASSERT(result.GetAccessorVerified()->GetChunkedArray()->chunk(0)->Equals(
            *StringVecToArray({ "Ada", "fallback", "Bobby" })));
    }

    Y_UNIT_TEST(KernelCallExpandsNullFreeCompositeDictionaryPart) {
        const auto firstPositions = UInt8VecToArray({ 0, std::nullopt, 1 });
        const auto secondPositions = UInt8VecToArray({ 0, 0 });
        NAccessor::TCompositeChunkedArray::TBuilder compositeBuilder(arrow::utf8());
        compositeBuilder.AddChunk(
            std::make_shared<NAccessor::TDictionaryArray>(StringVecToArray({ "Ada", "Bobby" }), firstPositions));
        compositeBuilder.AddChunk(
            std::make_shared<NAccessor::TDictionaryArray>(StringVecToArray({ "Carla" }), secondPositions));

        TAccessorsCollection resources(5);
        resources.AddVerified(1, compositeBuilder.Finish(), false);
        resources.AddConstantVerified(2, std::make_shared<arrow::StringScalar>("fallback"));
        const auto function = std::dynamic_pointer_cast<arrow::compute::ScalarFunction>(
            *arrow::compute::GetFunctionRegistry()->GetFunction("coalesce"));
        UNIT_ASSERT(function);
        TKernelFunction kernel(function);
        auto result = kernel.Call(TExecFunctionContext(TColumnChainInfo::BuildVector({ 1, 2 })), resources).DetachResult();

        UNIT_ASSERT(std::dynamic_pointer_cast<NAccessor::TTrivialChunkedArray>(result.GetAccessorVerified()));
        const auto chunks = result.GetAccessorVerified()->GetChunkedArray();
        UNIT_ASSERT_VALUES_EQUAL(chunks->num_chunks(), 2);
        UNIT_ASSERT(chunks->chunk(0)->Equals(
            *StringVecToArray({"Ada", "fallback", "Bobby"})));
        // Preserving dictionary encoding for the null-free part is a future optimization.
        UNIT_ASSERT(chunks->chunk(1)->Equals(*StringVecToArray({"Carla", "Carla"})));
    }

    Y_UNIT_TEST(KernelCallMapsCompositeDictionaryValues) {
        const auto firstPositions = UInt8VecToArray({ 0, 1, 0 });
        const auto secondPositions = UInt8VecToArray({ 0, 0 });
        NAccessor::TCompositeChunkedArray::TBuilder compositeBuilder(arrow::utf8());
        compositeBuilder.AddChunk(
            std::make_shared<NAccessor::TDictionaryArray>(StringVecToArray({ "Ada", "Bobby" }), firstPositions));
        compositeBuilder.AddChunk(
            std::make_shared<NAccessor::TTrivialArray>(StringVecToArray({ "Ann", std::nullopt })));
        compositeBuilder.AddChunk(
            std::make_shared<NAccessor::TDictionaryArray>(StringVecToArray({ "Carla" }), secondPositions));
        const auto input = compositeBuilder.Finish();

        TAccessorsCollection resources(7);
        resources.AddVerified(1, input, false);
        resources.AddConstantVerified(2, std::make_shared<arrow::StringScalar>("Ada"));
        const auto function = std::dynamic_pointer_cast<arrow::compute::ScalarFunction>(
            *arrow::compute::GetFunctionRegistry()->GetFunction("equal"));
        UNIT_ASSERT(function);
        TKernelFunction kernel(function);
        auto result = kernel.Call(TExecFunctionContext(TColumnChainInfo::BuildVector({ 1, 2 })), resources).DetachResult();

        // Preserving dictionary encoding within mixed results is a future optimization.
        UNIT_ASSERT(std::dynamic_pointer_cast<NAccessor::TTrivialChunkedArray>(result.GetAccessorVerified()));
        const auto chunks = result.GetAccessorVerified()->GetChunkedArray();
        UNIT_ASSERT_VALUES_EQUAL(chunks->num_chunks(), 3);
        UNIT_ASSERT(chunks->chunk(0)->Equals(*BoolVecToArray({true, false, true})));
        UNIT_ASSERT(chunks->chunk(1)->Equals(*BoolVecToArray({false, std::nullopt})));
        UNIT_ASSERT(chunks->chunk(2)->Equals(*BoolVecToArray({false, false})));
    }

    Y_UNIT_TEST(KernelCallHandlesMultipleDictionaries) {
        const auto values = StringVecToArray({ "Ada", "Bobby" });
        const auto firstPositions = UInt8VecToArray({ 0, 1, 0 });
        const auto secondPositions = UInt8VecToArray({ 1, 0, 0 });

        TAccessorsCollection resources(3);
        resources.AddVerified(1, std::make_shared<NAccessor::TDictionaryArray>(values, firstPositions), false);
        resources.AddVerified(2, std::make_shared<NAccessor::TDictionaryArray>(values, secondPositions), false);
        const auto function = std::dynamic_pointer_cast<arrow::compute::ScalarFunction>(
            *arrow::compute::GetFunctionRegistry()->GetFunction("equal"));
        UNIT_ASSERT(function);
        TKernelFunction kernel(function);
        auto result = kernel.Call(TExecFunctionContext(TColumnChainInfo::BuildVector({ 1, 2 })), resources).DetachResult();

        UNIT_ASSERT(result.GetAccessorVerified()->GetChunkedArray()->chunk(0)->Equals(
            *BoolVecToArray({ false, false, true })));
    }

    Y_UNIT_TEST(KernelCallHandlesDictionaryAndTrivial) {
        const auto values = StringVecToArray({ "Ada", "Bobby" });
        const auto positions = UInt8VecToArray({ 0, 1, 0 });
        const auto trivial = StringVecToArray({ "Bobby", "Ada", "Ada" });

        TAccessorsCollection resources(3);
        resources.AddVerified(1, std::make_shared<NAccessor::TDictionaryArray>(values, positions), false);
        resources.AddVerified(2, std::make_shared<NAccessor::TTrivialArray>(trivial), false);
        const auto function = std::dynamic_pointer_cast<arrow::compute::ScalarFunction>(
            *arrow::compute::GetFunctionRegistry()->GetFunction("equal"));
        UNIT_ASSERT(function);
        TKernelFunction kernel(function);
        auto result = kernel.Call(TExecFunctionContext(TColumnChainInfo::BuildVector({ 1, 2 })), resources).DetachResult();

        UNIT_ASSERT(result.GetAccessorVerified()->GetChunkedArray()->chunk(0)->Equals(
            *BoolVecToArray({ false, false, true })));
    }

    Y_UNIT_TEST(KernelCallHandlesCompositeTimestamp) {
        const auto timestampType = arrow::timestamp(arrow::TimeUnit::MICRO);
        NAccessor::TCompositeChunkedArray::TBuilder compositeBuilder(timestampType);
        compositeBuilder.AddChunk(std::make_shared<NAccessor::TTrivialArray>(NumVecToArray(timestampType, { 1, 2 })));
        compositeBuilder.AddChunk(std::make_shared<NAccessor::TTrivialArray>(NumVecToArray(timestampType, { 3 })));
        const auto input = compositeBuilder.Finish();
        const auto inputComposite = std::dynamic_pointer_cast<NAccessor::TCompositeChunkedArray>(input);
        UNIT_ASSERT(inputComposite);

        auto function = std::make_shared<arrow::compute::ScalarFunction>("uint64_identity", arrow::compute::Arity::Unary(), nullptr);
        arrow::compute::ScalarKernel kernel;
        kernel.signature = arrow::compute::KernelSignature::Make({ arrow::uint64() }, arrow::uint64());
        kernel.exec = [](arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch, arrow::Datum* result) {
            std::copy_n(batch.values[0].array()->GetValues<ui64>(1), batch.length, result->array()->GetMutableValues<ui64>(1));
            return arrow::Status::OK();
        };
        TStatusValidator::Validate(function->AddKernel(std::move(kernel)));

        TAccessorsCollection resources(3);
        resources.AddVerified(1, input, false);
        TKernelFunction functionCall(function);
        auto result = functionCall.Call(TExecFunctionContext(TColumnChainInfo::BuildVector({ 1 })), resources).DetachResult();

        UNIT_ASSERT(std::dynamic_pointer_cast<NAccessor::TTrivialChunkedArray>(result.GetAccessorVerified()));
        const auto chunks = result.GetAccessorVerified()->GetChunkedArray();
        UNIT_ASSERT_VALUES_EQUAL(chunks->num_chunks(), 2);
        UNIT_ASSERT(chunks->chunk(0)->Equals(*NumVecToArray(arrow::uint64(), {1, 2})));
        UNIT_ASSERT(chunks->chunk(1)->Equals(*NumVecToArray(arrow::uint64(), {3})));
        UNIT_ASSERT(input->GetDataType()->Equals(timestampType));
        UNIT_ASSERT(inputComposite->GetChunks()[0]->GetChunkedArray()->type()->Equals(timestampType));
        UNIT_ASSERT(inputComposite->GetChunks()[1]->GetChunkedArray()->type()->Equals(timestampType));
    }

    Y_UNIT_TEST(KernelCallExpandsMultiChunkCompositePart) {
        NAccessor::TCompositeChunkedArray::TBuilder innerBuilder(arrow::uint32());
        innerBuilder.AddChunk(std::make_shared<NAccessor::TTrivialArray>(NumVecToArray(arrow::uint32(), { 10, 11 })));
        innerBuilder.AddChunk(std::make_shared<NAccessor::TTrivialArray>(NumVecToArray(arrow::uint32(), { 12, 13 })));

        NAccessor::TCompositeChunkedArray::TBuilder outerBuilder(arrow::uint32());
        outerBuilder.AddChunk(std::make_shared<NAccessor::TTrivialArray>(NumVecToArray(arrow::uint32(), { 1 })));
        outerBuilder.AddChunk(innerBuilder.Finish());

        TAccessorsCollection resources(5);
        resources.AddVerified(1, outerBuilder.Finish(), false);
        TKernelFunction function(MakeSliceUnsafeFunction());
        const auto result = function.Call(TExecFunctionContext(TColumnChainInfo::BuildVector({ 1 })), resources)
                                .DetachResult()
                                .GetAccessorVerified();

        UNIT_ASSERT(std::dynamic_pointer_cast<NAccessor::TTrivialChunkedArray>(result));
        const auto values = TStatusValidator::GetValid(arrow::Concatenate(result->GetChunkedArray()->chunks()));
        UNIT_ASSERT(values->Equals(*NumVecToArray(arrow::uint32(), { 1, 10, 11, 12, 13 })));
    }
};

}
