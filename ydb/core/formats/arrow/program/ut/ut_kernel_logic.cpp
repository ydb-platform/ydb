#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/dictionary/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>
#include <ydb/core/formats/arrow/filter/filter.h>
#include <ydb/core/formats/arrow/program/assign_internal.h>
#include <ydb/core/formats/arrow/program/execution.h>
#include <ydb/core/formats/arrow/program/filter.h>
#include <ydb/core/formats/arrow/program/kernel_logic.h>
#include <ydb/core/formats/arrow/program/stream_logic.h>
#include <ydb/core/formats/arrow/program/visitor.h>

#include <ydb/library/formats/arrow/helpers/array.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/concatenate.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/exec.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/registry.h>
#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/types/binary_json/write.h>

#include <initializer_list>
#include <string_view>

namespace NKikimr::NArrow::NSSA {
namespace {

class TTestGetJsonPath: public TGetJsonPath {
public:
    using TGetJsonPath::ExtractArray;

    std::shared_ptr<NAccessor::IChunkedArray> ExtractArray(
        const std::shared_ptr<NAccessor::IChunkedArray>& input, const std::string_view path) const {
        return TGetJsonPath::ExtractArray(input, NAccessor::NSubColumns::ParseJsonPath(path).DetachResult());
    }
};

NAccessor::NSubColumns::TSettings BuildSettings(const double dictionaryFraction, const ui32 columnsLimit) {
    NAccessor::NSubColumns::TSettings settings(
        4, columnsLimit, 0, 0, NAccessor::NSubColumns::TDataAdapterContainer::GetDefault(), dictionaryFraction);
    settings.SetEnableNativeColumns(true);
    return settings;
}

std::shared_ptr<NAccessor::TSubColumnsArray> BuildSubColumns(
    const std::vector<TString>& jsons, const NAccessor::NSubColumns::TSettings& settings) {
    NAccessor::TTrivialArray::TPlainBuilder<arrow::BinaryType> builder;
    ui32 index = 0;
    for (const TString& json : jsons) {
        if (json != "null") {
            const auto value = NBinaryJson::SerializeToBinaryJson(json);
            const auto* binaryJson = std::get_if<NBinaryJson::TBinaryJson>(&value);
            UNIT_ASSERT(binaryJson);
            builder.AddRecord(index, std::string_view(binaryJson->data(), binaryJson->size()));
        }
        ++index;
    }
    auto sourceJson = builder.Finish(index);
    return NAccessor::TSubColumnsArray::Make(sourceJson, settings, sourceJson->GetDataType()).DetachResult();
}

TString ExtractJsonValue(const std::shared_ptr<NAccessor::TSubColumnsArray>& input, const std::string_view path) {
    auto result = TTestGetJsonPath().ExtractArray(input, path);
    UNIT_ASSERT_VALUES_EQUAL(result->GetRecordsCount(), input->GetRecordsCount());
    TString values;
    result->VisitValues([&](const std::shared_ptr<arrow::Array>& chunk) {
        UNIT_ASSERT(chunk->type_id() == arrow::Type::STRING);
        const auto* strings = static_cast<const arrow::StringArray*>(chunk.get());
        for (i64 index = 0; index < strings->length(); ++index) {
            if (strings->IsNull(index)) {
                values.append("<null>");
            } else {
                const auto value = strings->GetView(index);
                values.append(value.data(), value.size());
            }
            values.append(";");
        }
    });
    return values;
}

std::shared_ptr<NAccessor::IChunkedArray> BuildCompositePredicate() {
    NAccessor::TCompositeChunkedArray::TBuilder builder(arrow::uint8());
    builder.AddChunk(std::make_shared<NAccessor::TDictionaryArray>(
        NKikimr::NKernels::UInt8VecToArray({1}), NKikimr::NKernels::UInt8VecToArray({0, 0})));
    builder.AddChunk(std::make_shared<NAccessor::TDictionaryArray>(
        NKikimr::NKernels::UInt8VecToArray({0}), NKikimr::NKernels::UInt8VecToArray({0, 0})));
    return builder.Finish();
}

std::shared_ptr<NAccessor::IChunkedArray> BuildSlicedDictionaryPredicate() {
    auto predicate = std::make_shared<NAccessor::TDictionaryArray>(
        NKikimr::NKernels::UInt8VecToArray({1, std::nullopt}),
        NKikimr::NKernels::UInt8VecToArray({0, std::nullopt, 0}));
    auto sliced = predicate->ISlice(0, 3);
    UNIT_ASSERT(sliced->GetType() == NAccessor::IChunkedArray::EType::Dictionary);
    sliced->VisitDistinctValues([](const std::shared_ptr<arrow::Array>& values) {
        UNIT_ASSERT_VALUES_EQUAL(values->length(), 2);
    });
    return sliced;
}

class TStreamLogicTest {
private:
    TFailDataSource DataSource;
    TProcessorContext Context;
    TStreamLogicProcessor Processor;
    TExecutionNodeContext NodeContext;
    const ui32 OutputId;

public:
    TStreamLogicTest(const ui32 recordsCount, const std::initializer_list<ui32> inputIds, const ui32 outputId,
                     const NKikimr::NKernels::EOperation operation)
        : Context(DataSource, std::make_unique<NAccessor::TAccessorsCollection>(recordsCount), std::nullopt, false)
        , Processor(TColumnChainInfo::BuildVector(inputIds), TColumnChainInfo(outputId), operation)
        , OutputId(outputId)
    {
    }

    void AddAccessor(const ui32 inputId, const std::shared_ptr<NAccessor::IChunkedArray>& accessor) {
        Context.MutableResources().AddVerified(inputId, accessor, false);
    }

    void AddArray(const ui32 inputId, const std::shared_ptr<arrow::Array>& array) {
        AddAccessor(inputId, std::make_shared<NAccessor::TTrivialArray>(array));
    }

    void AddScalar(const ui32 inputId, const std::shared_ptr<arrow::Scalar>& scalar) {
        Context.MutableResources().AddConstantVerified(inputId, scalar);
    }

    void AddInput(const ui32 inputId, const std::shared_ptr<arrow::Array>& array) {
        AddArray(inputId, array);
    }

    void AddInput(const ui32 inputId, const std::shared_ptr<arrow::Scalar>& scalar) {
        AddScalar(inputId, scalar);
    }

    bool ProcessInput(const ui32 inputId) {
        const auto result = Processor.OnInputReady(inputId, Context, NodeContext);
        UNIT_ASSERT(result.IsSuccess());
        return *result;
    }

    const std::shared_ptr<NAccessor::IChunkedArray>& GetOutputAccessor() const {
        return Context.GetResources().GetAccessorVerified(OutputId);
    }

    void AssertResult(const std::vector<std::optional<ui8>>& expected) const {
        const auto actual = arrow::Concatenate(Context.GetResources().GetAccessorVerified(OutputId)->GetChunkedArray()->chunks()).ValueOrDie();
        UNIT_ASSERT(actual->Equals(*NKikimr::NKernels::UInt8VecToArray(expected)));
    }
};

void AssertFilteredRows(const std::shared_ptr<NAccessor::IChunkedArray>& predicate, const std::vector<std::optional<ui8>>& expected) {
    TFailDataSource dataSource;
    auto resources = std::make_unique<NAccessor::TAccessorsCollection>(predicate->GetRecordsCount());
    resources->AddVerified(1, predicate, false);
    TProcessorContext context(dataSource, std::move(resources), std::nullopt, false);
    UNIT_ASSERT(TFilterProcessor(TColumnChainInfo(1)).Execute(context, TExecutionNodeContext()).IsSuccess());

    std::vector<std::optional<ui8>> rows;
    for (ui8 i = 0; i < predicate->GetRecordsCount(); ++i) {
        rows.emplace_back(i);
    }
    auto filtered = context.GetResources().GetFilter().Apply(
        std::make_shared<NAccessor::TTrivialArray>(NKikimr::NKernels::UInt8VecToArray(rows)));
    UNIT_ASSERT(arrow::Concatenate(filtered->GetChunkedArray()->chunks()).ValueOrDie()->Equals(*NKikimr::NKernels::UInt8VecToArray(expected)));
}

void AssertAndResult(const std::shared_ptr<NAccessor::IChunkedArray>& predicate,
                     const std::vector<std::optional<ui8>>& expected) {
    TStreamLogicTest test(predicate->GetRecordsCount(), {1, 2}, 3, NKikimr::NKernels::EOperation::And);
    test.AddAccessor(1, predicate);
    std::vector<std::optional<ui8>> allTrue(predicate->GetRecordsCount(), 1);
    test.AddArray(2, NKikimr::NKernels::UInt8VecToArray(allTrue));
    UNIT_ASSERT(!test.ProcessInput(1));
    UNIT_ASSERT(!test.ProcessInput(2));
    test.AssertResult(expected);
}

template <class TInput>
void AssertNullConstantWithInput(const NKikimr::NKernels::EOperation operation, const std::shared_ptr<TInput>& input,
                                 const std::vector<std::optional<ui8>>& expected, const bool nullFirst) {
    TStreamLogicTest test(expected.size(), {1, 2}, 3, operation);
    test.AddScalar(1, arrow::MakeNullScalar(arrow::uint8()));
    test.AddInput(2, input);
    UNIT_ASSERT(!test.ProcessInput(nullFirst ? 1 : 2));
    UNIT_ASSERT(!test.ProcessInput(nullFirst ? 2 : 1));
    test.AssertResult(expected);
}
}

Y_UNIT_TEST_SUITE(JsonValue) {
    Y_UNIT_TEST(UsesNativeStringBuffers) {
        auto input = BuildSubColumns({ R"({"s":"x"})", R"({"s":"yy"})", R"({"s":"zzz"})" }, BuildSettings(0, 1024));
        auto accessor = input->GetPathAccessor(
            NAccessor::NSubColumns::ParseJsonPath("$.s").DetachResult(), input->GetRecordsCount()).DetachResult();
        const auto& source = accessor->GetChunkedArrayAccessor();
        UNIT_ASSERT(source->GetType() == NAccessor::IChunkedArray::EType::Array);

        auto result = TTestGetJsonPath().ExtractArray(input, "$.s");
        UNIT_ASSERT(source->GetDataType()->id() == arrow::Type::STRING);
        UNIT_ASSERT_VALUES_EQUAL(result.get(), source.get());
    }

    Y_UNIT_TEST(HandlesNestedAndAbsentPaths) {
        auto input = BuildSubColumns({ R"({"object":{"s":"x"}})", "null", R"({"object":{"s":"yy"}})" }, BuildSettings(0, 1024));
        UNIT_ASSERT_VALUES_EQUAL(ExtractJsonValue(input, "$.object.s"), "x;<null>;yy;");
        UNIT_ASSERT_VALUES_EQUAL(ExtractJsonValue(input, "$.absent"), "<null>;<null>;<null>;");
    }

    Y_UNIT_TEST(UsesNativeStringDictionary) {
        std::vector<TString> dictionaryDocs;
        for (ui32 index = 0; index < 40; ++index) {
            dictionaryDocs.emplace_back(TStringBuilder() << R"({"s":")" << (index % 2 ? "x" : "yy") << R"("})");
        }
        auto dictionaryInput = BuildSubColumns(dictionaryDocs, BuildSettings(1, 1024));
        auto accessor = dictionaryInput->GetPathAccessor(
            NAccessor::NSubColumns::ParseJsonPath("$.s").DetachResult(), dictionaryInput->GetRecordsCount()).DetachResult();
        const auto& source = accessor->GetChunkedArrayAccessor();
        UNIT_ASSERT(source->GetType() == NAccessor::IChunkedArray::EType::Dictionary);
        UNIT_ASSERT(source->GetDataType()->id() == arrow::Type::STRING);
        // Extract array provides the same instance as directly calling GetPathAccessor
        UNIT_ASSERT_VALUES_EQUAL(TTestGetJsonPath().ExtractArray(dictionaryInput, "$.s").get(), source.get());

        TString dictionaryExpected;
        for (ui32 index = 0; index < dictionaryDocs.size(); ++index) {
            dictionaryExpected.append(index % 2 ? "x;" : "yy;");
        }
        UNIT_ASSERT_VALUES_EQUAL(ExtractJsonValue(dictionaryInput, "$.s"), dictionaryExpected);
    }

    Y_UNIT_TEST(HandlesBinaryJson) {
        auto binaryJsonInput = BuildSubColumns({ R"({"value":"x"})", R"({"value":1})" }, BuildSettings(0, 1024));
        UNIT_ASSERT_VALUES_EQUAL(ExtractJsonValue(binaryJsonInput, "$.value"), "x;1;");
    }

    Y_UNIT_TEST(ReadsFromOthers) {
        auto input = BuildSubColumns({ R"({"s":"x"})", R"({"s":"yy"})" }, BuildSettings(0, 0));
        UNIT_ASSERT_VALUES_EQUAL(input->GetColumnsData().GetStats().GetColumnsCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(input->GetOthersData().GetStats().GetColumnsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ExtractJsonValue(input, "$.s"), "x;yy;");
    }

    Y_UNIT_TEST(ExecutesAcrossMultipleDataOwners) {
        auto first = BuildSubColumns({ R"({"s":"x"})" }, BuildSettings(0, 1024));
        auto second = BuildSubColumns({ R"({"s":"yy"})" }, BuildSettings(0, 1024));
        NAccessor::TCompositeChunkedArray::TBuilder builder(first->GetDataType());
        builder.AddChunk(first);
        builder.AddChunk(second);

        TAccessorsCollection resources(2);
        resources.AddVerified(1, builder.Finish(), false);
        resources.AddConstantVerified(2, std::make_shared<arrow::StringScalar>("$.s"));
        const auto result = TGetJsonPath().Execute(TColumnChainInfo::BuildVector({1, 2}), TColumnChainInfo::BuildVector({3}), resources);
        UNIT_ASSERT(result.IsSuccess() && *result);
        const auto output = arrow::Concatenate(resources.GetAccessorVerified(3)->GetChunkedArray()->chunks()).ValueOrDie();
        UNIT_ASSERT(output->GetScalar(0).ValueOrDie()->Equals(arrow::StringScalar("x")));
        UNIT_ASSERT(output->GetScalar(1).ValueOrDie()->Equals(arrow::StringScalar("yy")));
    }
};

Y_UNIT_TEST_SUITE(KernelLogic) {
    Y_UNIT_TEST(ToStringPreservesAddressForStringRepresentations) {
        const auto makeKernel = [](const std::shared_ptr<arrow::DataType>& input, const std::shared_ptr<arrow::DataType>& output) {
            arrow::compute::ScalarKernel kernel;
            kernel.signature = arrow::compute::KernelSignature::Make({ input }, output);
            return kernel;
        };

        UNIT_ASSERT(TToStringKernel(makeKernel(arrow::utf8(), arrow::binary())).GetOriginalAddressFromInput());
        UNIT_ASSERT(TToStringKernel(makeKernel(arrow::binary(), arrow::binary())).GetOriginalAddressFromInput());
        UNIT_ASSERT(!TToStringKernel(makeKernel(arrow::utf8(), arrow::utf8())).GetOriginalAddressFromInput());
    }

    Y_UNIT_TEST(CompositePredicateFiltersRows) {
        AssertFilteredRows(BuildCompositePredicate(), {0, 1});
    }

    Y_UNIT_TEST(CompositePredicatePreservesAndInput) {
        AssertAndResult(BuildCompositePredicate(), {1, 1, 0, 0});
    }

    Y_UNIT_TEST(SlicedDictionaryPredicateFiltersNullRow) {
        AssertFilteredRows(BuildSlicedDictionaryPredicate(), {0, 2});
    }

    Y_UNIT_TEST(SlicedDictionaryPredicatePreservesAndInput) {
        AssertAndResult(BuildSlicedDictionaryPredicate(), {1, std::nullopt, 1});
    }

    Y_UNIT_TEST(AllNullDictionaryInputAndTruePreservesNull) {
        auto allNullDict = std::make_shared<NAccessor::TDictionaryArray>(
            NKikimr::NKernels::UInt8VecToArray({1}),
            NKikimr::NKernels::UInt8VecToArray({std::nullopt, std::nullopt}));
        AssertAndResult(allNullDict, {std::nullopt, std::nullopt});
    }


    Y_UNIT_TEST(NullIntermediateAndResultDoesNotFinishStream) {
        TStreamLogicTest test(2, {1, 2, 3}, 4, NKikimr::NKernels::EOperation::And);
        test.AddArray(1, NKikimr::NKernels::UInt8VecToArray({std::nullopt, 1}));
        test.AddArray(2, NKikimr::NKernels::UInt8VecToArray({1, std::nullopt}));
        test.AddArray(3, NKikimr::NKernels::UInt8VecToArray({0, 0}));
        UNIT_ASSERT(!test.ProcessInput(1));
        UNIT_ASSERT(!test.ProcessInput(2));
        UNIT_ASSERT(test.ProcessInput(3));
        test.AssertResult({0, 0});
    }

    Y_UNIT_TEST(AllNullSparsePredicateAndTruePreservesNull) {
        auto predicate = std::make_shared<NAccessor::TSparsedArray>(nullptr, arrow::uint8(), 2);
        std::shared_ptr<arrow::Scalar> value;
        const auto oneValue = predicate->CheckOneValueAccessor(value);
        UNIT_ASSERT(oneValue && *oneValue);
        UNIT_ASSERT(value && !value->is_valid);
        AssertAndResult(predicate, {std::nullopt, std::nullopt});
    }

    Y_UNIT_TEST(NullOrFalseDoesNotFinishStream) {
        const auto values = NKikimr::NKernels::UInt8VecToArray({1, 1});
        const auto nullable = NKikimr::NKernels::UInt8VecToArray({std::nullopt, 1});
        auto predicate = std::make_shared<arrow::UInt8Array>(
            values->length(), values->data()->buffers[1], nullable->data()->buffers[0], nullable->null_count());
        UNIT_ASSERT(predicate->IsNull(0));
        UNIT_ASSERT_C(predicate->raw_values()[0] == 1, "NULL payload must be nonzero to exercise the Or finish check");

        TStreamLogicTest test(2, {1, 2}, 3, NKikimr::NKernels::EOperation::Or);
        test.AddArray(1, predicate);
        test.AddArray(2, NKikimr::NKernels::UInt8VecToArray({0, 0}));
        UNIT_ASSERT(!test.ProcessInput(1));
        UNIT_ASSERT(!test.ProcessInput(2));
        test.AssertResult({std::nullopt, 1});
    }

    Y_UNIT_TEST(NullOrTrueFinishesStream) {
        TStreamLogicTest test(2, {1, 2}, 3, NKikimr::NKernels::EOperation::Or);
        test.AddArray(1, NKikimr::NKernels::UInt8VecToArray({std::nullopt, 1}));
        test.AddArray(2, NKikimr::NKernels::UInt8VecToArray({1, 1}));
        UNIT_ASSERT(!test.ProcessInput(1));
        UNIT_ASSERT(test.ProcessInput(2));
        test.AssertResult({1, 1});
    }

    Y_UNIT_TEST(NullConstantAndInputCombinations) {
        for (const bool nullFirst : {true, false}) {
            AssertNullConstantWithInput(NKikimr::NKernels::EOperation::And,
                NKikimr::NKernels::UInt8VecToArray({1, 1}), {std::nullopt, std::nullopt}, nullFirst);
            AssertNullConstantWithInput(NKikimr::NKernels::EOperation::And,
                NKikimr::NKernels::UInt8VecToArray({1, 0}), {std::nullopt, 0}, nullFirst);
            AssertNullConstantWithInput(NKikimr::NKernels::EOperation::And,
                std::make_shared<arrow::UInt8Scalar>(1), {std::nullopt, std::nullopt}, nullFirst);
        }
        AssertNullConstantWithInput(NKikimr::NKernels::EOperation::And,
            arrow::MakeNullScalar(arrow::uint8()), {std::nullopt, std::nullopt}, true);
    }

    Y_UNIT_TEST(NullConstantOrInputCombinations) {
        for (const bool nullFirst : {true, false}) {
            AssertNullConstantWithInput(NKikimr::NKernels::EOperation::Or,
                NKikimr::NKernels::UInt8VecToArray({0, 0}), {std::nullopt, std::nullopt}, nullFirst);
            AssertNullConstantWithInput(NKikimr::NKernels::EOperation::Or,
                NKikimr::NKernels::UInt8VecToArray({0, 1}), {std::nullopt, 1}, nullFirst);
            AssertNullConstantWithInput(NKikimr::NKernels::EOperation::Or,
                std::make_shared<arrow::UInt8Scalar>(0), {std::nullopt, std::nullopt}, nullFirst);
        }
        AssertNullConstantWithInput(NKikimr::NKernels::EOperation::Or,
            arrow::MakeNullScalar(arrow::uint8()), {std::nullopt, std::nullopt}, true);
    }
};

void AssertCoalesceShortcut(const std::shared_ptr<NAccessor::IChunkedArray>& first, const std::shared_ptr<arrow::Scalar>& secondDefault,
                            const std::shared_ptr<arrow::DataType>& outputType, const bool expectedShortcut) {
    arrow::compute::ScalarKernel kernel;
    kernel.signature = arrow::compute::KernelSignature::Make({first->GetDataType(), secondDefault->type}, outputType);
    TAccessorsCollection resources(first->GetRecordsCount());
    resources.AddVerified(1, first, false);
    resources.AddVerified(2, std::make_shared<NAccessor::TSparsedArray>(secondDefault, secondDefault->type, first->GetRecordsCount()), false);

    auto result = TCoalesceKernelLogic(kernel).Execute(TColumnChainInfo::BuildVector({1, 2}),
                                                       TColumnChainInfo::BuildVector({3}), resources);

    UNIT_ASSERT(result.IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(*result, expectedShortcut);
    if (expectedShortcut) {
        UNIT_ASSERT_VALUES_EQUAL(resources.GetAccessorVerified(3).get(), first.get());
        UNIT_ASSERT_VALUES_EQUAL(resources.GetRecordsCountActualVerified(), first->GetRecordsCount());
    } else {
        // Output was not filled by shortcut, will be filled by kernel execution at later stage
        UNIT_ASSERT(!resources.GetAccessorOptional(3));
    }
}

class TCountingSparsedArray: public NAccessor::TSparsedArray {
private:
    using TBase = NAccessor::TSparsedArray;
    ui32& Materializations;

    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const override {
        ++Materializations;
        return TBase::DoGetLocalData(chunkCurrent, position);
    }

public:
    TCountingSparsedArray(const std::shared_ptr<arrow::Scalar>& defaultValue, const ui32 recordsCount, ui32& materializations)
        : TBase(defaultValue, defaultValue->type, recordsCount)
        , Materializations(materializations)
    {
    }
};

// Testing KernelLogic + kernel call integration inside TCalculationProcessor
void AssertCoalesceProcessor(const std::shared_ptr<NAccessor::IChunkedArray>& first, const bool expectedShortcut) {
    ui32 materializations = 0;
    auto second = std::make_shared<TCountingSparsedArray>(std::make_shared<arrow::UInt8Scalar>(1), first->GetRecordsCount(), materializations);
    auto resources = std::make_unique<TAccessorsCollection>(first->GetRecordsCount());
    resources->AddVerified(1, first, false);
    resources->AddVerified(2, second, false);
    TFailDataSource dataSource;
    TProcessorContext context(dataSource, std::move(resources), std::nullopt, false);

    const auto function = std::dynamic_pointer_cast<arrow::compute::ScalarFunction>(
        *arrow::compute::GetFunctionRegistry()->GetFunction("coalesce"));
    UNIT_ASSERT(function);
    arrow::compute::ScalarKernel kernel;
    const auto type = first->GetDataType();
    kernel.signature = arrow::compute::KernelSignature::Make({type, type}, type);
    auto processor = TCalculationProcessor::Build(TColumnChainInfo::BuildVector({1, 2}), TColumnChainInfo(3),
                                                  std::make_shared<TKernelFunction>(function), std::make_shared<TCoalesceKernelLogic>(kernel));
    UNIT_ASSERT(processor.IsSuccess());
    UNIT_ASSERT((*processor)->Execute(context, TExecutionNodeContext()).IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(materializations, expectedShortcut ? 0 : 1);
    const auto& output = context.GetResources().GetAccessorVerified(3);
    UNIT_ASSERT_VALUES_EQUAL(output->GetRecordsCount(), first->GetRecordsCount());
    if (expectedShortcut) {
        UNIT_ASSERT_VALUES_EQUAL(output.get(), first.get());
    }
    auto firstArray = arrow::Concatenate(first->GetChunkedArray()->chunks()).ValueOrDie();
    auto secondArray = arrow::MakeArrayFromScalar(*second->GetScalar(0), first->GetRecordsCount()).ValueOrDie();
    auto expected = arrow::compute::CallFunction("coalesce", {firstArray, secondArray}).ValueOrDie().make_array();
    auto actual = arrow::Concatenate(output->GetChunkedArray()->chunks()).ValueOrDie();
    UNIT_ASSERT(actual->Equals(*expected));
}

Y_UNIT_TEST_SUITE(CoalesceKernelLogic) {
    Y_UNIT_TEST(ReusesValidMonoSparseFirstInput) {
        auto first = NAccessor::TSparsedArray::BuildFalseArrayUI8(1000);
        AssertCoalesceShortcut(first, std::make_shared<arrow::UInt8Scalar>(0), first->GetDataType(), true);
    }

    Y_UNIT_TEST(FallsBackForNullSparseFirstInput) {
        const auto type = arrow::uint8();
        auto first = std::make_shared<NAccessor::TSparsedArray>(nullptr, type, 1000);
        AssertCoalesceShortcut(first, std::make_shared<arrow::UInt8Scalar>(0), type, false);
    }

    Y_UNIT_TEST(ReusesValidMonoStringFirstInput) {
        const auto type = arrow::utf8();
        auto first = std::make_shared<NAccessor::TSparsedArray>(std::make_shared<arrow::StringScalar>("first"), type, 1000);
        AssertCoalesceShortcut(first, std::make_shared<arrow::StringScalar>("second"), type, true);
    }

    Y_UNIT_TEST(ReusesValidMonoUInt64FirstInput) {
        const auto type = arrow::uint64();
        auto first = std::make_shared<NAccessor::TSparsedArray>(std::make_shared<arrow::UInt64Scalar>(42), type, 1000);
        AssertCoalesceShortcut(first, std::make_shared<arrow::UInt64Scalar>(0), type, true);
    }

    Y_UNIT_TEST(FallsBackWhenFirstTypeNeedsUnwrapping) {
        const auto outputType = arrow::uint8();
        auto firstType = arrow::struct_({arrow::field("item", outputType)});
        auto scalar = std::make_shared<arrow::StructScalar>(arrow::ScalarVector{arrow::MakeNullScalar(outputType)}, firstType);
        auto first = std::make_shared<NAccessor::TTrivialArray>(scalar);
        AssertCoalesceShortcut(first, std::make_shared<arrow::UInt8Scalar>(0), outputType, false);
    }

    Y_UNIT_TEST(FallsBackWhenOutputTypeDiffersFromInput) {
        auto firstType = arrow::timestamp(arrow::TimeUnit::SECOND);
        auto outputType = arrow::timestamp(arrow::TimeUnit::MICRO);
        auto first = std::make_shared<NAccessor::TSparsedArray>(std::make_shared<arrow::TimestampScalar>(42, firstType), firstType, 1000);
        AssertCoalesceShortcut(first, std::make_shared<arrow::TimestampScalar>(0, outputType), outputType, false);
    }

    Y_UNIT_TEST(ProcessorSkipsSparseSecondInputMaterializationOnShortcut) {
        AssertCoalesceProcessor(NAccessor::TSparsedArray::BuildFalseArrayUI8(1000), true);
    }

    Y_UNIT_TEST(ProcessorMaterializesSparseSecondInputOnFallback) {
        AssertCoalesceProcessor(std::make_shared<NAccessor::TSparsedArray>(nullptr, arrow::uint8(), 1000), false);
    }

    Y_UNIT_TEST(ReusesNullFreeNonUniformTrivialFirstInput) {
        auto first = std::make_shared<NAccessor::TTrivialArray>(NKikimr::NKernels::UInt8VecToArray({0, 1, 0}));
        AssertCoalesceShortcut(first, std::make_shared<arrow::UInt8Scalar>(1), first->GetDataType(), true);
    }

    Y_UNIT_TEST(ReusesNullFreeNonUniformSparseFirstInput) {
        const auto source = std::make_shared<NAccessor::TTrivialArray>(NKikimr::NKernels::UInt8VecToArray({0, 1, 0}));
        auto first = NAccessor::TSparsedArray::Make(*source, std::make_shared<arrow::UInt8Scalar>(0));
        AssertCoalesceShortcut(first, std::make_shared<arrow::UInt8Scalar>(1), first->GetDataType(), true);
    }

    Y_UNIT_TEST(ReusesNullFreeNonUniformDictionaryFirstInput) {
        auto first = std::make_shared<NAccessor::TDictionaryArray>(
            NKikimr::NKernels::UInt8VecToArray({0, 1}), NKikimr::NKernels::UInt8VecToArray({0, 1, 0}));
        AssertCoalesceShortcut(first, std::make_shared<arrow::UInt8Scalar>(1), first->GetDataType(), true);
    }

    Y_UNIT_TEST(ReusesNullFreeNonUniformCompositeFirstInput) {
        auto first = BuildCompositePredicate();
        AssertCoalesceShortcut(first, std::make_shared<arrow::UInt8Scalar>(1), first->GetDataType(), true);
    }

    Y_UNIT_TEST(FallsBackForMixedNullFirstInput) {
        auto first = std::make_shared<NAccessor::TTrivialArray>(NKikimr::NKernels::UInt8VecToArray({0, std::nullopt, 1}));
        AssertCoalesceShortcut(first, std::make_shared<arrow::UInt8Scalar>(1), first->GetDataType(), false);
    }

    Y_UNIT_TEST(FallsBackForMixedNullSparseFirstInput) {
        const auto source = std::make_shared<NAccessor::TTrivialArray>(NKikimr::NKernels::UInt8VecToArray({std::nullopt, 1, std::nullopt}));
        auto first = NAccessor::TSparsedArray::Make(*source, nullptr);
        AssertCoalesceShortcut(first, std::make_shared<arrow::UInt8Scalar>(1), first->GetDataType(), false);
    }

    Y_UNIT_TEST(FallsBackForMixedNullDictionaryFirstInput) {
        auto first = std::make_shared<NAccessor::TDictionaryArray>(
            NKikimr::NKernels::UInt8VecToArray({0, 1}), NKikimr::NKernels::UInt8VecToArray({0, std::nullopt, 1}));
        AssertCoalesceShortcut(first, std::make_shared<arrow::UInt8Scalar>(1), first->GetDataType(), false);
    }

    Y_UNIT_TEST(FallsBackWhenDictionaryReferencesNullValue) {
        auto first = std::make_shared<NAccessor::TDictionaryArray>(
            NKikimr::NKernels::UInt8VecToArray({0, std::nullopt}), NKikimr::NKernels::UInt8VecToArray({0, 1, 0}));
        AssertCoalesceShortcut(first, std::make_shared<arrow::UInt8Scalar>(1), first->GetDataType(), false);
    }

    Y_UNIT_TEST(ProcessorSkipsSparseSecondInputForNonUniformFirstInput) {
        auto first = std::make_shared<NAccessor::TTrivialArray>(NKikimr::NKernels::UInt8VecToArray({0, 1, 0}));
        AssertCoalesceProcessor(first, true);
    }
}; // Y_UNIT_TEST_SUITE(CoalesceKernelLogic)

Y_UNIT_TEST_SUITE(StreamLogic) {
    void AssertNeutralOperandForwardsSparseInput(const NKernels::EOperation operation, const bool neutralFirst) {
        auto source = std::make_shared<NAccessor::TTrivialArray>(NKikimr::NKernels::UInt8VecToArray({ 0, 1, 0 }));
        auto varying = NAccessor::TSparsedArray::Make(*source, std::make_shared<arrow::UInt8Scalar>(0));
        const ui32 recordsCount = varying->GetRecordsCount();
        auto neutral = operation == NKernels::EOperation::And
            ? NAccessor::TSparsedArray::BuildTrueArrayUI8(recordsCount)
            : NAccessor::TSparsedArray::BuildFalseArrayUI8(recordsCount);

        TStreamLogicTest test(recordsCount, {1, 2}, 3, operation);
        test.AddAccessor(1, neutralFirst ? neutral : varying);
        test.AddAccessor(2, neutralFirst ? varying : neutral);
        UNIT_ASSERT(!test.ProcessInput(1));
        UNIT_ASSERT(!test.ProcessInput(2));
        UNIT_ASSERT_VALUES_EQUAL(test.GetOutputAccessor().get(), varying.get());
    }

    void AssertAbsorbingOperandFinishesStream(const NKernels::EOperation operation, const bool absorbingFirst) {
        auto source = std::make_shared<NAccessor::TTrivialArray>(NKikimr::NKernels::UInt8VecToArray({ 0, 1, 0 }));
        auto varying = NAccessor::TSparsedArray::Make(*source, std::make_shared<arrow::UInt8Scalar>(0));
        const bool absorbingValue = operation == NKernels::EOperation::Or;
        auto absorbing = absorbingValue ? NAccessor::TSparsedArray::BuildTrueArrayUI8(varying->GetRecordsCount())
                                        : NAccessor::TSparsedArray::BuildFalseArrayUI8(varying->GetRecordsCount());

        TStreamLogicTest test(varying->GetRecordsCount(), { 1, 2 }, 3, operation);
        test.AddAccessor(1, absorbingFirst ? absorbing : varying);
        test.AddAccessor(2, absorbingFirst ? varying : absorbing);
        UNIT_ASSERT_VALUES_EQUAL(test.ProcessInput(1), absorbingFirst);
        const auto output = test.GetOutputAccessor();
        UNIT_ASSERT_VALUES_EQUAL(test.ProcessInput(2), true);
        if (absorbingFirst) {
            UNIT_ASSERT_VALUES_EQUAL(test.GetOutputAccessor().get(), output.get());
        }
        test.AssertResult({ absorbingValue, absorbingValue, absorbingValue });
    }

    void AssertAbsorbingScalarAfterNeutralAccumulator(const NKernels::EOperation operation) {
        const bool absorbingValue = operation == NKernels::EOperation::Or;
        const std::vector<std::optional<ui8>> expected(2, absorbingValue);
        auto neutral = absorbingValue ? NAccessor::TSparsedArray::BuildFalseArrayUI8(expected.size())
                                      : NAccessor::TSparsedArray::BuildTrueArrayUI8(expected.size());

        TStreamLogicTest test(expected.size(), {1, 2}, 3, operation);
        test.AddAccessor(1, neutral);
        test.AddScalar(2, std::make_shared<arrow::UInt8Scalar>(absorbingValue));
        UNIT_ASSERT(!test.ProcessInput(1));
        UNIT_ASSERT(test.ProcessInput(2));
        test.AssertResult(expected);
    }

    Y_UNIT_TEST(AndForwardsSparseInputAfterNeutralAccumulator) {
        AssertNeutralOperandForwardsSparseInput(NKernels::EOperation::And, true);
    }

    Y_UNIT_TEST(OrForwardsSparseInputAfterNeutralAccumulator) {
        AssertNeutralOperandForwardsSparseInput(NKernels::EOperation::Or, true);
    }

    Y_UNIT_TEST(AndKeepsSparseAccumulatorForNeutralInput) {
        AssertNeutralOperandForwardsSparseInput(NKernels::EOperation::And, false);
    }

    Y_UNIT_TEST(OrKeepsSparseAccumulatorForNeutralInput) {
        AssertNeutralOperandForwardsSparseInput(NKernels::EOperation::Or, false);
    }

    Y_UNIT_TEST(AndKeepsAbsorbingAccumulator) {
        AssertAbsorbingOperandFinishesStream(NKernels::EOperation::And, true);
    }

    Y_UNIT_TEST(OrKeepsAbsorbingAccumulator) {
        AssertAbsorbingOperandFinishesStream(NKernels::EOperation::Or, true);
    }

    Y_UNIT_TEST(AndFinishesOnAbsorbingInput) {
        AssertAbsorbingOperandFinishesStream(NKernels::EOperation::And, false);
    }

    Y_UNIT_TEST(OrFinishesOnAbsorbingInput) {
        AssertAbsorbingOperandFinishesStream(NKernels::EOperation::Or, false);
    }

    Y_UNIT_TEST(AndFinishesOnAbsorbingScalarAfterNeutralAccumulator) {
        AssertAbsorbingScalarAfterNeutralAccumulator(NKernels::EOperation::And);
    }

    Y_UNIT_TEST(OrFinishesOnAbsorbingScalarAfterNeutralAccumulator) {
        AssertAbsorbingScalarAfterNeutralAccumulator(NKernels::EOperation::Or);
    }

    Y_UNIT_TEST(StopsReadyInputBatchAfterKernelAbsorbingResult) {
        TFailDataSource dataSource;
        auto resources = std::make_unique<TAccessorsCollection>(2);
        resources->AddVerified(1, std::make_shared<NAccessor::TTrivialArray>(NKikimr::NKernels::UInt8VecToArray({ std::nullopt, 1 })), false);
        resources->AddVerified(2, std::make_shared<NAccessor::TTrivialArray>(NKikimr::NKernels::UInt8VecToArray({ 0, 0 })), false);
        ui32 materializations = 0;
        // Processing input 3 would materialize it.
        resources->AddVerified(3, std::make_shared<TCountingSparsedArray>(arrow::MakeNullScalar(arrow::uint8()), 2, materializations), false);

        TProcessorContext context(dataSource, std::move(resources), std::nullopt, false);
        NGraph::NExecution::TExecutionVisitor visitor(std::move(context));
        auto processor =
            std::make_shared<TStreamLogicProcessor>(TColumnChainInfo::BuildVector({ 1, 2, 3 }), TColumnChainInfo(4), NKernels::EOperation::And);
        NGraph::NExecution::TCompiledGraph::TNode node(5, processor);
        UNIT_ASSERT(visitor.OnEnter(node).IsSuccess());
        UNIT_ASSERT(visitor.OnComeback(node, TColumnChainInfo::BuildVector({ 1 })).IsSuccess());
        UNIT_ASSERT(visitor.OnComeback(node, TColumnChainInfo::BuildVector({ 2, 3 })).IsSuccess());

        UNIT_ASSERT_VALUES_EQUAL(materializations, 0);
        const auto& output = visitor.MutableContext().GetResources().GetAccessorVerified(4);
        UNIT_ASSERT(arrow::Concatenate(output->GetChunkedArray()->chunks()).ValueOrDie()->Equals(*NKikimr::NKernels::UInt8VecToArray({ 0, 0 })));
        const auto exit = visitor.OnExit(node);
        UNIT_ASSERT(exit.IsSuccess());
        UNIT_ASSERT(*exit == NGraph::NExecution::TCompiledGraph::IVisitor::EVisitStatus::Skipped);
    }

    void AssertFinishesOnPlainAbsorbingInputAfterNeutralAccumulator(const NKernels::EOperation operation) {
        const bool absorbingValue = operation == NKernels::EOperation::Or;
        const std::vector<std::optional<ui8>> expected(2, absorbingValue);
        auto neutral = absorbingValue ? NAccessor::TSparsedArray::BuildFalseArrayUI8(expected.size())
                                      : NAccessor::TSparsedArray::BuildTrueArrayUI8(expected.size());

        TStreamLogicTest test(expected.size(), { 1, 2 }, 3, operation);
        test.AddAccessor(1, neutral);
        test.AddAccessor(2, std::make_shared<NAccessor::TTrivialArray>(NKikimr::NKernels::UInt8VecToArray(expected)));
        UNIT_ASSERT(!test.ProcessInput(1));
        UNIT_ASSERT(test.ProcessInput(2));
        test.AssertResult(expected);
    }

    Y_UNIT_TEST(AndFinishesOnPlainAbsorbingInputAfterNeutralAccumulator) {
        AssertFinishesOnPlainAbsorbingInputAfterNeutralAccumulator(NKernels::EOperation::And);
    }

    Y_UNIT_TEST(OrFinishesOnPlainAbsorbingInputAfterNeutralAccumulator) {
        AssertFinishesOnPlainAbsorbingInputAfterNeutralAccumulator(NKernels::EOperation::Or);
    }

    Y_UNIT_TEST(AndDoesNotFinishForNullableInput) {
        TStreamLogicTest test(2, { 1, 2 }, 3, NKernels::EOperation::And);
        test.AddScalar(1, std::make_shared<arrow::UInt8Scalar>(1));
        test.AddArray(2, NKikimr::NKernels::UInt8VecToArray({ 0, std::nullopt }));
        UNIT_ASSERT(!test.ProcessInput(1));
        UNIT_ASSERT(!test.ProcessInput(2));
        test.AssertResult({ 0, std::nullopt });
    }
};

} // namespace NKikimr::NArrow::NSSA
