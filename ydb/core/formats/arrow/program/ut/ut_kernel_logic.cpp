#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/dictionary/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>
#include <ydb/core/formats/arrow/filter/filter.h>
#include <ydb/core/formats/arrow/program/execution.h>
#include <ydb/core/formats/arrow/program/filter.h>
#include <ydb/core/formats/arrow/program/kernel_logic.h>
#include <ydb/core/formats/arrow/program/stream_logic.h>

#include <ydb/library/arrow_kernels/ut_common.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/concatenate.h>
#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/types/binary_json/write.h>

#include <initializer_list>
#include <string_view>

namespace NKikimr::NArrow::NSSA {
namespace {

class TTestGetJsonPath: public TGetJsonPath {
public:
    using TGetJsonPath::ExtractArray;
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

    bool ProcessInput(const ui32 inputId) {
        const auto result = Processor.OnInputReady(inputId, Context, NodeContext);
        UNIT_ASSERT(result.IsSuccess());
        return *result;
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
}

Y_UNIT_TEST_SUITE(JsonValue) {
    Y_UNIT_TEST(UsesNativeStringBuffers) {
        auto input = BuildSubColumns({ R"({"s":"x"})", R"({"s":"yy"})", R"({"s":"zzz"})" }, BuildSettings(0, 1024));
        auto accessor = input->GetPathAccessor("$.s", input->GetRecordsCount()).DetachResult();
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
        auto accessor = dictionaryInput->GetPathAccessor("$.s", dictionaryInput->GetRecordsCount()).DetachResult();
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

    Y_UNIT_TEST(NullConstantAndTruePreservesNull) {
        TStreamLogicTest test(2, {1, 2}, 3, NKikimr::NKernels::EOperation::And);
        test.AddScalar(1, arrow::MakeNullScalar(arrow::uint8()));
        test.AddArray(2, NKikimr::NKernels::UInt8VecToArray({1, 1}));
        UNIT_ASSERT(!test.ProcessInput(1));
        UNIT_ASSERT(!test.ProcessInput(2));
        test.AssertResult({std::nullopt, std::nullopt});
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
};

} // namespace NKikimr::NArrow::NSSA
