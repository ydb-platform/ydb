#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/dictionary/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
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
        UNIT_ASSERT_VALUES_EQUAL(values->length(), 1);
    });
    return sliced;
}

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
    TFailDataSource dataSource;
    auto resources = std::make_unique<NAccessor::TAccessorsCollection>(predicate->GetRecordsCount());
    resources->AddVerified(1, predicate, false);
    std::vector<std::optional<ui8>> allTrue(predicate->GetRecordsCount(), 1);
    resources->AddVerified(2, std::make_shared<NAccessor::TTrivialArray>(NKikimr::NKernels::UInt8VecToArray(allTrue)), false);
    TProcessorContext context(dataSource, std::move(resources), std::nullopt, false);
    TStreamLogicProcessor processor(TColumnChainInfo::BuildVector({1, 2}), TColumnChainInfo(3), NKikimr::NKernels::EOperation::And);
    TExecutionNodeContext nodeContext;
    UNIT_ASSERT(processor.OnInputReady(1, context, nodeContext).IsSuccess());
    UNIT_ASSERT(processor.OnInputReady(2, context, nodeContext).IsSuccess());
    UNIT_ASSERT(arrow::Concatenate(context.GetResources().GetAccessorVerified(3)->GetChunkedArray()->chunks()).ValueOrDie()->Equals(*NKikimr::NKernels::UInt8VecToArray(expected)));
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
        TFailDataSource dataSource;
        auto resources = std::make_unique<NAccessor::TAccessorsCollection>(2);
        resources->AddConstantVerified(1, arrow::MakeNullScalar(arrow::uint8()));
        resources->AddVerified(2, std::make_shared<NAccessor::TTrivialArray>(NKikimr::NKernels::UInt8VecToArray({1, 1})), false);
        TProcessorContext context(dataSource, std::move(resources), std::nullopt, false);
        TStreamLogicProcessor processor(TColumnChainInfo::BuildVector({1, 2}), TColumnChainInfo(3), NKikimr::NKernels::EOperation::And);
        TExecutionNodeContext nodeContext;

        auto firstResult = processor.OnInputReady(1, context, nodeContext);
        UNIT_ASSERT(firstResult.IsSuccess());
        UNIT_ASSERT(!*firstResult);
        UNIT_ASSERT(processor.OnInputReady(2, context, nodeContext).IsSuccess());
        UNIT_ASSERT(arrow::Concatenate(context.GetResources().GetAccessorVerified(3)->GetChunkedArray()->chunks()).ValueOrDie()->Equals(*NKikimr::NKernels::UInt8VecToArray({std::nullopt, std::nullopt})));
    }

    Y_UNIT_TEST(NullIntermediateAndResultDoesNotFinishStream) {
        TFailDataSource dataSource;
        auto resources = std::make_unique<NAccessor::TAccessorsCollection>(2);
        resources->AddVerified(1, std::make_shared<NAccessor::TTrivialArray>(NKikimr::NKernels::UInt8VecToArray({std::nullopt, 1})), false);
        resources->AddVerified(2, std::make_shared<NAccessor::TTrivialArray>(NKikimr::NKernels::UInt8VecToArray({1, std::nullopt})), false);
        resources->AddVerified(3, std::make_shared<NAccessor::TTrivialArray>(NKikimr::NKernels::UInt8VecToArray({0, 0})), false);
        TProcessorContext context(dataSource, std::move(resources), std::nullopt, false);
        TStreamLogicProcessor processor(TColumnChainInfo::BuildVector({1, 2, 3}), TColumnChainInfo(4), NKikimr::NKernels::EOperation::And);
        TExecutionNodeContext nodeContext;

        UNIT_ASSERT(processor.OnInputReady(1, context, nodeContext).IsSuccess());
        auto secondResult = processor.OnInputReady(2, context, nodeContext);
        UNIT_ASSERT(secondResult.IsSuccess());
        UNIT_ASSERT(!*secondResult);
        UNIT_ASSERT(processor.OnInputReady(3, context, nodeContext).IsSuccess());
        UNIT_ASSERT(arrow::Concatenate(context.GetResources().GetAccessorVerified(4)->GetChunkedArray()->chunks()).ValueOrDie()->Equals(*NKikimr::NKernels::UInt8VecToArray({0, 0})));
    }
};

} // namespace NKikimr::NArrow::NSSA
