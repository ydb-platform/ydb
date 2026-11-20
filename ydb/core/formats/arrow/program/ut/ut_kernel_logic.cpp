#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/dictionary/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/formats/arrow/filter/filter.h>
#include <ydb/core/formats/arrow/program/execution.h>
#include <ydb/core/formats/arrow/program/filter.h>
#include <ydb/core/formats/arrow/program/stream_logic.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/concatenate.h>
#include <library/cpp/testing/unittest/registar.h>

#include <initializer_list>
#include <optional>
#include <vector>

namespace NKikimr::NArrow::NSSA {
namespace {

    std::shared_ptr<arrow::Array> MakeUInt8Array(const std::vector<std::optional<ui8>>& values) {
        arrow::UInt8Builder builder;
        for (const auto& value : values) {
            if (value) {
                UNIT_ASSERT(builder.Append(*value).ok());
            } else {
                UNIT_ASSERT(builder.AppendNull().ok());
            }
        }
        return builder.Finish().ValueOrDie();
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

        void AddInput(const ui32 inputId, const std::shared_ptr<arrow::Array>& array) {
            AddAccessor(inputId, std::make_shared<NAccessor::TTrivialArray>(array));
        }

        void AddInput(const ui32 inputId, const std::shared_ptr<arrow::Scalar>& scalar) {
            Context.MutableResources().AddConstantVerified(inputId, scalar);
        }

        bool ProcessInput(const ui32 inputId) {
            const auto result = Processor.OnInputReady(inputId, Context, NodeContext);
            UNIT_ASSERT(result.IsSuccess());
            return *result;
        }

        void AssertResult(const std::vector<std::optional<ui8>>& expected) const {
            const auto actual = arrow::Concatenate(Context.GetResources().GetAccessorVerified(OutputId)->GetChunkedArray()->chunks()).ValueOrDie();
            UNIT_ASSERT(actual->Equals(*MakeUInt8Array(expected)));
        }
    };

    template <class TInput>
    void AssertNullConstantWithInput(const NKikimr::NKernels::EOperation operation, const std::shared_ptr<TInput>& input,
                                        const std::vector<std::optional<ui8>>& expected, const bool nullFirst) {
        TStreamLogicTest test(expected.size(), {1, 2}, 3, operation);
        test.AddInput(1, arrow::MakeNullScalar(arrow::uint8()));
        test.AddInput(2, input);
        UNIT_ASSERT(!test.ProcessInput(nullFirst ? 1 : 2));
        UNIT_ASSERT(!test.ProcessInput(nullFirst ? 2 : 1));
        test.AssertResult(expected);
    }

    std::shared_ptr<NAccessor::IChunkedArray> BuildCompositePredicate() {
        NAccessor::TCompositeChunkedArray::TBuilder builder(arrow::uint8());
        builder.AddChunk(std::make_shared<NAccessor::TDictionaryArray>(
            MakeUInt8Array({1}), MakeUInt8Array({0, 0})));
        builder.AddChunk(std::make_shared<NAccessor::TDictionaryArray>(
            MakeUInt8Array({0}), MakeUInt8Array({0, 0})));
        return builder.Finish();
    }

    std::shared_ptr<NAccessor::IChunkedArray> BuildSlicedDictionaryPredicate() {
        auto predicate = std::make_shared<NAccessor::TDictionaryArray>(
            MakeUInt8Array({1, std::nullopt}), MakeUInt8Array({0, std::nullopt, 0}));
        auto sliced = predicate->ISlice(0, 3);
        UNIT_ASSERT(sliced->GetType() == NAccessor::IChunkedArray::EType::Dictionary);
        sliced->VisitDistinctValues([](const std::shared_ptr<arrow::Array>& values) {
            UNIT_ASSERT_VALUES_EQUAL(values->length(), 2);
        });
        return sliced;
    }

    void AssertFilteredRows(const std::shared_ptr<NAccessor::IChunkedArray>& predicate,
                            const std::vector<std::optional<ui8>>& expected) {
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
            std::make_shared<NAccessor::TTrivialArray>(MakeUInt8Array(rows)));
        UNIT_ASSERT(arrow::Concatenate(filtered->GetChunkedArray()->chunks()).ValueOrDie()->Equals(*MakeUInt8Array(expected)));
    }

    void AssertAndResult(const std::shared_ptr<NAccessor::IChunkedArray>& predicate,
                            const std::vector<std::optional<ui8>>& expected) {
        TStreamLogicTest test(predicate->GetRecordsCount(), {1, 2}, 3, NKikimr::NKernels::EOperation::And);
        test.AddAccessor(1, predicate);
        std::vector<std::optional<ui8>> allTrue(predicate->GetRecordsCount(), 1);
        test.AddInput(2, MakeUInt8Array(allTrue));
        UNIT_ASSERT(!test.ProcessInput(1));
        UNIT_ASSERT(!test.ProcessInput(2));
        test.AssertResult(expected);
    }

} // namespace

Y_UNIT_TEST_SUITE(KernelLogic) {
    Y_UNIT_TEST(NullConstantAndInputCombinations) {
        for (const bool nullFirst : {true, false}) {
            AssertNullConstantWithInput(NKikimr::NKernels::EOperation::And,
                                        MakeUInt8Array({1, 1}), {std::nullopt, std::nullopt}, nullFirst);
            AssertNullConstantWithInput(NKikimr::NKernels::EOperation::And,
                                        MakeUInt8Array({1, 0}), {std::nullopt, 0}, nullFirst);
            AssertNullConstantWithInput(NKikimr::NKernels::EOperation::And,
                                        std::make_shared<arrow::UInt8Scalar>(1), {std::nullopt, std::nullopt}, nullFirst);
        }
        AssertNullConstantWithInput(NKikimr::NKernels::EOperation::And,
                                    arrow::MakeNullScalar(arrow::uint8()), {std::nullopt, std::nullopt}, true);
    }

    Y_UNIT_TEST(NullConstantOrInputCombinations) {
        for (const bool nullFirst : {true, false}) {
            AssertNullConstantWithInput(NKikimr::NKernels::EOperation::Or,
                                        MakeUInt8Array({0, 0}), {std::nullopt, std::nullopt}, nullFirst);
            AssertNullConstantWithInput(NKikimr::NKernels::EOperation::Or,
                                        MakeUInt8Array({0, 1}), {std::nullopt, 1}, nullFirst);
            AssertNullConstantWithInput(NKikimr::NKernels::EOperation::Or,
                                        std::make_shared<arrow::UInt8Scalar>(0), {std::nullopt, std::nullopt}, nullFirst);
        }
        AssertNullConstantWithInput(NKikimr::NKernels::EOperation::Or,
                                    arrow::MakeNullScalar(arrow::uint8()), {std::nullopt, std::nullopt}, true);
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
            MakeUInt8Array({1}), MakeUInt8Array({std::nullopt, std::nullopt}));
        AssertAndResult(allNullDict, {std::nullopt, std::nullopt});
    }

    Y_UNIT_TEST(NullIntermediateAndResultDoesNotFinishStream) {
        TStreamLogicTest test(2, {1, 2, 3}, 4, NKikimr::NKernels::EOperation::And);
        test.AddInput(1, MakeUInt8Array({std::nullopt, 1}));
        test.AddInput(2, MakeUInt8Array({1, std::nullopt}));
        test.AddInput(3, MakeUInt8Array({0, 0}));
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
        const auto values = MakeUInt8Array({1, 1});
        const auto nullable = MakeUInt8Array({std::nullopt, 1});
        auto predicate = std::make_shared<arrow::UInt8Array>(
            values->length(), values->data()->buffers[1], nullable->data()->buffers[0], nullable->null_count());
        UNIT_ASSERT(predicate->IsNull(0));
        UNIT_ASSERT_C(predicate->raw_values()[0] == 1, "NULL payload must be nonzero to exercise the Or finish check");

        TStreamLogicTest test(2, {1, 2}, 3, NKikimr::NKernels::EOperation::Or);
        test.AddInput(1, predicate);
        test.AddInput(2, MakeUInt8Array({0, 0}));
        UNIT_ASSERT(!test.ProcessInput(1));
        UNIT_ASSERT(!test.ProcessInput(2));
        test.AssertResult({std::nullopt, 1});
    }

    Y_UNIT_TEST(NullOrTrueFinishesStream) {
        TStreamLogicTest test(2, {1, 2}, 3, NKikimr::NKernels::EOperation::Or);
        test.AddInput(1, MakeUInt8Array({std::nullopt, 1}));
        test.AddInput(2, MakeUInt8Array({1, 1}));
        UNIT_ASSERT(!test.ProcessInput(1));
        UNIT_ASSERT(test.ProcessInput(2));
        test.AssertResult({1, 1});
    }
}; // Y_UNIT_TEST_SUITE(KernelLogic)

} // namespace NKikimr::NArrow::NSSA
