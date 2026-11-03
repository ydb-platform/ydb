#include <ydb/core/formats/arrow/program/execution.h>
#include <ydb/core/formats/arrow/program/assign_internal.h>
#include <ydb/core/formats/arrow/program/filter.h>
#include <ydb/core/formats/arrow/program/functions.h>
#include <ydb/core/formats/arrow/program/stats.h>
#include <ydb/library/formats/arrow/helpers/array.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NArrow::NSSA {

    Y_UNIT_TEST_SUITE(ArrowRuntimeStats) {
        Y_UNIT_TEST(PlaceholderCalculationUsesKnownKernelDescription) {
            auto function = std::make_shared<arrow::compute::ScalarFunction>("local_function", arrow::compute::Arity(2), nullptr);
            auto processor = TCalculationProcessor::Build(TColumnChainInfo::BuildVector({1, 2}), TColumnChainInfo(3),
                std::make_shared<TKernelFunction>(function), std::make_shared<TCompareKernel>(false, TIndexCheckOperation::EOperation::Equals));
            UNIT_ASSERT(processor.IsSuccess());
            UNIT_ASSERT_STRINGS_EQUAL((*processor)->GetStatsFunctionDescription(), "TCompareKernel{Equals}");
            UNIT_ASSERT_STRINGS_EQUAL(BuildStatsExpression((*processor)->GetStatsFunctionDescription(), {"level", "7"}, 1),
                "TCompareKernel{Equals}(level, 7)");
        }

        Y_UNIT_TEST(SharedExpressionDagDescriptionsStayBounded) {
            std::vector<TString> labels = {"price", "quantity"};
            for (ui32 nodeId = 2; nodeId < 47; ++nodeId) {
                labels.push_back(BuildStatsExpression("add", {labels[nodeId - 1], labels[nodeId - 2]}, nodeId));
                UNIT_ASSERT(labels.back().size() <= MaxStatsDescriptionBytes);
            }
            UNIT_ASSERT_STRINGS_EQUAL(labels[2], "add(quantity, price)");
            UNIT_ASSERT(labels.back().EndsWith("... [expr #46]"));
        }

        Y_UNIT_TEST(ClippedDescriptionsKeepNodeIdentityAndUtf8) {
            TString largeConstant;
            for (ui32 i = 0; i < 1024; ++i) {
                largeConstant += "цена";
            }
            const auto first = BoundStatsDescription(largeConstant, 7);
            const auto second = BoundStatsDescription(largeConstant, 8);
            UNIT_ASSERT(first.size() <= MaxStatsDescriptionBytes);
            UNIT_ASSERT(second.size() <= MaxStatsDescriptionBytes);
            UNIT_ASSERT(first != second);
            UNIT_ASSERT(first.EndsWith("... [expr #7]"));
            UNIT_ASSERT_STRINGS_EQUAL(Utf8Truncate(first, first.size()), first);
            UNIT_ASSERT_STRINGS_EQUAL(BoundStatsDescription(largeConstant, 7), first);
            const auto expression = BuildStatsExpression(largeConstant, {largeConstant, largeConstant}, 9);
            UNIT_ASSERT(expression.size() <= MaxStatsDescriptionBytes);
            UNIT_ASSERT(expression.EndsWith("... [expr #9]"));
            UNIT_ASSERT_STRINGS_EQUAL(Utf8Truncate(expression, expression.size()), expression);
        }

        Y_UNIT_TEST(ShortDescriptionsRemainByteExact) {
            UNIT_ASSERT_STRINGS_EQUAL(BuildStatsExpression("add", {"price", "quantity"}, 7), "add(price, quantity)");
            UNIT_ASSERT_STRINGS_EQUAL(BoundStatsDescription("42", 8), "42");
        }

        Y_UNIT_TEST(ArgumentDomainCountsArraysOnceAndBroadcastsScalars) {
            const auto three = NKernels::NumVecToArray(arrow::int32(), {1, 2, 3});
            const auto two = NKernels::NumVecToArray(arrow::int32(), {1, 2});
            const arrow::Datum scalar(std::make_shared<arrow::Int32Scalar>(1));
            UNIT_ASSERT_VALUES_EQUAL(*GetStatsRowsPopulation({three, three}), 3);
            UNIT_ASSERT_VALUES_EQUAL(*GetStatsRowsPopulation({three, scalar}), 3);
            UNIT_ASSERT_VALUES_EQUAL(*GetStatsRowsPopulation({scalar, scalar}), 1);
            UNIT_ASSERT(!GetStatsRowsPopulation({three, two}));
        }

        Y_UNIT_TEST(RealCalculationIgnoresUnrelatedScalarRecordCount) {
            auto collector = std::make_shared<NScanStats::TCollector>(1);
            auto operation = collector->Start(NScanStats::TStep::Calculation, "add", "add(price, quantity)");
            const auto array = NKernels::NumVecToArray(arrow::int32(), {1, 2, 3});
            TAccessorsCollection resources(3);
            resources.AddInput(1, arrow::Datum(array), false);
            resources.AddInput(2, arrow::Datum(array), false);
            resources.AddCalculated(9, arrow::Datum(std::make_shared<arrow::Int32Scalar>(10)));
            UNIT_ASSERT_VALUES_EQUAL(resources.GetRecordsCountActualVerified(), 1);
            TSimpleFunction function(NKernels::EOperation::Add);
            {
                NScanStats::TScope scope(operation);
                auto result = function.Call(TExecFunctionContext(TColumnChainInfo::BuildVector({1, 2})), resources);
                UNIT_ASSERT_C(!result.IsFail(), result.GetErrorMessage());
                UNIT_ASSERT_VALUES_EQUAL(result->GetAccessorVerified()->GetRecordsCount(), 3);
            }
            operation->Finish();
            auto report = collector->Drain(true);
            UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(0).GetDetails().GetCalculation().GetRowsCount(), 3);
        }

        Y_UNIT_TEST(SequentialFilterUsesRetainedCandidates) {
            auto collector = std::make_shared<NScanStats::TCollector>(1);
            TFakeDataSource source;
            auto resources = std::make_unique<TAccessorsCollection>(4);
            resources->AddInput(1, arrow::Datum(NKernels::BoolVecToArray({true, false, true, true})), false);
            resources->AddInput(2, arrow::Datum(NKernels::BoolVecToArray({true, true, false, true})), false);
            TProcessorContext context(source, std::move(resources), std::nullopt, false);
            for (ui32 id : {1, 2}) {
                auto operation = collector->Start(NScanStats::TStep::Filter, "filter", "filter");
                {
                    NScanStats::TScope scope(operation);
                    auto result = TFilterProcessor(TColumnChainInfo(id)).Execute(context, TExecutionNodeContext());
                    UNIT_ASSERT_C(!result.IsFail(), result.GetErrorMessage());
                }
                operation->Finish();
            }
            auto report = collector->Drain(true);
            UNIT_ASSERT_VALUES_EQUAL(report->StepsSize(), 2);
            UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(0).GetDetails().GetFilter().GetInputRows(), 4);
            UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(0).GetDetails().GetFilter().GetOutputRows(), 3);
            UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(1).GetDetails().GetFilter().GetInputRows(), 3);
            UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(1).GetDetails().GetFilter().GetOutputRows(), 2);
        }
    } // Y_UNIT_TEST_SUITE(ArrowRuntimeStats)

} // namespace NKikimr::NArrow::NSSA
