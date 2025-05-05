#include "aggr_keys.h"
#include "collection.h"
#include "execution.h"

#include <util/string/join.h>

#ifndef WIN32
#ifdef NO_SANITIZE_THREAD
#undef NO_SANITIZE_THREAD
#endif
#include <AggregateFunctions/IAggregateFunction.h>
#else
namespace CH {
enum class AggFunctionId {
    AGG_UNSPECIFIED = 0,
    AGG_ANY = 1,
    AGG_COUNT = 2,
    AGG_MIN = 3,
    AGG_MAX = 4,
    AGG_SUM = 5,
    AGG_AVG = 6,
    //AGG_VAR = 7,
    //AGG_COVAR = 8,
    //AGG_STDDEV = 9,
    //AGG_CORR = 10,
    //AGG_ARG_MIN = 11,
    //AGG_ARG_MAX = 12,
    //AGG_COUNT_DISTINCT = 13,
    //AGG_QUANTILES = 14,
    //AGG_TOP_COUNT = 15,
    //AGG_TOP_SUM = 16,
    AGG_NUM_ROWS = 17,
};
struct GroupByOptions: public arrow::compute::ScalarAggregateOptions {
    struct Assign {
        AggFunctionId function = AggFunctionId::AGG_UNSPECIFIED;
        std::string result_column;
        std::vector<std::string> arguments;
    };

    std::shared_ptr<arrow::Schema> schema;
    std::vector<Assign> assigns;
    bool has_nullable_key = true;
};
}   // namespace CH
#endif

namespace NKikimr::NArrow::NSSA::NAggregation {

CH::AggFunctionId TWithKeysAggregationOption::GetHouseFunction(const EAggregate op) {
    switch (op) {
        case EAggregate::Some:
            return CH::AggFunctionId::AGG_ANY;
        case EAggregate::Count:
            return CH::AggFunctionId::AGG_COUNT;
        case EAggregate::Min:
            return CH::AggFunctionId::AGG_MIN;
        case EAggregate::Max:
            return CH::AggFunctionId::AGG_MAX;
        case EAggregate::Sum:
            return CH::AggFunctionId::AGG_SUM;
        case EAggregate::NumRows:
            return CH::AggFunctionId::AGG_NUM_ROWS;
        default:
            break;
    }
    return CH::AggFunctionId::AGG_UNSPECIFIED;
}

TConclusion<IResourceProcessor::EExecutionResult> TWithKeysAggregationProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    CH::GroupByOptions funcOpts;
    funcOpts.assigns.reserve(AggregationKeys.size() + Aggregations.size());
    funcOpts.has_nullable_key = false;

    std::vector<arrow::Datum> batch;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::set<ui32> fieldsUsage;
    for (auto& key : AggregationKeys) {
        AFL_VERIFY(fieldsUsage.emplace(key.GetColumnId()).second);
        batch.emplace_back(context.GetResources()->GetArrayVerified(key.GetColumnId()));
        fields.emplace_back(context.GetResources()->GetFieldVerified(key.GetColumnId()));
        funcOpts.assigns.emplace_back(CH::GroupByOptions::Assign{ .result_column = ::ToString(key.GetColumnId()) });

        if (!funcOpts.has_nullable_key) {
            arrow::Datum res = batch.back();
            if (res.is_array()) {
                funcOpts.has_nullable_key = res.array()->MayHaveNulls();
            } else {
                return TConclusionStatus::Fail("GROUP BY may be for record batch only.");
            }
        }
    }
    for (auto& aggr : Aggregations) {
        const CH::GroupByOptions::Assign gbAssign = [&aggr]() {
            CH::GroupByOptions::Assign descr;
            descr.function = TWithKeysAggregationOption::GetHouseFunction(aggr.GetAggregationId());
            descr.result_column = ::ToString(aggr.GetOutput().GetColumnId());
            descr.arguments.reserve(aggr.GetInputs().size());

            for (auto& colName : aggr.GetInputs()) {
                descr.arguments.push_back(::ToString(colName.GetColumnId()));
            }
            return descr;
        }();

        funcOpts.assigns.emplace_back(gbAssign);
        for (auto&& i : aggr.GetInputs()) {
            if (fieldsUsage.emplace(i.GetColumnId()).second) {
                batch.emplace_back(context.GetResources()->GetArrayVerified(i.GetColumnId()));
                fields.emplace_back(context.GetResources()->GetFieldVerified(i.GetColumnId()));
            }
        }
    }

    funcOpts.schema = std::make_shared<arrow::Schema>(fields);

    auto gbRes = arrow::compute::CallFunction(GetHouseGroupByName(), batch, &funcOpts, GetCustomExecContext());
    if (!gbRes.ok()) {
        return TConclusionStatus::Fail(gbRes.status().ToString());
    }
    auto gbBatch = (*gbRes).record_batch();
    context.GetResources()->Remove(AggregationKeys);

    for (auto& assign : funcOpts.assigns) {
        auto column = gbBatch->GetColumnByName(assign.result_column);
        if (!column) {
            return TConclusionStatus::Fail("No expected column in GROUP BY result.");
        }
        if (auto columnId = TryFromString<ui32>(assign.result_column)) {
            context.GetResources()->AddVerified(*columnId, column, false);
        } else {
            return TConclusionStatus::Fail("Incorrect column id from name: " + assign.result_column);
        }
    }
    return IResourceProcessor::EExecutionResult::Success;
}

TConclusion<std::shared_ptr<TWithKeysAggregationProcessor>> TWithKeysAggregationProcessor::TBuilder::Finish() {
    AFL_VERIFY(!Finished);
    Finished = true;
    if (Keys.empty()) {
        return TConclusionStatus::Fail("no keys for aggregation");
    }
    if (Aggregations.empty()) {
        return TConclusionStatus::Fail("no aggregations");
    }
    std::set<ui32> input;
    std::set<ui32> output;
    for (auto&& i : Keys) {
        input.emplace(i.GetColumnId());
    }
    for (auto&& i : Aggregations) {
        for (auto&& inp : i.GetInputs()) {
            input.emplace(inp.GetColumnId());
        }
        output.emplace(i.GetOutput().GetColumnId());
    }
    std::vector<TColumnChainInfo> inputChainColumns;
    for (auto&& i : input) {
        inputChainColumns.emplace_back(i);
    }
    std::vector<TColumnChainInfo> outputChainColumns;
    for (auto&& i : output) {
        outputChainColumns.emplace_back(i);
    }
    return std::shared_ptr<TWithKeysAggregationProcessor>(new TWithKeysAggregationProcessor(
        std::move(inputChainColumns), std::move(outputChainColumns), std::move(Keys), std::move(Aggregations)));
}

TConclusionStatus TWithKeysAggregationProcessor::TBuilder::AddGroupBy(
    const std::vector<TColumnChainInfo>& input, const TColumnChainInfo& output, const EAggregate aggrType) {
    if (input.size() > 1) {
        return TConclusionStatus::Fail("a lot of columns for aggregation: " + JoinSeq(", ", input));
    }
    AFL_VERIFY(!Finished);
    Aggregations.emplace_back(input, output, aggrType);
    return TConclusionStatus::Success();
}

TConclusion<arrow::Datum> TAggregateFunction::Call(
    const TExecFunctionContext& context, const std::shared_ptr<TAccessorsCollection>& resources) const {
    if (context.GetColumns().size() == 0 && AggregationType == NAggregation::EAggregate::NumRows) {
        auto rc = resources->GetRecordsCountActualOptional();
        if (!rc) {
            return TConclusionStatus::Fail("resources hasn't info about records count actual");
        } else {
            return arrow::Datum(std::make_shared<arrow::UInt64Scalar>(*rc));
        }
    } else {
        return TBase::Call(context, resources);
    }
}

}   // namespace NKikimr::NArrow::NSSA::NAggregation
