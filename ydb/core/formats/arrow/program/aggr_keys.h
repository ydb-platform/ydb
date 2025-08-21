#pragma once
#include "abstract.h"
#include "aggr_common.h"
#include "functions.h"

namespace CH {
enum class AggFunctionId;
}

namespace NKikimr::NArrow::NSSA::NAggregation {

class TAggregateFunction: public TInternalFunction {
private:
    using TBase = TInternalFunction;
    using TBase::TBase;
    const NAggregation::EAggregate AggregationType;

    std::vector<std::string> GetRegistryFunctionNames() const override {
        return { GetFunctionName(AggregationType), GetHouseFunctionName(AggregationType) };
    }
    virtual TConclusion<arrow::Datum> Call(const TExecFunctionContext& context, const TAccessorsCollection& resources) const override;

    TConclusion<arrow::Datum> PrepareResult(arrow::Datum&& datum) const override {
        if (!datum.is_scalar()) {
            return TConclusionStatus::Fail("Aggregate result is not a scalar.");
        }

        if (datum.scalar()->type->id() == arrow::Type::STRUCT) {
            if (AggregationType == EAggregate::Min) {
                const auto& minMax = datum.scalar_as<arrow::StructScalar>();
                return minMax.value[0];
            } else if (AggregationType == EAggregate::Max) {
                const auto& minMax = datum.scalar_as<arrow::StructScalar>();
                return minMax.value[1];
            } else {
                return TConclusionStatus::Fail("Unexpected struct result for aggregate function.");
            }
        }
        if (!datum.type()) {
            return TConclusionStatus::Fail("Aggregate result has no type.");
        }
        return std::move(datum);
    }

    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("function", ::ToString(GetAggregationType()));
        return result;
    }

    virtual std::shared_ptr<IResourcesAggregator> BuildResultsAggregator(const TColumnChainInfo& output) const override;

public:
    virtual bool IsAggregation() const override {
        return true;
    }

    TAggregateFunction(const EAggregate aggregationType, const std::shared_ptr<arrow::compute::FunctionOptions>& functionOptions = nullptr)
        : TBase(functionOptions, true)
        , AggregationType(aggregationType) {
    }

    NAggregation::EAggregate GetAggregationType() const {
        return AggregationType;
    }

    static const char* GetFunctionName(const EAggregate op) {
        switch (op) {
            case EAggregate::Count:
                return "count";
            case EAggregate::Min:
                return "min_max";
            case EAggregate::Max:
                return "min_max";
            case EAggregate::Sum:
                return "sum";
            case EAggregate::NumRows:
                return "num_rows";
#if 0   // TODO
	        case EAggregate::Avg:
	            return "mean";
#endif
            default:
                break;
        }
        return "";
    }

    static const char* GetHouseFunctionName(const EAggregate op) {
        switch (op) {
            case EAggregate::Some:
                return "ch.any";
            case EAggregate::Count:
                return "ch.count";
            case EAggregate::Min:
                return "ch.min";
            case EAggregate::Max:
                return "ch.max";
            case EAggregate::Sum:
                return "ch.sum";
#if 0   // TODO
	        case EAggregate::Avg:
	            return "ch.avg";
#endif
            case EAggregate::NumRows:
                return "ch.num_rows";
            default:
                break;
        }
        return "";
    }

    virtual TConclusionStatus CheckIO(
        const std::vector<TColumnChainInfo>& /*input*/, const std::vector<TColumnChainInfo>& output) const override {
        if (output.size() != 1) {
            return TConclusionStatus::Fail("output size != 1 (" + ::ToString(output.size()) + ")");
        }
        //        if (input.size() != 1) {
        //            return TConclusionStatus::Fail("input size != 1 (" + ::ToString(input.size()) + ")");
        //        }
        return TConclusionStatus::Success();
    }
};

class TWithKeysAggregationOption {
private:
    std::vector<TColumnChainInfo> Inputs;
    TColumnChainInfo Output;
    const EAggregate AggregationId;

public:
    EAggregate GetSecondaryAggregationId() const {
        return TAggregationsHelper::GetSecondaryAggregationId(AggregationId);
    }

    EAggregate GetAggregationId() const {
        return AggregationId;
    }

    TWithKeysAggregationOption(const std::vector<TColumnChainInfo>& input, const TColumnChainInfo& output, const EAggregate aggregationId)
        : Inputs(input)
        , Output(output)
        , AggregationId(aggregationId) {
        AFL_VERIFY(Inputs.size() <= 1);
    }

    TString DebugString() const;

    const std::vector<TColumnChainInfo>& GetInputs() const {
        return Inputs;
    }
    const TColumnChainInfo& GetOutput() const {
        return Output;
    }

    static CH::AggFunctionId GetHouseFunction(const EAggregate op);
};

class TWithKeysAggregationProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;

    std::vector<TColumnChainInfo> AggregationKeys;
    std::vector<TWithKeysAggregationOption> Aggregations;

    virtual std::shared_ptr<IResourcesAggregator> BuildResultsAggregator() const override;

    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;

    TWithKeysAggregationProcessor(std::vector<TColumnChainInfo>&& input, std::vector<TColumnChainInfo>&& output,
        std::vector<TColumnChainInfo>&& aggregationKeys, std::vector<TWithKeysAggregationOption>&& aggregations)
        : TBase(std::move(input), std::move(output), EProcessorType::Aggregation)
        , AggregationKeys(std::move(aggregationKeys))
        , Aggregations(std::move(aggregations)) {
    }
    virtual bool IsAggregation() const override {
        return true;
    }
    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", "AGGREGATION");
        if (AggregationKeys.size()) {
            auto& jsonKeys = result.InsertValue("keys", NJson::JSON_ARRAY);
            for (auto&& i : AggregationKeys) {
                jsonKeys.AppendValue(i.GetColumnId());
            }
        }
        auto& jsonOptions = result.InsertValue("options", NJson::JSON_ARRAY);
        for (auto&& i : Aggregations) {
            jsonOptions.AppendValue(i.DebugString());
        }
        return result;
    }

public:
    static const char* GetHouseGroupByName() {
        return "ch.group_by";
    }

    class TBuilder {
    private:
        std::vector<TColumnChainInfo> Keys;
        std::vector<TWithKeysAggregationOption> Aggregations;
        bool Finished = false;

    public:
        void AddKey(const TColumnChainInfo& key) {
            Keys.emplace_back(key);
        }

        TConclusionStatus AddGroupBy(const std::vector<TColumnChainInfo>& input, const TColumnChainInfo& output, const EAggregate aggrType);

        TConclusionStatus AddGroupBy(const TColumnChainInfo& input, const TColumnChainInfo& output, const EAggregate aggrType) {
            return AddGroupBy(std::vector<TColumnChainInfo>({ input }), output, aggrType);
        }

        TConclusion<std::shared_ptr<TWithKeysAggregationProcessor>> Finish();
    };
};

}   // namespace NKikimr::NArrow::NSSA::NAggregation
