#pragma once
#include "abstract.h"
#include "original.h"

namespace NKikimr::NArrow::NSSA {

class TReserveMemoryProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;

    THashMap<ui32, IDataSource::TDataAddress> DataAddresses;
    THashMap<ui32, IDataSource::TFetchIndexContext> IndexContext;
    THashMap<ui32, IDataSource::TFetchHeaderContext> HeaderContext;
    std::shared_ptr<IMemoryCalculationPolicy> Policy;

    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        if (DataAddresses.size()) {
            auto& arrAddr = result.InsertValue("data", NJson::JSON_ARRAY);
            for (auto&& i : DataAddresses) {
                arrAddr.AppendValue(i.second.DebugJson());
            }
        }
        if (IndexContext.size()) {
            auto& indexesArr = result.InsertValue("indexes", NJson::JSON_ARRAY);
            for (auto&& i : IndexContext) {
                indexesArr.AppendValue(i.second.DebugJson());
            }
        }
        if (HeaderContext.size()) {
            auto& headersArr = result.InsertValue("headers", NJson::JSON_ARRAY);
            for (auto&& i : HeaderContext) {
                headersArr.AppendValue(i.second.DebugJson());
            }
        }
        return result;
    }

    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const override {
        auto source = context.GetDataSource().lock();
        if (!source) {
            return TConclusionStatus::Fail("source was destroyed before (original fetch start)");
        }
        auto conclusion = source->StartReserveMemory(context, DataAddresses, IndexContext, HeaderContext, Policy);
        if (conclusion.IsFail()) {
            return conclusion;
        } else if (conclusion.GetResult()) {
            return EExecutionResult::InBackground;
        } else {
            return EExecutionResult::Success;
        }
    }

    virtual bool IsAggregation() const override {
        return false;
    }

    virtual ui64 DoGetWeight() const override {
        return 0;
    }

public:
    TReserveMemoryProcessor(const TOriginalColumnDataProcessor& original, const std::shared_ptr<IMemoryCalculationPolicy>& policy)
        : TBase({}, {}, EProcessorType::ReserveMemory)
        , DataAddresses(original.GetDataAddresses())
        , IndexContext(original.GetIndexContext())
        , HeaderContext(original.GetHeaderContext())
        , Policy(policy)
    {
        AFL_VERIFY(policy);
    }
};

}   // namespace NKikimr::NArrow::NSSA
