#pragma once
#include "abstract.h"

namespace NKikimr::NArrow::NSSA {

class TFilterProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;
    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;

    virtual bool IsAggregation() const override {
        return false;
    }

public:
    TFilterProcessor(const TColumnChainInfo& input)
        : TBase({ input }, {}, EProcessorType::Filter) {
    }
};

}   // namespace NKikimr::NArrow::NSSA
