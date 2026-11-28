#pragma once
#include "abstract.h"

namespace NKikimr::NArrow::NSSA {

class TFilterOperation: public IProgramOperation {
private:
    using TBase = IProgramOperation;
    virtual TConclusion<TExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;

    virtual bool IsAggregation() const override {
        return false;
    }

public:
    TFilterOperation(const TColumnChainInfo& input)
        : TBase({ input }, {}, EProcessorType::Filter) {
    }
};

}   // namespace NKikimr::NArrow::NSSA
