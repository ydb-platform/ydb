#pragma once
#include "abstract.h"

namespace NKikimr::NArrow::NSSA {

class TProjectionOperation: public IProgramOperation {
private:
    using TBase = IProgramOperation;

    virtual TConclusion<TExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;

    virtual bool IsAggregation() const override {
        return false;
    }

public:
    TProjectionOperation(std::vector<TColumnChainInfo>&& columns)
        : TBase(std::vector<TColumnChainInfo>(columns), {}, EProcessorType::Projection) {
    }
};

}   // namespace NKikimr::NArrow::NSSA
