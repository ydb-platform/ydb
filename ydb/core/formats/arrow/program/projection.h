#pragma once
#include "abstract.h"

namespace NKikimr::NArrow::NSSA {

class TProjectionProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;

    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;

    virtual bool IsAggregation() const override {
        return false;
    }

public:
    TProjectionProcessor(std::vector<TColumnChainInfo>&& columns)
        : TBase(std::vector<TColumnChainInfo>(columns), {}, EProcessorType::Projection) {
    }
};

}   // namespace NKikimr::NArrow::NSSA
