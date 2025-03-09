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

    std::optional<ui32> Limit;

public:
    TProjectionProcessor(std::vector<TColumnChainInfo>&& columns, const std::optional<ui32>& limit)
        : TBase(std::vector<TColumnChainInfo>(columns), {}, EProcessorType::Projection)
        , Limit(limit) {
    }
};

}   // namespace NKikimr::NArrow::NSSA
