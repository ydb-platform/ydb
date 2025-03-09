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

    YDB_READONLY_DEF(std::optional<ui32>, Limit);

public:
    TFilterProcessor(const TColumnChainInfo& input, const std::optional<ui32> limit)
        : TBase({ input }, {}, EProcessorType::Filter)
        , Limit(limit) {
    }
};

}   // namespace NKikimr::NArrow::NSSA
