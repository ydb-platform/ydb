#pragma once
#include "abstract.h"

namespace NKikimr::NArrow::NSSA {

class TFilterProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;
    virtual TConclusionStatus DoExecute(const std::shared_ptr<TAccessorsCollection>& resources, const TProcessorContext& context) const override;

    virtual bool IsAggregation() const override {
        return false;
    }

public:
    TFilterProcessor(std::vector<TColumnChainInfo>&& input)
        : TBase(std::move(input), {}, EProcessorType::Filter) {
        AFL_VERIFY(GetInput().size() == 1)("size", GetInput().size());
    }

    TFilterProcessor(const TColumnChainInfo& input)
        : TBase({ input }, {}, EProcessorType::Filter) {
    }
};

}   // namespace NKikimr::NArrow::NSSA
