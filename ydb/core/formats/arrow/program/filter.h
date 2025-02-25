#pragma once
#include "abstract.h"

namespace NKikimr::NArrow::NSSA {

class TFilterProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;
    const bool ReuseColumns;
    virtual TConclusionStatus DoExecute(const std::shared_ptr<TAccessorsCollection>& resources) const override;

    virtual bool IsAggregation() const override {
        return false;
    }

public:
    TFilterProcessor(std::vector<TColumnChainInfo>&& input, const bool reuseColumns = false)
        : TBase(std::move(input), {}, EProcessorType::Filter)
        , ReuseColumns(reuseColumns)
    {
        AFL_VERIFY(GetInput().size());
    }

    TFilterProcessor(const TColumnChainInfo& input, const bool reuseColumns = false)
        : TBase({ input }, {}, EProcessorType::Filter)
        , ReuseColumns(reuseColumns)
    {
    }
};

}   // namespace NKikimr::NArrow::NSSA
