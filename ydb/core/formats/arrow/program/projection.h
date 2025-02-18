#pragma once
#include "abstract.h"

namespace NKikimr::NArrow::NSSA {

class TProjectionProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;

    virtual TConclusionStatus DoExecute(const std::shared_ptr<TAccessorsCollection>& resources) const override;

public:
    TProjectionProcessor(std::vector<TColumnChainInfo>&& columns)
        : TBase(std::vector<TColumnChainInfo>(columns), {}, EProcessorType::Projection) {
    }
};

}   // namespace NKikimr::NArrow::NSSA
