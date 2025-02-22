#pragma once
#include "abstract.h"

namespace NKikimr::NArrow::NSSA {

class TConstProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;
    YDB_READONLY_DEF(std::shared_ptr<arrow::Scalar>, ScalarConstant);

    virtual TConclusionStatus DoExecute(const std::shared_ptr<TAccessorsCollection>& resources) const override;

    virtual bool IsAggregation() const override {
        return false;
    }

public:
    TConstProcessor(const std::shared_ptr<arrow::Scalar>& scalar, const ui32 columnId)
        : TBase(std::vector<TColumnChainInfo>(), std::vector<TColumnChainInfo>({ TColumnChainInfo(columnId) }), EProcessorType::Const)
        , ScalarConstant(scalar) {
        AFL_VERIFY(ScalarConstant);
    }
};

}   // namespace NKikimr::NArrow::NSSA
