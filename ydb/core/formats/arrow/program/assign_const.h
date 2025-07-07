#pragma once
#include "abstract.h"

namespace NKikimr::NArrow::NSSA {

class TConstProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;
    YDB_READONLY_DEF(std::shared_ptr<arrow::Scalar>, ScalarConstant);

    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;

    virtual NJson::TJsonValue DoDebugJson() const override;

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
