#pragma once
#include "abstract.h"
#include "execution.h"
#include "functions.h"
#include "kernel_logic.h"

namespace NKikimr::NArrow::NSSA {

class TIndexCheckerProcessor: public IResourceProcessor {
private:
    IDataSource::TCheckIndexContext IndexContext;
    using TBase = IResourceProcessor;

    virtual bool HasSubColumns() const override {
        return !!IndexContext.GetSubColumnName();
    }

    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;

    virtual bool IsAggregation() const override {
        return false;
    }

    virtual ui64 DoGetWeight() const override {
        return 2;
    }

    bool ApplyToFilterFlag = false;

    virtual TString DoGetSignalCategoryName() const override {
        return ::ToString(GetProcessorType()) + "::" + IndexContext.GetOperation().GetSignalId();
    }

public:
    void SetApplyToFilter() {
        ApplyToFilterFlag = true;
    }

    bool GetApplyToFilter() const {
        return ApplyToFilterFlag;
    }

    const IDataSource::TCheckIndexContext GetIndexContext() const {
        return IndexContext;
    }

    TIndexCheckerProcessor(const ui32 inputDataId, const ui32 inputConstId, const IDataSource::TCheckIndexContext& context, const ui32 outputId)
        : TBase({ inputDataId, inputConstId }, { outputId }, EProcessorType::CheckIndexData)
        , IndexContext(context) {
    }
};

}   // namespace NKikimr::NArrow::NSSA
