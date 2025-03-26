#pragma once
#include "abstract.h"
#include "execution.h"
#include "functions.h"
#include "kernel_logic.h"

namespace NKikimr::NArrow::NSSA {

class THeaderCheckerProcessor: public IResourceProcessor {
private:
    IDataSource::TCheckHeaderContext HeaderContext;
    using TBase = IResourceProcessor;

    virtual bool HasSubColumns() const override {
        return !!HeaderContext.GetSubColumnName();
    }

    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;

    virtual bool IsAggregation() const override {
        return false;
    }

    virtual ui64 DoGetWeight() const override {
        return 1;
    }

    bool ApplyToFilterFlag = false;

    virtual TString DoGetSignalCategoryName() const override {
        return ::ToString(GetProcessorType());
    }

public:
    void SetApplyToFilter() {
        ApplyToFilterFlag = true;
    }

    bool GetApplyToFilter() const {
        return ApplyToFilterFlag;
    }

    const IDataSource::TCheckHeaderContext GetHeaderContext() const {
        return HeaderContext;
    }

    THeaderCheckerProcessor(const ui32 inputDataId, const IDataSource::TCheckHeaderContext& context, const ui32 outputId)
        : TBase({ inputDataId }, { outputId }, EProcessorType::CheckHeaderData)
        , HeaderContext(context) {
    }
};

}   // namespace NKikimr::NArrow::NSSA
