#pragma once
#include "abstract.h"
#include "execution.h"
#include "functions.h"
#include "kernel_logic.h"

namespace NKikimr::NArrow::NSSA {

class TOriginalHeaderDataProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;
    IDataSource::TFetchHeaderContext HeaderContext;

    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;
    virtual NJson::TJsonValue DoDebugJson() const override;

    virtual bool HasSubColumns() const override {
        return !!HeaderContext.GetSubColumnName();
    }

    virtual bool IsAggregation() const override {
        return false;
    }

    virtual ui64 DoGetWeight() const override {
        return 1;
    }

    virtual TString DoGetSignalCategoryName() const override {
        return ::ToString(GetProcessorType());
    }

public:
    const IDataSource::TFetchHeaderContext& GetHeaderContext() const {
        return HeaderContext;
    }

    TOriginalHeaderDataProcessor(const ui32 outputId, const IDataSource::TFetchHeaderContext& headerContext)
        : TBase({}, { outputId }, EProcessorType::FetchHeaderData)
        , HeaderContext(headerContext) {
    }
};

class THeaderCheckerProcessor: public IResourceProcessor {
private:
    IDataSource::TFetchHeaderContext HeaderContext;
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

    const IDataSource::TFetchHeaderContext GetHeaderContext() const {
        return HeaderContext;
    }

    THeaderCheckerProcessor(const ui32 inputDataId, const IDataSource::TFetchHeaderContext& context, const ui32 outputId)
        : TBase({ inputDataId }, { outputId }, EProcessorType::CheckHeaderData)
        , HeaderContext(context) {
    }
};

}   // namespace NKikimr::NArrow::NSSA
