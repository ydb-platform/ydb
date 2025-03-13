#pragma once
#include "abstract.h"
#include "execution.h"
#include "functions.h"
#include "kernel_logic.h"

namespace NKikimr::NArrow::NSSA {

class TOriginalIndexDataProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;
    IDataSource::TFetchIndexContext IndexContext;

    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;
    virtual NJson::TJsonValue DoDebugJson() const override;

    virtual bool HasSubColumns() const override {
        return !!IndexContext.GetSubColumnName();
    }

    virtual bool IsAggregation() const override {
        return false;
    }

    virtual ui64 DoGetWeight() const override {
        return IndexContext.GetSubColumnName() ? 2 : 3;
    }

    virtual TString DoGetSignalCategoryName() const override {
        return ::ToString(GetProcessorType()) + "::" + ::ToString(IndexContext.GetOperation());
    }

public:
    const IDataSource::TFetchIndexContext& GetIndexContext() const {
        return IndexContext;
    }

    TOriginalIndexDataProcessor(const ui32 outputId, const IDataSource::TFetchIndexContext& indexContext)
        : TBase({}, { outputId }, EProcessorType::FetchIndexData)
        , IndexContext(indexContext) {
    }
};

class TIndexCheckerProcessor: public IResourceProcessor {
private:
    IDataSource::TFetchIndexContext IndexContext;
    using TBase = IResourceProcessor;

    virtual bool HasSubColumns() const override {
        return !!IndexContext.GetSubColumnName();
    }

    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;

    virtual bool IsAggregation() const override {
        return false;
    }

    virtual ui64 DoGetWeight() const override {
        return IndexContext.GetSubColumnName() ? 1 : 2;
    }

    bool ApplyToFilterFlag = false;

    virtual TString DoGetSignalCategoryName() const override {
        return ::ToString(GetProcessorType()) + "::" + ::ToString(IndexContext.GetOperation());
    }

public:
    void SetApplyToFilter() {
        ApplyToFilterFlag = true;
    }

    bool GetApplyToFilter() const {
        return ApplyToFilterFlag;
    }

    const IDataSource::TFetchIndexContext GetIndexContext() const {
        return IndexContext;
    }

    TIndexCheckerProcessor(const ui32 inputDataId, const ui32 inputConstId, const IDataSource::TFetchIndexContext& context, const ui32 outputId)
        : TBase({ inputDataId, inputConstId }, { outputId }, EProcessorType::CheckIndexData)
        , IndexContext(context) {
    }
};

}   // namespace NKikimr::NArrow::NSSA
