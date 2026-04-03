#pragma once
#include "abstract.h"
#include "execution.h"

namespace NKikimr::NArrow::NSSA {

class THierarchicalIndexCheckerProcessor: public IResourceProcessor {
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
        return 0;
    }

    virtual TString DoGetSignalCategoryName() const override {
        return ::ToString(GetProcessorType()) + "::" + IndexContext.GetOperation().GetSignalId();
    }

public:
    const IDataSource::TCheckIndexContext GetIndexContext() const {
        return IndexContext;
    }

    THierarchicalIndexCheckerProcessor(
        const ui32 inputConstId, const IDataSource::TCheckIndexContext& context, const ui32 outputId)
        : TBase({ inputConstId }, { outputId }, EProcessorType::CheckHierarchicalIndexData)
        , IndexContext(context) {
    }
};

}   // namespace NKikimr::NArrow::NSSA
