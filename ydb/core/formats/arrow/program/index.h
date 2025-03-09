#pragma once
#include "abstract.h"
#include "functions.h"
#include "kernel_logic.h"

namespace NKikimr::NArrow::NSSA {

class TOriginalIndexDataProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;
    const ui32 ColumnId;
    YDB_ACCESSOR_DEF(TString, SubColumnName);

    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;
    virtual NJson::TJsonValue DoDebugJson() const override;

    virtual bool IsAggregation() const override {
        return false;
    }

public:
    TOriginalIndexDataProcessor(const ui32 outputId, const ui32 columnId, const TString& subColumnName)
        : TBase({}, { outputId }, EProcessorType::FetchIndexData)
        , ColumnId(columnId)
        , SubColumnName(subColumnName) {
    }
};

class TIndexCheckerProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;

    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& /*context*/, const TExecutionNodeContext& /*nodeContext*/) const override;

    virtual bool IsAggregation() const override {
        return false;
    }

public:
    TIndexCheckerProcessor(const ui32 inputDataId, const ui32 inputConstId, const ui32 outputId)
        : TBase({ inputDataId, inputConstId }, { outputId }, EProcessorType::CheckIndexData) {
    }
};

}   // namespace NKikimr::NArrow::NSSA
