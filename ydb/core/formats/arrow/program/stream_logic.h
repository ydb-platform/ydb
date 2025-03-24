#pragma once
#include "abstract.h"
#include "functions.h"

#include <yql/essentials/core/arrow_kernels/request/request.h>

namespace NKikimr::NArrow::NSSA {

class TStreamLogicProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;
    static inline TAtomicCounter Counter = 0;
    const i64 FinishMarker = Counter.Inc();
    const NKernels::EOperation Operation;
    std::shared_ptr<IStepFunction> Function;

    virtual NJson::TJsonValue DoDebugJson() const override;
    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;

    virtual bool IsAggregation() const override {
        return false;
    }

    virtual ui64 DoGetWeight() const override;

public:
    NKernels::EOperation GetOperation() const {
        return Operation;
    }

    TConclusion<bool> OnInputReady(const ui32 inputId, const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const;

    TStreamLogicProcessor(std::vector<TColumnChainInfo>&& input, const TColumnChainInfo& output, const NKernels::EOperation op);
};

}   // namespace NKikimr::NArrow::NSSA
