#pragma once
#include "abstract.h"

namespace NKikimr::NArrow::NSSA {

class TDistinctProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;
    const ui32 KeyColumnId;
    mutable THashSet<TString> Seen;

    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;

    virtual bool IsAggregation() const override {
        return false;
    }

public:
    explicit TDistinctProcessor(const ui32 keyColumnId)
        : TBase({ TColumnChainInfo(keyColumnId) }, {}, EProcessorType::Filter)
        , KeyColumnId(keyColumnId) {
        AFL_VERIFY(KeyColumnId);
    }

    ui32 GetKeyColumnId() const {
        return KeyColumnId;
    }
};

} // namespace NKikimr::NArrow::NSSA
