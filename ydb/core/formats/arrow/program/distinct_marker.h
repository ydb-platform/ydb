#pragma once
#include "abstract.h"

namespace NKikimr::NArrow::NSSA {

// Stateless marker processor for DISTINCT(dictionary-only optimization).
// It does not perform filtering itself; filtering/early-stop is handled by reader sync points.
class TDistinctMarkerProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;
    const ui32 KeyColumnId;

    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;

    virtual bool IsAggregation() const override {
        return false;
    }

public:
    explicit TDistinctMarkerProcessor(const ui32 keyColumnId)
        : TBase({ TColumnChainInfo(keyColumnId) }, {}, EProcessorType::Filter)
        , KeyColumnId(keyColumnId)
    {
        AFL_VERIFY(KeyColumnId);
    }

    ui32 GetKeyColumnId() const {
        return KeyColumnId;
    }
};

} // namespace NKikimr::NArrow::NSSA

