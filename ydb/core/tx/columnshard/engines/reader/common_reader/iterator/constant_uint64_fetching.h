#pragma once
#include "constructor.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>

#include <ydb/library/formats/arrow/validation/validation.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>

namespace NKikimr::NOlap::NReader::NCommon {

class TConstantUInt64FetchLogic: public IKernelFetchLogic {
private:
    using TBase = IKernelFetchLogic;
    const ui64 Value;

    virtual void DoStart(TReadActionsCollection& /*nextRead*/, TFetchingResultContext& /*context*/) override {
    }

    virtual void DoOnDataReceived(TReadActionsCollection& /*nextRead*/, NBlobOperations::NRead::TCompositeReadBlobs& /*blobs*/) override {
    }

    virtual TConclusionStatus DoOnDataCollected(TFetchingResultContext& context) override {
        const ui32 recordsCount = context.GetSource()->GetRecordsCount();
        auto array = NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt64Scalar(Value), recordsCount));
        context.GetAccessors().AddVerified(GetEntityId(), std::make_shared<NArrow::NAccessor::TTrivialArray>(array), true);
        return TConclusionStatus::Success();
    }

public:
    TConstantUInt64FetchLogic(const ui32 entityId, const std::shared_ptr<IStoragesManager>& storagesManager, const ui64 value)
        : TBase(entityId, storagesManager)
        , Value(value)
    {
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon
