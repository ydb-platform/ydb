#pragma once

#include "dq_columns_resolve.h"
#include "dq_output.h"

#include <yql/essentials/public/udf/udf_value_builder.h>

#include <yql/essentials/minikql/mkql_alloc.h>

namespace NKikimr::NMiniKQL {
    class TTypeEnvironment;
    class THolderFactory;
} // namespace NKikimr::NMiniKQL

namespace NYql::NDq {

class IDqOutputConsumer : public TSimpleRefCount<IDqOutputConsumer>,
    public NKikimr::NMiniKQL::TWithDefaultMiniKQLAlloc {
private:
    bool IsFinishingFlag = false;
public:
    using TPtr = TIntrusivePtr<IDqOutputConsumer>;

public:
    virtual ~IDqOutputConsumer() = default;

    bool TryFinish() {
        IsFinishingFlag = true;
        return DoTryFinish();
    }
    virtual EDqFillLevel GetFillLevel() const = 0;
    virtual void Consume(NKikimr::NUdf::TUnboxedValue&& value) = 0;
    virtual void WideConsume(NKikimr::NUdf::TUnboxedValue values[], ui32 count) = 0;
    virtual void Consume(NDqProto::TCheckpoint&& checkpoint) = 0;
    virtual void Finish() = 0;
    bool IsFinishing() const {
        return IsFinishingFlag;
    }
    virtual TString DebugString() {
        return "";
    }
protected:
    virtual bool DoTryFinish() {
        return true;
    }
};

IDqOutputConsumer::TPtr CreateOutputMultiConsumer(TVector<IDqOutputConsumer::TPtr>&& consumers);

IDqOutputConsumer::TPtr CreateOutputMapConsumer(IDqOutput::TPtr output);

IDqOutputConsumer::TPtr CreateOutputHashPartitionConsumer(
    TVector<IDqOutput::TPtr>&& outputs,
    TVector<TColumnInfo>&& keyColumns, const  NKikimr::NMiniKQL::TType* outputType,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    TMaybe<ui8> minFillPercentage,
    const NDqProto::TTaskOutputHashPartition& hashPartition,
    NUdf::IPgBuilder* pgBuilder
);

IDqOutputConsumer::TPtr CreateOutputBroadcastConsumer(TVector<IDqOutput::TPtr>&& outputs, TMaybe<ui32> outputWidth);

} // namespace NYql::NDq
