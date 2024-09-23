#pragma once
#include "dq_input.h"
#include "dq_transport.h"

namespace NYql::NDq {

struct TDqAsyncInputBufferStats : TDqInputStats {
    ui64 InputIndex = 0;
    TString Type;
    ui64 RowsInMemory = 0;
    ui64 MaxMemoryUsage = 0;
};

class IDqAsyncInputBuffer : public IDqInput {
public:
    using TPtr = TIntrusivePtr<IDqAsyncInputBuffer>;
    TDqAsyncInputBufferStats PushStats;

    virtual ui64 GetInputIndex() const = 0;
    virtual const TDqAsyncInputBufferStats& GetPushStats() const = 0;

    virtual void Push(NKikimr::NMiniKQL::TUnboxedValueBatch&& batch, i64 space) = 0;
    virtual void Push(TDqSerializedBatch&& batch, i64 space) = 0;
 
    virtual void Finish() = 0;

    virtual bool IsPending() const { return false; };
};

IDqAsyncInputBuffer::TPtr CreateDqAsyncInputBuffer(ui64 inputIndex, const TString& type, NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes,
    TCollectStatsLevel level);

} // namespace NYql::NDq
