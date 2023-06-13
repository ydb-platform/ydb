#pragma once
#include "dq_output.h"

#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>

namespace NYql::NDq {

struct TDqAsyncOutputBufferStats : TDqOutputStats {
    const ui64 OutputIndex;

    explicit TDqAsyncOutputBufferStats(ui64 outputIndex)
        : OutputIndex(outputIndex) {}

    template<typename T>
    void FromProto(const T& f)
    {
        this->Chunks = f.GetChunks();
        this->Bytes = f.GetBytes();
        this->RowsIn = f.GetRowsIn();
        this->RowsOut = f.GetRowsOut();
        this->MaxMemoryUsage = f.GetMaxMemoryUsage();
    }
};

class IDqAsyncOutputBuffer : public IDqOutput {
public:
    using TPtr = TIntrusivePtr<IDqAsyncOutputBuffer>;

    virtual ui64 GetOutputIndex() const = 0;

    // Pop data to send. Return estimated size of returned data.
    [[nodiscard]]
    virtual ui64 Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, ui64 bytes) = 0;
    // Pop watermark
    [[nodiscard]]
    virtual bool Pop(NDqProto::TWatermark& watermark) = 0;
    // Pop chechpoint. Checkpoints may be taken from sink even after it is finished.
    [[nodiscard]]
    virtual bool Pop(NDqProto::TCheckpoint& checkpoint) = 0;

    virtual const TDqAsyncOutputBufferStats* GetStats() const = 0;
};

IDqAsyncOutputBuffer::TPtr CreateDqAsyncOutputBuffer(ui64 outputIndex, NKikimr::NMiniKQL::TType* outputType, ui64 maxStoredBytes,
    bool collectProfileStats);

} // namespace NYql::NDq
