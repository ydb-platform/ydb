#pragma once
#include "dq_output.h"
#include "dq_transport.h"

#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>

namespace NYql::NDq {

struct TDqAsyncOutputBufferStats : TDqOutputStats {
    ui64 OutputIndex = 0;
    TString Type;
    ui64 MaxMemoryUsage = 0;
    ui64 MaxRowsInMemory = 0;
};

class IDqAsyncOutputBuffer : public IDqOutput {
public:
    using TPtr = TIntrusivePtr<IDqAsyncOutputBuffer>;

    virtual ui64 GetOutputIndex() const = 0;
    virtual const TDqAsyncOutputBufferStats& GetPopStats() const = 0;

    // Pop data to send. Return estimated size of returned data.
    [[nodiscard]]
    virtual ui64 Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, ui64 bytes) = 0;
    [[nodiscard]]
    virtual ui64 Pop(TDqSerializedBatch&, ui64 bytes) = 0;
    // Pop watermark
    [[nodiscard]]
    virtual bool Pop(NDqProto::TWatermark& watermark) = 0;
    // Pop chechpoint. Checkpoints may be taken from sink even after it is finished.
    [[nodiscard]]
    virtual bool Pop(NDqProto::TCheckpoint& checkpoint) = 0;
};

IDqAsyncOutputBuffer::TPtr CreateDqAsyncOutputBuffer(ui64 outputIndex, const TString& type, NKikimr::NMiniKQL::TType* outputType, ui64 maxStoredBytes,
    TCollectStatsLevel level);

} // namespace NYql::NDq
