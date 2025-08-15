#pragma once
#include "dq_input.h"
#include "dq_transport.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NDq {

struct TDqInputChannelStats : TDqInputStats {
    ui64 ChannelId = 0;
    ui32 SrcStageId = 0;
    ui64 RowsInMemory = 0;
    ui64 MaxMemoryUsage = 0;
    TDuration DeserializationTime;
};

class IDqInputChannel : public IDqInput {
public:
    using TPtr = TIntrusivePtr<IDqInputChannel>;

    virtual ui64 GetChannelId() const = 0;
    virtual const TDqInputChannelStats& GetPushStats() const = 0;

    virtual void Push(TDqSerializedBatch&& data) = 0;

    virtual void Finish() = 0;

    // Checkpointing
    // After pause IDqInput::Pop() stops return batches that were pushed before pause
    // and returns Empty() after all the data before pausing was read.
    // Compute Actor can push data after pause, but program won't receive it until Resume() is called.
    virtual void PauseByCheckpoint() = 0;
    virtual void ResumeByCheckpoint() = 0;
    virtual bool IsPausedByCheckpoint() const = 0;
    // Watermarks
    // Called after receiving watermark (watermark position remembered,
    // but does not pause channel); watermark may be pushed behind new data
    // by reading or checkpoint
    virtual void AddWatermark(TInstant watermark) = 0;
    // Called after watermark is ready for TaskRunner (got watermarks on all channels;
    // implies channel must already contain greater-or-equal watermark);
    // Same as with PauseByCheckpoint, any data added adter Pause is not received until Resume;
    // If called after PauseByCheckpoint(), checkpoint takes priority
    virtual void PauseByWatermark(TInstant watermark) = 0;
    // Called after watermark processed by TaskRunner;
    // If called before PauseByCheckpoint, all watermarks greater than this
    // moved after checkpoint (with no data between checkpoint and watermark)
    virtual void ResumeByWatermark(TInstant watermark) = 0;
    virtual bool IsPausedByWatermark() const = 0;
};

IDqInputChannel::TPtr CreateDqInputChannel(ui64 channelId, ui32 srcStageId, NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes,
    TCollectStatsLevel level, const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion);

} // namespace NYql::NDq
