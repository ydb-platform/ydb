#pragma once
#include "dq_output.h"
#include "dq_channel_settings.h"

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/common/dq_serialized_batch.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>

namespace NYql::NDq {

struct TDqOutputChannelStats : public TDqOutputStats {
    ui64 ChannelId = 0;
    ui32 DstStageId = 0;
    ui64 MaxMemoryUsage = 0;
    ui64 MaxRowsInMemory = 0;
    TDuration SerializationTime;
    ui64 SpilledBytes = 0;
    ui64 SpilledRows = 0;
    ui64 SpilledBlobs = 0;
};

class IDqOutputChannel : public IDqOutput {
public:
    using TPtr = TIntrusivePtr<IDqOutputChannel>;

    virtual ui64 GetChannelId() const = 0;
    virtual ui64 GetValuesCount() const = 0;
    virtual const TDqOutputChannelStats& GetPopStats() const = 0;

    // <| consumer methods
    // can throw TDqChannelStorageException
    [[nodiscard]]
    virtual bool Pop(TDqSerializedBatch& data) = 0;
    // Pop watermark.
    [[nodiscard]]
    virtual bool Pop(NDqProto::TWatermark& watermark) = 0;
    // Pop chechpoint. Checkpoints may be taken from channel even after it is finished.
    [[nodiscard]]
    virtual bool Pop(NDqProto::TCheckpoint& checkpoint) = 0;
    // Only for data-queries
    // TODO: remove this method and create independent Data- and Stream-query implementations.
    //       Stream-query implementation should be without PopAll method.
    //       Data-query implementation should be one-shot for Pop (a-la PopAll) call and without ChannelStorage.
    // can throw TDqChannelStorageException
    [[nodiscard]]
    virtual bool PopAll(TDqSerializedBatch& data) = 0;
    // |>

    virtual ui64 Drop() = 0;

    virtual void Terminate() = 0;

    virtual void Bind(NActors::TActorId outputActorId, NActors::TActorId inputActorId) = 0;
};

struct TDqOutputChannelChunkSizeLimitExceeded : public yexception {
};

IDqOutputChannel::TPtr CreateDqOutputChannel(const TDqChannelSettings& settings, const TLogFunc& logFunc = {});

} // namespace NYql::NDq
