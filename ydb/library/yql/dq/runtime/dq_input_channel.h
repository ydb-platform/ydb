#pragma once
#include "dq_input.h"
#include "dq_channel_settings.h"
#include "dq_transport.h"

#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>

namespace NYql::NDq {

struct TDqInputChannelStats : TDqInputStats {
    ui64 ChannelId = 0;
    ui32 SrcStageId = 0;
    ui64 RowsInMemory = 0;
    ui64 MaxMemoryUsage = 0;
    TDuration DeserializationTime;
    TInstant PopTime;
    bool PopResult = false;
};

 struct IDqInputChannelCallbacks {
    virtual void TakeCheckpoint(const NDqProto::TCheckpoint& checkpoint, ui64 channelId) = 0;
    virtual ~IDqInputChannelCallbacks() = default;
};


class IDqInputChannel : public IDqInput {
public:
    using TPtr = TIntrusivePtr<IDqInputChannel>;

    virtual ui64 GetChannelId() const = 0;
    virtual const TDqInputChannelStats& GetPushStats() const = 0;

    virtual void Push(TDqSerializedBatch&& data) = 0;
    virtual void Push(TInstant watermark) = 0;

    virtual void Finish() = 0;

    virtual void Bind(NActors::TActorId outputActorId, NActors::TActorId inputActorId) = 0;
    virtual bool IsLocal() const = 0;

    virtual void SetCallback(IDqInputChannelCallbacks* callback) {
        Y_UNUSED(callback);
    };
};

IDqInputChannel::TPtr CreateDqInputChannel(const TDqChannelSettings& settings, const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv);

} // namespace NYql::NDq
