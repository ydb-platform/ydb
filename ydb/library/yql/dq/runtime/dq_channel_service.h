#pragma once

#include "dq_input_channel.h"
#include "dq_output_channel.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <ydb/library/yql/dq/proto/dq_transport.pb.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>

namespace NYql::NDq {

class IDqChannelService;

struct TDqChannelDesc {
    ui64 ChannelId = 0;
    ui32 SrcStageId = 0;
    ui32 DstStageId = 0;
};

struct TDqChannelParams {
    NKikimr::NMiniKQL::TType* RowType = nullptr;
    const NKikimr::NMiniKQL::THolderFactory* HolderFactory = nullptr;
    TDqChannelDesc Desc;
    TCollectStatsLevel Level = TCollectStatsLevel::None;
    NDqProto::EDataTransportVersion TransportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_FAST_PICKLE_1_0;
    NKikimr::NMiniKQL::EValuePackerVersion PackerVersion = NKikimr::NMiniKQL::EValuePackerVersion::V0;
};

struct TChannelInfo {
    TChannelInfo(ui64 channelId, NActors::TActorId outputActorId, NActors::TActorId inputActorId)
        : ChannelId(channelId), OutputActorId(outputActorId), InputActorId(inputActorId)
    {}
    ui64 ChannelId;
    NActors::TActorId OutputActorId;
    NActors::TActorId InputActorId;
};

class TDataChunk {
public:
    TDataChunk() = default;
    TDataChunk(TChunkedBuffer&& buffer, ui64 rows,
        NDqProto::EDataTransportVersion transportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_FAST_PICKLE_1_0,
        NKikimr::NMiniKQL::EValuePackerVersion packerVersion = NKikimr::NMiniKQL::EValuePackerVersion::V1,
        bool finished = false)
        : Buffer(buffer), Rows(rows), TransportVersion(transportVersion), PackerVersion(packerVersion), Finished(finished) {
        Bytes = Buffer.Size() + 1;
        Timestamp = TInstant::Now();
    }

    TDataChunk(bool finished) : Bytes(1), Finished(finished) {}

    TChunkedBuffer Buffer;
    ui64 Rows = 0;
    ui64 Bytes = 0;
    NDqProto::EDataTransportVersion TransportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_FAST_PICKLE_1_0;
    NKikimr::NMiniKQL::EValuePackerVersion PackerVersion = NKikimr::NMiniKQL::EValuePackerVersion::V1;
    bool Finished = false;
    TInstant Timestamp;
};

class IChannelBuffer {
public:
    virtual ~IChannelBuffer() {}

    TDqInputChannelStats PushStats;
    TDqOutputChannelStats PopStats;

    virtual EDqFillLevel GetFillLevel() const = 0;
    virtual void SetFillAggregator(std::shared_ptr<TDqFillAggregator> aggregator) = 0;
    virtual void Push(TDataChunk&& data) = 0;
    virtual bool IsEarlyFinished() = 0;
    virtual bool IsFlushed() = 0;
    virtual void PushTerminated() {}

    virtual bool IsEmpty() = 0;
    virtual bool Pop(TDataChunk& data) = 0;
    virtual void EarlyFinish() = 0;
    virtual void PopTerminated() {}

    void SendFinish();
};

// Channel usually created with unknown peer id which may be local or remote etc.
// But references to channel are used to create other objects and not be changed later
// We use recreatable buffers, they make late binding possible
// Most channel API calls are translated directly to buffer method calls

class IDqChannelService {
public:
    virtual ~IDqChannelService() {}
    virtual IDqOutputChannel::TPtr GetOutputChannel(const TDqChannelParams& params) = 0;
    virtual IDqInputChannel::TPtr GetInputChannel(const TDqChannelParams& params) = 0;
    virtual std::shared_ptr<IChannelBuffer> GetOutputBuffer(const TChannelInfo& info) = 0;
    virtual std::shared_ptr<IChannelBuffer> GetInputBuffer(const TChannelInfo& info) = 0;
};

inline NActors::TActorId MakeChannelServiceActorID(ui32 nodeId) {
    const char name[12] = { 'd', 'q', 'c', 'h', 'a', 'n', 'n', 'e', 'l', 's', '2', '0', };
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

struct TDqChannelLimits {
    ui64 LocalChannelInflightBytes  =  8_MB;    // max bytes per local channel
    ui64 RemoteChannelInflightBytes = 16_MB;    // max bytes per remote channel == output.push - input.pop
    ui64 NodeSessionIcInflightBytes = 64_MB;    // max bytes in network/IC per node-to-node session
};

NActors::IActor* CreateLocalChannelServiceActor(NActors::TActorSystem* actorSystem, ui32 nodeId, const TDqChannelLimits& limits, std::shared_ptr<IDqChannelService>& service);

} // namespace NYql::NDq
