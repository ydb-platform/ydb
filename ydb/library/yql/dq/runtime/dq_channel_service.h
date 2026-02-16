#pragma once

#include "dq_input_channel.h"
#include "dq_output_channel.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <ydb/library/yql/dq/proto/dq_transport.pb.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYql::NDq {

class IDqChannelService;

struct TChannelInfo {
    TChannelInfo(ui64 channelId, NActors::TActorId outputActorId, NActors::TActorId inputActorId)
        : ChannelId(channelId), OutputActorId(outputActorId), InputActorId(inputActorId)
    {}
    ui64 ChannelId;
    NActors::TActorId OutputActorId;
    NActors::TActorId InputActorId;
};

struct TChannelFullInfo : public TChannelInfo {
    TChannelFullInfo(ui64 channelId, NActors::TActorId outputActorId, NActors::TActorId inputActorId, ui32 srcStageId, ui32 dstStageId, TCollectStatsLevel level)
        : TChannelInfo(channelId, outputActorId, inputActorId), SrcStageId(srcStageId), DstStageId(dstStageId), Level(level)
    {}
    ui32 SrcStageId;
    ui32 DstStageId;
    TCollectStatsLevel Level;
};

class TDataChunk {
public:
    TDataChunk() = default;

    TDataChunk(TChunkedBuffer&& buffer, ui64 rows, NDqProto::EDataTransportVersion transportVersion,
        NKikimr::NMiniKQL::EValuePackerVersion packerVersion, bool leading, bool finished)
        : Buffer(buffer)
        , Rows(rows)
        , TransportVersion(transportVersion)
        , PackerVersion(packerVersion)
        , Leading(leading)
        , Finished(finished) {
        Bytes = Buffer.Size() + 1;
        Timestamp = TInstant::Now();
    }

    TDataChunk(TChunkedBuffer&& buffer, ui64 rows, bool leading, bool finished)
        : Buffer(buffer)
        , Rows(rows)
        , Leading(leading)
        , Finished(finished) {
        Bytes = Buffer.Size() + 1;
        Timestamp = TInstant::Now();
    }

    TDataChunk(bool leading, bool finished) : Bytes(1), Leading(leading), Finished(finished) {
        Timestamp = TInstant::Now();
    }

    TChunkedBuffer Buffer;

    ui64 Rows = 0;
    ui64 Bytes = 0;
    NDqProto::EDataTransportVersion TransportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_FAST_PICKLE_1_0;
    NKikimr::NMiniKQL::EValuePackerVersion PackerVersion = NKikimr::NMiniKQL::EValuePackerVersion::V1;
    bool Leading = false;
    bool Finished = false;
    TInstant Timestamp;
};

class IChannelBuffer {
public:
    IChannelBuffer(const TChannelFullInfo& info) : Info(info) {}
    virtual ~IChannelBuffer() {}

    TDqInputChannelStats PushStats;
    TDqOutputChannelStats PopStats;
    TChannelFullInfo Info;

    virtual EDqFillLevel GetFillLevel() const = 0;
    virtual void SetFillAggregator(std::shared_ptr<TDqFillAggregator> aggregator) = 0;
    virtual void Push(TDataChunk&& data) = 0;
    virtual bool IsFinished() = 0;
    virtual bool IsEarlyFinished() = 0;
    virtual void UpdatePopStats() {}

    virtual bool IsEmpty() = 0;
    virtual bool Pop(TDataChunk& data) = 0;
    virtual void EarlyFinish() = 0;
    virtual void UpdatePushStats() {}

    void SendFinish();
    bool GetLeading();
    bool Leading = true;
};

// Channel usually created with unknown peer id which may be local or remote etc.
// But references to channel are used to create other objects and not be changed later
// We use recreatable buffers, they make late binding possible
// Most channel API calls are translated directly to buffer method calls

class IDqChannelService {
public:
    virtual ~IDqChannelService() {}
    virtual IDqOutputChannel::TPtr GetOutputChannel(const TDqChannelSettings& settings) = 0;
    virtual IDqInputChannel::TPtr GetInputChannel(const TDqChannelSettings& settings) = 0;
    virtual std::shared_ptr<IChannelBuffer> GetOutputBuffer(const TChannelFullInfo& info, IDqChannelStorage::TPtr storage) = 0;
    virtual std::shared_ptr<IChannelBuffer> GetInputBuffer(const TChannelFullInfo& info) = 0;
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

NActors::IActor* CreateLocalChannelServiceActor(NActors::TActorSystem* actorSystem, ui32 nodeId,
    NMonitoring::TDynamicCounterPtr counters, const TDqChannelLimits& limits,
    ui32 poolId, std::shared_ptr<IDqChannelService>& service);

} // namespace NYql::NDq
