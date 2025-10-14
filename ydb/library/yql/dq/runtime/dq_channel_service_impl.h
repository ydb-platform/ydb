#pragma once

#include "dq_channel_service.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/common/rope_over_buffer.h>
#include <ydb/library/yql/dq/runtime/dq_packer_version_helper.h>

// Flow control design principles
//
// 1. There are several ui64 counters which grow monotonically
//
// 2. Local Channeels
// 2.1. PushStats.Bytes - pushed into Channel
// 2.2. PopStats.Bytes - popped from Channel
// 2.3. PushStats.Bytes >= PopStats.Bytes
// 2.4. Inflight == (PushStats.Bytes - PopStats.Bytes)
//
// 3. Network Channels
// 3.1. Output.PushStats.Bytes - pushed into Channel on the Sender side
// 3.2. Output.PopStats.Bytes - sent to IC on the Sender side
// 3.3. Input.PushStats.Bytes - received from IC on the Receiver side
// 3.4. Input.PopStats.Bytes - popped from Channel on the Receiver side
// 3.5. Receiver sends Input.{Push|Pop}Stats.Bytes to the Sender with every Ack
// 3.6. Output.PushStats.Bytes >= Output.PopStats.Bytes >= Input.PushStats.Bytes >= Input.PopStats.Bytes
// 3.7. Full Inflight == (Output.PushStats.Bytes - Input.PopStats.Bytes)
// 3.8. NET/IC Inflight == (Output.PopStats.Bytes - Input.PushStats.Bytes)
// 3.9. We're NOT interested in per channel NET/IC inflight rather in per session (between nodes)

// Guaranteed delivery, ordering and reconcilation
//
// 1. All messages are numbered sequentially in single node to node session starting from 1
// 2. Also node maintains monotically increased E
//
// (statement below are not valid already)
//
// 1. Output channel provides sequence number (of message in the channel) filled with proto
// 2. Input channels check this sequence and rejects out of order messages if any
// 3. Output channel resends rejected (by ack), lost (by timeout) and undelivered (by Undelivered) messages
// 4. Each retry increments Cookie in the message
// 5. Cookie is returned in Ack and used for additional control (for outdated Acks and excessive Retries)
// 6. Message order is preserved for now

namespace std {

template<>
struct hash<pair<NActors::TActorId, NActors::TActorId>> {
    size_t operator()(pair<NActors::TActorId, NActors::TActorId> const &p) const {
        return hash<NActors::TActorId>()(p.first) ^ hash<NActors::TActorId>()(p.second);
    }
};

}

namespace NYql::NDq {

inline bool operator==(const TChannelInfo& lhs, const TChannelInfo& rhs) {
    return lhs.ChannelId == rhs.ChannelId && lhs.OutputActorId == rhs.OutputActorId && lhs.InputActorId == rhs.InputActorId;
}

}

namespace std {

template<>
struct hash<NYql::NDq::TChannelInfo> {
    size_t operator()(NYql::NDq::TChannelInfo const &info) const {
        return std::hash<ui64>()(info.ChannelId) ^ hash<NActors::TActorId>()(info.OutputActorId) ^ hash<NActors::TActorId>()(info.InputActorId);
    }
};

}

namespace NYql::NDq {

class TOutputSerializer {
public:
    TOutputSerializer(std::shared_ptr<IChannelBuffer> buffer, NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion)
        : Buffer(buffer)
        , RowType(rowType)
        , TransportVersion(transportVersion)
        , PackerVersion(packerVersion) {
    }

    virtual ~TOutputSerializer() {};
    virtual void Push(NUdf::TUnboxedValue&& value) = 0;
    virtual void WidePush(NUdf::TUnboxedValue* values, ui32 width) = 0;
    virtual void Flush(bool finished) = 0;

    std::shared_ptr<IChannelBuffer> Buffer;
    NKikimr::NMiniKQL::TType* RowType;
    NDqProto::EDataTransportVersion TransportVersion;
    NKikimr::NMiniKQL::EValuePackerVersion PackerVersion;
};

class TInputDeserializer {
public:
    TInputDeserializer(NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion, const NKikimr::NMiniKQL::THolderFactory& holderFactory)
        : RowType(rowType)
        , TransportVersion(transportVersion)
        , PackerVersion(packerVersion)
        , HolderFactory(holderFactory) {
    }

    virtual ~TInputDeserializer() {};
    virtual void Deserialize(TChunkedBuffer&& data, NKikimr::NMiniKQL::TUnboxedValueBatch& batch) = 0;

    NKikimr::NMiniKQL::TType* RowType;
    NDqProto::EDataTransportVersion TransportVersion;
    NKikimr::NMiniKQL::EValuePackerVersion PackerVersion;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
};

std::unique_ptr<TOutputSerializer> CreateSerializer(const TDqChannelParams& params, std::shared_ptr<IChannelBuffer> buffer, bool local);
std::unique_ptr<TOutputSerializer> ConvertToLocalSerializer(std::unique_ptr<TOutputSerializer>&& serializer);
std::unique_ptr<TInputDeserializer> CreateDeserializer(NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion, const NKikimr::NMiniKQL::THolderFactory& holderFactory);

class TChannelStub : public IChannelBuffer {
public:
    TChannelStub(ui64 channelId) {
        PopStats.ChannelId = channelId;
        PushStats.ChannelId = channelId;
    }

    EDqFillLevel GetFillLevel() const override {
        return EDqFillLevel::HardLimit;
    }

    void SetFillAggregator(std::shared_ptr<TDqFillAggregator>) override {
        YQL_ENSURE(false, "Stub must be binded before SetFillAggregator");
    }

    void Push(TDataChunk&&) final {
        YQL_ENSURE(false, "Stub must be binded before Push");
    }

    bool IsEarlyFinished() final {
        return false;
    }

    bool IsFlushed() final {
        return true;
    }

    bool IsEmpty() final {
        return true;
    }

    bool Pop(TDataChunk&) final {
        return false;
    }

    void EarlyFinish() final {
        YQL_ENSURE(false, "Stub must be binded before EarlyFinish");
    }
};

class TLocalBufferRegistry;

class TLocalBuffer : public IChannelBuffer {
public:
    TLocalBuffer(const std::shared_ptr<TLocalBufferRegistry> registry, const TChannelInfo& info, NActors::TActorSystem* actorSystem, ui64 maxInflightBytes, ui64 minInflightBytes)
        : Registry(registry)
        , Info(info)
        , ActorSystem(actorSystem)
        , InflightBytes(0)
        , MaxInflightBytes(maxInflightBytes)
        , MinInflightBytes(minInflightBytes)
        , EarlyFinished(false)
    {
        PopStats.ChannelId = info.ChannelId;
        PushStats.ChannelId = info.ChannelId;
    }

    ~TLocalBuffer() override;

    EDqFillLevel GetFillLevel() const override;
    void SetFillAggregator(std::shared_ptr<TDqFillAggregator> aggregator) override;
    void Push(TDataChunk&& data) override;
    bool IsEarlyFinished() override;
    bool IsFlushed() override;

    bool IsEmpty() override;
    bool Pop(TDataChunk& data) override;
    void EarlyFinish() override;

    std::shared_ptr<TLocalBufferRegistry> Registry;
    TChannelInfo Info;
    NActors::TActorSystem* ActorSystem;
    mutable std::mutex Mutex;
    std::queue<TDataChunk> Queue;
    std::shared_ptr<TDqFillAggregator> Aggregator;
    EDqFillLevel FillLevel = EDqFillLevel::NoLimit;
    std::atomic<ui64> InflightBytes;
    const ui64 MaxInflightBytes; // NoLimit => HardLimit
    const ui64 MinInflightBytes; // HardLimit => NoLimit
    bool NeedToNotifyOutput = false;
    bool NeedToNotifyInput = false;
    std::atomic<bool> EarlyFinished;
};

class TOutputBuffer;
class TNodeState;

class TOutputDescriptor {
public:
    TOutputDescriptor(const TChannelInfo& info, NActors::TActorSystem* actorSystem, ui64 maxInflightBytes, ui64 minInflightBytes)
        : Info(info)
        , ActorSystem(actorSystem)
        , PushBytes(0)
        , PopBytes(0)
        , MaxInflightBytes(maxInflightBytes)
        , MinInflightBytes(minInflightBytes)
        , EarlyFinished(false)
        , Terminated(false)
    {}
    void AddPushBytes(ui64 bytes);
    void UpdatePopBytes(ui64 bytes);
    bool TryPushToWaitQueue(TDataChunk&& data);
    void AddInflight(ui64 bytes);
    void PushToWaitQueue(TDataChunk&& data);
    bool IsFlushed();
    void Terminate();
    bool IsTerminated();

    TChannelInfo Info;
    NActors::TActorSystem* ActorSystem;
    std::weak_ptr<TOutputBuffer> Buffer;
    mutable std::mutex Mutex;
    std::queue<TDataChunk> WaitQueue;
    std::shared_ptr<TDqFillAggregator> Aggregator;
    EDqFillLevel FillLevel = EDqFillLevel::NoLimit;
    std::atomic<ui64> PushBytes; // local
    std::atomic<ui64> PopBytes; // remote
    const ui64 MaxInflightBytes; // NoLimit => HardLimit
    const ui64 MinInflightBytes; // HardLimit => NoLimit
    bool NeedToNotifyOutput = false;
    std::atomic<bool> EarlyFinished;
    std::atomic<bool> Terminated;
    TInstant WaitTimestamp;
};

struct TOutputDescriptorCompare {
    constexpr bool operator()(const std::shared_ptr<TOutputDescriptor>& a, const std::shared_ptr<TOutputDescriptor>& b) const noexcept {
        return a->WaitTimestamp < b->WaitTimestamp;
    }
};

class TOutputItem {
public:

    enum EState {
        Init,
        Wait,
        Sent
    };

    TOutputItem(TDataChunk&& data, std::shared_ptr<TOutputDescriptor> descriptor)
        : Data(std::move(data)), Descriptor(descriptor), State(EState::Init) {
    }

    TDataChunk Data;
    std::shared_ptr<TOutputDescriptor> Descriptor;
    std::atomic<EState> State;
    ui64 UniqueId;
    ui64 RetryCount = 0;
};

class TOutputBuffer : public IChannelBuffer {
public:
    TOutputBuffer(std::shared_ptr<TNodeState> nodeState, std::shared_ptr<TOutputDescriptor> descriptor)
        : NodeState(nodeState), Descriptor(descriptor) {
        PushStats.ChannelId = descriptor->Info.ChannelId;
        PopStats.ChannelId = descriptor->Info.ChannelId;
    }

    ~TOutputBuffer() override;
    EDqFillLevel GetFillLevel() const override;
    void SetFillAggregator(std::shared_ptr<TDqFillAggregator>aggregator) override;
    void Push(TDataChunk&& data) override;
    bool IsEarlyFinished() override;
    bool IsFlushed() override;
    bool IsEmpty() override;
    bool Pop(TDataChunk& data) override;
    void EarlyFinish() override;

    std::shared_ptr<TNodeState> NodeState;
    std::shared_ptr<TOutputDescriptor> Descriptor;
};

class TInputItem {
public:

    TInputItem(TDataChunk&& data)
        : Data(std::move(data)) {
    }

    TDataChunk Data;
};

class TInputBuffer : public IChannelBuffer {
public:

    enum EState {
        Init,
        Binded,
        Deleted
    };

    TInputBuffer(NActors::TActorId nodeActorId, const TChannelInfo& info, NActors::TActorSystem* actorSystem)
        : NodeActorId(nodeActorId), Info(info), ActorSystem(actorSystem), State(EState::Init) {
        PushStats.ChannelId = info.ChannelId;
        PopStats.ChannelId = info.ChannelId;
    }

    ~TInputBuffer() override {
    }

    bool IsEmpty() override;

    EDqFillLevel GetFillLevel() const override {
        return EDqFillLevel::NoLimit;
    }

    void SetFillAggregator(std::shared_ptr<TDqFillAggregator>) override {
    }

    void Push(TDataChunk&&) override {
        Y_ENSURE(false);
    }

    void PushDataChunk(TDataChunk&& data);

    bool IsEarlyFinished() override {
        return false;
    }

    bool IsFlushed() override {
        return false;
    }

    bool Pop(TDataChunk& data) override;
    void EarlyFinish() override;

    NActors::TActorId NodeActorId;
    TChannelInfo Info;
    NActors::TActorSystem* ActorSystem;
    std::atomic<EState> State;
    mutable std::mutex Mutex;
    std::queue<TInputItem> Queue;
    bool NeedToNotify = false;
    bool EarlyFinished = false;
};

class TDqChannelService;

struct TEvPrivate {
    enum EEv {
        EvServiceLookup = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvServiceReply,
        EvEarlyFinish,
        EvUpdatePopBytes,
        EvProcessPending,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    struct TEvServiceLookup : public NActors::TEventLocal<TEvServiceLookup, EvServiceLookup> {
    };

    struct TEvServiceReply : public NActors::TEventLocal<TEvServiceReply, EvServiceReply> {
        std::shared_ptr<TDqChannelService> Service;
    };

    struct TEvEarlyFinish : public NActors::TEventLocal<TEvEarlyFinish, EvEarlyFinish> {
        TEvEarlyFinish(const TChannelInfo& info) : Info(info) {}
        TChannelInfo Info;
    };

    struct TEvUpdatePopBytes : public NActors::TEventLocal<TEvUpdatePopBytes, EvUpdatePopBytes> {
        TEvUpdatePopBytes(const TChannelInfo& info, ui64 bytes) : Info(info), Bytes(bytes) {}
        TChannelInfo Info;
        ui64 Bytes;
    };

    struct TEvProcessPending : public NActors::TEventLocal<TEvProcessPending, EvProcessPending> {
    };
};

class TNodeState {
public:
    TNodeState(NActors::TActorSystem* actorSystem, ui64 maxInflightBytes)
        : ActorSystem(actorSystem)
        , Subscribed(false)
        , MaxInflightBytes(maxInflightBytes)
    {}
    void Push(TDataChunk&& data, std::shared_ptr<TOutputDescriptor> descriptor);
    void Send(std::shared_ptr<TOutputItem> item);
    void Handle(TEvDqCompute::TEvChannelDataV2::TPtr& ev);
    void Handle(TEvDqCompute::TEvChannelAckV2::TPtr& ev);
    void Handle(TEvPrivate::TEvEarlyFinish::TPtr& ev);
    void Handle(TEvPrivate::TEvUpdatePopBytes::TPtr& ev);
    std::shared_ptr<TOutputBuffer> CreateOutputBuffer(const TChannelInfo& info, ui64 maxInflightBytes, ui64 minInflightBytes);
    std::shared_ptr<TInputBuffer> GetOrCreateInputBuffer(const TChannelInfo& info, bool binded);
    void TerminateDescriptor(const std::shared_ptr<TOutputDescriptor>& descriptor);
    void TerminateInputBuffer(const std::shared_ptr<TInputBuffer>& inputBuffer);

    NActors::TActorId NodeActorId;
    NActors::TActorId PeerActorId;
    mutable std::mutex Mutex;
    std::queue<std::shared_ptr<TOutputItem>> Queue;
    NActors::TActorSystem* ActorSystem;
    std::atomic<bool> Subscribed;
    std::unordered_map<TChannelInfo, std::shared_ptr<TOutputDescriptor>> OutputDescriptors;
    std::unordered_map<TChannelInfo, std::shared_ptr<TInputBuffer>> InputBuffers;
    std::unordered_map<TChannelInfo, TInstant> TerminatedInputs;
    std::unordered_map<TChannelInfo, TInstant> UnbindedInputs;
    bool Connected = false;
    std::weak_ptr<TNodeState> Self;
    ui64 LastUniqueId = 0;
    ui64 PeerUniqueId = 0;
    ui64 InflightBytes = 0;
    const ui64 MaxInflightBytes;
    const ui64 MaxInflightMessages = 64;
    std::priority_queue<std::shared_ptr<TOutputDescriptor>, std::vector<std::shared_ptr<TOutputDescriptor>>, TOutputDescriptorCompare> WaitersQueue;
};

class TDebugNodeState : public TNodeState {
public:
    TDebugNodeState(NActors::TActorSystem* actorSystem, ui64 maxInflightBytes)
        : TNodeState(actorSystem, maxInflightBytes), ChannelDataPaused(false), ChannelAckPaused(false)
    {}

    void PauseChannelData();
    void ResumeChannelData();
    void PauseChannelAck();
    void ResumeChannelAck();

    std::atomic<bool> ChannelDataPaused;
    std::atomic<bool> ChannelAckPaused;
};

class TLocalBufferRegistry {
public:
    TLocalBufferRegistry(NActors::TActorSystem* actorSystem, ui64 maxInflightBytes, ui64 minInflightBytes)
        : ActorSystem(actorSystem), MaxInflightBytes(maxInflightBytes), MinInflightBytes(minInflightBytes)
    {}

    std::shared_ptr<TLocalBuffer> GetOrCreateLocalBuffer(const std::shared_ptr<TLocalBufferRegistry>& registry, const TChannelInfo& info);
    void DeleteLocalBufferInfo(const TChannelInfo& info);

    NActors::TActorSystem* ActorSystem;
    const ui64 MaxInflightBytes;
    const ui64 MinInflightBytes;
    std::unordered_map<TChannelInfo, std::weak_ptr<TLocalBuffer>> LocalBuffers;
    mutable std::mutex Mutex;
};

class TDqChannelService : public IDqChannelService {
public:
    TDqChannelService(NActors::TActorSystem* actorSystem, ui32 nodeId, const TDqChannelLimits& limits)
        : ActorSystem(actorSystem)
        , NodeId(nodeId)
        , Limits(limits) {
        LocalBufferRegistry = std::make_shared<TLocalBufferRegistry>(actorSystem, Limits.LocalChannelInflightBytes, Limits.LocalChannelInflightBytes * 8 / 10);
    }

    std::shared_ptr<TNodeState> GetOrCreateNodeState(ui32 nodeId);
    std::shared_ptr<TDebugNodeState> CreateDebugNodeState(ui32 nodeId);

    // unbinded stubs
    std::shared_ptr<IChannelBuffer> GetOutputBuffer(ui64 channelId);
    std::shared_ptr<IChannelBuffer> GetInputBuffer(ui64 channelId);
    // binded helpers
    std::shared_ptr<IChannelBuffer> GetOutputBuffer(const TChannelInfo& info) final;
    std::shared_ptr<IChannelBuffer> GetInputBuffer(const TChannelInfo& info) final;
    // remote buffers
    std::shared_ptr<IChannelBuffer> GetRemoteOutputBuffer(const TChannelInfo& info);
    std::shared_ptr<IChannelBuffer> GetRemoteInputBuffer(const TChannelInfo& info);
    // local buffer
    std::shared_ptr<IChannelBuffer> GetLocalBuffer(const TChannelInfo& info);
    // unbinded channels
    IDqOutputChannel::TPtr GetOutputChannel(const TDqChannelParams& params) final;
    IDqInputChannel::TPtr GetInputChannel(const TDqChannelParams& params) final;

    NActors::TActorSystem* ActorSystem;
    ui32 NodeId;
    const TDqChannelLimits Limits;
    std::weak_ptr<TDqChannelService> Self;
    std::shared_ptr<TLocalBufferRegistry> LocalBufferRegistry;
    std::unordered_map<ui32, std::shared_ptr<TNodeState>> NodeStates;
    mutable std::mutex Mutex;
};

class TFastDqOutputChannel : public IDqOutputChannel {

public:
    TFastDqOutputChannel(std::weak_ptr<TDqChannelService> service, const TDqChannelParams& params, std::shared_ptr<IChannelBuffer> buffer, bool localChannel)
        : Service(service), Serializer(CreateSerializer(params, buffer, localChannel)), Desc(params.Desc) {
    }

    ~TFastDqOutputChannel() {
        if (!Finished) {
            Serializer->Flush(false);
        }
        Serializer->Buffer->PushTerminated();
    }

// IDqOutput

    const TDqOutputStats& GetPushStats() const override {
        return Serializer->Buffer->PushStats;
    }

    EDqFillLevel GetFillLevel() const override {
        return Serializer->Buffer->GetFillLevel();
    }

    EDqFillLevel UpdateFillLevel() override {
        return GetFillLevel();
    }

    void SetFillAggregator(std::shared_ptr<TDqFillAggregator> aggregator) override {
        return Serializer->Buffer->SetFillAggregator(aggregator);
    }

    void Push(NUdf::TUnboxedValue&& value) override {
        if (!Finished) {
            Serializer->Push(std::move(value));
        }
    }

    void WidePush(NUdf::TUnboxedValue* values, ui32 width) override {
        if (!Finished) {
            Serializer->WidePush(values, width);
        }
    }

    void Push(NDqProto::TWatermark&&) override {
        Y_ENSURE(false);
    }

    void Push(NDqProto::TCheckpoint&&) override {
        Y_ENSURE(false);
    }

    void Finish() override {
        if (!Finished) {
            Finished = true;
            Serializer->Flush(true);
        }
    }

    bool IsFinished() const override {
        return Finished && Serializer->Buffer->IsFlushed();
    }

    NKikimr::NMiniKQL::TType* GetOutputType() const override {
        return Serializer->RowType;
    }

// IDqOutput // Deprecated

    bool HasData() const override {
        Y_ENSURE(false);
        return false;
    }

// IDqOutputChannel

    ui64 GetChannelId() const override {
        return Serializer->Buffer->PushStats.ChannelId;
    }

    ui64 GetValuesCount() const override {
        Y_ENSURE(false);
        return 0;
    }

    const TDqOutputChannelStats& GetPopStats() const override {
        return Serializer->Buffer->PopStats;
    }

    bool Pop(TDqSerializedBatch&) override {
        return false;
    }

    bool Pop(NDqProto::TWatermark&) override {
        return false;
    }

    bool Pop(NDqProto::TCheckpoint&) override {
        return false;
    }

    bool PopAll(TDqSerializedBatch&) override {
        return false;
    }

    ui64 Drop() override {
        return 0;
    }

    void Terminate() override {
        // ???
    }

    void UpdateSettings(const TDqOutputChannelSettings::TMutable&) override {
        Y_ENSURE(false);
    }

    bool Bind(NActors::TActorId outputActorId, NActors::TActorId inputActorId) override;

    std::weak_ptr<TDqChannelService> Service;
    std::unique_ptr<TOutputSerializer> Serializer;
    TDqChannelDesc Desc;
    bool Finished = false;
    bool Binded = false;
    std::shared_ptr<TDqFillAggregator> Aggregator;
};

class TFastDqInputChannel : public IDqInputChannel {

public:

    TFastDqInputChannel(std::weak_ptr<TDqChannelService> service, const TDqChannelParams& params, std::shared_ptr<IChannelBuffer> buffer)
        : Service(service), Buffer(buffer), Desc(params.Desc) {
        Deserializer = CreateDeserializer(params.RowType, params.TransportVersion, params.PackerVersion, *params.HolderFactory);
    }

    ~TFastDqInputChannel() {
        Buffer->PopTerminated();
    }

// IDqInput

    const TDqInputStats& GetPopStats() const override {
        return Buffer->PopStats;
    }

    bool Empty() const override {
        return Buffer->IsEmpty();
    }

    bool Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch) override;

    bool IsFinished() const override {
        return Finished;
    }

    NKikimr::NMiniKQL::TType* GetInputType() const override {
        return Deserializer->RowType;
    }

// IDqInput // Deprecated

    i64 GetFreeSpace() const override {
        Y_ENSURE(false);
        return 0;
    }

    ui64 GetStoredBytes() const override {
        Y_ENSURE(false);
        return 0;
    }

    void PauseByCheckpoint() override {
        Y_ENSURE(false);
    }

    void ResumeByCheckpoint() override {
        Y_ENSURE(false);
    }

    bool IsPausedByCheckpoint() const override {
        Y_ENSURE(false);
    }

    void AddWatermark(TInstant) override {
        Y_ENSURE(false);
    }

    void PauseByWatermark(TInstant) override {
        Y_ENSURE(false);
    }

    void ResumeByWatermark(TInstant) override {
        Y_ENSURE(false);
    }

    bool IsPausedByWatermark() const override {
        Y_ENSURE(false);
    }

// IDqInputChannel

    ui64 GetChannelId() const override {
        return Buffer->PopStats.ChannelId;
    }

    const TDqInputChannelStats& GetPushStats() const override {
        return Buffer->PushStats;
    }

    void Push(TDqSerializedBatch&&) override {
        Y_ENSURE(false);
    }

    void Finish() override {
        Buffer->EarlyFinish();
    }

    bool Bind(NActors::TActorId outputActorId, NActors::TActorId inputActorId) override;

    std::weak_ptr<TDqChannelService> Service;
    std::shared_ptr<IChannelBuffer> Buffer;
    std::unique_ptr<TInputDeserializer> Deserializer;
    TDqChannelDesc Desc;
    bool Finished = false;
};

class TChannelServiceActor : public NActors::TActor<TChannelServiceActor> {
public:
    TChannelServiceActor(std::shared_ptr<TDqChannelService> channelService)
        : TActor(&TThis::StateFunc)
        , ChannelService(channelService)
    {}

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDqCompute::TEvChannelDataV2, Handle);
            hFunc(TEvPrivate::TEvServiceLookup, Handle);
        }
    }

    void Handle(TEvDqCompute::TEvChannelDataV2::TPtr& ev) {
        auto state = ChannelService->GetOrCreateNodeState(ev->Sender.NodeId());
        Send(ev->Forward(state->NodeActorId));
    }

    void Handle(TEvPrivate::TEvServiceLookup::TPtr& ev) {
        auto evReply = MakeHolder<TEvPrivate::TEvServiceReply>();
        evReply->Service = ChannelService;
        Send(ev->Sender, evReply.Release());
    }

    std::shared_ptr<TDqChannelService> ChannelService;
};

class TNodeSessionActor : public NActors::TActor<TNodeSessionActor> {
public:
    TNodeSessionActor(std::shared_ptr<TNodeState> nodeState)
        : TActor(&TThis::StateFunc)
        , NodeState(nodeState)
    {}

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDqCompute::TEvChannelDataV2, Handle);
            hFunc(TEvDqCompute::TEvChannelAckV2, Handle);
            hFunc(TEvPrivate::TEvEarlyFinish, Handle);
            hFunc(TEvPrivate::TEvUpdatePopBytes, Handle);
        }
    }

    void Handle(TEvDqCompute::TEvChannelDataV2::TPtr& ev) {
        NodeState->Handle(ev);
    }

    void Handle(TEvDqCompute::TEvChannelAckV2::TPtr& ev) {
        NodeState->Handle(ev);
    }

    void Handle(TEvPrivate::TEvEarlyFinish::TPtr& ev) {
        NodeState->Handle(ev);
    }

    void Handle(TEvPrivate::TEvUpdatePopBytes::TPtr& ev) {
        NodeState->Handle(ev);
    }

    std::shared_ptr<TNodeState> NodeState;
};

class TDebugNodeSessionActor : public NActors::TActor<TDebugNodeSessionActor> {
public:
    TDebugNodeSessionActor(std::shared_ptr<TDebugNodeState> nodeState)
        : TActor(&TThis::StateFunc)
        , NodeState(nodeState)
    {}

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDqCompute::TEvChannelDataV2, Handle);
            hFunc(TEvDqCompute::TEvChannelAckV2, Handle);
            hFunc(TEvPrivate::TEvEarlyFinish, Handle);
            hFunc(TEvPrivate::TEvUpdatePopBytes, Handle);
            hFunc(TEvPrivate::TEvProcessPending, Handle);
        }
    }

    void Handle(TEvDqCompute::TEvChannelDataV2::TPtr& ev) {
        if (NodeState->ChannelDataPaused.load()) {
            PendingChannelData.emplace(ev.Release());
        } else {
            while (!PendingChannelData.empty()) {
                NodeState->Handle(PendingChannelData.front());
                PendingChannelData.pop();
            }
            NodeState->Handle(ev);
        }
    }

    void Handle(TEvDqCompute::TEvChannelAckV2::TPtr& ev) {
        if (NodeState->ChannelAckPaused.load()) {
            PendingChannelAck.emplace(ev.Release());
        } else {
            while (!PendingChannelAck.empty()) {
                NodeState->Handle(PendingChannelAck.front());
                PendingChannelAck.pop();
            }
            NodeState->Handle(ev);
        }
    }

    void Handle(TEvPrivate::TEvEarlyFinish::TPtr& ev) {
        NodeState->Handle(ev);
    }

    void Handle(TEvPrivate::TEvUpdatePopBytes::TPtr& ev) {
        NodeState->Handle(ev);
    }

    void Handle(TEvPrivate::TEvProcessPending::TPtr& ) {
        if (!NodeState->ChannelDataPaused.load()) {
            while (!PendingChannelData.empty()) {
                NodeState->Handle(PendingChannelData.front());
                PendingChannelData.pop();
            }
        }
        if (!NodeState->ChannelAckPaused.load()) {
            while (!PendingChannelAck.empty()) {
                NodeState->Handle(PendingChannelAck.front());
                PendingChannelAck.pop();
            }
        }
    }

    std::shared_ptr<TDebugNodeState> NodeState;
    std::queue<TEvDqCompute::TEvChannelDataV2::TPtr> PendingChannelData;
    std::queue<TEvDqCompute::TEvChannelAckV2::TPtr> PendingChannelAck;
};

}

