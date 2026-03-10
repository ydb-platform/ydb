#pragma once

#include "dq_channel_service.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/mon/mon.h>

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

// Guaranteed delivery, ordering and reconciliation
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
    TOutputSerializer(std::shared_ptr<IChannelBuffer> buffer, NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion, TMaybe<size_t> bufferPageAllocSize)
        : Buffer(buffer)
        , RowType(rowType)
        , TransportVersion(transportVersion)
        , PackerVersion(packerVersion)
        , BufferPageAllocSize(bufferPageAllocSize)
    {}

    virtual ~TOutputSerializer() {};
    virtual void Push(NUdf::TUnboxedValue&& value) = 0;
    virtual void WidePush(NUdf::TUnboxedValue* values, ui32 width) = 0;
    virtual void Flush(bool finished) = 0;

    std::shared_ptr<IChannelBuffer> Buffer;
    NKikimr::NMiniKQL::TType* RowType;
    NDqProto::EDataTransportVersion TransportVersion;
    NKikimr::NMiniKQL::EValuePackerVersion PackerVersion;
    TMaybe<size_t> BufferPageAllocSize;
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

std::unique_ptr<TOutputSerializer> CreateSerializer(const TDqChannelSettings& settings, std::shared_ptr<IChannelBuffer> buffer, bool local);
std::unique_ptr<TOutputSerializer> ConvertToLocalSerializer(std::unique_ptr<TOutputSerializer>&& serializer);
std::unique_ptr<TInputDeserializer> CreateDeserializer(NKikimr::NMiniKQL::TType* rowType, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion, TMaybe<size_t> bufferPageAllocSize, const NKikimr::NMiniKQL::THolderFactory& holderFactory);

class TChannelStub : public IChannelBuffer {
public:
    TChannelStub(const TChannelFullInfo& info) : IChannelBuffer(info) {
    }

    ~TChannelStub() override {
        if (Aggregator) {
            Aggregator->SubCount(EDqFillLevel::HardLimit);
        }
    }

    EDqFillLevel GetFillLevel() const override {
        return EDqFillLevel::HardLimit;
    }

    void SetFillAggregator(std::shared_ptr<TDqFillAggregator> aggregator) override {
        Aggregator = aggregator;
        Aggregator->AddCount(EDqFillLevel::HardLimit);
    }

    void Push(TDataChunk&&) final {
        YQL_ENSURE(false, "Stub must be bound before Push");
    }

    bool IsFinished() final {
        return false;
    }

    bool IsEarlyFinished() final {
        return false;
    }

    bool IsEmpty() final {
        return true;
    }

    bool Pop(TDataChunk&) final {
        return false;
    }

    void EarlyFinish() final {
        YQL_ENSURE(false, "Stub must be bound before EarlyFinish");
    }

    void ExportPushStats(TDqAsyncStats&) override {}
    void ExportPopStats(TDqAsyncStats&) override {}

    std::shared_ptr<TDqFillAggregator> Aggregator;
};

struct TLoadingInfo {
    TLoadingInfo(ui32 blobId, ui32 bytes) : BlobId(blobId), Bytes(bytes) {}
    TBuffer Buffer;
    ui32 BlobId = 0;
    ui32 Bytes = 0;;
    bool Loaded = false;
};

class TLocalBufferRegistry;

class TLocalBuffer : public IChannelBuffer {
public:
    TLocalBuffer(const std::shared_ptr<TLocalBufferRegistry> registry, const TChannelFullInfo& info, NActors::TActorSystem* actorSystem, ui64 maxInflightBytes, ui64 minInflightBytes)
        : IChannelBuffer(info)
        , Registry(registry)
        , ActorSystem(actorSystem)
        , InflightBytes(0)
        , SpilledBytes(0)
        , MaxInflightBytes(maxInflightBytes)
        , MinInflightBytes(minInflightBytes)
        , NeedToNotifyOutput(false)
        , NeedToNotifyInput(false)
        , EarlyFinished(false)
        , OutputBound(false)
        , InputBound(false)
        , Finished(false)
    {
        PushStats.Level = info.Level;
        PopStats.Level = info.Level;
    }

    ~TLocalBuffer() override;

    EDqFillLevel GetFillLevel() const override;
    void SetFillAggregator(std::shared_ptr<TDqFillAggregator> aggregator) override;
    void Push(TDataChunk&& data) override;
    bool IsFinished() override;
    bool IsEarlyFinished() override;

    bool IsEmpty() override;
    bool Pop(TDataChunk& data) override;
    void EarlyFinish() override;

    void BindInput();
    void BindOutput();
    void BindStorage(std::shared_ptr<TLocalBuffer>& self, IDqChannelStorage::TPtr storage);
    void StorageWakeupHandler();

    void PushDataChunk(TDataChunk&& data);
    void NotifyInput(bool force);
    void NotifyOutput(bool force);

    void ExportPushStats(TDqAsyncStats& stats) override;
    void ExportPopStats(TDqAsyncStats& stats) override;

    std::shared_ptr<TLocalBufferRegistry> Registry;
    NActors::TActorSystem* ActorSystem;
    TDqThreadSafeStats PushStats;
    TDqThreadSafeStats PopStats;

    mutable std::mutex Mutex;
    mutable std::queue<TDataChunk> Queue;
    std::shared_ptr<TDqFillAggregator> Aggregator;
    EDqFillLevel FillLevel = EDqFillLevel::NoLimit;

    std::queue<ui32> SpilledChunkBytes;
    ui64 HeadBlobId = 0;
    ui64 TailBlobId = 0;
    std::queue<TLoadingInfo> LoadingQueue;
    IDqChannelStorage::TPtr Storage;

    std::atomic<ui64> InflightBytes;
    std::atomic<ui64> SpilledBytes;
    const ui64 MaxInflightBytes; // NoLimit => HardLimit
    const ui64 MinInflightBytes; // HardLimit => NoLimit
    bool FinishPushed = false;
    TInstant LastOutputNotificationTime;
    TInstant LastInputNotificationTime;
    TInstant FinishTime;

    std::atomic<bool> NeedToNotifyOutput;
    std::atomic<bool> NeedToNotifyInput;
    std::atomic<bool> EarlyFinished;
    std::atomic<bool> OutputBound;
    std::atomic<bool> InputBound;
    std::atomic<bool> Finished;
};

class TOutputBuffer;
class TNodeState;

class TOutputDescriptor {
public:
    TOutputDescriptor(const TChannelFullInfo& info, NActors::TActorSystem* actorSystem, ::NMonitoring::TDynamicCounters::TCounterPtr outputBufferBytes,
        ::NMonitoring::TDynamicCounters::TCounterPtr outputBufferChunks, ui64 maxInflightBytes, ui64 minInflightBytes)
        : Info(info)
        , ActorSystem(actorSystem)
        , GenMajor(0)
        , WaitQueueBytes(0)
        , WaitQueueSize(0)
        , PushBytes(0)
        , RemotePopBytes(0)
        , SpilledBytes(0)
        , NeedToNotifyOutput(false)
        , EarlyFinished(false)
        , Terminated(false)
        , Aborted(false)
        , Finished(false)
        , FinishPushed(false)
        , OutputBufferBytes(outputBufferBytes)
        , OutputBufferChunks(outputBufferChunks)
        , MaxInflightBytes(maxInflightBytes)
        , MinInflightBytes(minInflightBytes)
    {
        PushStats.Level = info.Level;
        PopStats.Level = info.Level;
    }

    void PushDataChunk(TDataChunk&& data, TNodeState* nodeState, std::shared_ptr<TOutputDescriptor> self);
    void AddPopChunk(ui64 bytes, ui64 rows);
    void UpdatePopBytes(ui64 bytes, TNodeState* nodeState, std::shared_ptr<TOutputDescriptor> self);
    bool CheckGenMajor(ui64 genMajor, const TString& errorMessage);
    /* bool PushToWaitQueue(TDataChunk&& data); */
    bool IsFinished();
    bool IsEarlyFinished();
    void Terminate();
    bool IsTerminatedOrAborted();
    void AbortChannel(const TString& message);
    void HandleUpdate(bool earlyFinish, ui64 popBytes, TNodeState* nodeState, std::shared_ptr<TOutputDescriptor> self);
    void BindStorage(std::shared_ptr<TOutputDescriptor>& self, std::shared_ptr<TNodeState>& nodeState, IDqChannelStorage::TPtr storage);
    void StorageWakeupHandler(TNodeState* nodeState, std::shared_ptr<TOutputDescriptor> self);

    TChannelFullInfo Info;
    NActors::TActorSystem* ActorSystem;
    std::atomic<ui64> GenMajor;
    TDqThreadSafeStats PushStats;
    TDqThreadSafeStats PopStats;

    std::queue<ui32> SpilledChunkBytes;
    ui64 HeadBlobId = 0;
    ui64 TailBlobId = 0;
    std::queue<TLoadingInfo> LoadingQueue;
    IDqChannelStorage::TPtr Storage;
    bool IsBound = false;

    mutable std::mutex WaitQueueMutex;
    std::atomic<ui64> WaitQueueBytes;
    std::atomic<ui64> WaitQueueSize;
    mutable std::queue<TDataChunk> WaitQueue;
    mutable TInstant WaitTimestamp;

    mutable std::mutex FlowControlMutex;
    std::shared_ptr<TDqFillAggregator> Aggregator;
    mutable EDqFillLevel FillLevel = EDqFillLevel::NoLimit;

    std::atomic<ui64> PushBytes;
    std::atomic<ui64> RemotePopBytes;
    std::atomic<ui64> SpilledBytes;

    std::atomic<bool> NeedToNotifyOutput;
    std::atomic<bool> EarlyFinished;
    std::atomic<bool> Terminated;
    std::atomic<bool> Aborted;
    std::atomic<bool> Finished;
    std::atomic<bool> FinishPushed;
    std::atomic<bool> Leading;

    ::NMonitoring::TDynamicCounters::TCounterPtr OutputBufferBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr OutputBufferChunks;

    const ui64 MaxInflightBytes; // NoLimit => HardLimit
    const ui64 MinInflightBytes; // HardLimit => NoLimit
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
    ui64 SeqNo;
};

class TOutputBuffer : public IChannelBuffer {
public:
    TOutputBuffer(std::shared_ptr<TNodeState> nodeState, std::shared_ptr<TOutputDescriptor> descriptor)
        : IChannelBuffer(descriptor->Info), NodeState(nodeState), Descriptor(descriptor) {
    }

    ~TOutputBuffer() override;
    EDqFillLevel GetFillLevel() const override;
    void SetFillAggregator(std::shared_ptr<TDqFillAggregator>aggregator) override;
    void Push(TDataChunk&& data) override;
    bool IsFinished() override;
    bool IsEarlyFinished() override;
    bool IsEmpty() override;
    bool Pop(TDataChunk& data) override;
    void EarlyFinish() override;
    void ExportPushStats(TDqAsyncStats& stats) override;
    void ExportPopStats(TDqAsyncStats& stats) override;

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

class TInputDescriptor {
public:

    TInputDescriptor(const TChannelFullInfo& info, NActors::TActorSystem* actorSystem,
      ::NMonitoring::TDynamicCounters::TCounterPtr inputBufferBytes, ::NMonitoring::TDynamicCounters::TCounterPtr inputBufferChunks)
        : Info(info)
        , ActorSystem(actorSystem)
        , QueueSize(0)
        , QueueBytes(0)
        , NeedToNotifyInput(false)
        , FinishPushed(false)
        , Finished(false)
        , EarlyFinished(false)
        , InputBufferBytes(inputBufferBytes)
        , InputBufferChunks(inputBufferChunks)
    {
        PushStats.Level = info.Level;
        PopStats.Level = info.Level;
    }

    bool IsEmpty();
    bool PushDataChunk(TDataChunk&& data);
    bool PopDataChunk(TDataChunk& data);
    ui32 GetQueueSize();

    bool IsFinished();
    bool IsEarlyFinished();
    bool EarlyFinish();
    void Terminate();

    TChannelFullInfo Info;
    NActors::TActorSystem* ActorSystem;
    ui64 PeerGenMajor = 0;
    NActors::TActorId PeerActorId;
    bool IsBound = false;
    TDqThreadSafeStats PushStats;
    TDqThreadSafeStats PopStats;

    mutable std::mutex QueueMutex;
    std::atomic<ui64> QueueSize;
    std::atomic<ui64> QueueBytes;
    mutable std::queue<TInputItem> Queue;

    std::atomic<bool> NeedToNotifyInput;
    std::atomic<bool> FinishPushed;
    std::atomic<bool> Finished;
    std::atomic<bool> EarlyFinished;

    ::NMonitoring::TDynamicCounters::TCounterPtr InputBufferBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr InputBufferChunks;
};

class TInputBuffer : public IChannelBuffer {
public:
    TInputBuffer(const std::shared_ptr<TNodeState>& nodeState, const std::shared_ptr<TInputDescriptor>& descriptor)
        : IChannelBuffer(descriptor->Info), NodeState(nodeState), Descriptor(descriptor) {
    }

    ~TInputBuffer() override;


    EDqFillLevel GetFillLevel() const override {
        return EDqFillLevel::NoLimit;
    }

    void SetFillAggregator(std::shared_ptr<TDqFillAggregator>) override {
    }

    void Push(TDataChunk&&) override;
    bool IsFinished() override;
    bool IsEarlyFinished() override;
    bool IsEmpty() override;
    bool Pop(TDataChunk& data) override;
    void EarlyFinish() override;
    void ExportPushStats(TDqAsyncStats& stats) override;
    void ExportPopStats(TDqAsyncStats& stats) override;

    std::shared_ptr<TNodeState> NodeState;
    std::shared_ptr<TInputDescriptor> Descriptor;
};

class TDqChannelService;

struct TEvPrivate {
    enum EEv {
        EvServiceLookup = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvServiceReply,
        EvProcessPending,
        EvSendWaiters,
        EvReconciliation,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    struct TEvServiceLookup : public NActors::TEventLocal<TEvServiceLookup, EvServiceLookup> {
    };

    struct TEvServiceReply : public NActors::TEventLocal<TEvServiceReply, EvServiceReply> {
        std::shared_ptr<TDqChannelService> Service;
    };

    struct TEvProcessPending : public NActors::TEventLocal<TEvProcessPending, EvProcessPending> {
        TEvProcessPending(ui32 maxCount) : MaxCount(maxCount) {}
        const ui32 MaxCount;
    };

    struct TEvSendWaiters : public NActors::TEventLocal<TEvSendWaiters, EvSendWaiters> {
    };

    struct TEvReconciliation : public NActors::TEventLocal<TEvReconciliation, EvReconciliation> {
        TEvReconciliation(ui64 genMajor, ui64 genMinor) : GenMajor(genMajor), GenMinor(genMinor) {}
        const ui64 GenMajor;
        const ui64 GenMinor;
    };
};

class TNodeState {
public:
    TNodeState(NActors::TActorSystem* actorSystem, ui32 nodeId, NMonitoring::TDynamicCounterPtr counters, const TDqChannelLimits& limits)
        : ActorSystem(actorSystem)
        , NodeId(nodeId)
        , Subscribed(false)
        , GenMajor(0), GenMinor(0)
        , PeerGenMajor(0), PeerGenMinor(0)
        , Limits(limits)
        , WaitersQueueSize(0)
        , Reconciliation(0)
        , WaiterBytes(0)
        , WaiterMessages(0)
    {
        OutputBufferCount = counters->GetCounter("OutputBuffer/Count", false);
        OutputBufferBytes = counters->GetCounter("OutputBuffer/Bytes", true);
        OutputBufferChunks = counters->GetCounter("OutputBuffer/Chunks", true);
        OutputBufferInflightBytes = counters->GetCounter("OutputBuffer/InflightBytes", false);
        OutputBufferInflightMessages = counters->GetCounter("OutputBuffer/InflightMessages", false);
        OutputBufferWaiterCount = counters->GetCounter("OutputBuffer/WaiterCount", false);
        OutputBufferWaiterBytes = counters->GetCounter("OutputBuffer/WaiterBytes", false);
        OutputBufferWaiterMessages = counters->GetCounter("OutputBuffer/WaiterMessages", false);
        InputBufferCount = counters->GetCounter("InputBuffer/Count", false);
        InputBufferBytes = counters->GetCounter("InputBuffer/Bytes", true);
        InputBufferChunks = counters->GetCounter("InputBuffer/Chunks", true);
    }

    virtual ~TNodeState();
    void PushDataChunk(TDataChunk&& data, std::shared_ptr<TOutputDescriptor> descriptor);
    void SendMessage(std::shared_ptr<TOutputItem> item);
    void HandleUndelivered(NActors::TEvents::TEvUndelivered::TPtr& ev);
    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr& ev);
    void HandleDiscovery(TEvDqCompute::TEvChannelDiscoveryV2::TPtr& ev);
    void HandleData(TEvDqCompute::TEvChannelDataV2::TPtr& ev);
    void HandleAck(TEvDqCompute::TEvChannelAckV2::TPtr& ev);
    void HandleUpdate(TEvDqCompute::TEvChannelUpdateV2::TPtr& ev);
    void HandleSendWaiters(TEvPrivate::TEvSendWaiters::TPtr& ev);
    std::shared_ptr<TOutputDescriptor> GetOrCreateOutputDescriptor(const TChannelFullInfo& info, bool bound, bool leading);
    std::shared_ptr<TInputDescriptor> GetOrCreateInputDescriptor(const TChannelFullInfo& info, bool bound, bool leading);
    void TerminateOutputDescriptor(const std::shared_ptr<TOutputDescriptor>& descriptor);
    void TerminateInputDescriptor(const std::shared_ptr<TInputDescriptor>& descriptor);
    void CleanupUnbound();
    void FailInputs(const NActors::TActorId& peerActorId, ui64 peerGenMajor);
    void SendAck(THolder<TEvDqCompute::TEvChannelAckV2>& evAck, ui64 cookie);
    void SendAckWithError(ui64 cookie, const TString& message);
    void HandleChannelData(TEvDqCompute::TEvChannelDataV2::TPtr& ev);
    void SendFromWaiters(ui64 deltaBytes);
    void ConnectSession(NActors::TActorId& sender, ui64 genMajor);
    virtual TString GetDebugInfo();
    void UpdateProgress(std::shared_ptr<TInputDescriptor>& descriptor);

    void HandleReconciliation(TEvPrivate::TEvReconciliation::TPtr& ev);
    void StartReconciliation(bool major);
    bool UpdateReconciliationDelay();
    void ScheduleReconciliation();
    void DoReconciliation();
    void SendDiscovery(NActors::TActorId actorId);

    NActors::TActorId NodeActorId;
    mutable std::mutex Mutex;
    mutable std::deque<std::shared_ptr<TOutputItem>> Queue;
    NActors::TActorSystem* ActorSystem;
    ui32 NodeId;
    std::atomic<bool> Subscribed;
    mutable std::unordered_map<TChannelInfo, std::shared_ptr<TOutputDescriptor>> OutputDescriptors;
    mutable std::unordered_map<TChannelInfo, std::shared_ptr<TInputDescriptor>> InputDescriptors;
    mutable std::queue<std::pair<TChannelInfo, TInstant>> UnboundInputs;
    mutable std::queue<std::pair<TChannelInfo, TInstant>> UnboundOutputs;
    bool Connected = false;
    std::weak_ptr<TNodeState> Self;
    // Sender
    ui64 GenMajor = 0;
    ui64 GenMinor = 0;
    ui64 SeqNo = 0;
    ui64 InflightBytes = 0;
    // Receiver
    NActors::TActorId PeerActorId;
    std::atomic<ui64> PeerGenMajor;
    std::atomic<ui64> PeerGenMinor;
    ui64 ConfirmedSeqNo = 0;
    TEvDqCompute::TEvChannelDataV2::TPtr OutOfOrderMessage;
    // ...
    const TDqChannelLimits Limits;
    const ui64 MaxInflightMessages = 8192;
    mutable std::priority_queue<std::shared_ptr<TOutputDescriptor>, std::vector<std::shared_ptr<TOutputDescriptor>>, TOutputDescriptorCompare> WaitersQueue;
    std::atomic<ui64> WaitersQueueSize;
    const TDuration UnboundWaitPeriod = TDuration::Minutes(10);
    std::atomic<ui64> Reconciliation;
    std::atomic<ui64> WaiterBytes;
    std::atomic<ui64> WaiterMessages;
    ::NMonitoring::TDynamicCounters::TCounterPtr OutputBufferCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr OutputBufferBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr OutputBufferChunks;
    ::NMonitoring::TDynamicCounters::TCounterPtr OutputBufferInflightBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr OutputBufferInflightMessages;
    ::NMonitoring::TDynamicCounters::TCounterPtr OutputBufferWaiterCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr OutputBufferWaiterBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr OutputBufferWaiterMessages;
    ::NMonitoring::TDynamicCounters::TCounterPtr InputBufferCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr InputBufferBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr InputBufferChunks;
    TDuration ReconciliationDelay = TDuration::Zero();
    bool ReReconciliation = false;
    ui64 ReconciliationCount = 0;
    const TDuration MinReconciliationDelay = TDuration::MilliSeconds(100);
    const TDuration MaxReconciliationDelay = TDuration::Seconds(10);
};

class TDebugNodeState : public TNodeState {
public:
    TDebugNodeState(NActors::TActorSystem* actorSystem, ui32 nodeId, NMonitoring::TDynamicCounterPtr counters, const TDqChannelLimits& limits)
        : TNodeState(actorSystem, nodeId, counters, limits)
        , ChannelDataPaused(false)
        , ChannelAckPaused(false)
        , DataLossProbability(0.0), DataLossCount(0)
        , AckLossProbability(0.0), AckLossCount(0)
        , NullMode(false)
    {}

    void HandleNullMode(TEvDqCompute::TEvChannelDataV2::TPtr& ev);

    void PauseChannelData();
    void ResumeChannelData();
    void PauseChannelAck();
    void ResumeChannelAck();
    void SetLossProbability(double dataLossProbability, ui64 dataLossCount, double ackLossProbability, ui64 ackLossCount);
    bool ShouldLooseData();
    bool ShouldLooseAck();
    void ProcessPending(ui32 maxCount);
    void SetNullMode();
    bool IsNullMode();

    std::atomic<bool> ChannelDataPaused;
    std::atomic<bool> ChannelAckPaused;
    std::atomic<double> DataLossProbability;
    std::atomic<ui64> DataLossCount;
    std::atomic<double> AckLossProbability;
    std::atomic<ui64> AckLossCount;
    std::atomic<bool> NullMode;
};

class TLocalBufferRegistry {
public:
    TLocalBufferRegistry(NActors::TActorSystem* actorSystem, NMonitoring::TDynamicCounterPtr counters,
        ui64 maxInflightBytes, ui64 minInflightBytes)
        : ActorSystem(actorSystem), MaxInflightBytes(maxInflightBytes), MinInflightBytes(minInflightBytes)
    {
        LocalBufferCount = counters->GetCounter("LocalBuffer/Count", false);
        LocalBufferBytes = counters->GetCounter("LocalBuffer/Bytes", true);
        LocalBufferChunks = counters->GetCounter("LocalBuffer/Chunks", true);
        LocalBufferLatency = counters->GetCounter("LocalBuffer/Latency", true);
    }
    ~TLocalBufferRegistry();
    std::shared_ptr<TLocalBuffer> GetOrCreateLocalBuffer(const std::shared_ptr<TLocalBufferRegistry>& registry, const TChannelFullInfo& info);
    void DeleteLocalBufferInfo(const TChannelInfo& info);

    NActors::TActorSystem* ActorSystem;
    const ui64 MaxInflightBytes;
    const ui64 MinInflightBytes;
    mutable std::mutex Mutex;
    mutable std::unordered_map<TChannelInfo, std::weak_ptr<TLocalBuffer>> LocalBuffers;
    ::NMonitoring::TDynamicCounters::TCounterPtr LocalBufferBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr LocalBufferChunks;
    ::NMonitoring::TDynamicCounters::TCounterPtr LocalBufferCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr LocalBufferLatency;
};

class TDqChannelService : public IDqChannelService {
public:
    TDqChannelService(NActors::TActorSystem* actorSystem, ui32 nodeId, NMonitoring::TDynamicCounterPtr counters, const TDqChannelLimits& limits, ui32 poolId)
        : ActorSystem(actorSystem)
        , NodeId(nodeId)
        , Counters(counters)
        , Limits(limits)
        , PoolId(poolId) {
        LocalBufferRegistry = std::make_shared<TLocalBufferRegistry>(actorSystem, counters, Limits.LocalChannelInflightBytes, Limits.LocalChannelInflightBytes * 8 / 10);
    }

    std::shared_ptr<TNodeState> GetOrCreateNodeState(ui32 nodeId);
    std::shared_ptr<TDebugNodeState> CreateDebugNodeState(ui32 nodeId);

    // unbinded stubs
    std::shared_ptr<IChannelBuffer> GetUnbindedBuffer(const TChannelFullInfo& info);
    // binded helpers
    std::shared_ptr<IChannelBuffer> GetOutputBuffer(const TChannelFullInfo& info, IDqChannelStorage::TPtr storage) final;
    std::shared_ptr<IChannelBuffer> GetInputBuffer(const TChannelFullInfo& info) final;
    // remote buffers
    std::shared_ptr<TOutputBuffer> GetRemoteOutputBuffer(const TChannelFullInfo& info, IDqChannelStorage::TPtr storage);
    std::shared_ptr<TInputBuffer> GetRemoteInputBuffer(const TChannelFullInfo& info);
    // local buffer
    std::shared_ptr<IChannelBuffer> GetLocalBuffer(const TChannelFullInfo& info, bool bindInput, IDqChannelStorage::TPtr storage);
    // unbinded channels
    IDqOutputChannel::TPtr GetOutputChannel(const TDqChannelSettings& settings) final;
    IDqInputChannel::TPtr GetInputChannel(const TDqChannelSettings& settings) final;
    // extras
    void CleanupUnbound();
    TString GetDebugInfo();

    NActors::TActorSystem* ActorSystem;
    ui32 NodeId;
    NMonitoring::TDynamicCounterPtr Counters;
    const TDqChannelLimits Limits;
    ui32 PoolId;
    std::weak_ptr<TDqChannelService> Self;
    std::shared_ptr<TLocalBufferRegistry> LocalBufferRegistry;
    mutable std::unordered_map<ui32, std::shared_ptr<TNodeState>> NodeStates;
    mutable std::mutex Mutex;
    const TDuration UnboundWaitPeriod = TDuration::Minutes(10);
};

class TFastDqOutputChannel : public IDqOutputChannel {

public:
    TFastDqOutputChannel(std::weak_ptr<TDqChannelService> service, const TDqChannelSettings& settings, std::shared_ptr<IChannelBuffer> buffer, bool localChannel)
        : Service(service), Serializer(CreateSerializer(settings, buffer, localChannel)), Storage(settings.ChannelStorage) {
        PushStats.Level = settings.Level;
        PopStats.ChannelId = settings.ChannelId;
        PopStats.DstStageId = settings.DstStageId;
        PopStats.Level = settings.Level;
    }

    mutable TDqOutputStats PushStats;
    mutable TDqOutputChannelStats PopStats;

// IDqOutput

    const TDqOutputStats& GetPushStats() const override {
        Serializer->Buffer->ExportPushStats(PushStats);
        return PushStats;
    }

    EDqFillLevel GetFillLevel() const override {
        return Serializer->Buffer->GetFillLevel();
    }

    EDqFillLevel UpdateFillLevel() override {
        return GetFillLevel();
    }

    void SetFillAggregator(std::shared_ptr<TDqFillAggregator> aggregator) override {
        Aggregator = aggregator;
        Serializer->Buffer->SetFillAggregator(aggregator);
    }

    void Push(NUdf::TUnboxedValue&& value) override {
        if (!Serializer->Buffer->IsFinished()) {
            Serializer->Push(std::move(value));
        }
    }

    void WidePush(NUdf::TUnboxedValue* values, ui32 width) override {
        if (!Serializer->Buffer->IsFinished()) {
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
        Serializer->Flush(true);
    }

    void Flush() override {
        Serializer->Flush(false);
    }

    bool IsFinished() const override {
        bool finishCheckResult = Serializer->Buffer->IsFinished();
        PopStats.FinishCheckTime = TInstant::Now();
        PopStats.FinishCheckResult = finishCheckResult;
        return finishCheckResult;
    }

    bool IsEarlyFinished() const override {
        return Serializer->Buffer->IsEarlyFinished();
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
        return PopStats.ChannelId;
    }

    ui64 GetValuesCount() const override {
        Y_ENSURE(false);
        return 0;
    }

    const TDqOutputChannelStats& GetPopStats() const override {
        Serializer->Buffer->ExportPopStats(PopStats);
        return PopStats;
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

    void Bind(NActors::TActorId outputActorId, NActors::TActorId inputActorId) override;

    bool IsLocal() const override {
        return IsLocalChannel;
    }

    std::weak_ptr<TDqChannelService> Service;
    std::unique_ptr<TOutputSerializer> Serializer;
    std::shared_ptr<TDqFillAggregator> Aggregator;
    IDqChannelStorage::TPtr Storage;
    bool IsLocalChannel = false;
};

class TFastDqInputChannel : public IDqInputChannel {

public:

    TFastDqInputChannel(std::weak_ptr<TDqChannelService> service, const TDqChannelSettings& settings, std::shared_ptr<IChannelBuffer> buffer)
        : Service(service), Buffer(buffer) {
        PushStats.ChannelId = settings.ChannelId;
        PushStats.SrcStageId = settings.SrcStageId;
        PushStats.Level = settings.Level;
        PopStats.Level = settings.Level;
        Deserializer = CreateDeserializer(settings.RowType, settings.TransportVersion, settings.PackerVersion, settings.BufferPageAllocSize, *settings.HolderFactory);
    }

    mutable TDqInputStats PopStats;
    mutable TDqInputChannelStats PushStats;

// IDqInput

    const TDqInputStats& GetPopStats() const override {
        Buffer->ExportPopStats(PopStats);
        return PopStats;
    }

    bool Empty() const override {
        return Buffer->IsEmpty();
    }

    bool Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, TMaybe<TInstant>& watermark) override;

    bool IsFinished() const override {
        return Buffer->IsFinished();
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
        return PushStats.ChannelId;
    }

    const TDqInputChannelStats& GetPushStats() const override {
        Buffer->ExportPushStats(PushStats);
        return PushStats;
    }

    void Push(TDqSerializedBatch&&) override {
        Y_ENSURE(false);
    }

    void Push(TInstant /* watermark */) override {
        Y_ENSURE(false);
    }

    void Finish() override {
        Buffer->EarlyFinish();
    }

    void Bind(NActors::TActorId outputActorId, NActors::TActorId inputActorId) override;

    bool IsLocal() const override {
        return IsLocalChannel;
    }

    std::weak_ptr<TDqChannelService> Service;
    std::shared_ptr<IChannelBuffer> Buffer;
    std::unique_ptr<TInputDeserializer> Deserializer;
    bool IsLocalChannel = false;
};

class TChannelServiceActor : public NActors::TActorBootstrapped<TChannelServiceActor> {
public:
    TChannelServiceActor(std::shared_ptr<TDqChannelService> channelService)
        : ChannelService(channelService)
    {}

    void Bootstrap() {

        NActors::TMon* mon = NKikimr::AppData()->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "kqp_channels", "KQP Channels", false,
                NActors::TActivationContext::ActorSystem(), SelfId());
        }

        Become(&TChannelServiceActor::StateFunc);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDqCompute::TEvChannelDiscoveryV2, Handle);
            hFunc(TEvDqCompute::TEvChannelDataV2, Handle);
            hFunc(TEvDqCompute::TEvChannelUpdateV2, Handle);
            hFunc(TEvPrivate::TEvServiceLookup, Handle);
            cFunc(NActors::TEvents::TEvWakeup::EventType, HandleWakeup);
            hFunc(NActors::NMon::TEvHttpInfo, Handle);
        }
    }

    void Handle(TEvDqCompute::TEvChannelDiscoveryV2::TPtr& ev) {
        auto state = ChannelService->GetOrCreateNodeState(ev->Sender.NodeId());
        Send(ev->Forward(state->NodeActorId));
        if (!CleanupScheduled) {
            // we need no cleanup until very first incoming msg
            CleanupScheduled = true;
            Schedule(TDuration::Seconds(30), new NActors::TEvents::TEvWakeup());
        }
    }

    void Handle(TEvDqCompute::TEvChannelDataV2::TPtr& ev) {
        auto state = ChannelService->GetOrCreateNodeState(ev->Sender.NodeId());
        Send(ev->Forward(state->NodeActorId));
    }

    void Handle(TEvDqCompute::TEvChannelUpdateV2::TPtr& ev) {
        auto state = ChannelService->GetOrCreateNodeState(ev->Sender.NodeId());
        Send(ev->Forward(state->NodeActorId));
    }

    void Handle(TEvPrivate::TEvServiceLookup::TPtr& ev) {
        auto evReply = MakeHolder<TEvPrivate::TEvServiceReply>();
        evReply->Service = ChannelService;
        Send(ev->Sender, evReply.Release());
    }

    void HandleWakeup() {
        ChannelService->CleanupUnbound();
        Schedule(TDuration::Seconds(30), new NActors::TEvents::TEvWakeup());
    }

    void Handle(NActors::NMon::TEvHttpInfo::TPtr& ev);

    std::shared_ptr<TDqChannelService> ChannelService;
    bool CleanupScheduled = false;
};

class TNodeSessionActor : public NActors::TActor<TNodeSessionActor> {
public:
    TNodeSessionActor(std::shared_ptr<TNodeState>& nodeState)
        : TActor(&TThis::StateFunc)
        , NodeState(nodeState)
    {}

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvents::TEvUndelivered, Handle);
            hFunc(NActors::TEvents::TEvWakeup, Handle);
            hFunc(TEvDqCompute::TEvChannelDiscoveryV2, Handle);
            hFunc(TEvDqCompute::TEvChannelDataV2, Handle);
            hFunc(TEvDqCompute::TEvChannelAckV2, Handle);
            hFunc(TEvDqCompute::TEvChannelUpdateV2, Handle);
            hFunc(TEvPrivate::TEvSendWaiters, Handle);
            hFunc(TEvPrivate::TEvReconciliation, Handle);
        }
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        NodeState->HandleUndelivered(ev);
    }

    void Handle(NActors::TEvents::TEvWakeup::TPtr& ev) {
        NodeState->HandleWakeup(ev);
    }

    void Handle(TEvDqCompute::TEvChannelDiscoveryV2::TPtr& ev) {
        NodeState->HandleDiscovery(ev);
    }

    void Handle(TEvDqCompute::TEvChannelDataV2::TPtr& ev) {
        NodeState->HandleData(ev);
    }

    void Handle(TEvDqCompute::TEvChannelAckV2::TPtr& ev) {
        NodeState->HandleAck(ev);
    }

    void Handle(TEvDqCompute::TEvChannelUpdateV2::TPtr& ev) {
        NodeState->HandleUpdate(ev);
    }

    void Handle(TEvPrivate::TEvSendWaiters::TPtr& ev) {
        NodeState->HandleSendWaiters(ev);
    }

    void Handle(TEvPrivate::TEvReconciliation::TPtr& ev) {
        NodeState->HandleReconciliation(ev);
    }

    std::shared_ptr<TNodeState> NodeState;
};

class TDebugNodeSessionActor : public NActors::TActor<TDebugNodeSessionActor> {
public:
    TDebugNodeSessionActor(std::shared_ptr<TDebugNodeState>& nodeState)
        : TActor(&TThis::StateFunc)
        , NodeState(nodeState)
    {}

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvents::TEvUndelivered, Handle);
            hFunc(NActors::TEvents::TEvWakeup, Handle);
            hFunc(TEvDqCompute::TEvChannelDiscoveryV2, Handle);
            hFunc(TEvDqCompute::TEvChannelDataV2, Handle);
            hFunc(TEvDqCompute::TEvChannelAckV2, Handle);
            hFunc(TEvDqCompute::TEvChannelUpdateV2, Handle);
            hFunc(TEvPrivate::TEvSendWaiters, Handle);
            hFunc(TEvPrivate::TEvProcessPending, Handle);
        }
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        NodeState->HandleUndelivered(ev);
    }

    void Handle(NActors::TEvents::TEvWakeup::TPtr& ev) {
        NodeState->HandleWakeup(ev);
    }

    void Handle(TEvDqCompute::TEvChannelDiscoveryV2::TPtr& ev) {
        NodeState->HandleDiscovery(ev);
    }

    void Handle(TEvDqCompute::TEvChannelDataV2::TPtr& ev) {
        if (NodeState->ShouldLooseData()) {
            return;
        }
        if (NodeState->ChannelDataPaused.load()) {
            PendingChannelData.emplace(ev.Release());
        } else {
            while (!PendingChannelData.empty()) {
                NodeState->HandleData(PendingChannelData.front());
                PendingChannelData.pop();
            }
            if (NodeState->IsNullMode()) {
                NodeState->HandleNullMode(ev);
            } else {
                NodeState->HandleData(ev);
            }
        }
    }

    void Handle(TEvDqCompute::TEvChannelAckV2::TPtr& ev) {
        if (NodeState->ShouldLooseAck()) {
            return;
        }
        if (NodeState->ChannelAckPaused.load()) {
            PendingChannelAck.emplace(ev.Release());
        } else {
            while (!PendingChannelAck.empty()) {
                NodeState->HandleAck(PendingChannelAck.front());
                PendingChannelAck.pop();
            }
            NodeState->HandleAck(ev);
        }
    }

    void Handle(TEvDqCompute::TEvChannelUpdateV2::TPtr& ev) {
        NodeState->HandleUpdate(ev);
    }

    void Handle(TEvPrivate::TEvSendWaiters::TPtr& ev) {
        NodeState->HandleSendWaiters(ev);
    }

    void Handle(TEvPrivate::TEvProcessPending::TPtr& ev) {
        auto maxCount = ev->Get()->MaxCount;

        if (!NodeState->ChannelDataPaused.load()) {
            while (!PendingChannelData.empty()) {
                if (NodeState->IsNullMode()) {
                    NodeState->HandleNullMode(PendingChannelData.front());
                } else {
                    NodeState->HandleData(PendingChannelData.front());
                }
                PendingChannelData.pop();
                if (maxCount && --maxCount == 0) {
                    return;
                }
            }
        }
        if (!NodeState->ChannelAckPaused.load()) {
            while (!PendingChannelAck.empty()) {
                NodeState->HandleAck(PendingChannelAck.front());
                PendingChannelAck.pop();
                if (maxCount && --maxCount == 0) {
                    return;
                }
            }
        }
    }

    std::shared_ptr<TDebugNodeState> NodeState;
    std::queue<TEvDqCompute::TEvChannelDataV2::TPtr> PendingChannelData;
    std::queue<TEvDqCompute::TEvChannelAckV2::TPtr> PendingChannelAck;
};

}

