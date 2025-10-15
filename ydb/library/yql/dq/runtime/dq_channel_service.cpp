#include <queue>
#include <mutex>

#include "dq_arrow_helpers.h"
#include "dq_channel_service_impl.h"

namespace NYql::NDq {

void IChannelBuffer::SendFinish() {
    Push(TDataChunk(true));
}

EDqFillLevel TLocalBuffer::GetFillLevel() const {
    return FillLevel;
}

TLocalBuffer::~TLocalBuffer() {
    Registry->DeleteLocalBufferInfo(Info);
}

void TLocalBuffer::SetFillAggregator(std::shared_ptr<TDqFillAggregator> aggregator) {
    std::lock_guard lock(Mutex);
    Aggregator = aggregator;
    Aggregator->AddCount(FillLevel);
}

void TLocalBuffer::Push(TDataChunk&& data) {
    PushStats.Chunks++;
    PushStats.Rows += data.Rows;
    PushStats.Bytes += data.Bytes;
    InflightBytes += data.Bytes;

    std::lock_guard lock(Mutex);

    Queue.emplace(std::move(data));

    if (FillLevel == EDqFillLevel::NoLimit && InflightBytes > MaxInflightBytes) {
        FillLevel = EDqFillLevel::HardLimit;
        if (Aggregator) {
            Aggregator->UpdateCount(EDqFillLevel::NoLimit, EDqFillLevel::HardLimit);
        }
        NeedToNotifyOutput = true;
    }
    if (NeedToNotifyInput) {
        NeedToNotifyInput = false;
        ActorSystem->Send(Info.InputActorId, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback});
    }
}

bool TLocalBuffer::IsEarlyFinished() {
    return EarlyFinished.load();
}

bool TLocalBuffer::IsFlushed() {
    std::lock_guard lock(Mutex);
    auto result = Queue.empty();
    if (!result && !NeedToNotifyOutput) {
        NeedToNotifyOutput = true;
    }
    return result;
}

bool TLocalBuffer::IsEmpty() {
    std::lock_guard lock(Mutex);
    auto result = Queue.empty();
    if (result) {
        NeedToNotifyInput = true;
    }
    return result;
}

bool TLocalBuffer::Pop(TDataChunk& data) {
    std::lock_guard lock(Mutex);
    if (Queue.empty()) {
        NeedToNotifyInput = true;
        return false;
    } else {
        data = std::move(Queue.front());
        Queue.pop();

        PopStats.Chunks++;
        PopStats.Rows += data.Rows;
        PopStats.Bytes += data.Bytes;
        Y_ENSURE(InflightBytes >= data.Bytes);
        InflightBytes -= data.Bytes;
        if (FillLevel == EDqFillLevel::HardLimit && InflightBytes <= MinInflightBytes) {
            FillLevel = EDqFillLevel::NoLimit;
            if (Aggregator) {
                Aggregator->UpdateCount(EDqFillLevel::HardLimit, EDqFillLevel::NoLimit);
            }
            if (NeedToNotifyOutput) {
                NeedToNotifyOutput = false;
                ActorSystem->Send(Info.OutputActorId, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback});
            }
        } else if (Queue.empty() && NeedToNotifyOutput) {
            NeedToNotifyOutput = false;
            ActorSystem->Send(Info.OutputActorId, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback});
        }

        return true;
    }
}

void TLocalBuffer::EarlyFinish() {
    EarlyFinished.store(true);
}

void TOutputDescriptor::AddPushBytes(ui64 bytes) {
    // lock expected here
    PushBytes += bytes;
    if (FillLevel == EDqFillLevel::NoLimit && PushBytes.load() > MaxInflightBytes + PopBytes.load()) {
        FillLevel = EDqFillLevel::HardLimit;
        if (Aggregator) {
            Aggregator->UpdateCount(EDqFillLevel::NoLimit, EDqFillLevel::HardLimit);
        }
        NeedToNotifyOutput = true;
    }
}

void TOutputDescriptor::UpdatePopBytes(ui64 bytes) {
    // lock expected here
    PopBytes.store(bytes);
    if (FillLevel == EDqFillLevel::HardLimit && PushBytes.load() <= MinInflightBytes + bytes) {
        FillLevel = EDqFillLevel::NoLimit;
        if (Aggregator) {
            Aggregator->UpdateCount(EDqFillLevel::HardLimit, EDqFillLevel::NoLimit);
        }
    } else if (PushBytes.load() > bytes) {
        return;
    }

    if (NeedToNotifyOutput) {
        NeedToNotifyOutput = false;
        ActorSystem->Send(Info.OutputActorId, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback});
    }
}

bool TOutputDescriptor::TryPushToWaitQueue(TDataChunk&& data) {
    std::lock_guard lock(Mutex);
    if (!WaitQueue.empty()) { // we are not allowed to reorder messages
        AddPushBytes(data.Bytes);
        WaitQueue.push(std::move(data));
        return true;
    } else {
        return false;
    }
}

void TOutputDescriptor::AddInflight(ui64 bytes) {
    std::lock_guard lock(Mutex);
    AddPushBytes(bytes);
}

void TOutputDescriptor::PushToWaitQueue(TDataChunk&& data) {
    std::lock_guard lock(Mutex);
    AddPushBytes(data.Bytes);
    WaitQueue.push(std::move(data));
}

bool TOutputDescriptor::IsFlushed() {
    std::lock_guard lock(Mutex);
    if (PushBytes.load() > PopBytes.load()) {
        NeedToNotifyOutput = true;
        return false;
    } else {
        return true;
    }
}

void TOutputDescriptor::Terminate() {
    Terminated.store(true);
}

bool TOutputDescriptor::IsTerminated() {
    return Terminated.load();
}

TOutputBuffer::~TOutputBuffer() {
    NodeState->TerminateDescriptor(Descriptor);
}

EDqFillLevel TOutputBuffer::GetFillLevel() const {
    std::lock_guard lock(Descriptor->Mutex);
    return Descriptor->FillLevel;
}

void TOutputBuffer::SetFillAggregator(std::shared_ptr<TDqFillAggregator> aggregator) {
    std::lock_guard lock(Descriptor->Mutex);
    Descriptor->Aggregator = aggregator;
    Descriptor->Aggregator->AddCount(Descriptor->FillLevel);
}

void TOutputBuffer::Push(TDataChunk&& data) {
    PushStats.Chunks++;
    PushStats.Rows += data.Rows;
    PushStats.Bytes += data.Bytes;
    NodeState->Push(std::move(data), Descriptor);
}

bool TOutputBuffer::IsEarlyFinished() {
    return Descriptor->EarlyFinished.load();
}

bool TOutputBuffer::IsFlushed() {
    return Descriptor->IsFlushed();
}

bool TOutputBuffer::IsEmpty() {
    return false;
}

bool TOutputBuffer::Pop(TDataChunk&) {
    return false;
}

void TOutputBuffer::EarlyFinish() {
    Descriptor->EarlyFinished.store(true);
}

bool TInputBuffer::IsEmpty() {
    std::lock_guard lock(Mutex);
    auto result = Queue.empty();
    if (result) {
        NeedToNotify = true;
    }
    return result;
}

void TInputBuffer::PushDataChunk(TDataChunk&& data) {
    std::lock_guard lock(Mutex);
    Queue.emplace(TInputItem(std::move(data)));
    if (NeedToNotify) {
        NeedToNotify = false;
        ActorSystem->Send(Info.InputActorId, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback});
    }
}

bool TInputBuffer::Pop(TDataChunk& data) {
    std::lock_guard lock(Mutex);
    if (Queue.empty()) {
        NeedToNotify = true;
        return false;
    } else {
        data = std::move(Queue.front().Data);
        Queue.pop();

        PopStats.Chunks++;
        PopStats.Rows += data.Rows;
        PopStats.Bytes += data.Bytes;

        ActorSystem->Send(NodeActorId, new TEvPrivate::TEvUpdatePopBytes(Info, PopStats.Bytes));

        return true;
    }
}

void TInputBuffer::EarlyFinish() {
    EarlyFinished = true;
    ActorSystem->Send(NodeActorId, new TEvPrivate::TEvEarlyFinish(Info));
}

TInputBufferProxy::~TInputBufferProxy() {
    NodeState->TerminateInputBuffer(Buffer);
}

bool TInputBufferProxy::IsEmpty() {
    return Buffer->IsEmpty();
}

EDqFillLevel TInputBufferProxy::GetFillLevel() const {
    return Buffer->GetFillLevel();
}

void TInputBufferProxy::SetFillAggregator(std::shared_ptr<TDqFillAggregator> aggregator) {
    Buffer->SetFillAggregator(aggregator);
}

void TInputBufferProxy::Push(TDataChunk&& data) {
    Buffer->Push(std::move(data));
}

bool TInputBufferProxy::IsEarlyFinished() {
    return Buffer->IsEarlyFinished();
}

bool TInputBufferProxy::IsFlushed() {
    return Buffer->IsFlushed();
}

bool TInputBufferProxy::Pop(TDataChunk& data) {
    return Buffer->Pop(data);
}

void TInputBufferProxy::EarlyFinish() {
    Buffer->EarlyFinish();
}

std::shared_ptr<TLocalBuffer> TLocalBufferRegistry::GetOrCreateLocalBuffer(const std::shared_ptr<TLocalBufferRegistry>& registry, const TChannelInfo& info) {
    std::lock_guard lock(Mutex);

    auto it = LocalBuffers.find(info);
    if (it != LocalBuffers.end()) {
        auto result = it->second.lock();
        if (result) {
            return result;
        } else {
            LocalBuffers.erase(it);
        }
    }
    auto result = std::make_shared<TLocalBuffer>(registry, info, ActorSystem, MaxInflightBytes, MinInflightBytes);
    LocalBuffers.emplace(info, result);

    return result;
}

void TLocalBufferRegistry::DeleteLocalBufferInfo(const TChannelInfo& info) {
    std::lock_guard lock(Mutex);
    LocalBuffers.erase(info);
}

void TNodeState::Push(TDataChunk&& data, std::shared_ptr<TOutputDescriptor> descriptor) {
    auto bytes = data.Bytes;

    if (descriptor->TryPushToWaitQueue(std::move(data))) {
        return;
    }

    {
        std::lock_guard lock(Mutex);
        if (InflightBytes < MaxInflightBytes && Queue.size() < MaxInflightMessages) {
            InflightBytes += data.Bytes;
            auto item = std::make_shared<TOutputItem>(std::move(data), descriptor);
            item->UniqueId = ++LastUniqueId;
            Queue.push(item);
            Send(item);
            descriptor->AddInflight(bytes);
            return;
        }
    }

    auto timestamp = data.Timestamp;
    descriptor->PushToWaitQueue(std::move(data));
    {
        std::lock_guard lock(Mutex);
        descriptor->WaitTimestamp = timestamp;
        WaitersQueue.push(descriptor);
    }
}

void TNodeState::Send(std::shared_ptr<TOutputItem> item) {
    auto ev = MakeHolder<TEvDqCompute::TEvChannelDataV2>();
    NActors::ActorIdToProto(item->Descriptor->Info.OutputActorId, ev->Record.MutableSrcActorId());
    NActors::ActorIdToProto(item->Descriptor->Info.InputActorId, ev->Record.MutableDstActorId());
    ev->Record.SetChannelId(item->Descriptor->Info.ChannelId);
    if (!item->Data.Buffer.Empty()) {
        ev->Record.SetPayloadId(ev->AddPayload(MakeReadOnlyRope(item->Data.Buffer)));
        ev->Record.SetTransportVersion(item->Data.TransportVersion);
        ev->Record.SetValuePackerVersion(ToProto(item->Data.PackerVersion));
    }
    ev->Record.SetRows(item->Data.Rows);
    ev->Record.SetSendTime(item->Data.Timestamp.MicroSeconds());
    ev->Record.SetBytes(item->Data.Bytes);
    if (item->Data.Finished) {
        ev->Record.SetFinished(true);
    }
    ev->Record.SetSendTime(TInstant::Now().MilliSeconds());
    ev->Record.SetSeqNo(item->UniqueId);
    // optional bool NoAck = 8;

    ui32 flags = NActors::IEventHandle::FlagTrackDelivery;
    if (!Subscribed.exchange(true)) {
        flags |=  NActors::IEventHandle::FlagSubscribeOnSession;
    }
    ActorSystem->Send(new NActors::IEventHandle(PeerActorId, NodeActorId, ev.Release(), flags, item->RetryCount));
    item->State.store(TOutputItem::EState::Sent);
}

void TNodeState::Handle(TEvDqCompute::TEvChannelDataV2::TPtr& ev) {
    if (!Connected) {
        std::lock_guard lock(Mutex);
        PeerActorId = ev->Sender;
        Connected = true;
    }

    auto& record = ev->Get()->Record;

    if (record.GetSeqNo() != PeerUniqueId + 1) {
        Cerr << "record.GetSeqNo() == " << record.GetSeqNo() << ", PeerUniqueId == " << PeerUniqueId << Endl;
    }

    Y_ENSURE(record.GetSeqNo() == PeerUniqueId + 1); // TBD: reconcilation
    PeerUniqueId++;

    auto buffer = GetOrCreateInputBuffer(
        TChannelInfo(record.GetChannelId(),
            NActors::ActorIdFromProto(record.GetSrcActorId()),
            NActors::ActorIdFromProto(record.GetDstActorId())
        ), false
    );

    TDataChunk data(TChunkedBuffer(), record.GetRows(), record.GetTransportVersion(), FromProto(record.GetValuePackerVersion()));
    if (ev->Get()->GetPayloadCount() > 0) {
        data.Buffer = MakeChunkedBuffer(ev->Get()->GetPayload(record.GetPayloadId()));
        data.Timestamp = TInstant::MicroSeconds(record.GetSendTime());
    }
    data.Bytes = record.GetBytes();
    Y_ENSURE(data.Bytes > data.Buffer.Size()); // record.GetBytes() == data.Buffer.Size() + const
    data.Finished = record.GetFinished();
    buffer->PushDataChunk(std::move(data));

    auto evAck = MakeHolder<TEvDqCompute::TEvChannelAckV2>();
    NActors::ActorIdToProto(buffer->Info.OutputActorId, evAck->Record.MutableSrcActorId());
    NActors::ActorIdToProto(buffer->Info.InputActorId, evAck->Record.MutableDstActorId());
    evAck->Record.SetChannelId(buffer->Info.ChannelId);
    evAck->Record.SetSeqNo(record.GetSeqNo());
    evAck->Record.SetStatus(NYql::NDqProto::TEvChannelAckV2::OK);
    if (buffer->EarlyFinished) {
        evAck->Record.SetEarlyFinished(true);
    }

    ui32 flags = NActors::IEventHandle::FlagTrackDelivery;
    if (!Subscribed.exchange(true)) {
        flags |=  NActors::IEventHandle::FlagSubscribeOnSession;
    }

    ActorSystem->Send(new NActors::IEventHandle(PeerActorId, NodeActorId, evAck.Release(), flags, ev->Cookie));
}

void TNodeState::Handle(TEvDqCompute::TEvChannelAckV2::TPtr& ev) {

    auto& record = ev->Get()->Record;

    TChannelInfo info(record.GetChannelId(), NActors::ActorIdFromProto(record.GetSrcActorId()), NActors::ActorIdFromProto(record.GetDstActorId()));

    std::lock_guard lock(Mutex);
    ui64 deltaBytes = 0;

    if (record.GetStatus() == NYql::NDqProto::TEvChannelAckV2::OK) {
        Y_ENSURE(!Queue.empty());
        auto& item = Queue.front();
        Y_ENSURE(item->UniqueId == record.GetSeqNo());
        Y_ENSURE(item->State.load() == TOutputItem::EState::Sent);
        deltaBytes += item->Data.Bytes;
        Queue.pop();
    };

    auto it = OutputDescriptors.find(info);
    if (it != OutputDescriptors.end()) {
        auto desc = it->second;
        if (!desc->IsTerminated()) {
            if (record.GetPopBytes()) {
                desc->UpdatePopBytes(record.GetPopBytes());
            }

            if (record.GetEarlyFinished() && desc->EarlyFinished.exchange(true)) {
                ActorSystem->Send(desc->Info.OutputActorId, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback});
            }
        }
    }

    Y_ENSURE(InflightBytes >= deltaBytes);

    while (InflightBytes - deltaBytes < MaxInflightBytes && Queue.size() < MaxInflightMessages && !WaitersQueue.empty()) {
        auto waiter = WaitersQueue.top();
        WaitersQueue.pop();
        Y_ENSURE(!waiter->WaitQueue.empty());

        if (waiter->IsTerminated()) {
            continue;
        }

        auto& data = waiter->WaitQueue.front();
        InflightBytes += data.Bytes;
        auto item = std::make_shared<TOutputItem>(std::move(data), waiter);
        item->UniqueId = ++LastUniqueId;
        Queue.push(item);

        waiter->WaitQueue.pop();
        if (!waiter->WaitQueue.empty()) {
            waiter->WaitTimestamp = waiter->WaitQueue.front().Timestamp;
            WaitersQueue.push(waiter);
        }

        Send(item);
    }

    InflightBytes -= deltaBytes;
}

void TNodeState::Handle(TEvPrivate::TEvEarlyFinish::TPtr& ev) {
    auto evAck = MakeHolder<TEvDqCompute::TEvChannelAckV2>();

    NActors::ActorIdToProto(ev->Get()->Info.OutputActorId, evAck->Record.MutableSrcActorId());
    NActors::ActorIdToProto(ev->Get()->Info.InputActorId, evAck->Record.MutableDstActorId());
    evAck->Record.SetChannelId(ev->Get()->Info.ChannelId);
    evAck->Record.SetStatus(NYql::NDqProto::TEvChannelAckV2::UNSPECIFIED);
    evAck->Record.SetEarlyFinished(true);

    ui32 flags = NActors::IEventHandle::FlagTrackDelivery;
    if (!Subscribed.exchange(true)) {
        flags |=  NActors::IEventHandle::FlagSubscribeOnSession;
    }

    ActorSystem->Send(new NActors::IEventHandle(PeerActorId, NodeActorId, evAck.Release(), flags));
}

void TNodeState::Handle(TEvPrivate::TEvUpdatePopBytes::TPtr& ev) {
    auto evAck = MakeHolder<TEvDqCompute::TEvChannelAckV2>();

    NActors::ActorIdToProto(ev->Get()->Info.OutputActorId, evAck->Record.MutableSrcActorId());
    NActors::ActorIdToProto(ev->Get()->Info.InputActorId, evAck->Record.MutableDstActorId());
    evAck->Record.SetChannelId(ev->Get()->Info.ChannelId);
    evAck->Record.SetStatus(NYql::NDqProto::TEvChannelAckV2::UNSPECIFIED);
    evAck->Record.SetPopBytes(ev->Get()->Bytes);

    ui32 flags = NActors::IEventHandle::FlagTrackDelivery;
    if (!Subscribed.exchange(true)) {
        flags |=  NActors::IEventHandle::FlagSubscribeOnSession;
    }

    ActorSystem->Send(new NActors::IEventHandle(PeerActorId, NodeActorId, evAck.Release(), flags));
}

std::shared_ptr<TOutputBuffer> TNodeState::CreateOutputBuffer(const TChannelInfo& info, ui64 maxInflightBytes, ui64 minInflightBytes) {
    auto descriptor = std::make_shared<TOutputDescriptor>(info, ActorSystem, maxInflightBytes, minInflightBytes);
    std::lock_guard lock(Mutex);
    auto [_, inserted] = OutputDescriptors.emplace(info, descriptor);
    Y_ENSURE(inserted);
    auto self = Self.lock();
    Y_ENSURE(self);
    auto result = std::make_shared<TOutputBuffer>(self, descriptor);
    descriptor->Buffer = result;
    return result;
}

std::shared_ptr<TInputBuffer> TNodeState::GetOrCreateInputBuffer(const TChannelInfo& info, bool binded) {
    std::lock_guard lock(Mutex);
    auto it = InputBuffers.find(info);
    if (it != InputBuffers.end()) {
        auto result = it->second;
        // if (desc.SrcStageId) {
        //     result->PushStats.SrcStageId = desc.SrcStageId;
        // }
        // if (desc.DstStageId) {
        //     result->PopStats.DstStageId = desc.DstStageId;
        // }
        if (binded) {
            result->IsBinded = true;
        }
        return result;
    }
    auto result = std::make_shared<TInputBuffer>(NodeActorId, info, ActorSystem);
    InputBuffers.emplace(info, result);
    if (!binded) {
        UnbindedInputs.emplace(info, TInstant::Now() + UnbindedWaitPeriod);
    }
    return result;
}

void TNodeState::TerminateDescriptor(const std::shared_ptr<TOutputDescriptor>& descriptor) {
    descriptor->Terminate();
    std::lock_guard lock(Mutex);
    OutputDescriptors.erase(descriptor->Info);
}

void TNodeState::TerminateInputBuffer(const std::shared_ptr<TInputBuffer>& inputBuffer) {
    InputBuffers.erase(inputBuffer->Info);
}

void TNodeState::CleanupUnbindedInputs() {
    std::lock_guard lock(Mutex);
    auto now = TInstant::Now();
    while (!UnbindedInputs.empty()) {
        auto& front = UnbindedInputs.front();
        if (front.second > now) {
            break;
        }
        InputBuffers.erase(front.first);
        UnbindedInputs.pop();
    }
}

void TDebugNodeState::PauseChannelData() {
    ChannelDataPaused.store(true);
}

void TDebugNodeState::ResumeChannelData() {
    ChannelDataPaused.store(false);
    ActorSystem->Send(new NActors::IEventHandle(NodeActorId, NodeActorId, new TEvPrivate::TEvProcessPending()));
}

void TDebugNodeState::PauseChannelAck() {
    ChannelAckPaused.store(true);
}

void TDebugNodeState::ResumeChannelAck() {
    ChannelAckPaused.store(false);
    ActorSystem->Send(new NActors::IEventHandle(NodeActorId, NodeActorId, new TEvPrivate::TEvProcessPending()));
}

std::shared_ptr<TNodeState> TDqChannelService::GetOrCreateNodeState(ui32 nodeId) {
    std::lock_guard lock(Mutex);
    auto it = NodeStates.find(nodeId);
    if (it != NodeStates.end()) {
        return it->second;
    } else {
        auto nodeState = std::make_shared<TNodeState>(ActorSystem, Limits.NodeSessionIcInflightBytes);
        nodeState->NodeActorId = ActorSystem->Register(new TNodeSessionActor(nodeState));
        nodeState->PeerActorId = MakeChannelServiceActorID(nodeId);
        nodeState->Self = nodeState;
        NodeStates.emplace(nodeId, nodeState);
        return nodeState;
    }
}

std::shared_ptr<TDebugNodeState> TDqChannelService::CreateDebugNodeState(ui32 nodeId) {
    std::lock_guard lock(Mutex);
    Y_ENSURE(NodeStates.find(nodeId) == NodeStates.end());

    auto nodeState = std::make_shared<TDebugNodeState>(ActorSystem, Limits.NodeSessionIcInflightBytes);
    nodeState->NodeActorId = ActorSystem->Register(new TDebugNodeSessionActor(nodeState));
    nodeState->PeerActorId = MakeChannelServiceActorID(nodeId);
    nodeState->Self = nodeState;
    NodeStates.emplace(nodeId, nodeState);
    return nodeState;
}

// unbinded stubs

std::shared_ptr<IChannelBuffer> TDqChannelService::GetOutputBuffer(ui64 channelId) {
    return std::make_shared<TChannelStub>(channelId);
}

std::shared_ptr<IChannelBuffer> TDqChannelService::GetInputBuffer(ui64 channelId) {
    return std::make_shared<TChannelStub>(channelId);
}

// binded helpers

std::shared_ptr<IChannelBuffer> TDqChannelService::GetOutputBuffer(const TChannelInfo& info) {
    Y_ENSURE(info.OutputActorId.NodeId() == NodeId);
    return (info.InputActorId.NodeId() == NodeId) ? GetLocalBuffer(info) : GetRemoteOutputBuffer(info);
}

std::shared_ptr<IChannelBuffer> TDqChannelService::GetInputBuffer(const TChannelInfo& info) {
    Y_ENSURE(info.InputActorId.NodeId() == NodeId);
    return (info.OutputActorId.NodeId() == NodeId) ? GetLocalBuffer(info) : GetRemoteInputBuffer(info);
}

// remote buffers

std::shared_ptr<IChannelBuffer> TDqChannelService::GetRemoteOutputBuffer(const TChannelInfo& info) {
    Y_ENSURE(info.InputActorId.NodeId() != NodeId);
    return GetOrCreateNodeState(info.InputActorId.NodeId())->CreateOutputBuffer(info,
        Limits.RemoteChannelInflightBytes, Limits.RemoteChannelInflightBytes * 8 / 10);
}

std::shared_ptr<IChannelBuffer> TDqChannelService::GetRemoteInputBuffer(const TChannelInfo& info) {
    Y_ENSURE(info.OutputActorId.NodeId() != NodeId);
    auto nodeState = GetOrCreateNodeState(info.OutputActorId.NodeId());
    return std::make_shared<TInputBufferProxy>(nodeState, nodeState->GetOrCreateInputBuffer(info, true));
}

// local buffer

std::shared_ptr<IChannelBuffer> TDqChannelService::GetLocalBuffer(const TChannelInfo& info) {
    Y_ENSURE(info.OutputActorId.NodeId() == NodeId);
    Y_ENSURE(info.InputActorId.NodeId() == NodeId);
    return LocalBufferRegistry->GetOrCreateLocalBuffer(LocalBufferRegistry, info);
}

// unbinded channels

IDqOutputChannel::TPtr TDqChannelService::GetOutputChannel(const TDqChannelParams& params) {
    Y_ENSURE(params.TransportVersion == NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_FAST_PICKLE_1_0
            || params.TransportVersion == NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_FAST_PICKLE_1_0);
    return new TFastDqOutputChannel(Self, params, GetOutputBuffer(params.Desc.ChannelId), false);
}

IDqInputChannel::TPtr TDqChannelService::GetInputChannel(const TDqChannelParams& params) {
    Y_ENSURE(params.TransportVersion == NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_FAST_PICKLE_1_0
            || params.TransportVersion == NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_FAST_PICKLE_1_0);
    return new TFastDqInputChannel(Self, params, GetInputBuffer(params.Desc.ChannelId));
}

void TDqChannelService::CleanupUnbindedInputs() {
    std::lock_guard lock(Mutex);
    for (auto& [_, nodeState] : NodeStates) {
        nodeState->CleanupUnbindedInputs();
    }
}

// TFastDqOutputChannel::

bool TFastDqInputChannel::Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch) {
    TDataChunk chunk;
    if (!Buffer->Pop(chunk)) {
        return false;
    }
    if (chunk.Finished) {
        Finished = true;
    }
    if (chunk.Buffer.Empty()) {
        return false;
    }
    if (chunk.TransportVersion != Deserializer->TransportVersion || chunk.PackerVersion != Deserializer->PackerVersion) {
        auto deserializer = CreateDeserializer(Deserializer->RowType, chunk.TransportVersion, chunk.PackerVersion, Deserializer->HolderFactory);
        Deserializer = std::move(deserializer);
    }
    Deserializer->Deserialize(std::move(chunk.Buffer), batch);
    Y_ENSURE(batch.RowCount() > 0);
    return true;
}

bool TFastDqInputChannel::Bind(NActors::TActorId outputActorId, NActors::TActorId inputActorId) {
    if (auto service = Service.lock()) {
        Buffer = service->GetInputBuffer(TChannelInfo(Desc.ChannelId, outputActorId, inputActorId));
        Service.reset();
        return true;
    }
    return false;
}

bool TFastDqOutputChannel::Bind(NActors::TActorId outputActorId, NActors::TActorId inputActorId) {
    if (auto service = Service.lock()) {
        if (inputActorId.NodeId() == service->NodeId) {
            Serializer = ConvertToLocalSerializer(std::move(Serializer));
        }
        Serializer->Buffer = service->GetOutputBuffer(TChannelInfo(Desc.ChannelId, outputActorId, inputActorId));
        Service.reset();
        return true;
    }
    return false;
}

NActors::IActor* CreateLocalChannelServiceActor(NActors::TActorSystem* actorSystem, ui32 nodeId, const TDqChannelLimits& limits, std::shared_ptr<IDqChannelService>& service) {
    auto channelService = std::make_shared<TDqChannelService>(actorSystem, nodeId, limits);
    channelService->Self = channelService;
    service = channelService;
    return new TChannelServiceActor(channelService);
}

} // namespace NYql::NDq
