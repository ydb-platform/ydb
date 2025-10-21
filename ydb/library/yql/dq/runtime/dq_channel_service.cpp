#include <queue>
#include <mutex>

#include "dq_arrow_helpers.h"
#include "dq_channel_service_impl.h"

#include <ydb/library/yql/dq/actors/dq.h>

#include <util/random/random.h>

#include <ydb/library/actors/core/log.h>

namespace NYql::NDq {

bool IChannelBuffer::GetLeading() {
    auto result = Leading;
    Leading = false;
    return result;
}

void IChannelBuffer::SendFinish() {
    Push(TDataChunk(GetLeading(), true));
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

    if (bytes <= PopBytes.load()) {
        return;
    }

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

bool TOutputDescriptor::CheckGenMajor(ui64 genMajor) {
    std::lock_guard lock(Mutex);
    if (GenMajor == 0) {
        GenMajor = genMajor;
    } else {
        if (GenMajor != genMajor) {
            AbortChannel("Reconcilation failed");
            return false;
        }
    }
    return true;
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

bool TOutputDescriptor::IsTerminatedOrAborted() {
    return Terminated.load() || Aborted.load();
}

void TOutputDescriptor::AbortChannel(const TString& message) {
    if (!Aborted.exchange(true)) {
        ActorSystem->Send(Info.InputActorId, NYql::NDq::TEvDq::TEvAbortExecution::InternalError(message).Release());
    }
}

void TOutputDescriptor::HandleUpdate(bool earlyFinished, ui64 popBytes) {
    if (!IsTerminatedOrAborted()) {
        if (popBytes) {
            std::lock_guard lock(Mutex);
            UpdatePopBytes(popBytes);
        }
        if (earlyFinished && !EarlyFinished.exchange(true)) {
            ActorSystem->Send(Info.OutputActorId, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback});
        }
    }
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
    Queue.emplace(std::move(data));
    if (NeedToNotify) {
        NeedToNotify = false;
        ActorSystem->Send(Info.InputActorId, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback});
    }
}

bool TInputBuffer::IsEarlyFinished() {
    return EarlyFinished.load();
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
        PopBytes += data.Bytes;

        ActorSystem->Send(NodeActorId, new TEvPrivate::TEvUpdateProgress(Info, IsEarlyFinished(), PopStats.Bytes));

        return true;
    }
}

void TInputBuffer::EarlyFinish() {
    EarlyFinished.store(true);
    ActorSystem->Send(NodeActorId, new TEvPrivate::TEvUpdateProgress(Info, true, PopBytes.load()));
}

ui64 TInputBuffer::GetPopBytes() {
    return PopBytes.load();
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

    if (Reconcilation.load() == 0) {
        // in Reconcilation state we do not send new messages
        std::lock_guard lock(Mutex);
        if (InflightBytes < MaxInflightBytes && Queue.size() < MaxInflightMessages) {
            if (descriptor->CheckGenMajor(GenMajor)) {
                descriptor->AddPushBytes(bytes);
                InflightBytes += data.Bytes;
                auto item = std::make_shared<TOutputItem>(std::move(data), descriptor);
                item->SeqNo = ++SeqNo;
                Queue.push_back(item);
                SendMessage(item);
                return;
            }
        }
    }

    if (!descriptor->IsTerminatedOrAborted()) {
        auto timestamp = data.Timestamp;
        descriptor->PushToWaitQueue(std::move(data));
        {
            std::lock_guard lock(Mutex);
            descriptor->WaitTimestamp = timestamp;
            WaitersQueue.push(descriptor);
        }
    }
}

void TNodeState::SendMessage(std::shared_ptr<TOutputItem> item) {
    auto ev = MakeHolder<TEvDqCompute::TEvChannelDataV2>();

    ev->Record.SetGenMajor(GenMajor);
    ev->Record.SetGenMinor(GenMinor);
    ev->Record.SetSeqNo(item->SeqNo);
    ev->Record.SetConfirmedSeqNo(ConfirmedSeqNo);

    NActors::ActorIdToProto(item->Descriptor->Info.OutputActorId, ev->Record.MutableSrcActorId());
    NActors::ActorIdToProto(item->Descriptor->Info.InputActorId, ev->Record.MutableDstActorId());
    ev->Record.SetChannelId(item->Descriptor->Info.ChannelId);

    if (!item->Data.Buffer.Empty()) {
        ev->Record.SetPayloadId(ev->AddPayload(MakeReadOnlyRope(item->Data.Buffer)));
        ev->Record.SetTransportVersion(item->Data.TransportVersion);
        ev->Record.SetValuePackerVersion(ToProto(item->Data.PackerVersion));
    }
    ev->Record.SetRows(item->Data.Rows);
    ev->Record.SetBytes(item->Data.Bytes);
    if (item->Data.Leading) {
        ev->Record.SetLeading(true);
    }
    if (item->Data.Finished) {
        ev->Record.SetFinished(true);
    }
    ev->Record.SetConfirmedPopBytes(item->Descriptor->PopBytes.load());

    ui32 flags = NActors::IEventHandle::FlagTrackDelivery;
    if (!Subscribed.exchange(true)) {
        flags |=  NActors::IEventHandle::FlagSubscribeOnSession;
    }
    ActorSystem->Send(new NActors::IEventHandle(PeerActorId, NodeActorId, ev.Release(), flags, item->SeqNo));
    item->State.store(TOutputItem::EState::Sent);
}

void TNodeState::FailInputs(const NActors::TActorId& peerActorId, ui64 peerGenMajor) {
    std::lock_guard lock(Mutex);

    if (InputBuffers.empty()) {
        return;
    }

    std::vector<TChannelInfo> failedBuffers;

    for (auto& [info, inputBuffer] : InputBuffers) {
        if (inputBuffer->PeerGenMajor) {
            if (inputBuffer->PeerActorId != peerActorId || inputBuffer->PeerGenMajor != peerGenMajor) {
                inputBuffer->Terminate();
                failedBuffers.push_back(info);
            }
        }
    }

    if (failedBuffers.size() == InputBuffers.size()) {
        InputBuffers.clear();
    } else {
        for (auto info : failedBuffers) {
            InputBuffers.erase(info);
        }
    }
}

void TNodeState::SendAck(THolder<TEvDqCompute::TEvChannelAckV2>& evAck, ui64 cookie) {
    ui32 flags = NActors::IEventHandle::FlagTrackDelivery;
    if (!Subscribed.exchange(true)) {
        flags |=  NActors::IEventHandle::FlagSubscribeOnSession;
    }

    ActorSystem->Send(new NActors::IEventHandle(PeerActorId, NodeActorId, evAck.Release(), flags, cookie));
}

void TNodeState::SendAckWithError(ui64 cookie) {
    auto evAck = MakeHolder<TEvDqCompute::TEvChannelAckV2>();

    evAck->Record.SetGenMajor(PeerGenMajor);
    evAck->Record.SetGenMinor(PeerGenMinor);
    evAck->Record.SetStatus(NYql::NDqProto::TEvChannelAckV2::ERROR);
    evAck->Record.SetSeqNo(ConfirmedSeqNo);

    SendAck(evAck, cookie);
}

void TNodeState::HandleChannelData(TEvDqCompute::TEvChannelDataV2::TPtr& ev) {

    auto& record = ev->Get()->Record;

    TChannelInfo info(record.GetChannelId(),
        NActors::ActorIdFromProto(record.GetSrcActorId()),
        NActors::ActorIdFromProto(record.GetDstActorId()));

    auto buffer = GetOrCreateInputBuffer(info, false, record.GetLeading());

    if (!buffer) {
        // do not auto create if not leading and fail sender
        SendAckWithError(ev->Cookie);
        return;
    }

    if (buffer->PeerGenMajor) {
        if (buffer->PeerActorId != PeerActorId || buffer->PeerGenMajor != PeerGenMajor) {
            buffer->Terminate();
            InputBuffers.erase(info);
            SendAckWithError(ev->Cookie);
            return;
        }
    } else {
        buffer->PeerActorId = PeerActorId;
        buffer->PeerGenMajor = PeerGenMajor;
    }

    TDataChunk data(TChunkedBuffer(), record.GetRows(), record.GetTransportVersion(),
      FromProto(record.GetValuePackerVersion()), record.GetLeading(), record.GetFinished());
    if (ev->Get()->GetPayloadCount() > 0) {
        data.Buffer = MakeChunkedBuffer(ev->Get()->GetPayload(record.GetPayloadId()));
        // data.Timestamp = TInstant::MicroSeconds(record.GetSendTime());
    }
    data.Bytes = record.GetBytes();
    Y_ENSURE(data.Bytes > data.Buffer.Size()); // record.GetBytes() == data.Buffer.Size() + const
    buffer->PushDataChunk(std::move(data));

    auto evAck = MakeHolder<TEvDqCompute::TEvChannelAckV2>();

    evAck->Record.SetGenMajor(PeerGenMajor);
    evAck->Record.SetGenMinor(PeerGenMinor);
    evAck->Record.SetStatus(NYql::NDqProto::TEvChannelAckV2::OK);
    evAck->Record.SetSeqNo(ConfirmedSeqNo);

    NActors::ActorIdToProto(info.OutputActorId, evAck->Record.MutableSrcActorId());
    NActors::ActorIdToProto(info.InputActorId, evAck->Record.MutableDstActorId());
    evAck->Record.SetChannelId(info.ChannelId);

    evAck->Record.SetEarlyFinished(buffer->IsEarlyFinished());
    evAck->Record.SetPopBytes(buffer->GetPopBytes());

    SendAck(evAck, ev->Cookie);
}

void TNodeState::Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {

    if (ev->Get()->Reason == NActors::TEvents::TEvUndelivered::ReasonActorUnknown) {
        PeerActorId = NActors::TActorId{};
        RestartSession();
        return;
    }

    switch (ev->Get()->SourceType) {
        case TEvDqCompute::TEvChannelDataV2::EventType: {
            auto seqNo = ev->Cookie;
            std::lock_guard lock(Mutex);
            GenMinor++;
            for (auto item : Queue) {
                if (item->SeqNo >= seqNo) {
                    SendMessage(item);
                }
            }
            break;
        }
        case TEvDqCompute::TEvChannelAckV2::EventType: {
            // ACKs are to be repeated periodically
            break;
        }
        case TEvDqCompute::TEvChannelUpdateV2::EventType: {
            // TBD: repeat Update from empty Input by schedule
            break;
        }
    }
}

void TNodeState::Handle(NActors::TEvents::TEvWakeup::TPtr& ev) {
    std::lock_guard lock(Mutex);
    if (Reconcilation.load() == ev->Cookie && !Queue.empty()) {
        SendMessage(Queue.front());
        ScheduleReconcilationGuard();
    }
}

void TNodeState::Handle(TEvDqCompute::TEvChannelDataV2::TPtr& ev) {

    auto& record = ev->Get()->Record;

    if (!Connected) {
        std::lock_guard lock(Mutex);
        PeerActorId = ev->Sender;
        PeerGenMajor = record.GetGenMajor();
        PeerGenMinor = 0;
        Connected = true;
    } else if (PeerActorId != ev->Sender || PeerGenMajor != record.GetGenMajor()) {
        PeerActorId = ev->Sender;
        PeerGenMajor = record.GetGenMajor();
        PeerGenMinor = 0;
        FailInputs(PeerActorId, PeerGenMajor);
        ConfirmedSeqNo = 0;
    }

    PeerGenMinor = std::max<ui64>(record.GetGenMinor(), PeerGenMinor);

    auto seqNo = record.GetSeqNo();

    if (seqNo <= ConfirmedSeqNo) {
        LOG_DEBUG_S(*ActorSystem, NKikimrServices::KQP_CHANNELS, "IGNORED DATA"
            << ", SeqNo " << seqNo << ", ConfirmedSeqNo " << ConfirmedSeqNo
            << ", NodeService " << NodeActorId << " from peer " << PeerActorId
        );
        return;
    }

    switch (seqNo - ConfirmedSeqNo) {
        case 1: {
            break;
        }
        case 2: {
            // allow 1 out of order message
            LOG_DEBUG_S(*ActorSystem, NKikimrServices::KQP_CHANNELS, "OUT OF ORDER DATA"
                << ", SeqNo " << seqNo << ", ConfirmedSeqNo " << ConfirmedSeqNo
                << ", NodeService " << NodeActorId << " from peer " << PeerActorId
            );
            OutOfOrderMessage = ev.Release();
            return;
        }
        default: {
            LOG_DEBUG_S(*ActorSystem, NKikimrServices::KQP_CHANNELS, "ASK RESEND DATA"
                << ", SeqNo " << seqNo << ", ConfirmedSeqNo " << ConfirmedSeqNo
                << ", NodeService " << NodeActorId << " from peer " << PeerActorId
            );
            auto evAck = MakeHolder<TEvDqCompute::TEvChannelAckV2>();

            evAck->Record.SetGenMajor(PeerGenMajor);
            evAck->Record.SetGenMinor(PeerGenMinor);
            evAck->Record.SetStatus(NYql::NDqProto::TEvChannelAckV2::RESEND);
            evAck->Record.SetSeqNo(ConfirmedSeqNo + 1);

            ui32 flags = NActors::IEventHandle::FlagTrackDelivery;
            if (!Subscribed.exchange(true)) {
                flags |=  NActors::IEventHandle::FlagSubscribeOnSession;
            }

            ActorSystem->Send(new NActors::IEventHandle(PeerActorId, NodeActorId, evAck.Release(), flags, ev->Cookie));
            return;
        }
    }

    // happy path

    ConfirmedSeqNo++;
    HandleChannelData(ev);

    if (OutOfOrderMessage) {
        auto& record = OutOfOrderMessage->Get()->Record;

        if (record.GetSeqNo() == ConfirmedSeqNo + 1) {
            ConfirmedSeqNo++;
            HandleChannelData(OutOfOrderMessage);
        }

        OutOfOrderMessage.Reset();
    }
}

void TNodeState::SendFromWaiters(ui64 deltaBytes) {
    Y_ENSURE(InflightBytes >= deltaBytes);

    while (InflightBytes - deltaBytes < MaxInflightBytes && Queue.size() < MaxInflightMessages && !WaitersQueue.empty()) {
        auto waiter = WaitersQueue.top();
        WaitersQueue.pop();
        Y_ENSURE(!waiter->WaitQueue.empty());

        if (waiter->IsTerminatedOrAborted()) {
            continue;
        }

        auto& data = waiter->WaitQueue.front();
        InflightBytes += data.Bytes;
        waiter->CheckGenMajor(GenMajor);
        auto item = std::make_shared<TOutputItem>(std::move(data), waiter);
        item->SeqNo = ++SeqNo;
        Queue.push_back(item);

        waiter->WaitQueue.pop();
        if (!waiter->WaitQueue.empty()) {
            waiter->WaitTimestamp = waiter->WaitQueue.front().Timestamp;
            WaitersQueue.push(waiter);
        }

        SendMessage(item);
    }

    InflightBytes -= deltaBytes;
}

void TNodeState::RestartSession() {
    std::lock_guard lock(Mutex);
    while (!Queue.empty()) {
        auto& item = Queue.front();
        item->Descriptor->AbortChannel("By Reconcilation");
    }
    GenMajor++;
    SeqNo = 0;
    Reconcilation.store(0);
    SendFromWaiters(InflightBytes);
}

void TNodeState::Handle(TEvDqCompute::TEvChannelAckV2::TPtr& ev) {

    auto& record = ev->Get()->Record;

    if (record.GetGenMajor() != GenMajor) {
        LOG_WARN_S(*ActorSystem, NKikimrServices::KQP_CHANNELS, "ACK IGNORED GenMajor "
            << GenMajor << ", record.GetGenMajor() " << record.GetGenMajor()
            << ", NodeService " << NodeActorId << " from peer " << ev->Sender
        );
        return;
    }

    TChannelInfo info(record.GetChannelId(), NActors::ActorIdFromProto(record.GetSrcActorId()), NActors::ActorIdFromProto(record.GetDstActorId()));
    ui64 deltaBytes = 0;
    std::lock_guard lock(Mutex);

    auto status = record.GetStatus();
    auto seqNo = record.GetSeqNo();
    if (seqNo <= ConfirmedSeqNo) {
        LOG_WARN_S(*ActorSystem, NKikimrServices::KQP_CHANNELS, "ACK IGNORED/O ConfirmedSeqNo "
            << ConfirmedSeqNo << ", record.GetSeqNo() " << record.GetSeqNo()
            << ", NodeService " << NodeActorId << " from peer " << ev->Sender
        );
        return;
    }

    while (!Queue.empty()) {
        auto& item = Queue.front();
        if (item->SeqNo >= seqNo) {
            break;
        }
        if (item->Descriptor->GenMajor != GenMajor) {
            item->Descriptor->AbortChannel(TStringBuilder() << "By Outdated GenMajor1 " << item->Descriptor->GenMajor << " vs " << GenMajor);
        }
        deltaBytes += item->Data.Bytes;
        Queue.pop_front();
    }

    if (Queue.empty()) {
        RestartSession();
        return;
    }

    auto& item = Queue.front();
    if (item->SeqNo != seqNo) {
        LOG_DEBUG_S(*ActorSystem, NKikimrServices::KQP_CHANNELS, "RESTART SESSION"
            << ", SeqNo " << seqNo << ", ConfirmedSeqNo " << ConfirmedSeqNo
            << ", NodeService " << NodeActorId << " from peer " << PeerActorId
        );
        RestartSession();
        return;
    }

    if (status == NYql::NDqProto::TEvChannelAckV2::RESEND) {
        // if we're reconcilating, ignore next RESENDs
        if (record.GetGenMinor() == GenMinor) {
            LOG_DEBUG_S(*ActorSystem, NKikimrServices::KQP_CHANNELS, "DO RESEND"
                << ", SeqNo " << seqNo << ", ConfirmedSeqNo " << ConfirmedSeqNo
                << ", NodeService " << NodeActorId << " from peer " << PeerActorId
            );
            GenMinor++;
            Reconcilation.store(GenMinor);
            SendMessage(item);
            ScheduleReconcilationGuard();
            if (deltaBytes) {
                // return to inflight possible confirmed prev msgs
                Y_ENSURE(InflightBytes >= deltaBytes);
                InflightBytes -= deltaBytes;
            }
        }
        return;
    }

    if (!item->Descriptor->IsTerminatedOrAborted()) {
        if (item->Descriptor->GenMajor != GenMajor) {
            item->Descriptor->AbortChannel(TStringBuilder() << "By Outdated GenMajor2 " << item->Descriptor->GenMajor << " vs " << GenMajor);
        } else if (status == NYql::NDqProto::TEvChannelAckV2::ERROR) {
            item->Descriptor->AbortChannel("By Remote Side");
        } else {
            auto earlyFinished = record.GetEarlyFinished();
            auto popBytes = record.GetPopBytes();
            if (earlyFinished || popBytes) {
                item->Descriptor->HandleUpdate(earlyFinished, popBytes);
            }
        }
    }

    deltaBytes += item->Data.Bytes;
    Queue.pop_front();

    if (Reconcilation.exchange(0) > 0) {
        if (!Queue.empty()) {
            LOG_DEBUG_S(*ActorSystem, NKikimrServices::KQP_CHANNELS, "DO REPEAT " << Queue.front()->SeqNo << "-" << Queue.back()->SeqNo
                << ", ConfirmedSeqNo " << ConfirmedSeqNo
                << ", NodeService " << NodeActorId << " from peer " << PeerActorId
            );
            for (auto item : Queue) {
                SendMessage(item);
            }
        }
    }

    SendFromWaiters(deltaBytes);
}

void TNodeState::Handle(TEvDqCompute::TEvChannelUpdateV2::TPtr& ev) {

    auto& record = ev->Get()->Record;

    if (record.GetGenMajor() != GenMajor) {
        LOG_WARN_S(*ActorSystem, NKikimrServices::KQP_CHANNELS, "UPDATE IGNORED GenMajor "
            << GenMajor << ", record.GetGenMajor() " << record.GetGenMajor()
            << ", NodeService " << NodeActorId << " from peer " << ev->Sender
        );
        return;
    }

    // GenMinor ???
    // ConfirmedSeqNo ???

    TChannelInfo info(record.GetChannelId(),
        NActors::ActorIdFromProto(record.GetSrcActorId()),
        NActors::ActorIdFromProto(record.GetDstActorId()));

    std::lock_guard lock(Mutex);

    auto it = OutputDescriptors.find(info);
    if (it == OutputDescriptors.end()) {
        LOG_WARN_S(*ActorSystem, NKikimrServices::KQP_CHANNELS, "UPDATE IGNORED not found desc "
            << info.ChannelId << ", " << info.OutputActorId << ", " << info.InputActorId
        );
        return;
    }

    auto descriptor = it->second;
    if (!descriptor->IsTerminatedOrAborted()) {
        if (descriptor->GenMajor != GenMajor) {
            descriptor->AbortChannel(TStringBuilder() << "By Outdated GenMajor2 " << descriptor->GenMajor << " vs " << GenMajor);
        } else {
            auto earlyFinished = record.GetEarlyFinished();
            auto popBytes = record.GetPopBytes();
            if (earlyFinished || popBytes) {
                descriptor->HandleUpdate(earlyFinished, popBytes);
            }
        }
    }
}

void TNodeState::Handle(TEvPrivate::TEvUpdateProgress::TPtr& ev) {
    auto evUpdate = MakeHolder<TEvDqCompute::TEvChannelUpdateV2>();

    evUpdate->Record.SetGenMajor(PeerGenMajor);
    evUpdate->Record.SetGenMinor(PeerGenMinor);
    // evUpdate->Record.SetSeqNo(ConfirmedSeqNo);

    NActors::ActorIdToProto(ev->Get()->Info.OutputActorId, evUpdate->Record.MutableSrcActorId());
    NActors::ActorIdToProto(ev->Get()->Info.InputActorId, evUpdate->Record.MutableDstActorId());
    evUpdate->Record.SetChannelId(ev->Get()->Info.ChannelId);

    evUpdate->Record.SetEarlyFinished(ev->Get()->EarlyFinished);
    evUpdate->Record.SetPopBytes(ev->Get()->PopBytes);

    ui32 flags = NActors::IEventHandle::FlagTrackDelivery;
    if (!Subscribed.exchange(true)) {
        flags |=  NActors::IEventHandle::FlagSubscribeOnSession;
    }

    ActorSystem->Send(new NActors::IEventHandle(PeerActorId, NodeActorId, evUpdate.Release(), flags));
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

std::shared_ptr<TInputBuffer> TNodeState::GetOrCreateInputBuffer(const TChannelInfo& info, bool binded, bool leading) {
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

    if (!binded && !leading) {
        return {};
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
    std::lock_guard lock(Mutex);
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

void TNodeState::ScheduleReconcilationGuard() {
    ActorSystem->Schedule(TDuration::MilliSeconds(100), new NActors::IEventHandle(NodeActorId, NodeActorId, new NActors::TEvents::TEvWakeup(), 0, GenMinor));
}

TString TNodeState::GetDebugInfo() {
    std::lock_guard lock(Mutex);
    TStringBuilder builder;

    builder << "TNodeState, NodeId=" << NodeActorId.NodeId() << ", Peer NodeId=" << PeerActorId.NodeId()
        << ", SeqNo=" << SeqNo << ", ConfirmedSeqNo=" << ConfirmedSeqNo << ", InflightBytes=" << InflightBytes
        << ", Reconcilation=" << Reconcilation.load()
        << Endl;

    for (auto& [info, descriptor] : OutputDescriptors) {
        builder << "  Output " << info.ChannelId << ", FL=" << (ui32)descriptor->FillLevel
            << ", IF:" << descriptor->IsFlushed() << ", TA=" << descriptor->IsTerminatedOrAborted()
            << ", EF: " << descriptor->EarlyFinished.load()
            << ", PP:" << descriptor->PushBytes.load() << ':' << descriptor->PopBytes.load() << Endl;
    }
    for (auto& [info, inputBuffer] : InputBuffers) {
        builder << "  Input " << info.ChannelId << ", Empty=" << inputBuffer->IsEmpty()
            << ", Pop=" << inputBuffer->GetPopBytes() << Endl;
    }

    std::unordered_map<TChannelInfo, std::shared_ptr<TOutputDescriptor>> OutputDescriptors;

    return builder;
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

void TDebugNodeState::SetLossProbability(double dataLossProbability, ui64 dataLossCount, double ackLossProbability, ui64 ackLossCount) {
    DataLossProbability.store(dataLossProbability);
    DataLossCount.store(dataLossCount);
    AckLossProbability.store(ackLossProbability);
    AckLossCount.store(ackLossCount);
}

bool TDebugNodeState::ShouldLooseData() {
    auto result = RandomNumber<double>() < DataLossProbability.load();
    if (auto count = DataLossCount.load()) {
        count--;
        DataLossCount.store(count);
        if (count == 0) {
            DataLossProbability.store(0.0);
        }
    }
    return result;
}

bool TDebugNodeState::ShouldLooseAck() {
    auto result = RandomNumber<double>() < AckLossProbability.load();
    if (auto count = AckLossCount.load()) {
        count--;
        AckLossCount.store(count);
        if (count == 0) {
            AckLossProbability.store(0.0);
        }
    }
    return result;
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
    return std::make_shared<TInputBufferProxy>(nodeState, nodeState->GetOrCreateInputBuffer(info, true, true));
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

TString TDqChannelService::GetDebugInfo() {
    TStringBuilder builder;

    builder << "TDqChannelService NodeId = " << NodeId << Endl;

    for (auto& [nodeId, nodeState] : NodeStates) {
        builder << nodeState->GetDebugInfo() << Endl;
    }

    return builder;
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

bool TFastDqOutputChannel::Bind(NActors::TActorId outputActorId, NActors::TActorId inputActorId) {
    if (auto service = Service.lock()) {
        if (inputActorId.NodeId() == service->NodeId) {
            Serializer = ConvertToLocalSerializer(std::move(Serializer));
        }
        auto buffer = service->GetOutputBuffer(TChannelInfo(Desc.ChannelId, outputActorId, inputActorId));
        if (Aggregator) {
            buffer->SetFillAggregator(Aggregator);
        }
        Serializer->Buffer = buffer;
        Service.reset();
        return true;
    }
    return false;
}

bool TFastDqInputChannel::Bind(NActors::TActorId outputActorId, NActors::TActorId inputActorId) {
    if (auto service = Service.lock()) {
        Buffer = service->GetInputBuffer(TChannelInfo(Desc.ChannelId, outputActorId, inputActorId));
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
