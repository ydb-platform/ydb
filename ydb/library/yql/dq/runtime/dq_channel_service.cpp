#include <queue>
#include <mutex>

#include "dq_arrow_helpers.h"
#include "dq_channel_service_impl.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/dq/actors/dq.h>

#include <util/random/random.h>

#define LOG_T(stream) LOG_TRACE_S(*ActorSystem, NKikimrServices::KQP_CHANNELS, stream)
#define LOG_D(stream) LOG_DEBUG_S(*ActorSystem, NKikimrServices::KQP_CHANNELS, stream)
#define LOG_I(stream) LOG_INFO_S(*ActorSystem, NKikimrServices::KQP_CHANNELS, stream)
#define LOG_N(stream) LOG_NOTICE_S(*ActorSystem, NKikimrServices::KQP_CHANNELS, stream)
#define LOG_W(stream) LOG_WARN_S(*ActorSystem, NKikimrServices::KQP_CHANNELS, stream)
#define LOG_E(stream) LOG_ERROR_S(*ActorSystem, NKikimrServices::KQP_CHANNELS, stream)

#define LOGA_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, stream)
#define LOGA_N(stream) LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, stream)
#define LOGA_E(stream) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, stream)

namespace NYql::NDq {

template<typename T>
void AppendNumber(TChunkedBuffer& rope, T data) {
    static_assert(std::is_integral_v<T>);
    rope.Append(TString(reinterpret_cast<const char*>(&data), sizeof(T)));
}

template<typename T>
T ReadNumber(TStringBuf& src) {
    static_assert(std::is_integral_v<T>);
    YQL_ENSURE(src.size() >= sizeof(T), "Premature end of spilled data");
    T result = ReadUnaligned<T>(src.data());
    src.Skip(sizeof(T));
    return result;
}

void BufferToData(TDataChunk& data, TBuffer&& buffer) {
    auto sharedBuffer = std::make_shared<TBuffer>(std::move(buffer));

    TStringBuf source(sharedBuffer->Data(), sharedBuffer->Size());

    data.Bytes = ReadNumber<ui32>(source);
    data.Rows = ReadNumber<ui32>(source);
    data.TransportVersion = static_cast<NDqProto::EDataTransportVersion>(ReadNumber<ui32>(source));
    data.PackerVersion = static_cast<NKikimr::NMiniKQL::EValuePackerVersion>(ReadNumber<ui32>(source));
    data.Leading = ReadNumber<bool>(source);
    data.Finished = ReadNumber<bool>(source);
    data.Timestamp = TInstant::MicroSeconds(ReadNumber<ui64>(source));

    ui64 size = ReadNumber<ui64>(source);
    YQL_ENSURE(size == source.size(), "Spilled data is corrupted");
    data.Buffer = TChunkedBuffer(source, sharedBuffer);
}

TChunkedBuffer DataToBuffer(TDataChunk&& data) {
    TChunkedBuffer result;

    AppendNumber<ui32>(result, data.Bytes);
    AppendNumber<ui32>(result, data.Rows);
    AppendNumber<ui32>(result, static_cast<ui32>(data.TransportVersion));
    AppendNumber<ui32>(result, static_cast<ui32>(data.PackerVersion));
    AppendNumber<bool>(result, data.Leading);
    AppendNumber<bool>(result, data.Finished);
    AppendNumber<ui64>(result, data.Timestamp.MicroSeconds());

    AppendNumber<ui64>(result, data.Buffer.Size());
    result.Append(std::move(data.Buffer));

    return result;
}

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
    if (!FinishPushed && !Finished.load()) {
        if (data.Finished) {
            FinishPushed = true;
        }

        (*Registry->LocalBufferChunks)++;
        *Registry->LocalBufferBytes += data.Bytes;
        PushDataChunk(std::move(data));
    }
}

void TLocalBuffer::PushDataChunk(TDataChunk&& data) {
    std::lock_guard lock(Mutex);

    if (PushStats.CollectBasic()) {
        PushStats.Chunks++;
        PushStats.Rows += data.Rows;
        PushStats.Bytes += data.Bytes;
        PushStats.Resume();
    }

    EDqFillLevel fillLevel = FillLevel;

    if (Storage) {
        if ((SpilledBytes.load() > 0) || InflightBytes.load() >= MaxInflightBytes) {
            // if there is something spilled and not loaded yet, continue to spill to avoid reordering
            SpilledChunkBytes.push(data.Bytes);
            SpilledBytes += data.Bytes;
            Storage->Put(++HeadBlobId, DataToBuffer(std::move(data)));
            // and always report soft/hard limit even if we have small inflight
            fillLevel = Storage->IsFull() ? EDqFillLevel::HardLimit : EDqFillLevel::SoftLimit;
        } else {
            InflightBytes += data.Bytes;
            Queue.push(std::move(data));
            fillLevel = InflightBytes.load() >= MaxInflightBytes ? EDqFillLevel::SoftLimit : EDqFillLevel::NoLimit;
        }
    } else {
        InflightBytes += data.Bytes;
        Queue.push(std::move(data));
        fillLevel = InflightBytes.load() >= MaxInflightBytes ? EDqFillLevel::HardLimit : EDqFillLevel::NoLimit;
    }

    if (FillLevel != fillLevel) {
        if (FillLevel != EDqFillLevel::NoLimit) {
            PopStats.TryPause();
        }
        if (Aggregator) {
            Aggregator->UpdateCount(FillLevel, fillLevel);
        }
        FillLevel = fillLevel;
        NeedToNotifyOutput.store(true);
    }

    NotifyInput(false);
}

bool TLocalBuffer::IsFinished() {
    auto result = Finished.load();
    if (!result) {
        NeedToNotifyInput.store(true);
        NeedToNotifyOutput.store(true);
    }
    return result;
}

bool TLocalBuffer::IsEarlyFinished() {
    return EarlyFinished.load();
}

bool TLocalBuffer::IsEmpty() {
    std::lock_guard lock(Mutex);
    auto result = Queue.empty();
    if (result) {
        NeedToNotifyInput.store(true);
    }
    return result;
}

bool TLocalBuffer::Pop(TDataChunk& data) {
    std::lock_guard lock(Mutex);

    if (Queue.empty()) {
        PushStats.TryPause();
        NeedToNotifyInput.store(true);
        return false;
    }

    data = std::move(Queue.front());
    Queue.pop();

    if (PopStats.CollectBasic()) {
        PopStats.Chunks++;
        PopStats.Rows += data.Rows;
        PopStats.Bytes += data.Bytes;
        PopStats.Resume();
    }
    *Registry->LocalBufferLatency += (TInstant::Now() - data.Timestamp).MicroSeconds();

    EDqFillLevel fillLevel = FillLevel;

    Y_ENSURE(InflightBytes.load() >= data.Bytes);
    InflightBytes -= data.Bytes;

    if (data.Finished) {
        if (!Finished.exchange(true)) {
            FinishTime = TInstant::Now();
        }
        fillLevel = EDqFillLevel::NoLimit;
    } else {
        while (InflightBytes.load() < MinInflightBytes && !SpilledChunkBytes.empty()) {
            auto bytes = SpilledChunkBytes.front();
            SpilledChunkBytes.pop();
            InflightBytes += bytes;
            Y_ENSURE(TailBlobId < HeadBlobId);

            TLoadingInfo info(++TailBlobId, bytes);
            info.Loaded = Storage->Get(info.BlobId, info.Buffer);
            if (LoadingQueue.empty() && info.Loaded) {
                TDataChunk data;
                BufferToData(data, std::move(info.Buffer));
                Queue.emplace(std::move(data));
                SpilledBytes -= bytes;
            } else {
                LoadingQueue.emplace(std::move(info));
            }
        }

        if (SpilledBytes.load() == 0 && InflightBytes.load() < MinInflightBytes) {
            fillLevel = EDqFillLevel::NoLimit;
        } else if (Storage) {
            fillLevel = Storage->IsFull() ? EDqFillLevel::HardLimit : EDqFillLevel::SoftLimit;
        } else if (InflightBytes.load() >= MaxInflightBytes) {
            fillLevel = EDqFillLevel::HardLimit;
        }
    }

    if (FillLevel != fillLevel) {
        if (Aggregator) {
            Aggregator->UpdateCount(FillLevel, fillLevel);
        }
        FillLevel = fillLevel;
        NotifyOutput(Queue.empty() || Finished.load());
    } else if (Queue.empty() || Finished.load())  {
        NotifyOutput(true);
    }

    return true;
}

void TLocalBuffer::EarlyFinish() {
    if (!EarlyFinished.exchange(true)) {
        if (OutputBound.load()) {
            if (!Finished.exchange(true)) {
                NotifyInput(true);
                NotifyOutput(true);
                FinishTime = TInstant::Now();

                std::lock_guard lock(Mutex);
                if (FillLevel != EDqFillLevel::NoLimit) {
                    if (Aggregator) {
                        Aggregator->UpdateCount(FillLevel, EDqFillLevel::NoLimit);
                    }
                    FillLevel = EDqFillLevel::NoLimit;
                }
            }
        }
    }
}

void TLocalBuffer::StorageWakeupHandler() {
    std::lock_guard lock(Mutex);

    if (FillLevel == EDqFillLevel::HardLimit && !Storage->IsFull()) {
        if (Aggregator) {
            Aggregator->UpdateCount(EDqFillLevel::HardLimit, EDqFillLevel::SoftLimit);
        }
        FillLevel = EDqFillLevel::SoftLimit;
        NotifyOutput(false);
    }

    ui32 chunksLoaded = 0;

    while (!LoadingQueue.empty()) {
        auto& info = LoadingQueue.front();

        if (!info.Loaded) {
            info.Loaded = Storage->Get(info.BlobId, info.Buffer);
        }
        if (!info.Loaded) {
            break;
        }

        TDataChunk data;
        BufferToData(data, std::move(info.Buffer));
        Queue.emplace(std::move(data));
        SpilledBytes -= info.Bytes;

        LoadingQueue.pop();
        chunksLoaded++;
    }

    if (chunksLoaded) {
        NotifyInput(false);
    }
}

void TLocalBuffer::BindInput() {
    if (!InputBound.exchange(true)) {
        NotifyOutput(false);
    }
}

void TLocalBuffer::BindOutput() {
    if (!OutputBound.exchange(true)) {
        if (EarlyFinished.load()) {
            if (!Finished.exchange(true)) {
                NotifyInput(true);
                NotifyOutput(true);
                FinishTime = TInstant::Now();
            }
        }
    }
}

void TLocalBuffer::BindStorage(std::shared_ptr<TLocalBuffer>& self, IDqChannelStorage::TPtr storage) {
    storage->SetWakeUpCallback([weakSelf=std::weak_ptr<TLocalBuffer>(self)]() {
        if (auto sharedSelf = weakSelf.lock(); sharedSelf) {
            sharedSelf->StorageWakeupHandler();
        }
    });
    Storage = std::move(storage);
}

void TLocalBuffer::NotifyInput(bool force) {
    if (NeedToNotifyInput.exchange(false) || force) {
        NActors::TActivationContext::Send<NActors::ESendingType::Tail>(
            new NActors::IEventHandle(Info.InputActorId, NActors::TActorId{}, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback})
        );
        LastInputNotificationTime = TInstant::Now();
    }
}

void TLocalBuffer::NotifyOutput(bool force) {
    if (NeedToNotifyOutput.exchange(false) || force) {
        NActors::TActivationContext::Send<NActors::ESendingType::Tail>(
            new NActors::IEventHandle(Info.OutputActorId, NActors::TActorId{}, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback})
        );
        LastOutputNotificationTime = TInstant::Now();
    }
}

void TLocalBuffer::ExportPushStats(TDqAsyncStats& stats) {
    PushStats.Export(stats);
}

void TLocalBuffer::ExportPopStats(TDqAsyncStats& stats) {
    PopStats.Export(stats);
}

void TOutputDescriptor::PushDataChunk(TDataChunk&& data, TNodeState* nodeState, std::shared_ptr<TOutputDescriptor> self) {

    if (PushStats.CollectBasic()) {
        PushStats.Chunks++;
        PushStats.Rows += data.Rows;
        PushStats.Bytes += data.Bytes;
        PushStats.Resume();
    }

    std::lock_guard lock(FlowControlMutex);

    if (FinishPushed.load()) {
        return;
    }

    if (data.Finished) {
        FinishPushed.store(true);
    }

    auto fillLevel = FillLevel;

    bool spilled = false;

    if (Storage) {
        if ((SpilledBytes.load() > 0) || (PushBytes.load() >= RemotePopBytes.load() + MaxInflightBytes)) {
            if (SpilledChunkBytes.empty()) {
                LOG_D("NodeId=" << nodeState->NodeId << ", ChannelId=" << Info.ChannelId << ", START SPILLING, PushBytes=" << PushBytes.load()
                    << ", PopBytes=" << RemotePopBytes.load() << ", MaxInflightBytes=" << MaxInflightBytes
                    << ", SpilledBytes=" << SpilledBytes.load() << ", data.Bytes=" << data.Bytes
                );
            }
            SpilledChunkBytes.push(data.Bytes);
            SpilledBytes += data.Bytes;
            Storage->Put(++HeadBlobId, DataToBuffer(std::move(data)));
            spilled = true;
            fillLevel = Storage->IsFull() ? EDqFillLevel::HardLimit : EDqFillLevel::SoftLimit;
        } else {
            PushBytes += data.Bytes;
        }
    } else {
        PushBytes += data.Bytes;
        if (PushBytes.load() >= RemotePopBytes.load() + MaxInflightBytes) {
            fillLevel = EDqFillLevel::HardLimit;
        }
    }

    if (FillLevel != fillLevel) {
        if (Aggregator) {
            Aggregator->UpdateCount(FillLevel, fillLevel);
        }
        FillLevel = fillLevel;
        NeedToNotifyOutput.store(true);
    }

    (*OutputBufferChunks)++;
    *OutputBufferBytes += data.Bytes;

    if (!spilled) {
        nodeState->PushDataChunk(std::move(data), self);
    }
}

void TOutputDescriptor::AddPopChunk(ui64 bytes, ui64 rows) {
    PopStats.Bytes += bytes;
    PopStats.Chunks++;
    PopStats.Rows += rows;
}

void TOutputDescriptor::UpdatePopBytes(ui64 bytes, TNodeState* nodeState, std::shared_ptr<TOutputDescriptor> self) {
    if (bytes <= RemotePopBytes.load()) {
        return;
    }

    {
        std::lock_guard lock(FlowControlMutex);
        if (bytes <= RemotePopBytes.load()) {
            return;
        }
        RemotePopBytes.store(bytes);

        while (PushBytes.load() < RemotePopBytes.load() + MaxInflightBytes && !SpilledChunkBytes.empty()) {
            auto bytes = SpilledChunkBytes.front();
            SpilledChunkBytes.pop();
            Y_ENSURE(TailBlobId < HeadBlobId);

            PushBytes += bytes;
            TLoadingInfo info(++TailBlobId, bytes);
            info.Loaded = Storage->Get(info.BlobId, info.Buffer);
            if (LoadingQueue.empty() && info.Loaded) {
                TDataChunk data;
                BufferToData(data, std::move(info.Buffer));
                nodeState->PushDataChunk(std::move(data), self);
                SpilledBytes -= bytes;
            } else {
                LoadingQueue.emplace(std::move(info));
            }
        }

        EDqFillLevel fillLevel = FillLevel;

        if (SpilledBytes.load() == 0 && PushBytes.load() < RemotePopBytes.load() + MinInflightBytes) {
            fillLevel = EDqFillLevel::NoLimit;
        } else if (Storage) {
            fillLevel = Storage->IsFull() ? EDqFillLevel::HardLimit : EDqFillLevel::SoftLimit;
        } else if (PushBytes.load() >= RemotePopBytes.load() + MaxInflightBytes) {
            fillLevel = EDqFillLevel::HardLimit;
        }

        if (FillLevel == fillLevel) {
            if (PushBytes.load() > RemotePopBytes.load()) {
                return;
            }
        } else {
            if (Aggregator) {
                Aggregator->UpdateCount(FillLevel, fillLevel);
            }
            FillLevel = fillLevel;
        }
    }

    auto flushed = PushBytes.load() == RemotePopBytes.load();

    if (flushed && FinishPushed.load()) {
        Finished.store(true);
    }

    if (NeedToNotifyOutput.exchange(false) || flushed) {
        ActorSystem->Send(Info.OutputActorId, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback});
    }
}

bool TOutputDescriptor::CheckGenMajor(ui64 genMajor, const TString& errorMessage) {
    auto prevGenMajor = GenMajor.exchange(genMajor);
    if (prevGenMajor && prevGenMajor != genMajor) {
        TStringBuilder builder;
        builder << "Descriptor.GenMajor=" << prevGenMajor << ", expected GenMajor=" << genMajor << ' ' << errorMessage;
        TString message = builder;
        LOG_W(message);
        AbortChannel(message);
        return false;
    }
    return true;
}

bool TOutputDescriptor::IsFinished() {
    auto result = Finished.load();
    if (!result) {
        NeedToNotifyOutput.store(true);
    }
    return result;
}

bool TOutputDescriptor::IsEarlyFinished() {
    return EarlyFinished.load();
}

void TOutputDescriptor::Terminate() {
    Terminated.store(true);
}

bool TOutputDescriptor::IsTerminatedOrAborted() {
    return Terminated.load() || Aborted.load();
}

void TOutputDescriptor::AbortChannel(const TString& message) {
    if (!Aborted.exchange(true)) {
        ActorSystem->Send(Info.InputActorId, NYql::NDq::TEvDq::TEvAbortExecution::InternalError(
            TStringBuilder() << "Channel: " << Info.ChannelId
            << ", SrcStageId: " << Info.SrcStageId << ", DstStageId: " << Info.DstStageId
            << ", " << message
        ).Release());
    }
}

void TOutputDescriptor::HandleUpdate(bool earlyFinish, ui64 popBytes, TNodeState* nodeState, std::shared_ptr<TOutputDescriptor> self) {
    if (!IsTerminatedOrAborted()) {
        if (earlyFinish) {
            EarlyFinished.store(true);
            PushDataChunk(TDataChunk(false, true), nodeState, self);
        }
        if (popBytes) {
            UpdatePopBytes(popBytes, nodeState, self);
        }
    }
}

void TOutputDescriptor::BindStorage(std::shared_ptr<TOutputDescriptor>& self, std::shared_ptr<TNodeState>& nodeState, IDqChannelStorage::TPtr storage) {
    storage->SetWakeUpCallback([weakSelf=std::weak_ptr<TOutputDescriptor>(self), weakNodeState=std::weak_ptr<TNodeState>(nodeState)]() {
        if (auto sharedSelf = weakSelf.lock(); sharedSelf) {
            if (auto sharedNodeState = weakNodeState.lock(); sharedNodeState) {
                sharedSelf->StorageWakeupHandler(sharedNodeState.get(), sharedSelf);
            }
        }
    });
    Storage = std::move(storage);
}

void TOutputDescriptor::StorageWakeupHandler(TNodeState* nodeState, std::shared_ptr<TOutputDescriptor> self) {
    std::lock_guard lock(FlowControlMutex);

    if (FillLevel == EDqFillLevel::HardLimit && !Storage->IsFull()) {
        if (Aggregator) {
            Aggregator->UpdateCount(EDqFillLevel::HardLimit, EDqFillLevel::SoftLimit);
        }
        FillLevel = EDqFillLevel::SoftLimit;
        if (NeedToNotifyOutput.exchange(false)) {
            ActorSystem->Send(Info.OutputActorId, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback});
        }
    }

    while (!LoadingQueue.empty()) {
        auto& info = LoadingQueue.front();

        if (!info.Loaded) {
            info.Loaded = Storage->Get(info.BlobId, info.Buffer);
        }
        if (!info.Loaded) {
            break;
        }

        TDataChunk data;
        BufferToData(data, std::move(info.Buffer));
        nodeState->PushDataChunk(std::move(data), self);
        SpilledBytes -= info.Bytes;

        LoadingQueue.pop();
    }
}

TOutputBuffer::~TOutputBuffer() {
    NodeState->TerminateOutputDescriptor(Descriptor);
}

EDqFillLevel TOutputBuffer::GetFillLevel() const {
    std::lock_guard lock(Descriptor->FlowControlMutex);
    return Descriptor->FillLevel;
}

void TOutputBuffer::SetFillAggregator(std::shared_ptr<TDqFillAggregator> aggregator) {
    std::lock_guard lock(Descriptor->FlowControlMutex);
    Descriptor->Aggregator = aggregator;
    Descriptor->Aggregator->AddCount(Descriptor->FillLevel);
}

void TOutputBuffer::Push(TDataChunk&& data) {
    if (!Descriptor->IsTerminatedOrAborted() && !Descriptor->IsFinished()) {
        Descriptor->PushDataChunk(std::move(data), NodeState.get(), Descriptor);
    }
}

bool TOutputBuffer::IsFinished() {
    return Descriptor->IsFinished();
}

bool TOutputBuffer::IsEarlyFinished() {
    return Descriptor->IsEarlyFinished();
}

bool TOutputBuffer::IsEmpty() {
    return false;
}

bool TOutputBuffer::Pop(TDataChunk&) {
    Y_ENSURE(false, "TOutputBuffer::Pop not allowed");
    return false;
}

void TOutputBuffer::EarlyFinish() {
    Y_ENSURE(false, "TOutputBuffer::EarlyFinish not allowed");
}

void TOutputBuffer::ExportPushStats(TDqAsyncStats& stats) {
    Descriptor->PushStats.Export(stats);
}

void TOutputBuffer::ExportPopStats(TDqAsyncStats& stats) {
    Descriptor->PopStats.Export(stats);
}

bool TInputDescriptor::IsEmpty() {
    std::lock_guard lock(QueueMutex);
    auto result = Queue.empty();
    if (result) {
        NeedToNotifyInput.store(true);
    }
    return result;
}

bool TInputDescriptor::PushDataChunk(TDataChunk&& data) {

    PushStats.Chunks++;
    PushStats.Bytes += data.Bytes;
    PushStats.Rows += data.Rows;

    (*InputBufferChunks)++;
    *InputBufferBytes += data.Bytes;

    std::lock_guard lock(QueueMutex);

    if (FinishPushed.load()) {
        return false;
    }

    if (data.Finished) {
        FinishPushed.store(true);
        if (EarlyFinished.load()) {
            std::queue<TInputItem> tmpQueue;
            Queue.swap(tmpQueue);
            QueueSize.store(0);
            PopStats.Bytes += QueueBytes.exchange(0) + data.Bytes;
            Finished.store(true);
            ActorSystem->Send(Info.InputActorId, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback});
            return true;
        }
    }

    QueueBytes += data.Bytes;
    QueueSize++;
    Queue.emplace(std::move(data));
    if (NeedToNotifyInput.exchange(false)) {
        ActorSystem->Send(Info.InputActorId, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback});
    }

    return false;
}

bool TInputDescriptor::IsFinished() {
    return Finished.load();
}

bool TInputDescriptor::IsEarlyFinished() {
    return EarlyFinished.load();
}

bool TInputDescriptor::PopDataChunk(TDataChunk& data) {
    std::lock_guard lock(QueueMutex);
    if (Queue.empty()) {
        NeedToNotifyInput.store(true);
        return false;
    } else {
        data = std::move(Queue.front().Data);
        PopStats.Chunks++;
        PopStats.Rows += data.Rows;
        PopStats.Bytes += data.Bytes;
        Queue.pop();
        QueueSize--;
        QueueBytes -= data.Bytes;
        if (data.Finished) {
            Finished.store(true);
        }
        return true;
    }
}

bool TInputDescriptor::EarlyFinish() {
    if (!EarlyFinished.exchange(true)) {
        std::lock_guard lock(QueueMutex);
        if (!Queue.empty()) {
            std::queue<TInputItem> tmpQueue;
            Queue.swap(tmpQueue);
            QueueSize.store(0);
            PopStats.Bytes += QueueBytes.exchange(0);
            if (FinishPushed.load()) {
                Finished.store(true);
                ActorSystem->Send(Info.InputActorId, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback});
            }
        }
        return true;
    }
    return false;
}

void TInputDescriptor::Terminate() {
}

ui32 TInputDescriptor::GetQueueSize() {
    std::lock_guard lock(QueueMutex);
    return  Queue.size();
}

TInputBuffer::~TInputBuffer() {
    NodeState->TerminateInputDescriptor(Descriptor);
}

bool TInputBuffer::IsEmpty() {
    return Descriptor->IsEmpty();
}

void TInputBuffer::Push(TDataChunk&&) {
    Y_ENSURE(false, "TInputBuffer::Push not allowed");
}

bool TInputBuffer::IsFinished() {
    return Descriptor->IsFinished();
}

bool TInputBuffer::IsEarlyFinished() {
    return Descriptor->IsEarlyFinished();
}

bool TInputBuffer::Pop(TDataChunk& data) {
    auto result = Descriptor->PopDataChunk(data);
    if (result) {
        NodeState->UpdateProgress(Descriptor);
    }
    return result;
}

void TInputBuffer::EarlyFinish() {
    if (Descriptor->EarlyFinish()) {
        NodeState->UpdateProgress(Descriptor);
    }
}

void TInputBuffer::ExportPushStats(TDqAsyncStats& stats) {
    Descriptor->PushStats.Export(stats);
}

void TInputBuffer::ExportPopStats(TDqAsyncStats& stats) {
    Descriptor->PopStats.Export(stats);
}

TLocalBufferRegistry::~TLocalBufferRegistry() {
    std::lock_guard lock(Mutex);
    *LocalBufferCount -= LocalBuffers.size();
}

std::shared_ptr<TLocalBuffer> TLocalBufferRegistry::GetOrCreateLocalBuffer(const std::shared_ptr<TLocalBufferRegistry>& registry, const TChannelFullInfo& info) {
    std::lock_guard lock(Mutex);

    auto it = LocalBuffers.find(info);
    if (it != LocalBuffers.end()) {
        auto result = it->second.lock();
        if (result) {
            if (info.SrcStageId) {
                result->Info.SrcStageId = info.SrcStageId;
            }
            if (info.DstStageId) {
                result->Info.DstStageId = info.DstStageId;
            }
            return result;
        } else {
            LocalBuffers.erase(it);
        }
    }
    auto result = std::make_shared<TLocalBuffer>(registry, info, ActorSystem, MaxInflightBytes, MinInflightBytes);
    LocalBuffers.emplace(info, result);
    (*LocalBufferCount)++;

    return result;
}

void TLocalBufferRegistry::DeleteLocalBufferInfo(const TChannelInfo& info) {
    (*LocalBufferCount)--;
    std::lock_guard lock(Mutex);
    LocalBuffers.erase(info);
}

TNodeState::~TNodeState() {
    *OutputBufferCount -= OutputDescriptors.size();
    *InputBufferCount -= InputDescriptors.size();
    *OutputBufferInflightBytes -= InflightBytes;
    *OutputBufferInflightMessages -= Queue.size();
    *OutputBufferWaiterCount -= WaitersQueue.size();
    *OutputBufferWaiterBytes -= WaiterBytes.load();
    *OutputBufferWaiterMessages -= WaiterMessages.load();
}

void TNodeState::PushDataChunk(TDataChunk&& data, std::shared_ptr<TOutputDescriptor> descriptor) {
    auto bytes = data.Bytes;
    auto rows = data.Rows;

    if (descriptor->WaitQueueSize.load()) {
        // we are not allowed to reorder messages
        std::lock_guard lock(descriptor->WaitQueueMutex);
        if (!descriptor->WaitQueue.empty()) {

            descriptor->WaitQueue.push(std::move(data));
            descriptor->WaitQueueBytes += bytes;
            descriptor->WaitQueueSize++;

            WaiterBytes += bytes;
            WaiterMessages++;
            *OutputBufferWaiterBytes += bytes;
            (*OutputBufferWaiterMessages)++;

            return;
        }
    }

    if (Reconciliation.load() == 0) {
        // in Reconciliation state we do not send new messages
        std::lock_guard lock(Mutex);
        if (InflightBytes < Limits.NodeSessionIcInflightBytes && Queue.size() < MaxInflightMessages) {
            if (descriptor->CheckGenMajor(GenMajor, "Inconsistent Send GenMajor")) {
                descriptor->AddPopChunk(bytes, rows);
                auto item = std::make_shared<TOutputItem>(std::move(data), descriptor);
                item->SeqNo = ++SeqNo;
                Queue.push_back(item);
                SendMessage(item);
                InflightBytes += bytes;
                *OutputBufferInflightBytes += bytes;
                (*OutputBufferInflightMessages)++;
                return;
            }
        }
    }

    bool result = false;

    std::lock_guard lock(descriptor->WaitQueueMutex);
    if (descriptor->WaitQueue.empty()) {
        descriptor->WaitTimestamp = data.Timestamp;
        result = true;
    }
    descriptor->WaitQueue.push(std::move(data));
    descriptor->WaitQueueBytes += bytes;
    descriptor->WaitQueueSize++;

    WaiterBytes += bytes;
    WaiterMessages++;
    *OutputBufferWaiterBytes += bytes;
    (*OutputBufferWaiterMessages)++;

    if (result) {
        std::lock_guard lock(Mutex);
        // rare race is possible when Queue was emptied during PushDataChunk, we should explicitly process waiters in this case
        auto forceSendWaiters = Queue.empty();
        WaitersQueue.push(descriptor);
        WaitersQueueSize++;
        (*OutputBufferWaiterCount)++;
        if (forceSendWaiters) {
            ActorSystem->Send(NodeActorId, new TEvPrivate::TEvSendWaiters());
        }
    }
}

void TNodeState::SendMessage(std::shared_ptr<TOutputItem> item) {
    Y_ENSURE(PeerActorId);
    auto ev = MakeHolder<TEvDqCompute::TEvChannelDataV2>();

    ev->Record.SetGenMajor(GenMajor);
    ev->Record.SetGenMinor(GenMinor);
    ev->Record.SetSeqNo(item->SeqNo);
    // ev->Record.SetConfirmedSeqNo(???);

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
    ev->Record.SetConfirmedPopBytes(item->Descriptor->RemotePopBytes.load());

    ui32 flags = NActors::IEventHandle::FlagTrackDelivery;
    if (!Subscribed.exchange(true)) {
        flags |=  NActors::IEventHandle::FlagSubscribeOnSession;
    }
    ActorSystem->Send(new NActors::IEventHandle(PeerActorId, NodeActorId, ev.Release(), flags, item->SeqNo));
    item->State.store(TOutputItem::EState::Sent);
}

void TNodeState::FailInputs(const NActors::TActorId& peerActorId, ui64 peerGenMajor) {
    if (InputDescriptors.empty()) {
        return;
    }

    std::vector<TChannelInfo> failedBuffers;

    for (auto& [info, descriptor] : InputDescriptors) {
        if (descriptor->PeerGenMajor) {
            if (descriptor->PeerActorId != peerActorId || descriptor->PeerGenMajor != peerGenMajor) {
                descriptor->Terminate();
                failedBuffers.push_back(info);
            }
        }
    }

    if (failedBuffers.size() == InputDescriptors.size()) {
        InputDescriptors.clear();
    } else {
        for (auto info : failedBuffers) {
            InputDescriptors.erase(info);
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

void TNodeState::SendAckWithError(ui64 cookie, const TString& message) {
    auto evAck = MakeHolder<TEvDqCompute::TEvChannelAckV2>();

    evAck->Record.SetGenMajor(PeerGenMajor);
    evAck->Record.SetGenMinor(PeerGenMinor);
    evAck->Record.SetStatus(NYql::NDqProto::TEvChannelAckV2::ERROR);
    evAck->Record.SetSeqNo(ConfirmedSeqNo);
    evAck->Record.SetMessage(message);

    SendAck(evAck, cookie);
}

void TNodeState::HandleChannelData(TEvDqCompute::TEvChannelDataV2::TPtr& ev) {

    auto& record = ev->Get()->Record;

    TChannelFullInfo info(record.GetChannelId(),
        NActors::ActorIdFromProto(record.GetSrcActorId()),
        NActors::ActorIdFromProto(record.GetDstActorId()), 0, 0, TCollectStatsLevel::None);

    auto descriptor = GetOrCreateInputDescriptor(info, false, record.GetLeading());
    if (!descriptor) {
        // do not auto create if not leading and fail sender
        SendAckWithError(ev->Cookie,
            TStringBuilder() << "Can't find peer for Info: {ChannelId: " << info.ChannelId
            << ", OutputActorId: " << info.OutputActorId
            << ", InputActorId: " << info.InputActorId << "} Leading:" << record.GetLeading()
        );
        return;
    }

    if (descriptor->PeerGenMajor) {
        if (descriptor->PeerActorId != PeerActorId || descriptor->PeerGenMajor != PeerGenMajor) {
            descriptor->Terminate();
            InputDescriptors.erase(info);
            SendAckWithError(ev->Cookie,
                TStringBuilder() << "Generation mismatch: " << descriptor->PeerActorId << " vs "
                << PeerActorId << " (actual), " << descriptor->PeerGenMajor << " vs " << PeerGenMajor << " (actual)"
            );
            return;
        }
    } else {
        descriptor->PeerActorId = PeerActorId;
        descriptor->PeerGenMajor = PeerGenMajor;
    }

    TDataChunk data(TChunkedBuffer(), record.GetRows(), record.GetTransportVersion(),
      FromProto(record.GetValuePackerVersion()), record.GetLeading(), record.GetFinished());
    if (ev->Get()->GetPayloadCount() > 0) {
        data.Buffer = MakeChunkedBuffer(ev->Get()->GetPayload(record.GetPayloadId()));
        // data.Timestamp = TInstant::MicroSeconds(record.GetSendTime());
    }
    data.Bytes = record.GetBytes();
    Y_ENSURE(data.Bytes > data.Buffer.Size()); // record.GetBytes() == data.Buffer.Size() + const
    if (descriptor->PushDataChunk(std::move(data))) {
        UpdateProgress(descriptor);
    }

    auto evAck = MakeHolder<TEvDqCompute::TEvChannelAckV2>();

    evAck->Record.SetGenMajor(PeerGenMajor);
    evAck->Record.SetGenMinor(PeerGenMinor);
    evAck->Record.SetStatus(NYql::NDqProto::TEvChannelAckV2::OK);
    evAck->Record.SetSeqNo(ConfirmedSeqNo);

    NActors::ActorIdToProto(info.OutputActorId, evAck->Record.MutableSrcActorId());
    NActors::ActorIdToProto(info.InputActorId, evAck->Record.MutableDstActorId());
    evAck->Record.SetChannelId(info.ChannelId);

    // evAck->Record.SetEarlyFinished(descriptor->IsEarlyFinished());
    // evAck->Record.SetPopBytes(descriptor->GetPopBytes());

    SendAck(evAck, ev->Cookie);
}

void TNodeState::HandleUndelivered(NActors::TEvents::TEvUndelivered::TPtr& ev) {

    if (ev->Get()->Reason == NActors::TEvents::TEvUndelivered::ReasonActorUnknown) {
        if (Reconciliation.load() == 0) {
            // ignore errors in recovery
            LOG_W("DATA UNDELIVERED, UNKNOWN ActorId to NodeId=" << NodeId << ", NodeActorId=" << NodeActorId << ", PeerActorId=" << PeerActorId << ", Sender=" << ev->Sender);
        }
        std::lock_guard lock(Mutex);
        StartReconciliation(true);
        return;
    }

    switch (ev->Get()->SourceType) {
        case TEvDqCompute::TEvChannelDataV2::EventType: {
            LOG_W("DATA UNDELIVERED, OTHER to NodeId=" << NodeId << ", NodeActorId=" << NodeActorId << ", PeerActorId=" << PeerActorId);
            std::lock_guard lock(Mutex);
            StartReconciliation(false);
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

void TNodeState::ConnectSession(NActors::TActorId& sender, ui64 genMajor) {
    std::lock_guard lock(Mutex);
    if (!Connected) {
        PeerActorId = sender;
        PeerGenMajor = genMajor;
        PeerGenMinor = 0;
        Connected = true;
        LOG_D("NODE CONNECTED, PeerGenMajor=" << PeerGenMajor << ", " << NodeActorId << " to " << PeerActorId);
    } else if (PeerActorId != sender || PeerGenMajor != genMajor) {
        PeerActorId = sender;
        PeerGenMajor = genMajor;
        PeerGenMinor = 0;
        FailInputs(PeerActorId, PeerGenMajor);
        LOG_W("NODE RECONNECTED, PeerGenMajor=" << PeerGenMajor << ", " << NodeActorId << " to " << PeerActorId);
        ConfirmedSeqNo = 0;
    }
}

void TNodeState::HandleDiscovery(TEvDqCompute::TEvChannelDiscoveryV2::TPtr& ev) {

    auto& record = ev->Get()->Record;
    ConnectSession(ev->Sender, record.GetGenMajor());

    auto evAck = MakeHolder<TEvDqCompute::TEvChannelAckV2>();

    evAck->Record.SetGenMajor(PeerGenMajor);
    evAck->Record.SetGenMinor(PeerGenMinor);
    evAck->Record.SetStatus(NYql::NDqProto::TEvChannelAckV2::OK);
    evAck->Record.SetSeqNo(ConfirmedSeqNo);

    ui32 flags = NActors::IEventHandle::FlagTrackDelivery;
    if (!Subscribed.exchange(true)) {
        flags |=  NActors::IEventHandle::FlagSubscribeOnSession;
    }

    ActorSystem->Send(new NActors::IEventHandle(PeerActorId, NodeActorId, evAck.Release(), flags, ev->Cookie));
}

void TNodeState::HandleData(TEvDqCompute::TEvChannelDataV2::TPtr& ev) {

    auto& record = ev->Get()->Record;
    ConnectSession(ev->Sender, record.GetGenMajor());

    PeerGenMinor = std::max<ui64>(record.GetGenMinor(), PeerGenMinor);

    auto seqNo = record.GetSeqNo();

    if (seqNo <= ConfirmedSeqNo) {
        LOG_W("DATA IGNORED, SeqNo=" << seqNo << ", ConfirmedSeqNo=" << ConfirmedSeqNo << ", " << NodeActorId << " from " << PeerActorId);
        return;
    }

    switch (seqNo - ConfirmedSeqNo) {
        case 1: {
            break;
        }
        case 2: {
            // allow 1 out of order message
            LOG_W("DATA OUT OF ORDER, SeqNo=" << seqNo << ", ConfirmedSeqNo=" << ConfirmedSeqNo << ", " << NodeActorId << " from " << PeerActorId);
            OutOfOrderMessage = ev.Release();
            return;
        }
        default: {
            LOG_W("DATA ASK RESEND, SeqNo=" << seqNo << ", ConfirmedSeqNo=" << ConfirmedSeqNo << "(+1), " << NodeActorId << " from " << PeerActorId);
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

    ui64 inflightBytes = 0;
    {
        std::lock_guard lock(Mutex); // ???
        if (Reconciliation.load() > 0) {
            return;
        }
        inflightBytes = InflightBytes;
    }

    Y_ENSURE(inflightBytes >= deltaBytes);

    while (inflightBytes - deltaBytes < Limits.NodeSessionIcInflightBytes) {
        std::shared_ptr<TOutputDescriptor> waiter;

        {
            std::lock_guard lock(Mutex);
            if (Queue.size() >= MaxInflightMessages) {
                break;
            }

            while (!WaitersQueue.empty()) {

                (*OutputBufferWaiterCount)--;

                if (WaitersQueue.top()->IsTerminatedOrAborted()) {

                    auto waitQueueBytes = WaitersQueue.top()->WaitQueueBytes.load();
                    auto waitQueueSize = WaitersQueue.top()->WaitQueueSize.load();
                    WaiterBytes -= waitQueueBytes;
                    WaiterMessages -= waitQueueSize;
                    *OutputBufferWaiterBytes -= waitQueueBytes;
                    *OutputBufferWaiterMessages -= waitQueueSize;

                    WaitersQueue.pop();
                    WaitersQueueSize--;
                    continue;
                }

                waiter = WaitersQueue.top();
                WaitersQueue.pop();
                WaitersQueueSize--;
                break;
            }
        }

        if (!waiter) {
            break;
        }

        // TODO: Handle delayed (spilled) data

        if (waiter->CheckGenMajor(GenMajor, "Inconsistent Waiter Gen")) {
            std::shared_ptr<TOutputItem> item;

            ui64 bytes = 0;

            {
                std::lock_guard lock(waiter->WaitQueueMutex);
                Y_ENSURE(!waiter->WaitQueue.empty());

                auto& data = waiter->WaitQueue.front();
                bytes = data.Bytes;

                waiter->AddPopChunk(data.Bytes, data.Rows);
                item = std::make_shared<TOutputItem>(std::move(data), waiter);
                waiter->WaitQueue.pop();
                waiter->WaitQueueBytes -= bytes;
                waiter->WaitQueueSize--;

                std::lock_guard lock1(Mutex);


                if (!waiter->WaitQueue.empty()) {
                    waiter->WaitTimestamp = waiter->WaitQueue.front().Timestamp;
                    WaitersQueue.push(waiter);
                    WaitersQueueSize++;
                    (*OutputBufferWaiterCount)++;
                }

                item->SeqNo = ++SeqNo;
                inflightBytes += bytes;
                InflightBytes += bytes;
                *OutputBufferInflightBytes += bytes;
                (*OutputBufferInflightMessages)++;
                Queue.push_back(item);
                SendMessage(item);
            }

            WaiterBytes -= bytes;
            WaiterMessages--;
            *OutputBufferWaiterBytes -= bytes;
            (*OutputBufferWaiterMessages)--;
        } else {
            auto waitQueueBytes = waiter->WaitQueueBytes.load();
            auto waitQueueSize = waiter->WaitQueueSize.load();
            WaiterBytes -= waitQueueBytes;
            WaiterMessages -= waitQueueSize;
            *OutputBufferWaiterBytes -= waitQueueBytes;
            *OutputBufferWaiterMessages -= waitQueueSize;
        }
    }

    {
        std::lock_guard lock(Mutex); // ???
        InflightBytes -= deltaBytes;
    }
}

void TNodeState::HandleAck(TEvDqCompute::TEvChannelAckV2::TPtr& ev) {

    auto& record = ev->Get()->Record;

    if (record.GetGenMajor() != GenMajor) {
        LOG_W("ACK IGNORED GenMajor=" << GenMajor << ", ack.GenMajor=" << record.GetGenMajor() << ", NodeActorId=" << NodeActorId << " from peer " << ev->Sender);
        return;
    }

    TChannelInfo info(record.GetChannelId(), NActors::ActorIdFromProto(record.GetSrcActorId()), NActors::ActorIdFromProto(record.GetDstActorId()));
    ui64 deltaBytes = 0;

    {
        std::lock_guard lock(Mutex);

        if (!Connected && PeerActorId == NActors::TActorId{}) {
            PeerActorId = ev->Sender;
            LOG_D("NODE PEER SET BY ACK, " << NodeActorId << " to " << PeerActorId);
        }

        auto status = record.GetStatus();
        auto seqNo = record.GetSeqNo();

        if (SeqNo < seqNo) {
            LOG_W("LARGE SEQ_NO, SeqNo=" << SeqNo << ", ack.SeqNo=" << seqNo << ", NodeActorId=" << NodeActorId << " from peer " << ev->Sender);
            StartReconciliation(true);
            return;
        }

        while (!Queue.empty()) {
            auto& item = Queue.front();
            if (item->SeqNo >= seqNo) {
                break;
            }
            if (item->Descriptor->GenMajor.load() != GenMajor) {
                item->Descriptor->AbortChannel(TStringBuilder() << "By Outdated GenMajor1 " << item->Descriptor->GenMajor.load() << " vs " << GenMajor);
            }
            deltaBytes += item->Data.Bytes;
            *OutputBufferInflightBytes -= item->Data.Bytes;
            (*OutputBufferInflightMessages)--;
            Queue.pop_front();
        }

        if (Queue.empty()) {
            if (status == NYql::NDqProto::TEvChannelAckV2::RESEND) {
                LOG_W("CAN'T RESEND, SeqNo=" << SeqNo << ", ack.SeqNo=" << seqNo << ", " << NodeActorId << " from peer " << ev->Sender);
                StartReconciliation(true);
                return;
            }
        } else {
            auto& item = Queue.front();

            if (item->SeqNo != seqNo) {
                // allow outdates/old acks
                if (seqNo > item->SeqNo) {
                    LOG_W("SEQ_NO DESYNC, SeqNo=" << seqNo << ", item.SeqNo=" << item->SeqNo << ", " << NodeActorId << " from peer " << ev->Sender);
                    StartReconciliation(true);
                    return;
                }
            } else {
                if (status == NYql::NDqProto::TEvChannelAckV2::RESEND) {
                    // if we're reconcilating, ignore next RESENDs
                    if (record.GetGenMinor() == GenMinor) {
                        LOG_W("RESEND DATA, SeqNo=" << seqNo << ", " << NodeActorId << " from peer " << PeerActorId);
                        StartReconciliation(false);
                    }
                    return;
                }

                if (!item->Descriptor->IsTerminatedOrAborted()) {
                    if (item->Descriptor->CheckGenMajor(GenMajor, "by Ack")) {
                        if (status == NYql::NDqProto::TEvChannelAckV2::ERROR) {
                            item->Descriptor->AbortChannel("(Peer) " + record.GetMessage());
                        } else {
                            auto earlyFinished = record.GetEarlyFinished();
                            auto popBytes = record.GetPopBytes();
                            if (earlyFinished || popBytes) {
                                item->Descriptor->HandleUpdate(earlyFinished, popBytes, this, item->Descriptor);
                            }
                        }
                    }
                }

                deltaBytes += item->Data.Bytes;
                *OutputBufferInflightBytes -= item->Data.Bytes;
                (*OutputBufferInflightMessages)--;
                Queue.pop_front();
            }
        }

        if (Reconciliation.exchange(0) > 0) {
            ReconciliationCount = 0;
            ReconciliationDelay = TDuration::Zero();
            if (!Queue.empty()) {
                LOG_D("DATA REPEAT, SeqNo=" << Queue.front()->SeqNo << '/' << Queue.back()->SeqNo << ", " << NodeActorId << " from peer " << PeerActorId);
                for (auto item : Queue) {
                    if (item->Descriptor->CheckGenMajor(GenMajor, "by Resend")) {
                        SendMessage(item);
                    }
                }
            }
        }
    }

    SendFromWaiters(deltaBytes);
}

void TNodeState::HandleUpdate(TEvDqCompute::TEvChannelUpdateV2::TPtr& ev) {

    auto& record = ev->Get()->Record;

    if (record.GetGenMajor() != GenMajor) {
        LOG_W("UPDATE IGNORED (by Gen) GenMajor=" << GenMajor << ", update.GenMajor=" << record.GetGenMajor() << ", " << NodeActorId << " from peer " << ev->Sender);
        return;
    }

    // GenMinor ???
    // ConfirmedSeqNo ???

    auto earlyFinished = record.GetEarlyFinished();
    auto popBytes = record.GetPopBytes();
    if (!earlyFinished && popBytes == 0) {
        LOG_W("UPDATE IGNORED EarlyFinished=False, PopBytes=0, " << NodeActorId << " from peer " << ev->Sender);
        return;
    }

    TChannelFullInfo info(record.GetChannelId(),
        NActors::ActorIdFromProto(record.GetSrcActorId()),
        NActors::ActorIdFromProto(record.GetDstActorId()), 0, 0, TCollectStatsLevel::None);

    auto descriptor = GetOrCreateOutputDescriptor(info, false, popBytes == 0);
    if (!descriptor) {
        LOG_W("UPDATE IGNORED EarlyFinished=" << earlyFinished << ", PopBytes=" << popBytes << ", " << NodeActorId << " from peer " << ev->Sender);
        return;
    }

    if (!descriptor->IsTerminatedOrAborted() && descriptor->CheckGenMajor(GenMajor, "Inconsistent GenMajor in HandleUpdate")) {
        descriptor->HandleUpdate(earlyFinished, popBytes, this, descriptor);
    }
}

void TNodeState::HandleSendWaiters(TEvPrivate::TEvSendWaiters::TPtr&) {
   SendFromWaiters(0);
}

void TNodeState::UpdateProgress(std::shared_ptr<TInputDescriptor>& descriptor) {
    auto evUpdate = MakeHolder<TEvDqCompute::TEvChannelUpdateV2>();

    evUpdate->Record.SetGenMajor(PeerGenMajor);
    evUpdate->Record.SetGenMinor(PeerGenMinor);
    // evUpdate->Record.SetSeqNo(ConfirmedSeqNo);

    NActors::ActorIdToProto(descriptor->Info.OutputActorId, evUpdate->Record.MutableSrcActorId());
    NActors::ActorIdToProto(descriptor->Info.InputActorId, evUpdate->Record.MutableDstActorId());
    evUpdate->Record.SetChannelId(descriptor->Info.ChannelId);

    evUpdate->Record.SetEarlyFinished(descriptor->EarlyFinished.load());
    evUpdate->Record.SetPopBytes(descriptor->PopStats.Bytes.load());

    ui32 flags = NActors::IEventHandle::FlagTrackDelivery;
    if (!Subscribed.exchange(true)) {
        flags |=  NActors::IEventHandle::FlagSubscribeOnSession;
    }

    ActorSystem->Send(new NActors::IEventHandle(PeerActorId, NodeActorId, evUpdate.Release(), flags));
}

std::shared_ptr<TOutputDescriptor> TNodeState::GetOrCreateOutputDescriptor(const TChannelFullInfo& info, bool bound, bool leading) {
    std::lock_guard lock(Mutex);
    auto it = OutputDescriptors.find(info);
    if (it != OutputDescriptors.end()) {
        auto result = it->second;
        if (bound) {
            result->IsBound = true;
            result->Info.SrcStageId = info.SrcStageId;
            result->Info.DstStageId = info.DstStageId;
            ActorSystem->Send(result->Info.OutputActorId, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback});
        }
        return result;
    }

    if (!bound && !leading) {
        return {};
    }

    auto result = std::make_shared<TOutputDescriptor>(info, ActorSystem, OutputBufferBytes, OutputBufferChunks, Limits.RemoteChannelInflightBytes, Limits.RemoteChannelInflightBytes * 8 / 10);
    OutputDescriptors.emplace(info, result);
    (*OutputBufferCount)++;
    if (bound) {
        result->IsBound = true;
    } else {
        UnboundOutputs.emplace(info, TInstant::Now() + UnboundWaitPeriod);
    }
    return result;
}

std::shared_ptr<TInputDescriptor> TNodeState::GetOrCreateInputDescriptor(const TChannelFullInfo& info, bool bound, bool leading) {
    std::lock_guard lock(Mutex);
    auto it = InputDescriptors.find(info);
    if (it != InputDescriptors.end()) {
        auto result = it->second;
        if (bound) {
            result->IsBound = true;
            result->Info.SrcStageId = info.SrcStageId;
            result->Info.DstStageId = info.DstStageId;
            ActorSystem->Send(result->Info.InputActorId, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback});
        }
        return result;
    }

    if (!bound && !leading) {
        return {};
    }

    auto result = std::make_shared<TInputDescriptor>(info, ActorSystem, InputBufferBytes, InputBufferChunks);
    InputDescriptors.emplace(info, result);
    (*InputBufferCount)++;
    if (bound) {
        result->IsBound = true;
    } else {
        UnboundInputs.emplace(info, TInstant::Now() + UnboundWaitPeriod);
    }
    return result;
}

void TNodeState::TerminateOutputDescriptor(const std::shared_ptr<TOutputDescriptor>& descriptor) {
    descriptor->Terminate();
    std::lock_guard lock(Mutex);
    OutputDescriptors.erase(descriptor->Info);
    (*OutputBufferCount)--;
}

void TNodeState::TerminateInputDescriptor(const std::shared_ptr<TInputDescriptor>& descriptor) {
    std::lock_guard lock(Mutex);
    InputDescriptors.erase(descriptor->Info);
    (*InputBufferCount)--;
}

void TNodeState::CleanupUnbound() {
    std::lock_guard lock(Mutex);
    auto now = TInstant::Now();
    while (!UnboundInputs.empty()) {
        auto& front = UnboundInputs.front();
        if (front.second > now) {
            break;
        }
        if (auto it = InputDescriptors.find(front.first); it != InputDescriptors.end()) {
            if (!it->second->IsBound) {
                InputDescriptors.erase(it);
            }
        }
        UnboundInputs.pop();
    }
    while (!UnboundOutputs.empty()) {
        auto& front = UnboundOutputs.front();
        if (front.second > now) {
            break;
        }
        if (auto it = OutputDescriptors.find(front.first); it != OutputDescriptors.end()) {
            if (!it->second->IsBound) {
                OutputDescriptors.erase(it);
            }
        }
        UnboundOutputs.pop();
    }
}

void TNodeState::HandleWakeup(NActors::TEvents::TEvWakeup::TPtr&) {
    // NOOP
}

void TNodeState::HandleReconciliation(TEvPrivate::TEvReconciliation::TPtr& ev) {
    if (ev->Get()->GenMajor == Reconciliation.load() /* GenMajor */ && ev->Get()->GenMinor == GenMinor) {
        std::lock_guard lock(Mutex);

        if (ReconciliationDelay >= MaxReconciliationDelay && !ReReconciliation) {
            ReReconciliation = true;
            GenMajor++;
            GenMinor = 1;
        }
        UpdateReconciliationDelay();
        DoReconciliation();
        ScheduleReconciliation();
    }
}

void TNodeState::StartReconciliation(bool major) {
    if (Reconciliation.load() == 0) {
        if (major) {
            GenMajor++;
            GenMinor = 1;
        } else {
            GenMinor++;
        }

        ReReconciliation = false;
        Reconciliation.store(GenMajor);

        if (!UpdateReconciliationDelay()) {
            DoReconciliation();
        }
        ScheduleReconciliation();
    }
}

bool TNodeState::UpdateReconciliationDelay() {
    if (ReconciliationDelay) {
        if (ReconciliationDelay < MaxReconciliationDelay) {
            ReconciliationDelay *= 2;
        }
        return true;
    } else {
        ReconciliationDelay = MinReconciliationDelay;
        return false;
    }
}

void TNodeState::ScheduleReconciliation() {
    ActorSystem->Schedule(ReconciliationDelay, new NActors::IEventHandle(NodeActorId, NodeActorId, new TEvPrivate::TEvReconciliation(GenMajor, GenMinor)));
}

void TNodeState::DoReconciliation() {

    ReconciliationCount++;
    if (ReconciliationCount > 1) {
        LOG_W("NODE RECONCILIATION x" << ReconciliationCount << ", to NodeId=" << NodeId << ", NodeActorId=" << NodeActorId
            << ", Gen=" << GenMajor << '.' << GenMinor << ", Next Delay=" << ReconciliationDelay);
    } else {
        LOG_I("NODE RECONCILIATION, to NodeId=" << NodeId << ", NodeActorId=" << NodeActorId
            << ", Queue" << (Queue.empty() ? " IS EMPTY" : ".front().SeqNo=" + ToString(Queue.front()->SeqNo))
            << ", Gen=" << GenMajor << '.' << GenMinor << ", Next Delay=" << ReconciliationDelay);
    }

    ui32 delta = 0;
    while (!Queue.empty()) {
        if (Queue.front()->Descriptor->CheckGenMajor(GenMajor, "by Reconciliation")) {
            break;
        }
        delta += Queue.front()->Data.Bytes;
        Queue.pop_front();
    }
    InflightBytes -= delta;

    if (PeerActorId == NActors::TActorId{}) {
        SendDiscovery(MakeChannelServiceActorID(NodeId));
    } else if (Queue.empty()) {
        SendDiscovery(PeerActorId);
    } else {
        SendMessage(Queue.front());
    }
}

void TNodeState::SendDiscovery(NActors::TActorId actorId) {
    auto evDiscovery = MakeHolder<TEvDqCompute::TEvChannelDiscoveryV2>();

    evDiscovery->Record.SetGenMajor(GenMajor);

    ui32 flags = NActors::IEventHandle::FlagTrackDelivery;
    if (!Subscribed.exchange(true)) {
        flags |=  NActors::IEventHandle::FlagSubscribeOnSession;
    }

    ActorSystem->Send(new NActors::IEventHandle(actorId, NodeActorId, evDiscovery.Release(), flags));
}

TString TNodeState::GetDebugInfo() {
    std::lock_guard lock(Mutex);
    TStringBuilder builder;

    builder << "TNodeState, NodeId=" << NodeActorId.NodeId() << ", Peer NodeId=" << PeerActorId.NodeId()
        << ", SeqNo=" << SeqNo << ", ConfirmedSeqNo=" << ConfirmedSeqNo << ", InflightBytes=" << InflightBytes
        << ", Reconciliation=" << Reconciliation.load()
        << Endl;

    for (auto& [info, descriptor] : OutputDescriptors) {
        builder << "  Output " << info.ChannelId << ", FL=" << (ui32)descriptor->FillLevel
            << ", IF:" << descriptor->IsFinished() << ", TA=" << descriptor->IsTerminatedOrAborted()
            << ", EF: " << descriptor->EarlyFinished.load()
            << ", PP:" << descriptor->PushBytes.load() << ':' << descriptor->RemotePopBytes.load() << Endl;
    }
    for (auto& [info, descriptor] : InputDescriptors) {
        builder << "  Input " << info.ChannelId << ", Empty=" << descriptor->IsEmpty()
            << ", Queue.size()=" << descriptor->GetQueueSize() << Endl;
    }

    std::unordered_map<TChannelInfo, std::shared_ptr<TOutputDescriptor>> OutputDescriptors;

    return builder;
}

void TDebugNodeState::HandleNullMode(TEvDqCompute::TEvChannelDataV2::TPtr& ev) {

    auto& record = ev->Get()->Record;
    ConnectSession(ev->Sender, record.GetGenMajor());

    PeerGenMinor = std::max<ui64>(record.GetGenMinor(), PeerGenMinor);

    auto seqNo = record.GetSeqNo();

    ConfirmedSeqNo = seqNo;

    TChannelFullInfo info(record.GetChannelId(),
        NActors::ActorIdFromProto(record.GetSrcActorId()),
        NActors::ActorIdFromProto(record.GetDstActorId()), 0, 0, TCollectStatsLevel::None);

    auto descriptor = GetOrCreateInputDescriptor(info, false, record.GetLeading());
    if (!descriptor) {
        // do not auto create if not leading and fail sender
        SendAckWithError(ev->Cookie, "[TDebugNodeState] Can't find peer for not leading message");
        return;
    }

    descriptor->PopStats.Bytes += record.GetBytes();

    auto evAck = MakeHolder<TEvDqCompute::TEvChannelAckV2>();

    evAck->Record.SetGenMajor(PeerGenMajor);
    evAck->Record.SetGenMinor(PeerGenMinor);
    evAck->Record.SetStatus(NYql::NDqProto::TEvChannelAckV2::OK);
    evAck->Record.SetSeqNo(ConfirmedSeqNo);

    NActors::ActorIdToProto(info.OutputActorId, evAck->Record.MutableSrcActorId());
    NActors::ActorIdToProto(info.InputActorId, evAck->Record.MutableDstActorId());
    evAck->Record.SetChannelId(info.ChannelId);

    // evAck->Record.SetEarlyFinished(descriptor->IsEarlyFinished());
    evAck->Record.SetPopBytes(descriptor->PopStats.Bytes.load());

    SendAck(evAck, ev->Cookie);
}

void TDebugNodeState::PauseChannelData() {
    ChannelDataPaused.store(true);
}

void TDebugNodeState::ResumeChannelData() {
    ChannelDataPaused.store(false);
    ActorSystem->Send(new NActors::IEventHandle(NodeActorId, NodeActorId, new TEvPrivate::TEvProcessPending(0)));
}

void TDebugNodeState::PauseChannelAck() {
    ChannelAckPaused.store(true);
}

void TDebugNodeState::ResumeChannelAck() {
    ChannelAckPaused.store(false);
    ActorSystem->Send(new NActors::IEventHandle(NodeActorId, NodeActorId, new TEvPrivate::TEvProcessPending(0)));
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

void TDebugNodeState::ProcessPending(ui32 maxCount) {
    ActorSystem->Send(new NActors::IEventHandle(NodeActorId, NodeActorId, new TEvPrivate::TEvProcessPending(maxCount)));
}

void TDebugNodeState::SetNullMode() {
    NullMode.store(true);
}

bool TDebugNodeState::IsNullMode() {
    return NullMode.load();
}

std::shared_ptr<TNodeState> TDqChannelService::GetOrCreateNodeState(ui32 nodeId) {
    std::lock_guard lock(Mutex);
    auto it = NodeStates.find(nodeId);
    if (it != NodeStates.end()) {
        return it->second;
    } else {
        auto nodeState = std::make_shared<TNodeState>(ActorSystem, nodeId, Counters, Limits);
        nodeState->NodeActorId = ActorSystem->Register(new TNodeSessionActor(nodeState), NActors::TMailboxType::HTSwap, PoolId);
        nodeState->Self = nodeState;
        NodeStates.emplace(nodeId, nodeState);
        LOG_N("NODE SESSION CREATED, to NodeId=" << nodeId << ", NodeActorId=" << nodeState->NodeActorId << ", MaxInflight=" << Limits.NodeSessionIcInflightBytes << " bytes");
        nodeState->StartReconciliation(true);
        return nodeState;
    }
}

std::shared_ptr<TDebugNodeState> TDqChannelService::CreateDebugNodeState(ui32 nodeId) {
    std::lock_guard lock(Mutex);
    Y_ENSURE(NodeStates.find(nodeId) == NodeStates.end());

    auto nodeState = std::make_shared<TDebugNodeState>(ActorSystem, nodeId, Counters, Limits);
    nodeState->NodeActorId = ActorSystem->Register(new TDebugNodeSessionActor(nodeState));
    nodeState->Self = nodeState;
    NodeStates.emplace(nodeId, nodeState);
    LOG_N("DEBUG NODE SESSION CREATED, to NodeId=" << nodeId << ", NodeActorId=" << nodeState->NodeActorId << ", MaxInflight=" << Limits.NodeSessionIcInflightBytes << " bytes");
    nodeState->StartReconciliation(true);
    return nodeState;
}

// unbinded stubs

std::shared_ptr<IChannelBuffer> TDqChannelService::GetUnbindedBuffer(const TChannelFullInfo& info) {
    return std::make_shared<TChannelStub>(info);
}

// binded helpers

std::shared_ptr<IChannelBuffer> TDqChannelService::GetOutputBuffer(const TChannelFullInfo& info, IDqChannelStorage::TPtr storage) {
    Y_ENSURE(info.OutputActorId.NodeId() == NodeId);
    return (info.InputActorId.NodeId() == NodeId) ? GetLocalBuffer(info, false, storage) : GetRemoteOutputBuffer(info, storage);
}

std::shared_ptr<IChannelBuffer> TDqChannelService::GetInputBuffer(const TChannelFullInfo& info) {
    Y_ENSURE(info.InputActorId.NodeId() == NodeId);
    return (info.OutputActorId.NodeId() == NodeId) ? GetLocalBuffer(info, true, nullptr) : GetRemoteInputBuffer(info);
}

// remote buffers

std::shared_ptr<TOutputBuffer> TDqChannelService::GetRemoteOutputBuffer(const TChannelFullInfo& info, IDqChannelStorage::TPtr storage) {
    Y_ENSURE(info.InputActorId.NodeId() != NodeId);
    auto nodeState = GetOrCreateNodeState(info.InputActorId.NodeId());
    auto descriptor = nodeState->GetOrCreateOutputDescriptor(info, true, true);
    if (storage) {
        descriptor->BindStorage(descriptor, nodeState, storage);
    }
    return std::make_shared<TOutputBuffer>(nodeState, descriptor);
}

std::shared_ptr<TInputBuffer> TDqChannelService::GetRemoteInputBuffer(const TChannelFullInfo& info) {
    Y_ENSURE(info.OutputActorId.NodeId() != NodeId);
    auto nodeState = GetOrCreateNodeState(info.OutputActorId.NodeId());
    return std::make_shared<TInputBuffer>(nodeState, nodeState->GetOrCreateInputDescriptor(info, true, true));
}

// local buffer

std::shared_ptr<IChannelBuffer> TDqChannelService::GetLocalBuffer(const TChannelFullInfo& info, bool bindInput, IDqChannelStorage::TPtr storage) {
    Y_ENSURE(info.OutputActorId.NodeId() == NodeId);
    Y_ENSURE(info.InputActorId.NodeId() == NodeId);

    auto buffer = LocalBufferRegistry->GetOrCreateLocalBuffer(LocalBufferRegistry, info);
    if (bindInput) {
        buffer->BindInput();
    } else {
        buffer->BindOutput();
    }
    if (storage) {
        buffer->BindStorage(buffer, storage);
    }
    return buffer;
}

// unbinded channels

IDqOutputChannel::TPtr TDqChannelService::GetOutputChannel(const TDqChannelSettings& settings) {
    auto buffer = GetUnbindedBuffer(TChannelFullInfo(settings.ChannelId, {}, {}, settings.SrcStageId, settings.DstStageId, settings.Level));
    return new TFastDqOutputChannel(Self, settings, buffer, false);
}

IDqInputChannel::TPtr TDqChannelService::GetInputChannel(const TDqChannelSettings& settings) {
    auto buffer = GetUnbindedBuffer(TChannelFullInfo(settings.ChannelId, {}, {}, settings.SrcStageId, settings.DstStageId, settings.Level));
    return new TFastDqInputChannel(Self, settings, buffer);
}

void TDqChannelService::CleanupUnbound() {
    std::lock_guard lock(Mutex);

    for (auto& [_, nodeState] : NodeStates) {
        nodeState->CleanupUnbound();
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

bool TFastDqInputChannel::Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, TMaybe<TInstant>& watermark) {
    Y_UNUSED(watermark);

    TDataChunk chunk;
    auto popResult = Buffer->Pop(chunk) && !chunk.Buffer.Empty();

    if (popResult) {
        if (chunk.TransportVersion != Deserializer->TransportVersion || chunk.PackerVersion != Deserializer->PackerVersion) {
            auto deserializer = CreateDeserializer(Deserializer->RowType, chunk.TransportVersion, chunk.PackerVersion, Nothing(), Deserializer->HolderFactory);
            Deserializer = std::move(deserializer);
        }
        Deserializer->Deserialize(std::move(chunk.Buffer), batch);
        Y_ENSURE(batch.RowCount() > 0);
    }

    PushStats.PopTime = TInstant::Now();
    PushStats.PopResult = popResult;

    return popResult;
}

void TFastDqOutputChannel::Bind(NActors::TActorId outputActorId, NActors::TActorId inputActorId) {
    IsLocalChannel = outputActorId.NodeId() == inputActorId.NodeId();
    auto service = Service.lock();
    Y_ENSURE(service, "Channel has been binded or service is not available");

    if (IsLocalChannel) {
        Serializer = ConvertToLocalSerializer(std::move(Serializer));
    }
    Serializer->Buffer->Info.OutputActorId = outputActorId;
    Serializer->Buffer->Info.InputActorId = inputActorId;
    auto buffer = service->GetOutputBuffer(Serializer->Buffer->Info, Storage);
    if (Aggregator) {
        buffer->SetFillAggregator(Aggregator);
    }
    Serializer->Buffer = buffer;
    Service.reset();
}

void TFastDqInputChannel::Bind(NActors::TActorId outputActorId, NActors::TActorId inputActorId) {
    IsLocalChannel = outputActorId.NodeId() == inputActorId.NodeId();
    auto service = Service.lock();
    Y_ENSURE(service, "Channel has been binded or service is not available");

    Buffer->Info.OutputActorId = outputActorId;
    Buffer->Info.InputActorId = inputActorId;
    auto buffer = service->GetInputBuffer(Buffer->Info);
    Buffer = buffer;
    Service.reset();
}

void TChannelServiceActor::Handle(NActors::NMon::TEvHttpInfo::TPtr& ev) {
    std::lock_guard lock(ChannelService->Mutex);
    TStringStream str;
    HTML(str) {
        PRE() {
            str << "Channel Service NodeId: " << ChannelService->NodeId << Endl;

            str << Endl << "Local Buffers:";

            TABLE_SORTABLE_CLASS("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH_ATTRS({{"title", "ChannelId"}}) {str << "Id";}
                        TABLEH() {str << "OutputActorId";}
                        TABLEH() {str << "InputActorId";}
                        TABLEH_ATTRS({{"title", "SrcStageId"}}) {str << "Src";}
                        TABLEH_ATTRS({{"title", "DstStageId"}}) {str << "Dst";}
                        TABLEH() {str << "Fill (Agg)";}
                        TABLEH_ATTRS({{"title", "OutputBound"}}) {str << "O";}
                        TABLEH_ATTRS({{"title", "InputBound"}}) {str << "I";}
                        TABLEH_ATTRS({{"title", "IsFinished"}}) {str << "F";}
                        TABLEH_ATTRS({{"title", "EarlyFinished"}}) {str << "EF";}
                        TABLEH() {str << "PushBytes";}
                        TABLEH() {str << "PopBytes";}
                        TABLEH() {str << "OutputNotificationTime";}
                        TABLEH() {str << "InputNotificationTime";}
                        TABLEH() {str << "InflightBytes";}
                        TABLEH() {str << "QueueSize";}
                        TABLEH() {str << "SpilledBytes";}
                        TABLEH() {str << "LoadingQueueSize";}
                        TABLEH() {str << "HeadBlobId";}
                        TABLEH() {str << "TailBlobId";}
                    }
                }
                TABLEBODY() {
                    auto registry = ChannelService->LocalBufferRegistry;
                    std::lock_guard lock(registry->Mutex);
                    for (auto& [info, weakBuffer] : registry->LocalBuffers) {
                        auto sharedBuffer = weakBuffer.lock();
                        if (sharedBuffer) {
                            TABLER() {
                                TABLED() {str << sharedBuffer->Info.ChannelId;}
                                TABLED() {
                                    HREF(TStringBuilder() << "/node/" << sharedBuffer->Info.OutputActorId.NodeId() << "/actors/kqp_node?ca=" << sharedBuffer->Info.OutputActorId)  {
                                        str << sharedBuffer->Info.OutputActorId;
                                    }
                                }
                                TABLED() {
                                    HREF(TStringBuilder() << "/node/" << sharedBuffer->Info.InputActorId.NodeId() << "/actors/kqp_node?ca=" << sharedBuffer->Info.InputActorId)  {
                                        str << sharedBuffer->Info.InputActorId;
                                    }
                                }
                                TABLED() {str << sharedBuffer->Info.SrcStageId;}
                                TABLED() {str << sharedBuffer->Info.DstStageId;}
                                TABLED() {
                                    str << FillLevelToString(sharedBuffer->FillLevel);
                                    if (sharedBuffer->Aggregator) {
                                        str << " (" << FillLevelToString(sharedBuffer->Aggregator->GetFillLevel()) << ")";
                                    }
                                }
                                TABLED() {str << sharedBuffer->OutputBound.load();}
                                TABLED() {str << sharedBuffer->InputBound.load();}
                                TABLED() {str << sharedBuffer->Finished.load();}
                                TABLED() {str << sharedBuffer->EarlyFinished.load();}
                                TABLED() {str << sharedBuffer->PushStats.Bytes.load();}
                                TABLED() {str << sharedBuffer->PopStats.Bytes.load();}
                                TABLED() {str << sharedBuffer->LastOutputNotificationTime;}
                                TABLED() {str << sharedBuffer->LastInputNotificationTime;}
                                TABLED() {str << sharedBuffer->InflightBytes.load();}
                                TABLED() {str << sharedBuffer->Queue.size();}
                                TABLED() {str << sharedBuffer->SpilledBytes.load();}
                                TABLED() {str << sharedBuffer->LoadingQueue.size();}
                                TABLED() {str << sharedBuffer->HeadBlobId;}
                                TABLED() {str << sharedBuffer->TailBlobId;}
                            }
                        }
                    }
                }
            }

            str << Endl << "Node Sessions:";

            TABLE_SORTABLE_CLASS ("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {str << "PeerNodeId";}
                        TABLEH() {str << "PeerActorId";}
                        TABLEH() {str << "NodeActorId";}
                        TABLEH() {str << "GenMajor";}
                        TABLEH() {str << "GenMinor";}
                        TABLEH() {str << "SeqNo";}
                        TABLEH() {str << "InflightBytes";}
                        TABLEH() {str << "WaitersQueueSize";}
                        TABLEH() {str << "WaitersMessages";}
                        TABLEH() {str << "PeerGenMajor";}
                        TABLEH() {str << "PeerGenMinor";}
                        TABLEH() {str << "ConfirmedSeqNo";}
                    }
                }
                TABLEBODY() {
                    for (auto& [nodeId, state] : ChannelService->NodeStates) {
                        std::lock_guard lock(state->Mutex);
                        TABLER() {
                            TABLED() {str << nodeId;}
                            TABLED() {str << state->PeerActorId;}
                            TABLED() {str << state->NodeActorId;}
                            TABLED() {str << state->GenMajor;}
                            TABLED() {str << state->GenMinor;}
                            TABLED() {str << state->SeqNo;}
                            TABLED() {str << state->InflightBytes;}
                            TABLED() {str << state->WaitersQueue.size();}
                            TABLED() {str << state->WaiterMessages.load();}
                            TABLED() {str << state->PeerGenMajor;}
                            TABLED() {str << state->PeerGenMinor;}
                            TABLED() {str << state->ConfirmedSeqNo;}
                        }
                    }
                }
            }

            str << Endl << "Output Descriptors:";

            TABLE_SORTABLE_CLASS ("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH_ATTRS({{"title", "ChannelId"}}) {str << "Id";}
                        TABLEH() {str << "PeerNodeId";}
                        TABLEH_ATTRS({{"title", "SrcStageId"}}) {str << "Src";}
                        TABLEH_ATTRS({{"title", "DstStageId"}}) {str << "Dst";}
                        TABLEH() {str << "Fill (Agg)";}
                        TABLEH() {str << "PushBytes";}
                        TABLEH() {str << "PopBytes";}
                        TABLEH_ATTRS({{"title", "FinishPushed"}}) {str << "Fp";}
                        TABLEH_ATTRS({{"title", "Finished"}}) {str << "F";}
                        TABLEH_ATTRS({{"title", "EarlyFinished"}}) {str << "EF";}
                        TABLEH_ATTRS({{"title", "Terminated"}}) {str << "T";}
                        TABLEH_ATTRS({{"title", "Aborted"}}) {str << "A";}
                        TABLEH_ATTRS({{"title", "Bound"}}) {str << "B";}
                        TABLEH() {str << "MaxInflightBytes";}
                        TABLEH() {str << "MinInflightBytes";}
                        TABLEH() {str << "InflightBytes";}
                        TABLEH() {str << "WaitQueueBytes";}
                        TABLEH() {str << "SpilledBytes";}
                        TABLEH() {str << "LoadingQueueSize";}
                        TABLEH() {str << "HeadBlobId";}
                        TABLEH() {str << "TailBlobId";}
                        TABLEH() {str << "OutputActorId";}
                        TABLEH() {str << "InputActorId";}
                    }
                }
                TABLEBODY() {
                    for (auto& [nodeId, state] : ChannelService->NodeStates) {
                        for (auto& [info, descriptor] : state->OutputDescriptors) {
                            auto pushBytes = descriptor->PushBytes.load();
                            auto popBytes = descriptor->RemotePopBytes.load();
                            TABLER() {
                                TABLED() {str << info.ChannelId;}
                                TABLED() {str << nodeId;}
                                TABLED() {str << descriptor->Info.SrcStageId;}
                                TABLED() {str << descriptor->Info.DstStageId;}
                                TABLED() {
                                    str << FillLevelToString(descriptor->FillLevel);
                                    if (descriptor->Aggregator) {
                                        str << " (" << FillLevelToString(descriptor->Aggregator->GetFillLevel()) << ")";
                                    }
                                }
                                TABLED() {str << pushBytes;}
                                TABLED() {str << popBytes;}
                                TABLED() {str << descriptor->FinishPushed.load();}
                                TABLED() {str << descriptor->Finished.load();}
                                TABLED() {str << descriptor->EarlyFinished.load();}
                                TABLED() {str << descriptor->Terminated.load();}
                                TABLED() {str << descriptor->Aborted.load();}
                                TABLED() {str << descriptor->IsBound;}
                                TABLED() {str << descriptor->MaxInflightBytes;}
                                TABLED() {str << descriptor->MinInflightBytes;}
                                TABLED() {str << (pushBytes - popBytes);}
                                TABLED() {str << descriptor->WaitQueueBytes.load();}
                                TABLED() {str << descriptor->SpilledBytes.load();}
                                TABLED() {str << descriptor->LoadingQueue.size();}
                                TABLED() {str << descriptor->HeadBlobId;}
                                TABLED() {str << descriptor->TailBlobId;}
                                TABLED() {
                                    HREF(TStringBuilder() << "/node/" << info.OutputActorId.NodeId() << "/actors/kqp_node?ca=" << info.OutputActorId)  {
                                        str << info.OutputActorId;
                                    }
                                }
                                TABLED() {
                                    HREF(TStringBuilder() << "/node/" << info.InputActorId.NodeId() << "/actors/kqp_node?ca=" << info.InputActorId)  {
                                        str << info.InputActorId;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            str << Endl << "Input Descriptors:";

            TABLE_SORTABLE_CLASS ("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH_ATTRS({{"title", "ChannelId"}}) {str << "Id";}
                        TABLEH() {str << "PeerNodeId";}
                        TABLEH_ATTRS({{"title", "SrcStageId"}}) {str << "Src";}
                        TABLEH_ATTRS({{"title", "DstStageId"}}) {str << "Dst";}
                        TABLEH() {str << "PushBytes";}
                        TABLEH() {str << "QueueSize";}
                        TABLEH() {str << "QueueBytes";}
                        TABLEH() {str << "PopBytes";}
                        TABLEH_ATTRS({{"title", "FinishPushed"}}) {str << "Fp";}
                        TABLEH_ATTRS({{"title", "EarlyFinished"}}) {str << "EF";}
                        TABLEH_ATTRS({{"title", "Finished"}}) {str << "F";}
                        TABLEH_ATTRS({{"title", "Bound"}}) {str << "B";}
                        TABLEH() {str << "OutputActorId";}
                        TABLEH() {str << "InputActorId";}
                    }
                }
                TABLEBODY() {
                    for (auto& [nodeId, state] : ChannelService->NodeStates) {
                        for (auto& [info, descriptor] : state->InputDescriptors) {
                            TABLER() {
                                TABLED() {str << info.ChannelId;}
                                TABLED() {str << nodeId;}
                                TABLED() {str << descriptor->Info.SrcStageId;}
                                TABLED() {str << descriptor->Info.DstStageId;}
                                TABLED() {str << descriptor->PushStats.Bytes.load();}
                                TABLED() {str << descriptor->QueueSize.load();}
                                TABLED() {str << descriptor->QueueBytes.load();}
                                TABLED() {str << descriptor->PopStats.Bytes.load();}
                                TABLED() {str << descriptor->FinishPushed.load();}
                                TABLED() {str << descriptor->EarlyFinished.load();}
                                TABLED() {str << descriptor->Finished.load();}
                                TABLED() {str << descriptor->IsBound;}
                                TABLED() {
                                    HREF(TStringBuilder() << "/node/" << info.OutputActorId.NodeId() << "/actors/kqp_node?ca=" << info.OutputActorId)  {
                                        str << info.OutputActorId;
                                    }
                                }
                                TABLED() {
                                    HREF(TStringBuilder() << "/node/" << info.InputActorId.NodeId() << "/actors/kqp_node?ca=" << info.InputActorId)  {
                                        str << info.InputActorId;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Send(ev->Sender, new NActors::NMon::TEvHttpInfoRes(str.Str()));
}

NActors::IActor* CreateLocalChannelServiceActor(NActors::TActorSystem* actorSystem, ui32 nodeId,
        NMonitoring::TDynamicCounterPtr counters, const TDqChannelLimits& limits,
        ui32 poolId, std::shared_ptr<IDqChannelService>& service) {
    auto channelService = std::make_shared<TDqChannelService>(actorSystem, nodeId, counters, limits, poolId);
    channelService->Self = channelService;
    service = channelService;
    return new TChannelServiceActor(channelService);
}

} // namespace NYql::NDq
