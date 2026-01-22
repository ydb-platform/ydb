#include "write_session.h"

#include <ydb/public/sdk/cpp/src/client/topic/common/log_lazy.h>
#include <ydb/public/sdk/cpp/src/client/topic/common/simple_blocking_helpers.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <util/digest/murmur.h>

#include <library/cpp/threading/future/wait/wait.h>


namespace NYdb::inline Dev::NTopic {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TWriteSession

TWriteSession::TWriteSession(
        const TWriteSessionSettings& settings,
         std::shared_ptr<TTopicClient::TImpl> client,
         std::shared_ptr<TGRpcConnectionsImpl> connections,
         TDbDriverStatePtr dbDriverState)
    : TContextOwner(settings, std::move(client), std::move(connections), std::move(dbDriverState)) {
}

void TWriteSession::Start(const TDuration& delay) {
    TryGetImpl()->Start(delay);
}

NThreading::TFuture<uint64_t> TWriteSession::GetInitSeqNo() {
    return TryGetImpl()->GetInitSeqNo();
}

std::optional<TWriteSessionEvent::TEvent> TWriteSession::GetEvent(bool block) {
    return TryGetImpl()->EventsQueue->GetEvent(block);
}

std::vector<TWriteSessionEvent::TEvent> TWriteSession::GetEvents(bool block, std::optional<size_t> maxEventsCount) {
    return TryGetImpl()->EventsQueue->GetEvents(block, maxEventsCount);
}

NThreading::TFuture<void> TWriteSession::WaitEvent() {
    return TryGetImpl()->EventsQueue->WaitEvent();
}

void TWriteSession::WriteEncoded(TContinuationToken&& token, std::string_view data, ECodec codec, ui32 originalSize,
                                 std::optional<uint64_t> seqNo, std::optional<TInstant> createTimestamp) {
    auto message = TWriteMessage::CompressedMessage(std::move(data), codec, originalSize);
    if (seqNo.has_value())
        message.SeqNo(*seqNo);
    if (createTimestamp.has_value())
        message.CreateTimestamp(*createTimestamp);
    TryGetImpl()->WriteInternal(std::move(token), std::move(message));
}

void TWriteSession::WriteEncoded(TContinuationToken&& token, TWriteMessage&& message,
                                 TTransactionBase* tx)
{
    if (tx) {
        message.Tx(*tx);
    }
    TryGetImpl()->WriteInternal(std::move(token), std::move(message));
}

void TWriteSession::Write(TContinuationToken&& token, std::string_view data, std::optional<uint64_t> seqNo,
                          std::optional<TInstant> createTimestamp) {
    TWriteMessage message{std::move(data)};
    if (seqNo.has_value())
        message.SeqNo(*seqNo);
    if (createTimestamp.has_value())
        message.CreateTimestamp(*createTimestamp);
    TryGetImpl()->WriteInternal(std::move(token), std::move(message));
}

void TWriteSession::Write(TContinuationToken&& token, TWriteMessage&& message,
                          TTransactionBase* tx) {
    if (tx) {
        message.Tx(*tx);
    }
    TryGetImpl()->WriteInternal(std::move(token), std::move(message));
}

bool TWriteSession::Close(TDuration closeTimeout) {
    return TryGetImpl()->Close(closeTimeout);
}

TWriteSession::~TWriteSession() {
    TryGetImpl()->Close(TDuration::Zero());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TKeyedWriteSession

TKeyedWriteSession::TKeyedWriteSession(
    const TKeyedWriteSessionSettings& settings,
    std::shared_ptr<TTopicClient::TImpl> client,
    std::shared_ptr<TGRpcConnectionsImpl> connections,
    TDbDriverStatePtr dbDriverState
): Connections(connections), Client(client), DbDriverState(dbDriverState), Settings(settings), MemoryUsage(0) {    
    if (settings.ProducerIdPrefix_.empty()) {
        ythrow TContractViolation("ProducerIdPrefix is required for KeyedWriteSession");
    }

    if (!settings.ProducerId_.empty()) {
        ythrow TContractViolation("ProducerId should be empty for KeyedWriteSession, use ProducerIdPrefix instead");
    }

    if (!settings.MessageGroupId_.empty()) {
        ythrow TContractViolation("MessageGroupId should be empty for KeyedWriteSession");
    }
    
    TDescribeTopicSettings describeTopicSettings;
    auto topicConfig = client->DescribeTopic(settings.Path_, describeTopicSettings).GetValueSync();
    const auto& partitions = topicConfig.GetTopicDescription().GetPartitions();
    auto partitionChooserStrategy = settings.PartitionChooserStrategy_;

    ui64 idx = 0;
    switch (partitionChooserStrategy) {
    case TKeyedWriteSessionSettings::EPartitionChooserStrategy::Bound:
        PartitioningKeyHasher = settings.PartitioningKeyHasher_;
        for (const auto& partition : partitions) {
            if (idx > 0 && !partition.GetFromBound().has_value() && !partition.GetToBound().has_value()) {
                Y_ABORT("Unbounded partition is not supported for Bound partition chooser strategy");
            }

            Partitions.push_back(
                TPartitionInfo().
                PartitionId(partition.GetPartitionId()).
                FromBound(TPartitionBound().Value(partition.GetFromBound())).
                ToBound(TPartitionBound().Value(partition.GetToBound())));
        }

        PartitionChooser = std::make_unique<TBoundPartitionChooser>(this);
        for (size_t i = 0; i < Partitions.size(); ++i) {
            PartitionsIndex[TPartitionBound().Value(Partitions[i].FromBound_.Value_)] = i;
        }
        break;        
    case TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash:
        for (const auto& partition : partitions) {
            Partitions.push_back(
                TPartitionInfo().
                PartitionId(partition.GetPartitionId()));
        }
        PartitionChooser = std::make_unique<THashPartitionChooser>(this);
        break;
    default:
        Y_ABORT("Unreachable");
    }

    MessagesNotEmptyPromise = NThreading::NewPromise();
    MessagesNotEmptyFuture = MessagesNotEmptyPromise.GetFuture();
    ClosePromise = NThreading::NewPromise();
    CloseFuture = ClosePromise.GetFuture();

    Futures.reserve(Partitions.size());
    // Initialize per-partition futures to a valid, non-ready future to avoid TFutures being uninitialized
    // (NThreading::WaitAny throws on uninitialized futures).
    for (size_t i = 0; i < Partitions.size(); ++i) {
        Futures.push_back(CloseFuture);
    }

    EventsProcessedPromise = NThreading::NewPromise();
    EventsProcessedFuture = EventsProcessedPromise.GetFuture();
    EventsOutputQueue.push_back(TWriteSessionEvent::TReadyToAcceptEvent(IssueContinuationToken()));
    EventsProcessedPromise.TrySetValue();
    MainWorker = std::thread([this]() { RunMainWorker(); });
}

const std::vector<TKeyedWriteSession::TPartitionInfo>& TKeyedWriteSession::GetPartitions() const {
    return Partitions;
}

TKeyedWriteSession::WrappedWriteSessionPtr TKeyedWriteSession::CreateWriteSession(ui64 partition) {
    auto producerId = std::format("{}_{}", Settings.ProducerIdPrefix_, partition);
    auto alteredSettings = Settings;
    alteredSettings
        .DirectWriteToPartition(true)
        .PartitionId(Partitions[partition].PartitionId_)
        .ProducerId(producerId)
        .MessageGroupId(producerId)
        .MaxMemoryUsage(std::numeric_limits<ui64>::max());
    auto writeSession = std::make_shared<WriteSessionWrapper>(
        Client->CreateWriteSession(alteredSettings),
        partition,
        Settings.SubSessionIdleTimeout_
    );

    SessionsIndex.emplace(partition, writeSession);

    SubscribeToPartition(partition);
    return writeSession;
}

void TKeyedWriteSession::RunEventLoop(ui64 partition, WrappedWriteSessionPtr wrappedSession) {
    while (true) {
        auto event = wrappedSession->Session->GetEvent(false);
        if (!event) {
            break;
        }
        
        if (auto sessionClosedEvent = std::get_if<TSessionClosedEvent>(&*event); sessionClosedEvent) {
            HandleSessionClosedEvent(std::move(*sessionClosedEvent));
            break;
        }

        if (auto readyToAcceptEvent = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
            HandleReadyToAcceptEvent(partition, std::move(*readyToAcceptEvent));
            continue;
        }

        if (auto acksEvent = std::get_if<TWriteSessionEvent::TAcksEvent>(&*event)) {
            HandleAcksEvent(partition, std::move(*acksEvent));
            if (wrappedSession->RemoveFromQueue(1)) {
                IdlerSessions.insert(wrappedSession);
            }
            continue;
        }
    }
}

void TKeyedWriteSession::SubscribeToPartition(ui64 partition) {
    auto sessionIter = SessionsIndex.find(partition);
    Y_ABORT_UNLESS(sessionIter != SessionsIndex.end());

    auto wrappedSession = sessionIter->second;
    auto newFuture = sessionIter->second->Session->WaitEvent();
    Futures[partition] = newFuture;
    newFuture.Subscribe([this, partition](const NThreading::TFuture<void>&) {
        std::lock_guard lock(GlobalLock);
        this->ReadyFutures.insert(partition);
    });
}

TKeyedWriteSession::WrappedWriteSessionPtr TKeyedWriteSession::GetWriteSession(ui64 partition) { 
    auto sessionIter = SessionsIndex.find(partition);    
    if (sessionIter == SessionsIndex.end()) {
        return CreateWriteSession(partition);
    }

    if (sessionIter->second->IsExpired()) {
        DestroyWriteSession(sessionIter, TDuration::Zero());
        return CreateWriteSession(partition);
    }

    return sessionIter->second;
}

void TKeyedWriteSession::SaveMessage(TWriteMessage&& message, ui64 partition, TTransactionBase* tx) {
    const bool wasEmpty = PendingMessages.empty();
    PendingMessages.push_back(TMessageInfo(std::move(message), partition, tx));

    if (wasEmpty) {
        MessagesNotEmptyPromise.TrySetValue();
    }
}

void TKeyedWriteSession::AddReadyToAcceptEvent() {
    EventsOutputQueue.push_back(TWriteSessionEvent::TReadyToAcceptEvent(IssueContinuationToken()));
    EventsProcessedPromise.TrySetValue();
}

bool TKeyedWriteSession::IsMemoryUsageOK() const {
    return MemoryUsage < Settings.MaxMemoryUsage_ / 2;
}

void TKeyedWriteSession::Write(TContinuationToken&&, const std::string& key, TWriteMessage&& message, TTransactionBase* tx) {
    std::lock_guard lock(GlobalLock);
    if (Closed.load()) {
        return;
    }

    MemoryUsage += message.Data.size();
    auto partition = PartitionChooser->ChoosePartition(key);
    SaveMessage(std::move(message), partition, tx);

    if (IsMemoryUsageOK()) {
        AddReadyToAcceptEvent();
    }
}

void TKeyedWriteSession::HandleAcksEvent(ui64 partition, TWriteSessionEvent::TEvent&& event) {
    auto [queueIt, _] = PartitionsEventQueues.try_emplace(partition, std::list<TWriteSessionEvent::TEvent>());
    queueIt->second.push_back(std::move(event));
}

void TKeyedWriteSession::HandleReadyToAcceptEvent(ui64 partition, TWriteSessionEvent::TReadyToAcceptEvent&& event) {
    auto [queueIt, _] = ContinuationTokens.try_emplace(partition, std::deque<TContinuationToken>());
    queueIt->second.push_back(std::move(event.ContinuationToken));
}

void TKeyedWriteSession::HandleSessionClosedEvent(TSessionClosedEvent&& event) {
    if (event.IsSuccess()) {
        return;
    }

    if (!CloseEvent.has_value()) {
        CloseEvent = std::move(event);
    }
    NonBlockingClose();
}

bool TKeyedWriteSession::IsQueueEmpty() {
    std::lock_guard lock(GlobalLock);
    return InFlightMessages.empty() && PendingMessages.empty();
}

bool TKeyedWriteSession::Close(TDuration closeTimeout) {
    if (Closed.exchange(true)) {
        return IsQueueEmpty();
    }

    SetCloseDeadline(closeTimeout);

    ClosePromise.TrySetValue();
    if (!MainWorker.joinable()) {
        return IsQueueEmpty();
    }

    if (MainWorker.get_id() == std::this_thread::get_id()) {
        MainWorker.detach();
    } else {
        MainWorker.join();
    }

    return IsQueueEmpty();
}

void TKeyedWriteSession::AddSessionClosedEvent() {
    if (!Closed.load()) {
        return;
    }

    if (!CloseEvent.has_value()) {
        CloseEvent = TSessionClosedEvent(EStatus::SUCCESS, {});
    }

    if (EventsOutputQueue.empty() && InFlightMessages.empty() && PendingMessages.empty()) {
        EventsOutputQueue.push_back(*CloseEvent);
        EventsProcessedPromise.TrySetValue();
    }
}

void TKeyedWriteSession::NonBlockingClose() {
    Closed.store(true);
    ClosePromise.TrySetValue();
}

void TKeyedWriteSession::SetCloseDeadline(const TDuration& closeTimeout) {
    std::lock_guard lock(GlobalLock);
    CloseDeadline = TInstant::Now() + closeTimeout;
}

TKeyedWriteSession::~TKeyedWriteSession() {
    if (MainWorker.joinable()) {
        Close(TDuration::Zero());
    }
}

void TKeyedWriteSession::DestroyWriteSession(TSessionsIndexIterator& it, const TDuration& closeTimeout, bool alreadyClosed) {
    if (it == SessionsIndex.end() || !it->second) {
        return;
    }

    if (!alreadyClosed) {
        it->second->Session->Close(closeTimeout);
    }

    auto partition = it->second->Partition;
    ReadyFutures.erase(partition);
    Futures[partition] = CloseFuture;
    it = SessionsIndex.erase(it);
}

NThreading::TFuture<void> TKeyedWriteSession::WaitEvent() {
    std::lock_guard lock(GlobalLock);

    if (!EventsOutputQueue.empty()) {
        return NThreading::MakeFuture();
    }

    if (EventsProcessedFuture.IsReady()) {
        EventsProcessedPromise = NThreading::NewPromise();
        EventsProcessedFuture = EventsProcessedPromise.GetFuture();
    }

    return EventsProcessedFuture;
}

void TKeyedWriteSession::TransferEventsToOutputQueue() {
    bool eventsTransferred = false;
    bool shouldAddReadyToAcceptEvent = false;
    std::unordered_map<ui64, std::deque<TWriteSessionEvent::TWriteAck>> acks;

    auto buildOutputAckEvent = [](std::deque<TWriteSessionEvent::TWriteAck>& acksQueue) -> TWriteSessionEvent::TAcksEvent {
        TWriteSessionEvent::TAcksEvent ackEvent;
        auto ack = acksQueue.front();
        ackEvent.Acks.push_back(std::move(ack));
        acksQueue.pop_front();
        return ackEvent;
    };

    while (!InFlightMessages.empty()) {
        const auto& head = InFlightMessages.front();

        auto remainingAcks = acks.find(head.Partition);
        if (remainingAcks != acks.end() && remainingAcks->second.size() > 0) {
            EventsOutputQueue.push_back(buildOutputAckEvent(remainingAcks->second));
            InFlightMessages.pop_front();
            continue;
        }
        
        const auto& eventsQueueIt = PartitionsEventQueues.find(head.Partition);
        if (eventsQueueIt == PartitionsEventQueues.end() || eventsQueueIt->second.empty()) {
            // No events for this message yet, stop processing (preserve order)
            break;
        }
        
        auto event = std::move(eventsQueueIt->second.front());
        auto acksEvent = std::get_if<TWriteSessionEvent::TAcksEvent>(&event);
        Y_ABORT_UNLESS(acksEvent, "Expected AcksEvent only in PartitionsEventQueues");

        std::deque<TWriteSessionEvent::TWriteAck> acksQueue;
        std::copy(acksEvent->Acks.begin(), acksEvent->Acks.end(), std::back_inserter(acksQueue));
        EventsOutputQueue.push_back(buildOutputAckEvent(acksQueue));
        acks[head.Partition] = std::move(acksQueue);
        eventsQueueIt->second.pop_front();
        eventsTransferred = true;

        bool wasMemoryUsageOk = IsMemoryUsageOK();
        MemoryUsage -= head.Message.Data.size();

        // Check if we need to add ReadyToAcceptEvent after removing this message
        if (IsMemoryUsageOK() && !wasMemoryUsageOk) {
            shouldAddReadyToAcceptEvent = true;
        }
       
        InFlightMessages.pop_front();
    }

    if (shouldAddReadyToAcceptEvent) {
        AddReadyToAcceptEvent();
    }

    if (eventsTransferred) {
        EventsProcessedPromise.TrySetValue();
    }
}

void TKeyedWriteSession::WaitSomeAction(std::unique_lock<std::mutex>& lock) {
    std::vector<NThreading::TFuture<void>> futures;
    futures.push_back(EventsProcessedFuture);
    futures.push_back(CloseFuture);
    lock.unlock();
    NThreading::WaitAny(futures).Wait();
    lock.lock();

    if (EventsProcessedFuture.IsReady()) {
        EventsProcessedPromise = NThreading::NewPromise();
        EventsProcessedFuture = EventsProcessedPromise.GetFuture();
    }
}

std::optional<TWriteSessionEvent::TEvent> TKeyedWriteSession::GetEvent(bool block) {
    std::unique_lock lock(GlobalLock);

    if (EventsOutputQueue.empty() && block) {
        WaitSomeAction(lock);
    }

    if (EventsOutputQueue.empty()) {
        return std::nullopt;
    }

    auto event = std::move(EventsOutputQueue.front());
    EventsOutputQueue.pop_front();

    AddSessionClosedEvent();
    return event;
}

std::vector<TWriteSessionEvent::TEvent> TKeyedWriteSession::GetEvents(bool block, std::optional<size_t> maxEventsCount) {
    std::unique_lock lock(GlobalLock);

    while (!Closed.load() && maxEventsCount.has_value() && EventsOutputQueue.size() < maxEventsCount.value() && block) {
        WaitSomeAction(lock);
    }

    std::vector<TWriteSessionEvent::TEvent> events;
    events.reserve(maxEventsCount.value_or(EventsOutputQueue.size()));
    while (!EventsOutputQueue.empty() && events.size() < maxEventsCount.value_or(EventsOutputQueue.size())) {
        auto event = std::move(EventsOutputQueue.front());
        events.push_back(std::move(event));
        EventsOutputQueue.pop_front();
    }

    AddSessionClosedEvent();
    return events;
}

std::optional<TContinuationToken> TKeyedWriteSession::GetContinuationToken(ui64 partition) {
    // Only check under lock, never wait here. Waiting in GetContinuationToken would
    // prevent RunMainWorker from processing ReadyFutures and re-subscribing to
    // partition WaitEvent() futures; without re-subscription we stop receiving new
    // ReadyToAccept events and can deadlock. The caller must WaitForEvents() in the
    // main loop when nullopt is returned.
    std::lock_guard lock(GlobalLock);

    if (!SessionsIndex.contains(partition)) {
        return std::nullopt;
    }

    auto it = ContinuationTokens.find(partition);
    if (it != ContinuationTokens.end() && !it->second.empty()) {
        auto token = std::move(it->second.front());
        it->second.pop_front();
        if (it->second.empty()) {
            ContinuationTokens.erase(it);
        }
        return token;
    }

    return std::nullopt;
}

TDuration TKeyedWriteSession::GetCloseTimeout() {
    std::lock_guard lock(GlobalLock);
    auto now = TInstant::Now();
    if (CloseDeadline <= now) {
        return TDuration::Zero();
    }
    return CloseDeadline - now;
}

void TKeyedWriteSession::WaitForEvents() {
    if (Closed.load()) {
        return;
    }

    auto partitionsWaitFuture = NThreading::WaitAny(Futures);

    std::vector<NThreading::TFuture<void>> futures;
    futures.push_back(partitionsWaitFuture);
    futures.push_back(MessagesNotEmptyFuture);
    futures.push_back(CloseFuture);
    NThreading::WaitAny(futures).Wait();
}

void TKeyedWriteSession::CleanIdleSessions() {
    for (auto it = IdlerSessions.begin(); it != IdlerSessions.end(); it = IdlerSessions.erase(it)) {
        if (!(*it)->IsExpired()) {
            break;
        }
        
        auto sessionIter = SessionsIndex.find((*it)->Partition);
        DestroyWriteSession(sessionIter, TDuration::Zero());
    }
}

void TKeyedWriteSession::HandleReadyFutures(std::unique_lock<std::mutex>& lock) {
    while (!ReadyFutures.empty()) {
        auto idx = *ReadyFutures.begin();
        Y_ABORT_UNLESS(Futures[idx].IsReady(), "Future is not ready");
        RunEventLoop(idx, GetWriteSession(idx));

        ReadyFutures.erase(idx);
        lock.unlock();
        SubscribeToPartition(idx);
        lock.lock();
    }
}

void TKeyedWriteSession::RunMainWorker() {
    while (true) {
        TMessageInfo* msgToSend = nullptr;
        bool didWrite = false;
        
        {
            std::unique_lock lock(GlobalLock);
            HandleReadyFutures(lock);
            TransferEventsToOutputQueue();
            if (Closed.load() && ((InFlightMessages.empty() && PendingMessages.empty()) || CloseDeadline <= TInstant::Now())) {
                break;
            }

            CleanIdleSessions();
            if (!PendingMessages.empty()) {
                msgToSend = &PendingMessages.front();
            }
        }

        WrappedWriteSessionPtr writeSession = nullptr;
        if (msgToSend) {
            auto partition = msgToSend->Partition;
            writeSession = GetWriteSession(partition);
            auto continuationToken = GetContinuationToken(partition);
            if (continuationToken) {
                auto msgToSave = *msgToSend;
                writeSession->Session->Write(std::move(*continuationToken), std::move(msgToSend->Message), msgToSend->Tx);
                InFlightMessages.push_back(std::move(msgToSave));
                didWrite = true;
            }
        }

        std::unique_lock lock(GlobalLock);
        if (didWrite) {
            PendingMessages.pop_front();
            if (writeSession->AddToQueue(1)) {
                IdlerSessions.erase(writeSession);
            }
        }

        if (PendingMessages.empty() || !didWrite) {
            // When waiting for a token (!didWrite with Pending), MessagesNotEmpty is already
            // ready from the push — reset it so WaitAny blocks until a partition Future is
            // ready; otherwise we spin and starve RunEventLoop which produces the token.
            if (PendingMessages.empty() && MessagesNotEmptyFuture.IsReady()) {
                MessagesNotEmptyPromise = NThreading::NewPromise();
                MessagesNotEmptyFuture = MessagesNotEmptyPromise.GetFuture();
            }
            lock.unlock();
            WaitForEvents();
        }
    }

    // Close all sessions and add SessionClosedEvent when all messages are processed
    auto closeTimeout = GetCloseTimeout();
    auto sessionsToClose = SessionsIndex.size();
    for (auto it = SessionsIndex.begin(); it != SessionsIndex.end(); ) {
        DestroyWriteSession(it, closeTimeout / sessionsToClose);
    }

    // Add SessionClosedEvent only if needed
    std::lock_guard lock(GlobalLock);
    AddSessionClosedEvent();
}

TWriterCounters::TPtr TKeyedWriteSession::GetCounters() {
    // what should we return here?
    return nullptr;
}

TKeyedWriteSession::TBoundPartitionChooser::TBoundPartitionChooser(TKeyedWriteSession* session) : Session(session) {}

ui64 TKeyedWriteSession::TBoundPartitionChooser::ChoosePartition(const std::string& key) {
    auto hashedKey = Session->PartitioningKeyHasher(key);

    auto lowerBound = Session->PartitionsIndex.lower_bound(TPartitionBound().Value(hashedKey));
    if (lowerBound == Session->PartitionsIndex.end()) {
        return Session->Partitions.size() - 1;
    }

    Y_ABORT_IF(lowerBound == Session->PartitionsIndex.begin(), "Lower bound is the first element");
    return std::prev(lowerBound)->second;
}

TKeyedWriteSession::THashPartitionChooser::THashPartitionChooser(TKeyedWriteSession* session) : Session(session) {}

ui64 TKeyedWriteSession::THashPartitionChooser::ChoosePartition(const std::string& key) {
    return std::hash<std::string>{}(key) % Session->Partitions.size();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSimpleBlockingWriteSession

TSimpleBlockingWriteSession::TSimpleBlockingWriteSession(
        const TWriteSessionSettings& settings,
        std::shared_ptr<TTopicClient::TImpl> client,
        std::shared_ptr<TGRpcConnectionsImpl> connections,
        TDbDriverStatePtr dbDriverState
) {
    auto subSettings = settings;
    if (settings.EventHandlers_.AcksHandler_) {
        LOG_LAZY(dbDriverState->Log, TLOG_WARNING, "TSimpleBlockingWriteSession: Cannot use AcksHandler, resetting.");
        subSettings.EventHandlers_.AcksHandler({});
    }
    if (settings.EventHandlers_.ReadyToAcceptHandler_) {
        LOG_LAZY(dbDriverState->Log, TLOG_WARNING, "TSimpleBlockingWriteSession: Cannot use ReadyToAcceptHandler, resetting.");
        subSettings.EventHandlers_.ReadyToAcceptHandler({});
    }
    if (settings.EventHandlers_.SessionClosedHandler_) {
        LOG_LAZY(dbDriverState->Log, TLOG_WARNING, "TSimpleBlockingWriteSession: Cannot use SessionClosedHandler, resetting.");
        subSettings.EventHandlers_.SessionClosedHandler({});
    }
    if (settings.EventHandlers_.CommonHandler_) {
        LOG_LAZY(dbDriverState->Log, TLOG_WARNING, "TSimpleBlockingWriteSession: Cannot use CommonHandler, resetting.");
        subSettings.EventHandlers_.CommonHandler({});
    }
    Writer = std::make_shared<TWriteSession>(subSettings, client, connections, dbDriverState);
    Writer->Start(TDuration::Zero());
}

uint64_t TSimpleBlockingWriteSession::GetInitSeqNo() {
    return Writer->GetInitSeqNo().GetValueSync();
}

bool TSimpleBlockingWriteSession::Write(
        std::string_view data, std::optional<uint64_t> seqNo, std::optional<TInstant> createTimestamp, const TDuration& blockTimeout
) {
    auto message = TWriteMessage(std::move(data))
        .SeqNo(seqNo)
        .CreateTimestamp(createTimestamp);
    return Write(std::move(message), nullptr, blockTimeout);
}

bool TSimpleBlockingWriteSession::Write(
        TWriteMessage&& message, TTransactionBase* tx, const TDuration& blockTimeout
) {
    auto continuationToken = WaitForToken(blockTimeout);
    if (continuationToken.has_value()) {
        Writer->Write(std::move(*continuationToken), std::move(message), tx);
        return true;
    }
    return false;
}

std::optional<TContinuationToken> TSimpleBlockingWriteSession::WaitForToken(const TDuration& timeout) {
    return NDetail::WaitForToken(*Writer, Closed, timeout);
}

TWriterCounters::TPtr TSimpleBlockingWriteSession::GetCounters() {
    return Writer->GetCounters();
}

bool TSimpleBlockingWriteSession::IsAlive() const {
    return !Closed.load();
}

bool TSimpleBlockingWriteSession::Close(TDuration closeTimeout) {
    Closed.store(true);
    return Writer->Close(std::move(closeTimeout));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSimpleBlockingKeyedWriteSession

TSimpleBlockingKeyedWriteSession::TSimpleBlockingKeyedWriteSession(
    const TKeyedWriteSessionSettings& settings,
    std::shared_ptr<TTopicClient::TImpl> client,
    std::shared_ptr<TGRpcConnectionsImpl> connections,
    TDbDriverStatePtr dbDriverState
): Writer(std::make_shared<TKeyedWriteSession>(settings, client, connections, dbDriverState)) {
    ClosePromise = NThreading::NewPromise();
    CloseFuture = ClosePromise.GetFuture();
}

void TSimpleBlockingKeyedWriteSession::RunEventLoop() {
    while (true) {
        auto event = Writer->GetEvent(false);
        if (!event) {
            break;
        }

        if (auto readyToAcceptEvent = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
            ContinuationTokensQueue.push(std::move(readyToAcceptEvent->ContinuationToken));
            continue;
        }
        if (std::get_if<TSessionClosedEvent>(&*event)) {
            Closed.store(true);
            return;
        }
        if (auto acksEvent = std::get_if<TWriteSessionEvent::TAcksEvent>(&*event)) {
            HandleAcksEvent(std::move(*acksEvent));
        }
    }
}

void TSimpleBlockingKeyedWriteSession::HandleAcksEvent(const TWriteSessionEvent::TAcksEvent& acksEvent) {
    for (auto ack : acksEvent.Acks) {
        AckedSeqNos.insert(ack.SeqNo);
    }
}

template<typename F>
bool TSimpleBlockingKeyedWriteSession::Wait(const TDuration& timeout, F&& stopFunc) {
    std::unique_lock lock(Lock);

    auto deadline = TInstant::Now() + timeout;
    auto remainingTime = timeout;
    while (true) {
        if (TInstant::Now() > deadline) {
            return false;
        }

        RunEventLoop();

        if (stopFunc()) {
            return true;
        }

        if (Closed.load()) {
            return false;
        }

        std::vector<NThreading::TFuture<void>> futures;
        futures.push_back(CloseFuture);
        futures.push_back(Writer->WaitEvent());
        lock.unlock();
        NThreading::WaitAny(futures).Wait(remainingTime);
        lock.lock();
        remainingTime = deadline - TInstant::Now();
    }
}

std::optional<TContinuationToken> TSimpleBlockingKeyedWriteSession::GetContinuationToken(const TDuration& timeout) {
    std::optional<TContinuationToken> token;

    Wait(timeout, [&]() {
        if (!ContinuationTokensQueue.empty()) {
            token = std::move(ContinuationTokensQueue.front());
            ContinuationTokensQueue.pop();
            return true;
        }
        return false;
    });

    return token;
}

bool TSimpleBlockingKeyedWriteSession::WaitForAck(ui64 seqNo, const TDuration& timeout) {
    return Wait(timeout, [&]() {
        if (AckedSeqNos.contains(seqNo)) {
            AckedSeqNos.erase(seqNo);
            return true;
        }
        return false;
    });
}

bool TSimpleBlockingKeyedWriteSession::Write(const std::string& key, TWriteMessage&& message, TTransactionBase* tx, const TDuration& blockTimeout) {
    auto continuationToken = GetContinuationToken(blockTimeout);
    if (!continuationToken) {
        return false;
    }

    ui64 seqNo = message.SeqNo_.value_or(0);
    Writer->Write(std::move(*continuationToken), std::move(key), std::move(message), tx);
    return WaitForAck(seqNo, blockTimeout);
}

bool TSimpleBlockingKeyedWriteSession::Close(TDuration closeTimeout) {
    {
        std::lock_guard lock(Lock);
        Closed.store(true);
        ClosePromise.TrySetValue();
    }
    return Writer->Close(closeTimeout);
}

TWriterCounters::TPtr TSimpleBlockingKeyedWriteSession::GetCounters() {
    return nullptr;
}

} // namespace NYdb::NTopic
