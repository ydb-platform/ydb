#include "write_session.h"
#include "util.h"

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
    TDescribeTopicSettings describeTopicSettings;
    auto topicConfig = client->DescribeTopic(settings.Path_, describeTopicSettings).GetValueSync();
    const auto& partitions = topicConfig.GetTopicDescription().GetPartitions();
    auto partitionChooserStrategy = settings.PartitionChooserStrategy_;

    ui64 idx = 0;
    switch (partitionChooserStrategy) {
    case TKeyedWriteSessionSettings::EPartitionChooserStrategy::Bound:
        PartitioningKeyHasher = settings.PartitioningKeyHasher_;
        for (const auto& partition : partitions) {
            Partitions.push_back(
                TPartitionInfo().
                PartitionId(partition.GetPartitionId()).
                Bounded(true).
                FromBound(TPartitionBound().Value(partition.GetFromBound())).
                ToBound(TPartitionBound().Value(partition.GetToBound())));

            PartitionsPrimaryIndex[partition.GetPartitionId()] = idx;
            PartitionsIndex[TPartitionBound().Value(partition.GetFromBound())] = idx++;
        }
        AddPartitionsBounds();
        PartitionChooser = std::make_unique<TBoundPartitionChooser>(this);
        break;        
    case TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash:
        for (const auto& partition : partitions) {
            Partitions.push_back(
                TPartitionInfo().
                PartitionId(partition.GetPartitionId()).
                Bounded(false));

            PartitionsPrimaryIndex[partition.GetPartitionId()] = idx;
            PartitionsIndex[TPartitionBound().Value(partition.GetFromBound())] = idx++;
        }
        PartitionChooser = std::make_unique<THashPartitionChooser>(this);
        break;
    default:
        Y_ABORT("Unreachable");
    }

    // Futures layout:
    // 0..Partitions.size(): per-partition WaitEvent futures
    // Partitions.size(): close future
    // Partitions.size() + 1: events-processed future
    Futures.resize(2 + Partitions.size());
    CloseFutureIndex = Partitions.size();
    MessagesNotEmptyFutureIndex = Partitions.size() + 1;
    MessagesNotEmptyPromise = NThreading::NewPromise();
    Futures[MessagesNotEmptyFutureIndex] = MessagesNotEmptyPromise.GetFuture();

    ClosePromise = NThreading::NewPromise();
    CloseFuture = ClosePromise.GetFuture();

    Futures[CloseFutureIndex] = CloseFuture;

    // Initialize per-partition futures to a valid, non-ready future to avoid TFutures being uninitialized
    // (NThreading::WaitAny throws on uninitialized futures).
    for (size_t i = 0; i < Partitions.size(); ++i) {
        Futures[i] = CloseFuture;
    }

    EventsProcessedPromise = NThreading::NewPromise();
    EventsProcessedFuture = EventsProcessedPromise.GetFuture();
    
    MessageSenderWorker = std::thread([this]() { RunMessageSender(); });

    // Initial token to let the user start writing right away (contract: tokens come from ReadyToAccept events).
    {
        std::lock_guard lock(GlobalLock);
        EventsOutputQueue.push_back(TWriteSessionEvent::TReadyToAcceptEvent(IssueContinuationToken()));
        if (EventsProcessedFuture.Initialized() && !EventsProcessedFuture.IsReady()) {
            EventsProcessedPromise.TrySetValue();
        }
    }
}

void TKeyedWriteSession::AddPartitionsBounds() {
    Y_ABORT_UNLESS(Partitions.size() > 0, "Partitions should be initialized");
    if (Partitions[0].Bounded_ || Partitions.size() == 1) {
        return;
    }

    std::string prevBound;
    auto partitionCount = Partitions.size();
    for (ui32 i = 0; i < partitionCount; ++i) {
        Partitions[i].Bounded(true);
        if (i > 0) {
            Partitions[i].FromBound(TPartitionBound().Value(prevBound));
        }

        if (i != (partitionCount - 1)) {
            auto range = RangeFromShardNumber(i, partitionCount);
            auto toBound = AsKeyBound(range.second);
            Partitions[i].ToBound(TPartitionBound().Value(toBound));
            prevBound = toBound;
        }
    }
}

void TKeyedWriteSession::AddReadyFuture(ui64 index) {
    std::lock_guard lock(ReadyFuturesLock);
    ReadyFutures.push_back(index);
}

TKeyedWriteSession::WrappedWriteSessionPtr TKeyedWriteSession::CreateWriteSession(ui64 partitionId) {
    CleanExpiredSessions();

    auto producerId = std::format("{}_{}", Settings.ProducerId_, partitionId);
    auto alteredSettings = Settings;
    alteredSettings.DirectWriteToPartition(true);
    alteredSettings.PartitionId(partitionId);  
    alteredSettings.ProducerId(producerId);
    alteredSettings.MessageGroupId(producerId);
    alteredSettings.MaxMemoryUsage(std::numeric_limits<ui64>::max());
    auto writeSession = std::make_shared<WriteSessionWrapper>(WriteSessionWrapper{
        .Session = Client->CreateWriteSession(alteredSettings),
        .PartitionId = partitionId,
        .ExpirationTime = TInstant::Now() + Settings.SubSessionIdleTimeout_,
    });

    WrappedWriteSessionPtr resultSession = nullptr;
    {
        std::unique_lock lock(GlobalLock);
        auto [it, inserted] = SessionsIndex.try_emplace(partitionId, writeSession);
        resultSession = inserted ? writeSession : it->second;
    }

    auto partitionIndexIt = PartitionsPrimaryIndex.find(partitionId);
    auto partitionIndex = partitionIndexIt->second;
    auto self = weak_from_this();

    Futures[partitionIndex] = resultSession->Session->WaitEvent();
    Futures[partitionIndex].Subscribe([self, partitionId](const NThreading::TFuture<void>&) {
        if (auto s = self.lock()) {
            s->RunEventLoop(partitionId);
        }
    });

    return resultSession;
}

bool TKeyedWriteSession::RunEventLoop(ui64 partitionId) {
    std::unique_lock lock(GlobalLock);

    auto sessionIter = SessionsIndex.find(partitionId);
    if (sessionIter == SessionsIndex.end()) {
        return true;
    }

    bool gotSessionClosedEvent = false;
    bool wasAck = false;
    while (true) {
        auto event = sessionIter->second->Session->GetEvent(false);
        if (!event) {
            break;
        }

        if (auto sessionClosedEvent = std::get_if<TSessionClosedEvent>(&*event); sessionClosedEvent) {
            HandleSessionClosedEvent(std::move(*sessionClosedEvent));
            gotSessionClosedEvent = true;
            break;
        }

        if (auto readyToAcceptEvent = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
            HandleReadyToAcceptEvent(partitionId, *readyToAcceptEvent);
            continue;
        }

        wasAck = true;
        HandleAcksEvent(partitionId, *event);
    }

    auto partitionIndex = PartitionsPrimaryIndex.find(partitionId);
    Y_ABORT_UNLESS(partitionIndex != PartitionsPrimaryIndex.end());
    if (!gotSessionClosedEvent) {
        Futures[partitionIndex->second] = sessionIter->second->Session->WaitEvent();
        {
            auto self = weak_from_this();
            ui64 capturedPartitionId = partitionId; // Capture partitionId, not index

            lock.unlock();
            Futures[partitionIndex->second].Subscribe([self, capturedPartitionId](const NThreading::TFuture<void>&) {
                if (auto s = self.lock()) {
                    s->RunEventLoop(capturedPartitionId);
                }
            });
            lock.lock();
        }
    } else {
        Futures[partitionIndex->second] = CloseFuture;
    }

    if (wasAck) {
        TransferEventsToOutputQueue();
    }
    return gotSessionClosedEvent;
}

TKeyedWriteSession::WrappedWriteSessionPtr TKeyedWriteSession::GetWriteSession(ui64 partitionId) { 
    auto sessionIter = SessionsIndex.find(partitionId);    
    if (sessionIter == SessionsIndex.end()) {
        return CreateWriteSession(partitionId);
    }

    if (sessionIter->second->IsExpired()) {
        DestroyWriteSession(sessionIter, TDuration::Zero());
        return CreateWriteSession(partitionId);
    }

    return sessionIter->second;
}

void TKeyedWriteSession::SaveMessage(TWriteMessage&& message, ui64 partitionId, TTransactionBase* tx) {
    const bool wasEmpty = PendingMessages.empty();
    PendingMessages.push_back(TMessageInfo(std::move(message), partitionId, tx));

    if (wasEmpty) {
        MessagesNotEmptyPromise.TrySetValue();
    }
}

void TKeyedWriteSession::AddReadyToAcceptEvent() {
    EventsOutputQueue.push_back(TWriteSessionEvent::TReadyToAcceptEvent(IssueContinuationToken()));
    if (EventsProcessedFuture.Initialized() && !EventsProcessedFuture.IsReady()) {
        EventsProcessedPromise.TrySetValue();
    }
}

void TKeyedWriteSession::Write(TContinuationToken&&, const std::string& key, TWriteMessage&& message, TTransactionBase* tx) {
    std::lock_guard lock(GlobalLock);
    if (Closed.load()) {
        return;
    }

    MemoryUsage += message.Data.size();
    const auto& partitionInfo = PartitionChooser->ChoosePartition(key);
    SaveMessage(std::move(message), partitionInfo.PartitionId_, tx);

    if (MemoryUsage < Settings.MaxMemoryUsage_ / 2) {
        AddReadyToAcceptEvent();
    }
}

void TKeyedWriteSession::HandleAcksEvent(ui64 partitionId, TWriteSessionEvent::TEvent& event) {
    auto [queueIt, _] = PartitionsEventQueues.try_emplace(partitionId, std::list<TWriteSessionEvent::TEvent>());
    queueIt->second.push_back(std::move(event));
}

void TKeyedWriteSession::HandleReadyToAcceptEvent(ui64 partitionId, TWriteSessionEvent::TReadyToAcceptEvent& event) {
    ContinuationTokens.emplace(partitionId, std::move(event.ContinuationToken));
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

void TKeyedWriteSession::CleanExpiredSessions() {
    ui64 cleaned = 0;
    for (auto it = SessionsIndex.begin(); it != SessionsIndex.end() && cleaned < MAX_CLEANED_SESSIONS_COUNT; ) {
        if (cleaned > MAX_CLEANED_SESSIONS_COUNT) {
            break;
        }

        if (!it->second->IsExpired()) {
            ++it;
            continue;
        }
        DestroyWriteSession(it, TDuration::Zero());
        ++cleaned;
    }
}

bool TKeyedWriteSession::Close(TDuration closeTimeout) {
    const bool wasClosed = Closed.exchange(true);
    if (wasClosed) {
        return false;
    }

    {
        std::lock_guard lock(GlobalLock);
        CloseTimeout = closeTimeout;
    }

    ClosePromise.TrySetValue();
    if (!MessageSenderWorker.joinable()) {
        return true;
    }

    if (MessageSenderWorker.get_id() == std::this_thread::get_id()) {
        MessageSenderWorker.detach();
    } else {
        MessageSenderWorker.join();
    }

    return true;
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
    }
}

void TKeyedWriteSession::NonBlockingClose() {
    Closed.store(true);
    ClosePromise.TrySetValue();
}

TKeyedWriteSession::~TKeyedWriteSession() {
    if (MessageSenderWorker.joinable()) {
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

    RunEventLoop(it->second->PartitionId);

    auto partitionIndex = PartitionsPrimaryIndex.find(it->second->PartitionId);
    Y_ABORT_UNLESS(partitionIndex != PartitionsPrimaryIndex.end());

    Futures[partitionIndex->second] = CloseFuture;
    it = SessionsIndex.erase(it);
}

NThreading::TFuture<void> TKeyedWriteSession::WaitEvent() {
    std::lock_guard lock(GlobalLock);

    if (!EventsOutputQueue.empty()) {
        return NThreading::MakeFuture();
    }

    if (!EventsProcessedFuture.Initialized() || EventsProcessedFuture.IsReady()) {
        EventsProcessedPromise = NThreading::NewPromise();
        EventsProcessedFuture = EventsProcessedPromise.GetFuture();
    }

    return EventsProcessedFuture;
}

void TKeyedWriteSession::TransferEventsToOutputQueue() {
    bool hasEvents = false;
    bool shouldAddReadyEvent = false;
    std::unordered_map<ui64, std::deque<TWriteSessionEvent::TWriteAck>> acks;

    auto buildOutputAckEvent = [](std::deque<TWriteSessionEvent::TWriteAck>& acksQueue) -> TWriteSessionEvent::TAcksEvent {
        TWriteSessionEvent::TAcksEvent ackEvent;
        auto ack = acksQueue.front();
        ackEvent.Acks.push_back(std::move(ack));
        acksQueue.pop_front();
        return ackEvent;
    };

    while (true) {
        if (InFlightMessages.empty()) {
            break;
        }

        const auto& head = InFlightMessages.front();

        auto remainingAcksCount = acks.find(head.PartitionId);
        if (remainingAcksCount != acks.end() && remainingAcksCount->second.size() > 0) {
            EventsOutputQueue.push_back(buildOutputAckEvent(remainingAcksCount->second));
            InFlightMessages.pop_front();
            continue;
        }

        const auto& eventsQueueIt = PartitionsEventQueues.find(head.PartitionId);
        if (eventsQueueIt == PartitionsEventQueues.end() || eventsQueueIt->second.empty()) {
            // No events for this message yet, stop processing (preserve order)
            break;
        }
        
        auto event = std::move(eventsQueueIt->second.front());
        auto acksEvent = std::get_if<TWriteSessionEvent::TAcksEvent>(&event);
        if (!acksEvent) {
            continue;
        }

        std::deque<TWriteSessionEvent::TWriteAck> acksQueue;
        std::copy(acksEvent->Acks.begin(), acksEvent->Acks.end(), std::back_inserter(acksQueue));
        EventsOutputQueue.push_back(buildOutputAckEvent(acksQueue));
        acks[head.PartitionId] = std::move(acksQueue);
        eventsQueueIt->second.pop_front();
        hasEvents = true;

        bool wasMemoryUsageOk = MemoryUsage < Settings.MaxMemoryUsage_;
        MemoryUsage -= head.Message.Data.size();

        // Check if we need to add ReadyToAcceptEvent after removing this message
        if (MemoryUsage < Settings.MaxMemoryUsage_ / 2 && !wasMemoryUsageOk) {
            shouldAddReadyEvent = true;
        }

        InFlightMessages.pop_front();
    }

    if (shouldAddReadyEvent) {
        AddReadyToAcceptEvent();
    }

    if (hasEvents && !EventsProcessedFuture.IsReady()) {
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

std::optional<TContinuationToken> TKeyedWriteSession::GetContinuationToken(ui64 partitionId) {
    while (true) {
        std::unique_lock lock(GlobalLock);
        auto it = ContinuationTokens.find(partitionId);
        if (it != ContinuationTokens.end()) {
            auto token = std::move(it->second);
            ContinuationTokens.erase(it);
            return token;
        }

        if (!SessionsIndex.contains(partitionId)) {
            return std::nullopt;
        }

        lock.unlock();
        WaitForEvents();
    }
}

void TKeyedWriteSession::WaitForEvents() {
    if (Closed.load()) {
        NThreading::WaitAny(Futures).Wait(CloseTimeout);
        return;
    }

    NThreading::WaitAny(Futures).Wait();
}

void TKeyedWriteSession::RunMessageSender() {
    while (true) {
        TMessageInfo* msgToSend = nullptr;
        bool shouldWait = false;

        {
            std::lock_guard lock(GlobalLock);
            if (Closed.load() && InFlightMessages.empty() && PendingMessages.empty()) {
                break;
            }

            // to remove close future and messages not empty future when session is closed
            while (Closed.load() && Futures.size() > Partitions.size()) {
                Futures.pop_back();
            }

            if (PendingMessages.empty()) {
                if (Futures.size() > MessagesNotEmptyFutureIndex && (!Futures[MessagesNotEmptyFutureIndex].Initialized() || Futures[MessagesNotEmptyFutureIndex].IsReady())) {
                    MessagesNotEmptyPromise = NThreading::NewPromise();
                    Futures[MessagesNotEmptyFutureIndex] = MessagesNotEmptyPromise.GetFuture();
                }
    
                shouldWait = !Closed.load() && !InFlightMessages.empty();
            } else {
                msgToSend = &PendingMessages.front();
            }
        }

        if (msgToSend) {
            auto partitionId = msgToSend->PartitionId;
            auto writeSession = GetWriteSession(partitionId);
            auto continuationToken = GetContinuationToken(partitionId);
    
            if (!continuationToken) {
                continue;
            }
        
            auto msgToSave = *msgToSend;
            writeSession->Session->Write(std::move(*continuationToken), std::move(msgToSend->Message), msgToSend->Tx);
            PendingMessages.pop_front();
            InFlightMessages.push_back(std::move(msgToSave));
            continue;
        }

        if (shouldWait) {
            WaitForEvents();
        }
    }

    // Close all sessions and add SessionClosedEvent when all messages are processed
    {
        auto sessionsToClose = SessionsIndex.size();
        for (auto it = SessionsIndex.begin(); it != SessionsIndex.end(); ) {
            DestroyWriteSession(it, CloseTimeout / sessionsToClose);
        }
        // Add SessionClosedEvent only if not already added by Close()
        AddSessionClosedEvent();
    }
}

TWriterCounters::TPtr TKeyedWriteSession::GetCounters() {
    // what should we return here?
    return nullptr;
}

TKeyedWriteSession::TBoundPartitionChooser::TBoundPartitionChooser(TKeyedWriteSession* session) : Session(session) {}

const TKeyedWriteSession::TPartitionInfo& TKeyedWriteSession::TBoundPartitionChooser::ChoosePartition(const std::string& key) {
    auto lowerBound = Session->PartitionsIndex.lower_bound(TPartitionBound().Value(key));
    if (lowerBound == Session->PartitionsIndex.end()) {
        Y_ABORT_UNLESS(Session->Partitions.back() < key);
        return Session->Partitions.back();
    }

    Y_ABORT_IF(lowerBound == Session->PartitionsIndex.begin(), "Lower bound is the first element");
    return Session->Partitions[std::prev(lowerBound)->second];
}

TKeyedWriteSession::THashPartitionChooser::THashPartitionChooser(TKeyedWriteSession* session) : Session(session) {}

const TKeyedWriteSession::TPartitionInfo& TKeyedWriteSession::THashPartitionChooser::ChoosePartition(const std::string& key) {
    return Session->Partitions[std::hash<std::string>{}(key) % Session->Partitions.size()];
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
    GotEventsPromise = NThreading::NewPromise();
    GotEventsFuture = GotEventsPromise.GetFuture();
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
        if (auto sessionClosedEvent = std::get_if<TSessionClosedEvent>(&*event)) {
            std::cerr << "Session closed: " << sessionClosedEvent->DebugString() << std::endl;
            Closed.store(true);
            return;
        }
        if (auto acksEvent = std::get_if<TWriteSessionEvent::TAcksEvent>(&*event)) {
            HandleAcksEvent(*acksEvent);
        }
    }
}

void TSimpleBlockingKeyedWriteSession::HandleAcksEvent(const TWriteSessionEvent::TAcksEvent& acksEvent) {
    for (auto ack : acksEvent.Acks) {
        AckedSeqNos.insert(ack.SeqNo);
    }
}

void TSimpleBlockingKeyedWriteSession::RecreateGotEventsPromise() {
    GotEventsPromise.TrySetValue();
    GotEventsPromise = NThreading::NewPromise();
    GotEventsFuture = GotEventsPromise.GetFuture();
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
        
        std::vector<NThreading::TFuture<void>> futures;
        futures.push_back(GotEventsFuture);
        futures.push_back(CloseFuture);
        futures.push_back(Writer->WaitEvent());
        lock.unlock();
        NThreading::WaitAny(futures).Wait(remainingTime);
        lock.lock();

        remainingTime = deadline - TInstant::Now();
        RecreateGotEventsPromise();
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
