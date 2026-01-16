#include "write_session.h"

#include <ydb/public/sdk/cpp/src/client/topic/common/log_lazy.h>
#include <ydb/public/sdk/cpp/src/client/topic/common/simple_blocking_helpers.h>

#include <library/cpp/threading/future/wait/wait.h>

#include <algorithm>

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
): Connections(connections), Client(client), DbDriverState(dbDriverState), Settings(settings) {
    MessagesNotEmptyPromise = NThreading::NewPromise();
    MessagesNotEmptyFuture = MessagesNotEmptyPromise.GetFuture();
    ClosePromise = NThreading::NewPromise();
    CloseFuture = ClosePromise.GetFuture();
    EventsProcessedPromise = NThreading::NewPromise();
    EventsProcessedFuture = EventsProcessedPromise.GetFuture();

    TDescribeTopicSettings describeTopicSettings;
    auto topicConfig = client->DescribeTopic(settings.Path_, describeTopicSettings).GetValueSync();
    const auto& partitions = topicConfig.GetTopicDescription().GetPartitions();
    auto partitionChooserStrategy = settings.PartitionChooserStrategy_;

    if (partitionChooserStrategy == TKeyedWriteSessionSettings::EPartitionChooserStrategy::Auto) {
        partitionChooserStrategy = AutoPartitioningEnabled(topicConfig.GetTopicDescription()) ?
            TKeyedWriteSessionSettings::EPartitionChooserStrategy::Bound :
            TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash;
    }

    switch (partitionChooserStrategy) {
    case TKeyedWriteSessionSettings::EPartitionChooserStrategy::Bound:
        for (const auto& partition : partitions) {
            TPartitionInfo partitionInfo;
            partitionInfo.PartitionId(partition.GetPartitionId());
            Partitions.push_back(
                TPartitionInfo().
                PartitionId(partition.GetPartitionId()).
                Bounded(true).
                FromBound(TPartitionBound().Value(partition.GetFromBound())).
                ToBound(TPartitionBound().Value(partition.GetToBound())));

            auto iter = std::prev(Partitions.end());
            PartitionsIndex[partitionInfo.FromBound_] = iter;
        }
        PartitionChooser = std::make_unique<TBoundPartitionChooser>(this);
        break;
    case TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash:
        for (const auto& partition : partitions) {
            Partitions.push_back(
                TPartitionInfo().
                PartitionId(partition.GetPartitionId()).
                Bounded(false));

            auto iter = std::prev(Partitions.end());
            PartitionsIndex[iter->FromBound_] = iter;
        }
        PartitionChooser = std::make_unique<THashPartitionChooser>(this);
        break;
    default:
        Y_ABORT("Unreachable");
    }
    
    MessageSenderWorker = std::thread([this]() { RunMessageSender(); });

    // Initial token to let the user start writing right away (contract: tokens come from ReadyToAccept events).
    {
        std::lock_guard lock(GlobalLock);
        EventsGlobalQueue.push_back(TWriteSessionEvent::TReadyToAcceptEvent(IssueContinuationToken()));
        EventsProcessedPromise.TrySetValue();
    }
}

TKeyedWriteSession::WrappedWriteSessionPtr TKeyedWriteSession::CreateWriteSession(ui64 partitionId) {
    CleanExpiredSessions();

    auto alteredSettings = Settings;
    alteredSettings.DirectWriteToPartition(true);
    alteredSettings.PartitionId(partitionId);  
    auto writeSession = std::make_shared<WriteSessionWrapper>(WriteSessionWrapper{
        .Session = Client->CreateWriteSession(alteredSettings),
        .PartitionId = partitionId,
        .ExpirationTime = TInstant::Now() + Settings.SessionTimeout_,
    });

    auto [it, inserted] = SessionsIndex.try_emplace(partitionId, writeSession);
    return inserted ? writeSession : it->second;
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

void TKeyedWriteSession::Write(TContinuationToken&&, const std::string& key, TWriteMessage&& message, TTransactionBase* tx) {
    std::lock_guard lock(GlobalLock);
    const auto& partitionInfo = PartitionChooser->ChoosePartition(key);
    SaveMessage(std::move(message), partitionInfo.PartitionId_, tx);
    
    if (PendingMessages.size() + InFlightMessages.size() < MAX_MESSAGES_IN_MEMORY) {
        EventsGlobalQueue.push_back(TWriteSessionEvent::TReadyToAcceptEvent(IssueContinuationToken()));
        EventsProcessedPromise.TrySetValue();
    }
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
        it->second->Session->Close(TDuration::Zero());
        it = SessionsIndex.erase(it);
        ++cleaned;
    }
}

bool TKeyedWriteSession::Close(TDuration closeTimeout) {
    CloseTimeout = closeTimeout;
    const bool wasClosed = Closed.exchange(true);
    if (!wasClosed) {
        ClosePromise.TrySetValue();
    }

    if (MessageSenderWorker.joinable()) {
        if (MessageSenderWorker.get_id() == std::this_thread::get_id()) {
            MessageSenderWorker.detach();
        } else {
            MessageSenderWorker.join();
        }
    }
    return true;
}

void TKeyedWriteSession::DestroyWriteSession(TSessionsIndexIterator& it, const TDuration& closeTimeout) {
    if (it == SessionsIndex.end() || !it->second) {
        return;
    }
    it->second->Session->Close(closeTimeout);
    it = SessionsIndex.erase(it);
}

NThreading::TFuture<void> TKeyedWriteSession::WaitEvent() {
    std::lock_guard lock(GlobalLock);

    if (!EventsGlobalQueue.empty()) {
        return NThreading::MakeFuture();
    }

    if (!EventsProcessedFuture.Initialized() || EventsProcessedFuture.IsReady()) {
        EventsProcessedPromise = NThreading::NewPromise();
        EventsProcessedFuture = EventsProcessedPromise.GetFuture();
    }

    return EventsProcessedFuture;
}

void TKeyedWriteSession::TransferEventsToGlobalQueue() {
    while (true) {
        if (InFlightMessages.empty()) {
            break;
        }

        const auto& head = InFlightMessages.front();
        if (!PartitionsWithEvents.contains(head.PartitionId)) {
            break;
        }
        
        const auto& eventsQueueIt = PartitionsEventQueues.find(head.PartitionId);
        Y_ABORT_UNLESS(eventsQueueIt != PartitionsEventQueues.end());

        EventsGlobalQueue.push_back(std::move(eventsQueueIt->second.front()));
        eventsQueueIt->second.pop_front();

        PartitionsWithEvents.erase(head.PartitionId);
        InFlightMessages.pop_front();
    }
}

void TKeyedWriteSession::WaitSomeAction(std::unique_lock<std::mutex>& lock) {
    std::vector<NThreading::TFuture<void>> futures;
    futures.push_back(EventsProcessedFuture);
    futures.push_back(CloseFuture);
    lock.unlock();
    NThreading::WaitAny(futures).Wait();
    lock.lock();
}

std::optional<TWriteSessionEvent::TEvent> TKeyedWriteSession::GetEvent(bool block) {
    std::unique_lock lock(GlobalLock);

    if (EventsGlobalQueue.empty() && block) {
        WaitSomeAction(lock);
    }

    if (EventsGlobalQueue.empty()) {
        return std::nullopt;
    }

    auto event = std::move(EventsGlobalQueue.front());
    EventsGlobalQueue.pop_front();
    return std::move(event);
}

std::vector<TWriteSessionEvent::TEvent> TKeyedWriteSession::GetEvents(bool block, std::optional<size_t> maxEventsCount) {
    std::unique_lock lock(GlobalLock);

    while (maxEventsCount.has_value() && EventsGlobalQueue.size() < maxEventsCount.value() && block) {
        WaitSomeAction(lock);
        if (Closed.load()) {
            break;
        }
    }

    std::vector<TWriteSessionEvent::TEvent> events;
    events.reserve(maxEventsCount.value_or(EventsGlobalQueue.size()));
    while (!EventsGlobalQueue.empty() && events.size() < maxEventsCount.value_or(EventsGlobalQueue.size())) {
        events.push_back(std::move(EventsGlobalQueue.front()));
        EventsGlobalQueue.pop_front();
    }
    return events;
}

void TKeyedWriteSession::ProcessEventsUntilReadyToAccept(ui64 partitionId) {
    auto [it, _] = PartitionsEventQueues.try_emplace(partitionId, std::list<TWriteSessionEvent::TEvent>());
    auto sessionIter = SessionsIndex.find(partitionId);
    if (sessionIter == SessionsIndex.end()) {
        return;
    }

    while (true) {
        auto event = sessionIter->second->Session->GetEvent(true);
        Y_ABORT_UNLESS(event);
        auto readyToAcceptEvent = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event);
        if (readyToAcceptEvent) {
            ContinuationTokens.emplace(partitionId, std::move(readyToAcceptEvent->ContinuationToken));
            break;
        }

        it->second.push_back(std::move(*event));
        PartitionsWithEvents.insert(partitionId);
    }
}

TContinuationToken TKeyedWriteSession::GetContinuationToken(ui64 partitionId) {
    auto it = ContinuationTokens.find(partitionId);
    if (it == ContinuationTokens.end()) {
        ProcessEventsUntilReadyToAccept(partitionId);
    }

    it = ContinuationTokens.find(partitionId);
    Y_ABORT_UNLESS(it != ContinuationTokens.end());
    auto token = std::move(it->second);
    ContinuationTokens.erase(it);

    return token;
}

void TKeyedWriteSession::WaitForEvents(const NThreading::TFuture<void>& messagesFuture) {
    std::vector<NThreading::TFuture<void>> futures;
    std::vector<ui64> partitions;
    
    futures.reserve(2 + SessionsIndex.size());
    futures.push_back(messagesFuture);
    futures.push_back(CloseFuture);
    for (auto& [_, s] : SessionsIndex) {
        futures.push_back(s->Session->WaitEvent());
        partitions.push_back(s->PartitionId);
    }

    NThreading::WaitAny(futures).Wait();

    bool hasEvents = false;
    std::lock_guard lock(GlobalLock);
    for (size_t i = 2; i < futures.size(); ++i) {
        if (!futures[i].IsReady()) {
            continue;
        }

        hasEvents = true;
        auto ws = SessionsIndex.find(partitions[i-2]);
        Y_ABORT_UNLESS(ws != SessionsIndex.end());
        auto event = ws->second->Session->GetEvent(true);
        if (!event) {
            continue;
        }

        if (std::get_if<TSessionClosedEvent>(&*event)) {
            DestroyWriteSession(ws, TDuration::Zero());
            continue;
        }

        if (auto readyToAcceptEvent = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
            ContinuationTokens.emplace(partitions[i-2], std::move(readyToAcceptEvent->ContinuationToken));
            continue;
        }

        PartitionsWithEvents.insert(partitions[i-2]);
        auto [it, _] = PartitionsEventQueues.try_emplace(partitions[i-2], std::list<TWriteSessionEvent::TEvent>());
        it->second.push_back(std::move(*event));
    }

    TransferEventsToGlobalQueue();
    if (hasEvents && EventsProcessedFuture.Initialized()) {
        EventsProcessedPromise.TrySetValue();
    }
}

void TKeyedWriteSession::RunMessageSender() {
    while (true) {
        std::optional<TMessageInfo> msgToSend;
        bool shouldWait = false;
        NThreading::TFuture<void> messagesFuture;

        {
            std::lock_guard lock(GlobalLock);
            if (Closed.load() && PendingMessages.empty() && InFlightMessages.empty()) {
                break;
            }

            if (!PendingMessages.empty()) {
                auto it = PendingMessages.begin();
                InFlightMessages.push_back(*it);
                msgToSend.emplace(std::move(*it));
                PendingMessages.erase(it);
            } else {
                if (!MessagesNotEmptyFuture.Initialized() || MessagesNotEmptyFuture.IsReady()) {
                    MessagesNotEmptyPromise = NThreading::NewPromise();
                    MessagesNotEmptyFuture = MessagesNotEmptyPromise.GetFuture();
                }
                messagesFuture = MessagesNotEmptyFuture;
                shouldWait = !Closed.load() || !InFlightMessages.empty();
            }
        }

        if (msgToSend) {
            auto writeSession = GetWriteSession(msgToSend->PartitionId);
            auto continuationToken = GetContinuationToken(msgToSend->PartitionId);
            writeSession->Session->Write(std::move(continuationToken), std::move(msgToSend->Message), msgToSend->Tx);
            continue;
        }

        if (shouldWait) {
            WaitForEvents(messagesFuture);
        }
    }

    for (auto& [_, ws] : SessionsIndex) {
        ws->Session->Close(CloseTimeout);
    }
}

TWriterCounters::TPtr TKeyedWriteSession::GetCounters() {
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
    return *std::prev(lowerBound)->second;
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
): Writer(std::make_shared<TKeyedWriteSession>(settings, client, connections, dbDriverState)) {}

bool TSimpleBlockingKeyedWriteSession::Write([[maybe_unused]] const std::string& key, [[maybe_unused]] TWriteMessage&& message, [[maybe_unused]] TTransactionBase* tx,
    [[maybe_unused]] const TDuration& blockTimeout) {
    return true;
}

bool TSimpleBlockingKeyedWriteSession::Close(TDuration closeTimeout) {
    Closed.store(true);
    return Writer->Close(std::move(closeTimeout));
    return true;
}

TWriterCounters::TPtr TSimpleBlockingKeyedWriteSession::GetCounters() {
    return nullptr;
}

} // namespace NYdb::NTopic
