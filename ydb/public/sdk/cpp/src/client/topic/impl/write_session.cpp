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
    : TContextOwner(settings, std::move(client), std::move(connections), std::move(dbDriverState))
{
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
    if (seqNo.has_value()) {
        message.SeqNo(*seqNo);
    }
    if (createTimestamp.has_value()) {
        message.CreateTimestamp(*createTimestamp);
    }
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
    if (seqNo.has_value()) {
        message.SeqNo(*seqNo);
    }
    if (createTimestamp.has_value()) {
        message.CreateTimestamp(*createTimestamp);
    }
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TPartitionInfo

bool TKeyedWriteSession::TPartitionInfo::InRange(const std::string_view key) const {
    if (FromBound_ > key) {
        return false;
    }
    if (ToBound_.has_value() && *ToBound_ <= key) {
        return false;
    }
    return true;
}

bool TKeyedWriteSession::TPartitionInfo::operator<(const std::string_view key) const {
    return FromBound_ < key;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TWriteSessionWrapper

TKeyedWriteSession::TWriteSessionWrapper::TWriteSessionWrapper(WriteSessionPtr session, ui64 partition)
    : Session(std::move(session))
    , Partition(static_cast<ui32>(partition))
    , QueueSize(0)
{}

bool TKeyedWriteSession::TWriteSessionWrapper::IsQueueEmpty() const {
    return QueueSize == 0;
}

bool TKeyedWriteSession::TWriteSessionWrapper::AddToQueue(ui64 delta) {
    bool idle = QueueSize == 0;
    QueueSize += delta;
    return idle;
}

bool TKeyedWriteSession::TWriteSessionWrapper::RemoveFromQueue(ui64 delta) {
    Y_ABORT_UNLESS(QueueSize >= delta, "RemoveFromQueue: underflow");
    QueueSize -= delta;
    return QueueSize == 0;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TIdleSession

bool TKeyedWriteSession::TIdleSession::Less(const std::shared_ptr<TIdleSession>& other) const {
    if (EmptySince == other->EmptySince) {
        return Session->Partition < other->Session->Partition;
    }

    return EmptySince < other->EmptySince;
}

bool TKeyedWriteSession::TIdleSession::Comparator::operator()(
    const std::shared_ptr<TIdleSession>& first,
    const std::shared_ptr<TIdleSession>& second) const {
    return first->Less(second);
}

bool TKeyedWriteSession::TIdleSession::IsExpired() const {
    return TInstant::Now() - EmptySince > IdleTimeout;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSplittedPartitionActor

TKeyedWriteSession::TSplittedPartitionActor::TSplittedPartitionActor(TKeyedWriteSession* session, ui32 partitionId, ui64 partitionIdx, std::shared_ptr<TSessionsActor> sessionsActor)
    : Session(session), SessionsActor(sessionsActor), PartitionId(partitionId), PartitionIdx(partitionIdx) {}

void TKeyedWriteSession::TSplittedPartitionActor::DoStep() {
    std::unique_lock lock(Lock);
    auto self = weak_from_this();
    switch (State) {
        case EState::Init:
            DescribeTopicFuture = Session->Client->DescribeTopic(Session->Settings.Path_, TDescribeTopicSettings());
            lock.unlock();
            DescribeTopicFuture.Subscribe([self](const NThreading::TFuture<TDescribeTopicResult>&) {
                auto selfPtr = CheckAlive(self);
                if (!selfPtr) {
                    return;
                }

                std::lock_guard lock(selfPtr->Lock);
                selfPtr->HandleDescribeResult();
            });
            lock.lock();
            MoveTo(EState::PendingDescribe);
            break;
        case EState::GotDescribe:
            LaunchGetMaxSeqNoFutures(lock);
            MoveTo(EState::PendingMaxSeqNo);
            break;
        case EState::PendingDescribe:
        case EState::PendingMaxSeqNo:
        case EState::Done:
            break;
        case EState::GotMaxSeqNo:
            if (!Session->ResendMessages(PartitionId, MaxSeqNo)) {
                MoveTo(EState::Done);
            }
            break;
    }
}

std::shared_ptr<TKeyedWriteSession::TSplittedPartitionActor> TKeyedWriteSession::TSplittedPartitionActor::CheckAlive(const std::weak_ptr<TSplittedPartitionActor>& self) {
    auto selfPtr = self.lock();
    if (!selfPtr) {
        return nullptr;
    }

    if (selfPtr->Session->Closed.load()) {
        return nullptr;
    }

    return selfPtr;
}

void TKeyedWriteSession::TSplittedPartitionActor::MoveTo(EState state) {
    State = state;
}

void TKeyedWriteSession::TSplittedPartitionActor::UpdateMaxSeqNo(ui64 maxSeqNo) {
    MaxSeqNo = std::max(MaxSeqNo, maxSeqNo);
}

bool TKeyedWriteSession::TSplittedPartitionActor::IsDone() {
    std::lock_guard lock(Lock);
    return State == EState::Done;
}

void TKeyedWriteSession::TSplittedPartitionActor::HandleDescribeResult() {
    std::vector<ui32> newPartitions;
    const auto& partitions = DescribeTopicFuture.GetValue().GetTopicDescription().GetPartitions();
    for (const auto& partition : partitions) {
        if (partition.GetPartitionId() != PartitionId) {
            continue;
        }
        
        std::copy(partition.GetChildPartitionIds().begin(), partition.GetChildPartitionIds().end(), std::back_inserter(newPartitions));
        break;
    }

    std::lock_guard lock(Session->GlobalLock);
    const auto& splittedPartition = Session->Partitions[PartitionIdx];
    Session->PartitionsIndex.erase(splittedPartition.FromBound_);
    for (const auto& newPartitionId : newPartitions) {
        auto partitionDescribeInfo = std::find_if(partitions.begin(), partitions.end(), [newPartitionId](const auto& partition) {
            return partition.GetPartitionId() == newPartitionId;
        });
        Y_ABORT_UNLESS(partitionDescribeInfo != partitions.end(), "Partition describe info not found");
        Session->PartitionIdsMapping[newPartitionId] = Session->Partitions.size();
        Session->PartitionsIndex[partitionDescribeInfo->GetFromBound().value_or("")] = Session->Partitions.size();
        Session->Partitions.push_back(
            TPartitionInfo()
            .PartitionId(newPartitionId)
            .FromBound(partitionDescribeInfo->GetFromBound().value_or(""))
            .ToBound(partitionDescribeInfo->GetToBound()));
    }
    MoveTo(EState::GotDescribe);
}

void TKeyedWriteSession::TSplittedPartitionActor::LaunchGetMaxSeqNoFutures(std::unique_lock<std::mutex>& lock) {
    Y_ABORT_UNLESS(DescribeTopicFuture.IsReady(), "DescribeTopicFuture is not ready yet");

    std::unordered_map<ui32, ui32> partitionToParent;
    const auto& partitions = DescribeTopicFuture.GetValue().GetTopicDescription().GetPartitions();
    for (const auto& partition : partitions) {
        auto parentPartitions = partition.GetParentPartitionIds();
        if (parentPartitions.empty()) {
            continue;
        }

        // we consider here that each partition has only one parent partition
        partitionToParent[partition.GetPartitionId()] = parentPartitions.front();
    }

    std::vector<ui32> ancestors;
    ui32 currentPartition = PartitionId;
    while (true) {
        ancestors.push_back(currentPartition);

        auto parentPartition = partitionToParent.find(currentPartition);
        if (parentPartition == partitionToParent.end()) {
            break;
        }
        currentPartition = parentPartition->second;
    }

    auto sessionsActor = SessionsActor.lock();
    Y_ABORT_UNLESS(sessionsActor, "SessionsActor is not set");

    NotReadyFutures = ancestors.size();
    for (const auto& ancestor : ancestors) {
        auto wrappedSession = sessionsActor->GetWriteSession(ancestor, false);
        Y_ABORT_UNLESS(wrappedSession, "Write session not found");
        WriteSessions.push_back(wrappedSession);

        auto future = wrappedSession->Session->GetInitSeqNo();
        lock.unlock();
        auto self = weak_from_this();
        future.Subscribe([self](const NThreading::TFuture<uint64_t>& result) {
            auto selfPtr = CheckAlive(self);
            if (!selfPtr) {
                return;
            }

            std::lock_guard lock(selfPtr->Lock);
            selfPtr->UpdateMaxSeqNo(result.GetValue());
            if (--selfPtr->NotReadyFutures == 0) {
                selfPtr->MoveTo(EState::GotMaxSeqNo);
            }
        });
        lock.lock();
        GetMaxSeqNoFutures.push_back(future);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TEventsActor

TKeyedWriteSession::TEventsActor::TEventsActor(TKeyedWriteSession* session, std::shared_ptr<TSessionsActor> sessionsActor)
    : Session(session), SessionsActor(sessionsActor)
{
    NotReadyPromise = NThreading::NewPromise();
    NotReadyFuture = NotReadyPromise.GetFuture();
    
    // Initialize per-partition futures to a valid, non-ready future to avoid TFutures being uninitialized
    // (NThreading::WaitAny throws on uninitialized futures).
    Futures.resize(Session->Partitions.size(), NotReadyFuture);
}

void TKeyedWriteSession::TEventsActor::HandleAcksEvent(ui64 partition, TWriteSessionEvent::TAcksEvent&& event) {
    auto [queueIt, _] = PartitionsEventQueues.try_emplace(partition, std::list<TWriteSessionEvent::TEvent>());
    queueIt->second.push_back(TWriteSessionEvent::TEvent(std::move(event)));
}

void TKeyedWriteSession::TEventsActor::HandleReadyToAcceptEvent(ui64 partition, TWriteSessionEvent::TReadyToAcceptEvent&& event) {
    auto [queueIt, _] = Session->ContinuationTokens.try_emplace(partition, std::deque<TContinuationToken>());
    queueIt->second.push_back(std::move(event.ContinuationToken));
}

TSessionClosedEvent TKeyedWriteSession::TEventsActor::GetCloseEvent() {
    if (CloseEvent.has_value()) {
        return CloseEvent.value();
    }

    return TSessionClosedEvent(EStatus::SUCCESS, {});
}
    
void TKeyedWriteSession::TEventsActor::HandleSessionClosedEvent(TSessionClosedEvent&& event, ui64 partition) {
    if (event.IsSuccess()) {
        return;
    }

    if (event.GetStatus() == EStatus::OVERLOADED) {
        Session->HandleAutoPartitioning(partition);
        return;
    }

    if (!CloseEvent.has_value()) {
        CloseEvent = std::move(event);
    }
    Session->NonBlockingClose();
}

void TKeyedWriteSession::TEventsActor::RunEventLoop(WrappedWriteSessionPtr wrappedSession, ui64 partition) {
    while (true) {
        auto event = wrappedSession->Session->GetEvent(false);
        if (!event) {
            break;
        }

        if (auto sessionClosedEvent = std::get_if<TSessionClosedEvent>(&*event); sessionClosedEvent) {
            HandleSessionClosedEvent(std::move(*sessionClosedEvent), partition);
            break;
        }

        if (auto readyToAcceptEvent = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
            HandleReadyToAcceptEvent(partition, std::move(*readyToAcceptEvent));
            continue;
        }

        if (auto acksEvent = std::get_if<TWriteSessionEvent::TAcksEvent>(&*event)) {
            auto sessionsActor = Get(SessionsActor);
            sessionsActor->OnReadFromSession(wrappedSession);
            HandleAcksEvent(partition, std::move(*acksEvent));
            continue;
        }
    }
}

void TKeyedWriteSession::TEventsActor::DoStep() {
    std::unique_lock lock(Lock);
    auto sessionActor = Get(SessionsActor);

    while (!ReadyFutures.empty()) {
        auto idx = *ReadyFutures.begin();
        RunEventLoop(sessionActor->GetWriteSession(idx), idx);

        ReadyFutures.erase(idx);
        lock.unlock();
        SubscribeToPartition(idx);
        lock.lock();
    }

    TransferEventsToOutputQueue();
}

void TKeyedWriteSession::TEventsActor::SubscribeToPartition(ui64 partition) {
    auto wrappedSession = Get(SessionsActor)->GetWriteSession(partition);
    auto newFuture = wrappedSession->Session->WaitEvent();
    if (partition >= Futures.size()) {
        Futures.resize(partition + 1, NotReadyFuture);
    }
    Futures[partition] = newFuture;
    newFuture.Subscribe([this, partition](const NThreading::TFuture<void>&) {
        std::lock_guard lock(Lock);
        this->ReadyFutures.insert(partition);
    });
}

void TKeyedWriteSession::TEventsActor::TransferEventsToOutputQueue() {
    bool eventsTransferred = false;
    bool shouldAddReadyToAcceptEvent = false;
    std::unordered_map<ui64, std::deque<TWriteSessionEvent::TWriteAck>> acks;

    auto buildOutputAckEvent = [](std::deque<TWriteSessionEvent::TWriteAck>& acksQueue, ui64 expectedSeqNo) -> TWriteSessionEvent::TAcksEvent {
        TWriteSessionEvent::TAcksEvent ackEvent;
        Y_ENSURE(acksQueue.front().SeqNo == expectedSeqNo, TStringBuilder() << "Expected seqNo=" << expectedSeqNo << " but got " << acksQueue.front().SeqNo);
        auto ack = std::move(acksQueue.front());
        ackEvent.Acks.push_back(std::move(ack));
        acksQueue.pop_front();
        return ackEvent;
    };

    while (!Session->InFlightMessages.empty()) {
        const auto& head = Session->InFlightMessages.front();
        Y_ENSURE(head.Message.SeqNo_.has_value(), "SeqNo is not set");

        auto remainingAcks = acks.find(head.Partition);
        if (remainingAcks != acks.end() && remainingAcks->second.size() > 0) {
            Session->EventsOutputQueue.push_back(buildOutputAckEvent(remainingAcks->second, *head.Message.SeqNo_));
            Session->PopInFlightMessage();
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
        Session->EventsOutputQueue.push_back(buildOutputAckEvent(acksQueue, *head.Message.SeqNo_));
        acks[head.Partition] = std::move(acksQueue);
        eventsQueueIt->second.pop_front();
        eventsTransferred = true;

        bool wasMemoryUsageOk = Session->IsMemoryUsageOK();
        Session->MemoryUsage -= head.Message.Data.size();

        // Check if we need to add ReadyToAcceptEvent after removing this message
        if (Session->IsMemoryUsageOK() && !wasMemoryUsageOk) {
            shouldAddReadyToAcceptEvent = true;
        }

        Session->PopInFlightMessage();
    }

    if (shouldAddReadyToAcceptEvent) {
        Session->AddReadyToAcceptEvent();
    }

    if (eventsTransferred) {
        Session->EventsProcessedPromise.TrySetValue();
    }
}

void TKeyedWriteSession::TEventsActor::Wait() {
    std::vector<NThreading::TFuture<void>> futures;

    auto partitionsWaitFuture = NThreading::WaitAny(Futures);
    futures.push_back(partitionsWaitFuture);
    futures.push_back(Session->MessagesNotEmptyFuture);

    if (!Session->Closed.load()) {
        futures.push_back(Session->CloseFuture);
        NThreading::WaitAny(futures).Wait();
        return;
    }

    NThreading::WaitAny(futures).Wait(Session->GetCloseTimeout());
}

void TKeyedWriteSession::TEventsActor::UnsubscribeFromPartition(ui64 partition) {
    ReadyFutures.erase(partition);
    if (partition < Futures.size()) {
        Futures[partition] = NotReadyFuture;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSessionsActor

TKeyedWriteSession::TSessionsActor::TSessionsActor(TKeyedWriteSession* session)
    : Session(session) {}

TKeyedWriteSession::WrappedWriteSessionPtr TKeyedWriteSession::TSessionsActor::GetWriteSession(ui64 partition, bool directToPartition) {
    auto sessionIter = SessionsIndex.find(partition);
    if (sessionIter == SessionsIndex.end()) {
        return CreateWriteSession(partition);
    }

    if (!directToPartition) {
        DestroyWriteSession(sessionIter, TDuration::Zero(), false);
        return CreateWriteSession(partition);
    }

    return sessionIter->second;
}

std::string TKeyedWriteSession::TSessionsActor::GetProducerId(ui64 partitionId) {
    return std::format("{}_{}", Session->Settings.ProducerIdPrefix_, partitionId);
}

TKeyedWriteSession::WrappedWriteSessionPtr TKeyedWriteSession::TSessionsActor::CreateWriteSession(ui64 partition, bool directToPartition) {
    auto partitionId = Session->Partitions[partition].PartitionId_;
    auto producerId = GetProducerId(partitionId);
    TWriteSessionSettings alteredSettings = Session->Settings;
    alteredSettings
        .ProducerId(producerId)
        .MessageGroupId(producerId)
        .MaxMemoryUsage(std::numeric_limits<ui64>::max());

    if (directToPartition) {    
        alteredSettings.DirectWriteToPartition(true);
        alteredSettings.PartitionId(partitionId);
    }
    auto writeSession = std::make_shared<TWriteSessionWrapper>(
        Session->Client->CreateWriteSession(alteredSettings),
        partition);

    SessionsIndex.emplace(partition, writeSession);

    Get(EventsActor)->SubscribeToPartition(partition);
    return writeSession;
}

void TKeyedWriteSession::TSessionsActor::DestroyWriteSession(TSessionsIndexIterator& it, TDuration closeTimeout, bool mustBeEmpty) {
    if (it == SessionsIndex.end() || !it->second) {
        return;
    }

    Y_ABORT_UNLESS(!mustBeEmpty || it->second->Session->Close(closeTimeout), "There are still messages in flight");
    const ui64 partition = it->second->Partition;
    it = SessionsIndex.erase(it);
    
    Get(EventsActor)->UnsubscribeFromPartition(partition);
}

void TKeyedWriteSession::TSessionsActor::OnReadFromSession(WrappedWriteSessionPtr wrappedSession) {
    if (wrappedSession->RemoveFromQueue(1)) {
        Y_ABORT_UNLESS(!wrappedSession->IdleSession, "IdleSession is already set");
        auto idleSessionPtr = std::make_shared<TIdleSession>(wrappedSession.get(), TInstant::Now(), Session->Settings.SubSessionIdleTimeout_);
        IdlerSessions.insert(idleSessionPtr);
        IdlerSessionsIndex[wrappedSession->Partition] = idleSessionPtr;
        wrappedSession->IdleSession = idleSessionPtr;
    }
}

void TKeyedWriteSession::TSessionsActor::OnWriteToSession(WrappedWriteSessionPtr wrappedSession) {
    if (wrappedSession->AddToQueue(1) && wrappedSession->IdleSession) {
        IdlerSessions.erase(wrappedSession->IdleSession);
        wrappedSession->IdleSession.reset();
    }
}

void TKeyedWriteSession::TSessionsActor::DoStep() {
    for (auto it = IdlerSessions.begin(); it != IdlerSessions.end(); it = IdlerSessions.erase(it)) {
        if (!(*it)->IsExpired()) {
            break;
        }

        auto sessionIter = SessionsIndex.find((*it)->Session->Partition);
        DestroyWriteSession(sessionIter, TDuration::Zero());
    }
}

void TKeyedWriteSession::TSessionsActor::Die(TDuration timeout) {
    auto sessionsToClose = SessionsIndex.size();
    for (auto it = SessionsIndex.begin(); it != SessionsIndex.end();) {
        DestroyWriteSession(it, timeout / sessionsToClose, false);
    }
}

void TKeyedWriteSession::TSessionsActor::SetEventsActor(std::shared_ptr<TEventsActor> eventsActor) {
    EventsActor = eventsActor;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TMessagesActor

TKeyedWriteSession::TMessagesActor::TMessagesActor(TKeyedWriteSession* session, std::shared_ptr<TSessionsActor> sessionsActor)
    : Session(session), SessionsActor(sessionsActor) {}

void TKeyedWriteSession::TMessagesActor::DoStep() {
    auto sessionsActor = Get(SessionsActor);
    while (!PendingMessages.empty()) {
        auto& head = PendingMessages.front();
        Y_ENSURE(head.Message.SeqNo_.has_value(), "SeqNo is not set");

        auto continuationToken = GetContinuationToken(head.Partition);
        if (!continuationToken) {
            break;
        }

        auto wrappedSession = sessionsActor->GetWriteSession(head.Partition);
        Y_ABORT_UNLESS(wrappedSession, "WrappedSession is not set");
        
        auto msgToSave = head;
        wrappedSession->Session->Write(std::move(*continuationToken), std::move(head.Message), head.Tx);
        PendingMessages.pop_front();
        PushInFlightMessage(msgToSave.Partition, std::move(msgToSave));
    }
}

void TKeyedWriteSession::TMessagesActor::PushInFlightMessage(ui64 partition, TMessageInfo&& message) {
    InFlightMessages.push_back(std::move(message));
    auto [listIt, _] = InFlightMessagesIndex.try_emplace(partition, std::list<std::list<TMessageInfo>::iterator>());
    listIt->second.push_back(std::prev(InFlightMessages.end()));
}

void TKeyedWriteSession::TMessagesActor::HandleAck() {
    PopInFlightMessage();
}

void TKeyedWriteSession::TMessagesActor::PopInFlightMessage() {
    Y_ABORT_UNLESS(!InFlightMessages.empty());
    const ui64 partition = InFlightMessages.front().Partition;
    const auto it = InFlightMessages.begin();

    auto mapIt = InFlightMessagesIndex.find(partition);
    if (mapIt != InFlightMessagesIndex.end()) {
        auto& list = mapIt->second;
        for (auto listIt = list.begin(); listIt != list.end(); ++listIt) {
            if (*listIt == it) {
                list.erase(listIt);
                break;
            }
        }
        if (list.empty()) {
            InFlightMessagesIndex.erase(mapIt);
        }
    }

    InFlightMessages.pop_front();
}

bool TKeyedWriteSession::TMessagesActor::IsMemoryUsageOK() const {
    return MemoryUsage <= Session->Settings.MaxMemoryUsage_;
}

void TKeyedWriteSession::TMessagesActor::AddMessage(const std::string& key, TWriteMessage&& message, ui64 partition, TTransactionBase* tx) {
    const bool wasEmpty = PendingMessages.empty();
    PendingMessages.push_back(TMessageInfo(key, std::move(message), partition, tx));

    if (wasEmpty) {
        Session->MessagesNotEmptyPromise.TrySetValue();
    }
}

std::optional<TContinuationToken> TKeyedWriteSession::TMessagesActor::GetContinuationToken(ui64 partition) {
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

void TKeyedWriteSession::TMessagesActor::HandleContinuationToken(ui64 partition, TContinuationToken&& continuationToken) {
    auto [it, _] = ContinuationTokens.try_emplace(partition, std::deque<TContinuationToken>());
    it->second.push_back(std::move(continuationToken));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TKeyedWriteSession

TKeyedWriteSession::TKeyedWriteSession(
    const TKeyedWriteSessionSettings& settings,
    std::shared_ptr<TTopicClient::TImpl> client,
    std::shared_ptr<TGRpcConnectionsImpl> connections,
    TDbDriverStatePtr dbDriverState)
    : MainWorker(TThread::TParams(RunMainWorkerThread, this).SetName("KeyedWriteSessionMainWorker")),
    Connections(connections),
    Client(client),
    DbDriverState(dbDriverState),
    Settings(settings),
    MemoryUsage(0)
{
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

    for (const auto& partition : partitions) {
        if (!partition.GetActive()) {
            continue;
        }

        auto partitionId = partition.GetPartitionId();
        PartitionIdsMapping[partitionId] = Partitions.size();
        Partitions.push_back(
            TPartitionInfo()
            .PartitionId(partitionId)
            .FromBound(partition.GetFromBound().value_or(""))
            .ToBound(partition.GetToBound()));
    
    }

    switch (partitionChooserStrategy) {
        case TKeyedWriteSessionSettings::EPartitionChooserStrategy::Bound:
            PartitioningKeyHasher = settings.PartitioningKeyHasher_;
            PartitionChooserActor = std::make_unique<TBoundPartitionChooserActor>(this);
            for (size_t i = 0; i < Partitions.size(); ++i) {
                if (i > 0 && Partitions[i].FromBound_.empty() && !Partitions[i].ToBound_.has_value()) {
                    Y_ABORT("Unbounded partition is not supported for Bound partition chooser strategy");
                }

                PartitionsIndex[Partitions[i].FromBound_] = i;
            }
            break;
        case TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash:
            Y_ABORT_UNLESS(!Partitions.empty(), "KeyedWriteSession with Hash partition chooser requires at least one active partition");
            PartitionChooserActor = std::make_unique<THashPartitionChooserActor>(this);
            break;
    }

    MessagesNotEmptyPromise = NThreading::NewPromise();
    MessagesNotEmptyFuture = MessagesNotEmptyPromise.GetFuture();
    ClosePromise = NThreading::NewPromise();
    CloseFuture = ClosePromise.GetFuture();

    NotReadyPromise = NThreading::NewPromise();
    NotReadyFuture = NotReadyPromise.GetFuture();

    Futures.reserve(Partitions.size());
    // Initialize per-partition futures to a valid, non-ready future to avoid TFutures being uninitialized
    // (NThreading::WaitAny throws on uninitialized futures).
    for (size_t i = 0; i < Partitions.size(); ++i) {
        Futures.push_back(NotReadyFuture);
    }

    EventsProcessedPromise = NThreading::NewPromise();
    EventsProcessedFuture = EventsProcessedPromise.GetFuture();
    EventsOutputQueue.push_back(TWriteSessionEvent::TReadyToAcceptEvent(IssueContinuationToken()));
    EventsProcessedPromise.TrySetValue();

    SessionsActor = std::make_shared<TSessionsActor>(this);
    EventsActor = std::make_shared<TEventsActor>(this, SessionsActor);
    SessionsActor->SetEventsActor(EventsActor);

    MainWorker.Start();
}

const std::vector<TKeyedWriteSession::TPartitionInfo>& TKeyedWriteSession::GetPartitions() const {
    return Partitions;
}

void TKeyedWriteSession::SaveMessage(const std::string& key, TWriteMessage&& message, ui64 partition, TTransactionBase* tx) {
    const bool wasEmpty = PendingMessages.empty();
    PendingMessages.push_back(TMessageInfo(key, std::move(message), partition, tx));

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
    auto partition = PartitionChooserActor->ChoosePartition(key);
    SaveMessage(key, std::move(message), partition, tx);

    if (IsMemoryUsageOK()) {
        AddReadyToAcceptEvent();
    }
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
    if (!MainWorker.Running()) {
        return IsQueueEmpty();
    }

    if (MainWorker.Id() == TThread::CurrentThreadId()) {
        MainWorker.Detach();
    } else {
        MainWorker.Join();
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
    if (MainWorker.Running()) {
        Close(TDuration::Zero());
    }
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

void TKeyedWriteSession::PopInFlightMessage() {
    Y_ABORT_UNLESS(!InFlightMessages.empty());
    const ui64 partition = InFlightMessages.front().Partition;
    const auto it = InFlightMessages.begin();

    auto mapIt = InFlightMessagesIndex.find(partition);
    if (mapIt != InFlightMessagesIndex.end()) {
        auto& list = mapIt->second;
        for (auto listIt = list.begin(); listIt != list.end(); ++listIt) {
            if (*listIt == it) {
                list.erase(listIt);
                break;
            }
        }
        if (list.empty()) {
            InFlightMessagesIndex.erase(mapIt);
        }
    }

    InFlightMessages.pop_front();
}

void TKeyedWriteSession::PushInFlightMessage(ui64 partition, TMessageInfo&& message) {
    InFlightMessages.push_back(std::move(message));
    auto [listIt, _] = InFlightMessagesIndex.try_emplace(partition, std::list<std::list<TMessageInfo>::iterator>());
    listIt->second.push_back(std::prev(InFlightMessages.end()));
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
    // Only check, never wait here. Waiting in GetContinuationToken would
    // prevent RunMainWorker from processing ReadyFutures and re-subscribing to
    // partition WaitEvent() futures; without re-subscription we stop receiving new
    // ReadyToAccept events and can deadlock. The caller must WaitForEvents() in the
    // main loop when nullopt is returned.
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

void TKeyedWriteSession::RunSplittedPartitionActors() {
    if (SplittedPartitionActors.empty()) {
        return;
    }

    std::vector<ui64> toRemove;
    for (const auto& [partition, splittedPartitionActor] : SplittedPartitionActors) {
        if (splittedPartitionActor->IsDone()) {
            toRemove.push_back(partition);
            continue;
        }

        splittedPartitionActor->DoStep();
    }

    for (const auto& partition : toRemove) {
        SplittedPartitionActors.erase(partition);
    }
}

void TKeyedWriteSession::RemoveSplittedPartition(ui32 partitionId) {
    auto partitionIt = PartitionIdsMapping.find(partitionId);
    Y_ABORT_UNLESS(partitionIt != PartitionIdsMapping.end(), "Partition not found");
    SplittedPartitionActors.erase(partitionIt->second);
}

void* TKeyedWriteSession::RunMainWorkerThread(void* arg) {
    auto session = static_cast<TKeyedWriteSession*>(arg);
    session->RunMainWorker();
    return nullptr;
}

void TKeyedWriteSession::RunMainWorker() {
    while (true) {
        TMessageInfo* msgToSend = nullptr;
        bool didWrite = false;

        RunSplittedPartitionActors();
        {
            std::unique_lock lock(GlobalLock);
            EventsActor->DoStep();
            if (Closed.load() && ((InFlightMessages.empty() && PendingMessages.empty()) || CloseDeadline <= TInstant::Now())) {
                break;
            }

            SessionsActor->DoStep();
            if (!PendingMessages.empty()) {
                msgToSend = &PendingMessages.front();
            }
        }

        WrappedWriteSessionPtr writeSession = nullptr;
        if (msgToSend) {
            auto partition = msgToSend->Partition;
            writeSession = SessionsActor->GetWriteSession(partition);
            auto continuationToken = GetContinuationToken(partition);
            if (continuationToken) {
                auto msgToSave = *msgToSend;
                writeSession->Session->Write(std::move(*continuationToken), std::move(msgToSend->Message), msgToSend->Tx);
                PushInFlightMessage(partition, std::move(msgToSave));
                didWrite = true;
            }
        }

        std::unique_lock lock(GlobalLock);
        if (didWrite) {
            PendingMessages.pop_front();
            SessionsActor->OnWriteToSession(writeSession);
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
            EventsActor->Wait();
        }
    }

    // Close all sessions and add SessionClosedEvent when all messages are processed
    auto closeTimeout = GetCloseTimeout();
    SessionsActor->Die(closeTimeout);

    // Add SessionClosedEvent only if needed
    std::lock_guard lock(GlobalLock);
    AddSessionClosedEvent();
}

void TKeyedWriteSession::HandleAutoPartitioning(ui64 partition) {
    auto splittedPartitionActor = std::make_shared<TSplittedPartitionActor>(this, Partitions[partition].PartitionId_, partition, SessionsActor);
    SplittedPartitionActors.try_emplace(partition, splittedPartitionActor);
}

bool TKeyedWriteSession::ResendMessages(ui64 partition, ui64 afterSeqNo) {
    auto indexQueue = InFlightMessagesIndex.find(partition);
    Y_ABORT_UNLESS(indexQueue != InFlightMessagesIndex.end(), "Index queue not found");
    for (auto it = indexQueue->second.begin(); it != indexQueue->second.end(); ++it) {
        auto& message = **it;
        Y_ABORT_UNLESS(message.Message.SeqNo_.has_value(), "SeqNo is not set");
        if (*message.Message.SeqNo_ <= afterSeqNo || message.Resent) {
            continue;
        }

        auto messageToResend = message;
        if (!ResendMessage(std::move(messageToResend))) {
            return false;
        }

        message.Resent = true;
    }

    return true;
}

bool TKeyedWriteSession::ResendMessage(TMessageInfo&& message) {
    auto partition = PartitionChooserActor->ChoosePartition(message.Key);
    auto continuationToken = GetContinuationToken(partition);
    if (continuationToken) {
        auto writeSession = SessionsActor->GetWriteSession(partition);
        writeSession->Session->Write(std::move(*continuationToken), std::move(message.Message), message.Tx);
        return true;
    }

    return false;
}

std::string TKeyedWriteSession::GetProducerId(ui64 partition) {
    return std::format("{}_{}", Settings.ProducerIdPrefix_, partition);
}

TWriterCounters::TPtr TKeyedWriteSession::GetCounters() {
    // what should we return here?
    return nullptr;
}

TKeyedWriteSession::TBoundPartitionChooserActor::TBoundPartitionChooserActor(TKeyedWriteSession* session)
    : Session(session)
{}

ui32 TKeyedWriteSession::TBoundPartitionChooserActor::ChoosePartition(const std::string_view key) {
    auto hashedKey = Session->PartitioningKeyHasher(key);

    auto lowerBound = Session->PartitionsIndex.lower_bound(hashedKey);
    if (lowerBound == Session->PartitionsIndex.end()) {
        return Session->Partitions.size() - 1;
    }

    if (lowerBound->first == hashedKey) {
        return lowerBound->second;
    }
    Y_ABORT_IF(lowerBound == Session->PartitionsIndex.begin(), "Lower bound is the first element");

    return std::prev(lowerBound)->second;
}

TKeyedWriteSession::THashPartitionChooserActor::THashPartitionChooserActor(TKeyedWriteSession* session)
    : Session(session)
{
}

ui32 TKeyedWriteSession::THashPartitionChooserActor::ChoosePartition(const std::string_view key) {
    return std::hash<std::string_view>{}(key) % Session->Partitions.size();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSimpleBlockingWriteSession

TSimpleBlockingWriteSession::TSimpleBlockingWriteSession(
    const TWriteSessionSettings& settings,
    std::shared_ptr<TTopicClient::TImpl> client,
    std::shared_ptr<TGRpcConnectionsImpl> connections,
    TDbDriverStatePtr dbDriverState) {
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
    std::string_view data, std::optional<uint64_t> seqNo, std::optional<TInstant> createTimestamp, const TDuration& blockTimeout) {
    auto message = TWriteMessage(std::move(data))
                        .SeqNo(seqNo)
                        .CreateTimestamp(createTimestamp);
    return Write(std::move(message), nullptr, blockTimeout);
}

bool TSimpleBlockingWriteSession::Write(
    TWriteMessage&& message, TTransactionBase* tx, const TDuration& blockTimeout) {
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
    TDbDriverStatePtr dbDriverState)
    : Writer(std::make_shared<TKeyedWriteSession>(settings, client, connections, dbDriverState))
{
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

template <typename F>
bool TSimpleBlockingKeyedWriteSession::Wait(const TDuration& timeout, F&& stopFunc) {
    std::unique_lock lock(Lock);

    auto deadline = TInstant::Now() + timeout;
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
        NThreading::WaitAny(futures).Wait(deadline);
        lock.lock();
    }
}

std::optional<TContinuationToken> TSimpleBlockingKeyedWriteSession::GetContinuationToken(TDuration timeout) {
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

bool TSimpleBlockingKeyedWriteSession::WaitForAck(ui64 seqNo, TDuration timeout) {
    return Wait(timeout, [&]() {
        if (AckedSeqNos.contains(seqNo)) {
            AckedSeqNos.erase(seqNo);
            return true;
        }
        return false;
    });
}

bool TSimpleBlockingKeyedWriteSession::Write(const std::string& key, TWriteMessage&& message, TTransactionBase* tx, TDuration blockTimeout) {
    auto continuationToken = GetContinuationToken(blockTimeout);
    if (!continuationToken) {
        return false;
    }

    ui64 seqNo = message.SeqNo_.value_or(0);
    Writer->Write(std::move(*continuationToken), std::move(key), std::move(message), tx);
    return WaitForAck(seqNo, blockTimeout);
}

bool TSimpleBlockingKeyedWriteSession::Close(TDuration closeTimeout) {
    Closed.store(true);
    ClosePromise.TrySetValue();
    return Writer->Close(closeTimeout);
}

TWriterCounters::TPtr TSimpleBlockingKeyedWriteSession::GetCounters() {
    return nullptr;
}

} // namespace NYdb::inline Dev::NTopic
