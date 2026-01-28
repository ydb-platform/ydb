#include "write_session.h"

#include <ydb/public/sdk/cpp/src/client/topic/common/log_lazy.h>
#include <ydb/public/sdk/cpp/src/client/topic/common/simple_blocking_helpers.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <util/digest/murmur.h>

#include <library/cpp/threading/future/wait/wait.h>
#include <library/cpp/threading/future/subscription/wait_any.h>

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
// TKeyedWriteSession::TPartitionInfo

bool TKeyedWriteSession::TPartitionInfo::InRange(const std::string_view key) const {
    if (FromBound_ > key) {
        return false;
    }
    if (ToBound_.has_value() && *ToBound_ <= key) {
        return false;
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TKeyedWriteSession::TWriteSessionWrapper

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
// TKeyedWriteSession::TIdleSession

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
// TKeyedWriteSession::TSplittedPartitionWorker

TKeyedWriteSession::TSplittedPartitionWorker::TSplittedPartitionWorker(TKeyedWriteSession* session, ui32 partitionId, ui64 partitionIdx)
    : Session(session), PartitionId(partitionId), PartitionIdx(partitionIdx)
{}

void TKeyedWriteSession::TSplittedPartitionWorker::DoWork() {
    std::unique_lock lock(Lock);
    switch (State) {
        case EState::Init:
            DescribeTopicFuture = Session->Client->DescribeTopic(Session->Settings.Path_, TDescribeTopicSettings());
            lock.unlock();
            DescribeTopicFuture.Subscribe([this](const NThreading::TFuture<TDescribeTopicResult>&) {
                std::lock_guard lock(Lock);
                HandleDescribeResult();
            });
            lock.lock();
            if (State == EState::Init) {
                MoveTo(EState::PendingDescribe);
            }
            break;
        case EState::GotDescribe:
            LaunchGetMaxSeqNoFutures(lock);
            if (State == EState::GotDescribe) {
                MoveTo(EState::PendingMaxSeqNo);
            }
            break;
        case EState::PendingDescribe:
        case EState::PendingMaxSeqNo:
        case EState::Done:
            break;
        case EState::GotMaxSeqNo:
            Session->MessagesWorker->ScheduleResendMessages(PartitionId, MaxSeqNo);
            for (const auto& child : Session->Partitions[PartitionIdx].Children_) {
                Session->Partitions[child].Locked(false);
            }
            MoveTo(EState::Done);
            break;
    }
}

void TKeyedWriteSession::TSplittedPartitionWorker::MoveTo(EState state) {
    State = state;
}

void TKeyedWriteSession::TSplittedPartitionWorker::UpdateMaxSeqNo(ui64 maxSeqNo) {
    MaxSeqNo = std::max(MaxSeqNo, maxSeqNo);
}

bool TKeyedWriteSession::TSplittedPartitionWorker::IsDone() {
    std::lock_guard lock(Lock);
    return State == EState::Done;
}

void TKeyedWriteSession::TSplittedPartitionWorker::HandleDescribeResult() {
    std::vector<ui64> newPartitions;
    const auto& partitions = DescribeTopicFuture.GetValue().GetTopicDescription().GetPartitions();
    for (const auto& partition : partitions) {
        if (partition.GetPartitionId() != PartitionId) {
            continue;
        }
        
        for (const auto& childPartition : partition.GetChildPartitionIds()) {
            newPartitions.push_back(childPartition);
        }
        break;
    }

    std::lock_guard lock(Session->GlobalLock);
    std::vector<ui32> children;
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
            .ToBound(partitionDescribeInfo->GetToBound())
            .Locked(true));
        children.push_back(Session->Partitions.size() - 1);
    }
    Session->Partitions[PartitionIdx].Children(children);
    MoveTo(EState::GotDescribe);
}

void TKeyedWriteSession::TSplittedPartitionWorker::LaunchGetMaxSeqNoFutures(std::unique_lock<std::mutex>& lock) {
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

    NotReadyFutures = ancestors.size();
    for (const auto& ancestor : ancestors) {
        auto wrappedSession = Session->SessionsWorker->GetWriteSession(ancestor, false);
        Y_ABORT_UNLESS(wrappedSession, "Write session not found");
        WriteSessions.push_back(wrappedSession);

        auto future = wrappedSession->Session->GetInitSeqNo();
        lock.unlock();
        future.Subscribe([this](const NThreading::TFuture<uint64_t>& result) {
            std::lock_guard lock(Lock);
            UpdateMaxSeqNo(result.GetValue());
            if (--NotReadyFutures == 0) {
                MoveTo(EState::GotMaxSeqNo);   
            }
        });
        lock.lock();
        GetMaxSeqNoFutures.push_back(future);
    }
}

NThreading::TFuture<void> TKeyedWriteSession::TSplittedPartitionWorker::Wait() {
    if (DescribeTopicFuture.Initialized() && !DescribeTopicFuture.IsReady()) {
        return DescribeTopicFuture.IgnoreResult();
    }

    return NThreading::NWait::WaitAny(GetMaxSeqNoFutures);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TKeyedWriteSession::TEventsWorkerWrapper

TKeyedWriteSession::TEventsWorker::TEventsWorker(TKeyedWriteSession* session)
    : Session(session)
{
    NotReadyPromise = NThreading::NewPromise();
    NotReadyFuture = NotReadyPromise.GetFuture();
    EventsPromise = NThreading::NewPromise();
    EventsFuture = EventsPromise.GetFuture();

    // Initialize per-partition futures to a valid, non-ready future to avoid TFutures being uninitialized
    // (NThreading::WaitAny throws on uninitialized futures).
    Futures.resize(Session->Partitions.size(), NotReadyFuture);

    AddReadyToAcceptEvent();
}

void TKeyedWriteSession::TEventsWorker::HandleAcksEvent(ui64 partition, TWriteSessionEvent::TAcksEvent&& event) {
    auto [queueIt, _] = PartitionsEventQueues.try_emplace(partition, std::list<TWriteSessionEvent::TEvent>());
    queueIt->second.push_back(TWriteSessionEvent::TEvent(std::move(event)));
}

void TKeyedWriteSession::TEventsWorker::HandleReadyToAcceptEvent(ui64 partition, TWriteSessionEvent::TReadyToAcceptEvent&& event) {
    Session->MessagesWorker->HandleContinuationToken(partition, std::move(event.ContinuationToken));
}
    
void TKeyedWriteSession::TEventsWorker::HandleSessionClosedEvent(TSessionClosedEvent&& event, ui64 partition) {
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

void TKeyedWriteSession::TEventsWorker::RunEventLoop(WrappedWriteSessionPtr wrappedSession, ui64 partition) {
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
            Session->SessionsWorker->OnReadFromSession(wrappedSession);
            HandleAcksEvent(partition, std::move(*acksEvent));
            continue;
        }
    }
}

void TKeyedWriteSession::TEventsWorker::DoWork() {
    std::unique_lock lock(Lock);

    while (!ReadyFutures.empty()) {
        LOG_LAZY(Session->DbDriverState->Log, TLOG_DEBUG, "TKeyedWriteSession: Running event loop");
        auto idx = *ReadyFutures.begin();
        RunEventLoop(Session->SessionsWorker->GetWriteSession(idx), idx);

        ReadyFutures.erase(idx);
        lock.unlock();
        SubscribeToPartition(idx);
        lock.lock();
    }

    TransferEventsToOutputQueue();
}

void TKeyedWriteSession::TEventsWorker::SubscribeToPartition(ui64 partition) {
    if (auto it = Session->SplittedPartitionWorkers.find(partition); it != Session->SplittedPartitionWorkers.end()) {
        Futures[partition] = NotReadyFuture;
        return;
    }

    auto wrappedSession = Session->SessionsWorker->GetWriteSession(partition);
    auto newFuture = wrappedSession->Session->WaitEvent();
    while (partition >= Futures.size()) {
        Futures.push_back(NotReadyFuture);
    }
    newFuture.Subscribe([this, partition](const NThreading::TFuture<void>&) {
        std::lock_guard lock(Lock);
        this->ReadyFutures.insert(partition);
    });
    Futures[partition] = newFuture;
}

void TKeyedWriteSession::TEventsWorker::HandleNewMessage() {
    std::lock_guard lock(Lock);
    if (Session->MessagesWorker->IsMemoryUsageOK()) {
        AddReadyToAcceptEvent();
    }
}

void TKeyedWriteSession::TEventsWorker::AddReadyToAcceptEvent() {
    EventsOutputQueue.push_back(TWriteSessionEvent::TReadyToAcceptEvent(IssueContinuationToken()));
    EventsPromise.TrySetValue();
}

void TKeyedWriteSession::TEventsWorker::AddSessionClosedEvent() {
    if (!Session->Closed.load()) {
        return;
    }

    if (!CloseEvent.has_value()) {
        CloseEvent = TSessionClosedEvent(EStatus::SUCCESS, {});
    }

    if (EventsOutputQueue.empty() && Session->MessagesWorker->IsQueueEmpty()) {
        EventsOutputQueue.push_back(*CloseEvent);
        EventsPromise.TrySetValue();
    }
}

void TKeyedWriteSession::TEventsWorker::TransferEventsToOutputQueue() {
    bool eventsTransferred = false;
    bool shouldAddReadyToAcceptEvent = false;
    std::unordered_map<ui64, std::deque<TWriteSessionEvent::TWriteAck>> acks;

    auto messagesWorker = Session->MessagesWorker;
    auto buildOutputAckEvent = [](std::deque<TWriteSessionEvent::TWriteAck>& acksQueue, std::optional<ui64> expectedSeqNo) -> TWriteSessionEvent::TAcksEvent {
        TWriteSessionEvent::TAcksEvent ackEvent;

        if (expectedSeqNo.has_value()) {
            Y_ENSURE(acksQueue.front().SeqNo == expectedSeqNo.value(), TStringBuilder() << "Expected seqNo=" << expectedSeqNo.value() << " but got " << acksQueue.front().SeqNo);
        }
    
        auto ack = std::move(acksQueue.front());
        ackEvent.Acks.push_back(std::move(ack));
        acksQueue.pop_front();
        return ackEvent;
    };
    auto finishWithAck = [messagesWorker, &shouldAddReadyToAcceptEvent]() {
        bool wasMemoryUsageOk = messagesWorker->IsMemoryUsageOK();
        messagesWorker->HandleAck();
        if (messagesWorker->IsMemoryUsageOK() && !wasMemoryUsageOk) {
            shouldAddReadyToAcceptEvent = true;
        }
    };

    while (messagesWorker->HasInFlightMessages()) {
        const auto& head = messagesWorker->GetFrontInFlightMessage();

        auto remainingAcks = acks.find(head.Partition);
        if (remainingAcks != acks.end() && remainingAcks->second.size() > 0) {
            EventsOutputQueue.push_back(buildOutputAckEvent(remainingAcks->second, head.Message.SeqNo_));
            finishWithAck();
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
        EventsOutputQueue.push_back(buildOutputAckEvent(acksQueue, head.Message.SeqNo_));
        acks[head.Partition] = std::move(acksQueue);
        eventsQueueIt->second.pop_front();
        eventsTransferred = true;

        finishWithAck();
    }

    // this case handles situation:
    // 1st message is written to partition 0
    // 2nd message is written to partition 1
    // 3rd message is written to partition 0
    // 4th message is written to partition 1
    // but AcksEvent for partition 0 looks like:
    // [ack1, ack3]
    // In this case we can not just forget about ack3, because 3rd message is in-flight
    // so we will push 'AcksEvent' back to the queue for partition 0
    for (auto& [partition, acksQueue] : acks) {
        if (acksQueue.size() > 0) {
            TWriteSessionEvent::TAcksEvent ackEvent;
            std::copy(acksQueue.begin(), acksQueue.end(), std::back_inserter(ackEvent.Acks));
            PartitionsEventQueues[partition].push_front(std::move(ackEvent));
        }
    }

    if (shouldAddReadyToAcceptEvent) {
        AddReadyToAcceptEvent();
    }

    if (eventsTransferred) {
        EventsPromise.TrySetValue();
    }
}

std::list<TWriteSessionEvent::TEvent>::iterator TKeyedWriteSession::TEventsWorker::AckQueueBegin(ui64 partition) {
    auto [queueIt, _] = PartitionsEventQueues.try_emplace(partition, std::list<TWriteSessionEvent::TEvent>());
    return queueIt->second.begin();
}

std::list<TWriteSessionEvent::TEvent>::iterator TKeyedWriteSession::TEventsWorker::AckQueueEnd(ui64 partition) {
    auto [queueIt, _] = PartitionsEventQueues.try_emplace(partition, std::list<TWriteSessionEvent::TEvent>());
    return queueIt->second.end();
}

std::optional<TWriteSessionEvent::TEvent> TKeyedWriteSession::TEventsWorker::GetEvent(bool block) {
    std::unique_lock lock(Lock);
    AddSessionClosedEvent();

    if (EventsOutputQueue.empty() && block) {
        lock.unlock();
        WaitEvent().Wait();
        lock.lock();
    }

    if (EventsOutputQueue.empty()) {
        return std::nullopt;
    }

    auto event = std::move(EventsOutputQueue.front());
    EventsOutputQueue.pop_front();

    return event;
}

std::vector<TWriteSessionEvent::TEvent> TKeyedWriteSession::TEventsWorker::GetEvents(bool block, std::optional<size_t> maxEventsCount) {
    std::unique_lock lock(Lock);
    AddSessionClosedEvent();

    while (!Session->Closed.load() && maxEventsCount.has_value() && EventsOutputQueue.size() < maxEventsCount.value() && block) {
        lock.unlock();
        WaitEvent().Wait();
        lock.lock();
    }

    std::vector<TWriteSessionEvent::TEvent> events;
    events.reserve(maxEventsCount.value_or(EventsOutputQueue.size()));
    while (!EventsOutputQueue.empty() && events.size() < maxEventsCount.value_or(EventsOutputQueue.size())) {
        auto event = std::move(EventsOutputQueue.front());
        events.push_back(std::move(event));
        EventsOutputQueue.pop_front();
    }

    return events;
}

NThreading::TFuture<void> TKeyedWriteSession::TEventsWorker::Wait() {
    return NThreading::NWait::WaitAny(Futures);
}

NThreading::TFuture<void> TKeyedWriteSession::TEventsWorker::WaitEvent() {
    std::unique_lock lock(Lock);

    if (!EventsOutputQueue.empty()) {
        return NThreading::MakeFuture();
    }

    if (EventsFuture.IsReady()) {
        EventsPromise = NThreading::NewPromise();
        EventsFuture = EventsPromise.GetFuture();
    }

    AddSessionClosedEvent();

    return EventsFuture;
}

void TKeyedWriteSession::TEventsWorker::UnsubscribeFromPartition(ui64 partition) {
    ReadyFutures.erase(partition);
    if (partition < Futures.size()) {
        Futures[partition] = NotReadyFuture;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TKeyedWriteSession::TSessionsWorker

TKeyedWriteSession::TSessionsWorker::TSessionsWorker(TKeyedWriteSession* session)
    : Session(session) {}

TKeyedWriteSession::WrappedWriteSessionPtr TKeyedWriteSession::TSessionsWorker::GetWriteSession(ui64 partition, bool directToPartition) {
    auto sessionIter = SessionsIndex.find(partition);
    if (sessionIter == SessionsIndex.end()) {
        return CreateWriteSession(partition, directToPartition);
    }

    if (!directToPartition) {
        SessionsIndex.erase(sessionIter);
        return CreateWriteSession(partition, directToPartition);
    }

    return sessionIter->second;
}

std::string TKeyedWriteSession::TSessionsWorker::GetProducerId(ui64 partitionId) {
    return std::format("{}_{}", Session->Settings.ProducerIdPrefix_, partitionId);
}

TKeyedWriteSession::WrappedWriteSessionPtr TKeyedWriteSession::TSessionsWorker::CreateWriteSession(ui64 partition, bool directToPartition) {
    auto partitionId = Session->Partitions[partition].PartitionId_;
    auto producerId = GetProducerId(partitionId);
    TWriteSessionSettings alteredSettings = Session->Settings;

    alteredSettings
        .ProducerId(producerId)
        .MessageGroupId(producerId)
        .MaxMemoryUsage(std::numeric_limits<ui64>::max())
        .RetryPolicy(IRetryPolicy::GetExponentialBackoffPolicy(
            TDuration::MilliSeconds(10),
            TDuration::MilliSeconds(200),
            TDuration::Seconds(30),
            std::numeric_limits<size_t>::max(),
            TDuration::Max(),
            2.0,
            [](EStatus status) -> ERetryErrorClass {
                if (status == EStatus::OVERLOADED) {
                    return ERetryErrorClass::NoRetry;
                }

                return GetRetryErrorClass(status);
            }
        ))
        .EventHandlers(TWriteSessionSettings::TEventHandlers()
                        .ReadyToAcceptHandler(nullptr)
                        .AcksHandler(nullptr)
                        .SessionClosedHandler(nullptr));
    
    if (directToPartition) {    
        alteredSettings.DirectWriteToPartition(true);
        alteredSettings.PartitionId(partitionId);
    }
    auto writeSession = std::make_shared<TWriteSessionWrapper>(
        Session->Client->CreateWriteSession(alteredSettings),
        partition);

    SessionsIndex.emplace(partition, writeSession);

    Session->EventsWorker->SubscribeToPartition(partition);
    return writeSession;
}

void TKeyedWriteSession::TSessionsWorker::DestroyWriteSession(TSessionsIndexIterator& it, TDuration closeTimeout, bool mustBeEmpty) {
    if (it == SessionsIndex.end() || !it->second) {
        return;
    }

    Y_ABORT_UNLESS(!mustBeEmpty || it->second->Session->Close(closeTimeout), "There are still messages in flight");
    const ui64 partition = it->second->Partition;
    it = SessionsIndex.erase(it);
    
    Session->EventsWorker->UnsubscribeFromPartition(partition);
}

void TKeyedWriteSession::TSessionsWorker::OnReadFromSession(WrappedWriteSessionPtr wrappedSession) {
    if (wrappedSession->RemoveFromQueue(1)) {
        Y_ABORT_UNLESS(!wrappedSession->IdleSession, "IdleSession is already set");
        auto idleSessionPtr = std::make_shared<TIdleSession>(wrappedSession.get(), TInstant::Now(), Session->Settings.SubSessionIdleTimeout_);
        IdlerSessions.insert(idleSessionPtr);
        IdlerSessionsIndex[wrappedSession->Partition] = idleSessionPtr;
        wrappedSession->IdleSession = idleSessionPtr;
    }
}

void TKeyedWriteSession::TSessionsWorker::OnWriteToSession(WrappedWriteSessionPtr wrappedSession) {
    if (wrappedSession->AddToQueue(1) && wrappedSession->IdleSession) {
        IdlerSessions.erase(wrappedSession->IdleSession);
        wrappedSession->IdleSession.reset();
    }
}

void TKeyedWriteSession::TSessionsWorker::DoWork() {
    for (auto it = IdlerSessions.begin(); it != IdlerSessions.end(); it = IdlerSessions.erase(it)) {
        if (!(*it)->IsExpired()) {
            break;
        }

        auto sessionIter = SessionsIndex.find((*it)->Session->Partition);
        DestroyWriteSession(sessionIter, TDuration::Zero());
    }
}

void TKeyedWriteSession::TSessionsWorker::Die(TDuration timeout) {
    auto sessionsToClose = SessionsIndex.size();
    for (auto it = SessionsIndex.begin(); it != SessionsIndex.end();) {
        DestroyWriteSession(it, timeout / sessionsToClose, false);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TKeyedWriteSession::TMessagesWorker

TKeyedWriteSession::TMessagesWorker::TMessagesWorker(TKeyedWriteSession* session)
    : Session(session)
{
    MessagesNotEmptyPromise = NThreading::NewPromise();
    MessagesNotEmptyFuture = MessagesNotEmptyPromise.GetFuture();
}

void TKeyedWriteSession::TMessagesWorker::DoWork() {
    auto sessionsWorker = Session->SessionsWorker;
    while (!PendingMessages.empty()) {
        auto& head = PendingMessages.front();
        if (Session->Partitions[head.Partition].Locked_) {
            break;
        }

        auto msgToSave = head;
        auto wrappedSession = sessionsWorker->GetWriteSession(msgToSave.Partition);
        if (!SendMessage(wrappedSession, std::move(head))) {
            break;
        }

        PendingMessages.pop_front();
        sessionsWorker->OnWriteToSession(wrappedSession);
        PushInFlightMessage(msgToSave.Partition, std::move(msgToSave));
    }

    std::vector<ui64> partitionsConsumed;
    for (auto& [partition, iter] : MessagesToResend) {
        auto inFlightMessagesIndexChain = InFlightMessagesIndex.find(partition);
        Y_ABORT_UNLESS(inFlightMessagesIndexChain != InFlightMessagesIndex.end(), "InFlightMessagesIndex not found");
        
        while (iter != inFlightMessagesIndexChain->second.end()) {
            auto msgToSend = **iter;
            auto wrappedSession = sessionsWorker->GetWriteSession(msgToSend.Partition);
            if (!SendMessage(wrappedSession, std::move(msgToSend))) {
                break;
            }

            sessionsWorker->OnWriteToSession(wrappedSession);
            ++iter;
        }

        if (iter == inFlightMessagesIndexChain->second.end()) {
            partitionsConsumed.push_back(partition);
        }
    }

    for (const auto& partition : partitionsConsumed) {
        MessagesToResend.erase(partition);
    }

    if (PendingMessages.empty() && MessagesNotEmptyFuture.IsReady()) {
        MessagesNotEmptyPromise = NThreading::NewPromise();
        MessagesNotEmptyFuture = MessagesNotEmptyPromise.GetFuture();
    }
}

bool TKeyedWriteSession::TMessagesWorker::SendMessage(WrappedWriteSessionPtr wrappedSession, TMessageInfo&& message) {    
    auto continuationToken = GetContinuationToken(message.Partition);
    if (!continuationToken) {
        return false;
    }
    
    wrappedSession->Session->Write(std::move(*continuationToken), std::move(message.Message), message.Tx);
    return true;
}

void TKeyedWriteSession::TMessagesWorker::PushInFlightMessage(ui64 partition, TMessageInfo&& message) {
    InFlightMessages.push_back(std::move(message));
    auto [listIt, _] = InFlightMessagesIndex.try_emplace(partition, std::list<std::list<TMessageInfo>::iterator>());
    listIt->second.push_back(std::prev(InFlightMessages.end()));
}

void TKeyedWriteSession::TMessagesWorker::HandleAck() {
    PopInFlightMessage();
}

void TKeyedWriteSession::TMessagesWorker::PopInFlightMessage() {
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

    Y_ABORT_UNLESS(it->Message.Data.size() <= MemoryUsage, "MemoryUsage is less than the size of the message");
    MemoryUsage -= it->Message.Data.size();
    InFlightMessages.pop_front();
}

bool TKeyedWriteSession::TMessagesWorker::IsMemoryUsageOK() const {
    return MemoryUsage <= Session->Settings.MaxMemoryUsage_;
}

void TKeyedWriteSession::TMessagesWorker::AddMessage(const std::string& key, TWriteMessage&& message, ui64 partition, TTransactionBase* tx) {
    const bool wasEmpty = PendingMessages.empty();
    PendingMessages.push_back(TMessageInfo(key, std::move(message), partition, tx));
    MemoryUsage += message.Data.size();

    if (wasEmpty) {
        MessagesNotEmptyPromise.TrySetValue();
    }
}

std::optional<TContinuationToken> TKeyedWriteSession::TMessagesWorker::GetContinuationToken(ui64 partition) {
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

void TKeyedWriteSession::TMessagesWorker::HandleContinuationToken(ui64 partition, TContinuationToken&& continuationToken) {
    auto [it, _] = ContinuationTokens.try_emplace(partition, std::deque<TContinuationToken>());
    it->second.push_back(std::move(continuationToken));
}

NThreading::TFuture<void> TKeyedWriteSession::TMessagesWorker::Wait() {
    return MessagesNotEmptyFuture;
}

bool TKeyedWriteSession::TMessagesWorker::IsQueueEmpty() const {
    return PendingMessages.empty() && InFlightMessages.empty();
}

const TKeyedWriteSession::TMessageInfo& TKeyedWriteSession::TMessagesWorker::GetFrontInFlightMessage() const {
    Y_ABORT_UNLESS(!InFlightMessages.empty());
    return InFlightMessages.front();
}

bool TKeyedWriteSession::TMessagesWorker::HasInFlightMessages() const {
    return !InFlightMessages.empty();
}

void TKeyedWriteSession::TMessagesWorker::ScheduleResendMessages(ui64 partition, ui64 afterSeqNo) {
    auto it = InFlightMessagesIndex.find(partition);
    if (it == InFlightMessagesIndex.end()) {
        return;
    }

    auto& list = it->second;
    auto resendIt = list.begin();
    auto ackQueueIt = Session->EventsWorker->AckQueueBegin(partition);
    size_t ackIdx = 0;
    auto ackQueueEnd = Session->EventsWorker->AckQueueEnd(partition);
    std::vector<TWriteSessionEvent::TWriteAck> acksToSend;

    while (resendIt != list.end()) {
        if (!(*resendIt)->Message.SeqNo_.has_value() || (*resendIt)->Message.SeqNo_.value() > afterSeqNo) {
            break;
        }

        if (ackQueueIt == ackQueueEnd) {
            // this case can happend if the message was sent, but session was closed before the ack was received
            TWriteSessionEvent::TWriteAck ack;
            Y_ENSURE((*resendIt)->Message.SeqNo_.has_value(), "SeqNo is not set");
            ack.SeqNo = (*resendIt)->Message.SeqNo_.value();
            acksToSend.push_back(std::move(ack));
        } else {
            auto acksEvent = std::get_if<TWriteSessionEvent::TAcksEvent>(&*ackQueueIt);
            if (ackIdx == acksEvent->Acks.size()) {
                ++ackQueueIt;
                ackIdx = 0;
            } else {
                ++ackIdx;
            }
        }
        ++resendIt;     
    }

    if (!acksToSend.empty()) {
        TWriteSessionEvent::TAcksEvent event;
        event.Acks = std::move(acksToSend);
        Session->EventsWorker->HandleAcksEvent(partition, std::move(event));
    }

    for (auto iter = resendIt; iter != list.end(); ++iter) {
        auto newPartition = Session->PartitionChooser->ChoosePartition((*iter)->Key);
        (*iter)->Partition = newPartition;

        auto [listIt, _] = InFlightMessagesIndex.try_emplace(newPartition, std::list<std::list<TMessageInfo>::iterator>());
        listIt->second.push_back(*iter);
    }

    if (resendIt != list.end()) {
        for (const auto& child : Session->Partitions[partition].Children_) {
            if (auto childList = InFlightMessagesIndex.find(child); childList != InFlightMessagesIndex.end()) {
                MessagesToResend.try_emplace(child, childList->second.begin());
            }
        }
        list.clear();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TKeyedWriteSession

TKeyedWriteSession::TKeyedWriteSession(
    const TKeyedWriteSessionSettings& settings,
    std::shared_ptr<TTopicClient::TImpl> client,
    std::shared_ptr<TGRpcConnectionsImpl> connections,
    TDbDriverStatePtr dbDriverState)
    : MainWorker(TThread::TParams(RunMainWorkerThread, this).SetName("MainWorker")),
    Connections(connections),
    Client(client),
    DbDriverState(dbDriverState),
    Settings(settings)
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
            PartitionChooser = std::make_unique<TBoundPartitionChooser>(this);
            for (size_t i = 0; i < Partitions.size(); ++i) {
                if (i > 0 && Partitions[i].FromBound_.empty() && !Partitions[i].ToBound_.has_value()) {
                    ythrow TContractViolation("Unbounded partition is not supported for Bound partition chooser strategy");
                }

                PartitionsIndex[Partitions[i].FromBound_] = i;
            }
            break;
        case TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash:
            PartitionChooser = std::make_unique<THashPartitionChooser>(this);
            break;
    }

    ClosePromise = NThreading::NewPromise();
    CloseFuture = ClosePromise.GetFuture();

    SessionsWorker = std::make_shared<TSessionsWorker>(this);
    MessagesWorker = std::make_shared<TMessagesWorker>(this);
    EventsWorker = std::make_shared<TEventsWorker>(this);

    // Start handlers executor for user callbacks (Acks/ReadyToAccept/SessionClosed/Common).
    Settings.EventHandlers_.HandlersExecutor_->Start();

    MainWorker.Start();
}

const std::vector<TKeyedWriteSession::TPartitionInfo>& TKeyedWriteSession::GetPartitions() const {
    return Partitions;
}

void TKeyedWriteSession::Write(TContinuationToken&&, const std::string& key, TWriteMessage&& message, TTransactionBase* tx) {
    std::lock_guard lock(GlobalLock);
    if (Closed.load()) {
        return;
    }

    [[unlikely]] if ((message.SeqNo_.has_value() && SeqNoStrategy == ESeqNoStrategy::WithoutSeqNo)
        || (!message.SeqNo_.has_value() && SeqNoStrategy == ESeqNoStrategy::WithSeqNo)) {
        ythrow TContractViolation("Can not mix messages with and without seqNo");
    }

    if (SeqNoStrategy == ESeqNoStrategy::NotInitialized) {
        SeqNoStrategy = message.SeqNo_.has_value() ? ESeqNoStrategy::WithSeqNo : ESeqNoStrategy::WithoutSeqNo;
    }

    auto partition = PartitionChooser->ChoosePartition(key);
    MessagesWorker->AddMessage(key, std::move(message), partition, tx);
    EventsWorker->HandleNewMessage();
}

bool TKeyedWriteSession::Close(TDuration closeTimeout) {
    if (Closed.exchange(true)) {
        return MessagesWorker->IsQueueEmpty();
    }

    SetCloseDeadline(closeTimeout);

    ClosePromise.TrySetValue();
    if (!MainWorker.Running()) {
        RunUserEventLoop();
        return MessagesWorker->IsQueueEmpty();
    }

    if (MainWorker.Id() == TThread::CurrentThreadId()) {
        MainWorker.Detach();
    } else {
        MainWorker.Join();
    }

    RunUserEventLoop();
    return MessagesWorker->IsQueueEmpty();
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
    Settings.EventHandlers_.HandlersExecutor_->Stop();
}

NThreading::TFuture<void> TKeyedWriteSession::WaitEvent() {
    return EventsWorker->WaitEvent();
}

std::optional<TWriteSessionEvent::TEvent> TKeyedWriteSession::GetEvent(bool block) {
    return EventsWorker->GetEvent(block);
}

std::vector<TWriteSessionEvent::TEvent> TKeyedWriteSession::GetEvents(bool block, std::optional<size_t> maxEventsCount) {
    return EventsWorker->GetEvents(block, maxEventsCount);
}

TDuration TKeyedWriteSession::GetCloseTimeout() {
    std::lock_guard lock(GlobalLock);
    auto now = TInstant::Now();
    if (CloseDeadline <= now) {
        return TDuration::Zero();
    }
    return CloseDeadline - now;
}

void TKeyedWriteSession::RunSplittedPartitionWorkers() {
    if (SplittedPartitionWorkers.empty()) {
        return;
    }

    std::vector<ui64> toRemove;
    for (const auto& [partition, splittedPartitionWorker] : SplittedPartitionWorkers) {
        if (splittedPartitionWorker->IsDone()) {
            toRemove.push_back(partition);
            continue;
        }

        splittedPartitionWorker->DoWork();
    }

    for (const auto& partition : toRemove) {
        SplittedPartitionWorkers.erase(partition);
    }
}

void* TKeyedWriteSession::RunMainWorkerThread(void* arg) {
    auto session = static_cast<TKeyedWriteSession*>(arg);
    session->RunMainWorker();
    return nullptr;
}

void TKeyedWriteSession::Wait() {
    std::vector<NThreading::TFuture<void>> futures{
        EventsWorker->Wait(),
        MessagesWorker->Wait()
    };

    for (const auto& [partition, splittedPartitionWorker] : SplittedPartitionWorkers) {
        futures.push_back(splittedPartitionWorker->Wait());
    }

    if (!Closed.load()) {
        futures.push_back(CloseFuture);
        NThreading::NWait::WaitAny(futures).Wait();
        return;
    }

    NThreading::NWait::WaitAny(futures).Wait(GetCloseTimeout());
}

void TKeyedWriteSession::RunUserEventLoop() {
    if (!Settings.EventHandlers_.AcksHandler_ &&
        !Settings.EventHandlers_.ReadyToAcceptHandler_ &&
        !Settings.EventHandlers_.SessionClosedHandler_) {
        return;
    }

    auto handlersExecutor = Settings.EventHandlers_.HandlersExecutor_;
    if (!handlersExecutor) {
        return;
    }

    while (true) {
        auto event = GetEvent(false);
        if (!event) {
            break;
        }

        if (auto* readyToAcceptEvent = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
            if (Settings.EventHandlers_.ReadyToAcceptHandler_) {
                handlersExecutor->Post(
                    [this, ev = std::move(*readyToAcceptEvent)]() mutable {
                        Settings.EventHandlers_.ReadyToAcceptHandler_(ev);
                    });
            } else {
                handlersExecutor->Post(
                    [this, ev = std::move(*event)]() mutable {
                        Settings.EventHandlers_.CommonHandler_(ev);
                    });
            }
            continue;
        }

        if (auto* acksEvent = std::get_if<TWriteSessionEvent::TAcksEvent>(&*event)) {
            if (Settings.EventHandlers_.AcksHandler_) {
                handlersExecutor->Post(
                    [this, ev = std::move(*acksEvent)]() mutable {
                        Settings.EventHandlers_.AcksHandler_(ev);
                    });
            } else {
                handlersExecutor->Post(
                    [this, ev = std::move(*event)]() mutable {
                        Settings.EventHandlers_.CommonHandler_(ev);
                    });
            }
            continue;
        }

        if (auto* sessionClosedEvent = std::get_if<TSessionClosedEvent>(&*event)) {
            if (Settings.EventHandlers_.SessionClosedHandler_) {
                handlersExecutor->Post(
                    [this, ev = std::move(*sessionClosedEvent)]() mutable {
                        Settings.EventHandlers_.SessionClosedHandler_(ev);
                    });
            } else if (Settings.EventHandlers_.CommonHandler_) {
                handlersExecutor->Post(
                    [this, ev = std::move(*event)]() mutable {
                        Settings.EventHandlers_.CommonHandler_(ev);
                    });
            }
            break;
        }
    }
}

void TKeyedWriteSession::RunMainWorker() {
    while (true) {
        RunSplittedPartitionWorkers();
        {
            std::unique_lock lock(GlobalLock);
            EventsWorker->DoWork();
            if (Closed.load() && (MessagesWorker->IsQueueEmpty() || CloseDeadline <= TInstant::Now())) {
                break;
            }

            SessionsWorker->DoWork();
            MessagesWorker->DoWork();
        }
        RunUserEventLoop();
        Wait();
    }

    // Close all sessions and add SessionClosedEvent when all messages are processed
    auto closeTimeout = GetCloseTimeout();
    SessionsWorker->Die(closeTimeout);
}

void TKeyedWriteSession::HandleAutoPartitioning(ui64 partition) {
    auto splittedPartitionWorker = std::make_shared<TSplittedPartitionWorker>(this, Partitions[partition].PartitionId_, partition);
    SplittedPartitionWorkers.try_emplace(partition, splittedPartitionWorker);
}

std::string TKeyedWriteSession::GetProducerId(ui64 partition) {
    return std::format("{}_{}", Settings.ProducerIdPrefix_, partition);
}

TWriterCounters::TPtr TKeyedWriteSession::GetCounters() {
    // what should we return here?
    return nullptr;
}

TKeyedWriteSession::TBoundPartitionChooser::TBoundPartitionChooser(TKeyedWriteSession* session)
    : Session(session)
{}

ui32 TKeyedWriteSession::TBoundPartitionChooser::ChoosePartition(const std::string_view key) {
    auto hashedKey = Session->PartitioningKeyHasher(key);

    auto lowerBound = Session->PartitionsIndex.lower_bound(hashedKey);
    if (lowerBound != Session->PartitionsIndex.end() && lowerBound->first == hashedKey) {
        return lowerBound->second;
    }

    Y_ABORT_IF(lowerBound == Session->PartitionsIndex.begin(), "Lower bound is the first element");
    return std::prev(lowerBound)->second;
}

TKeyedWriteSession::THashPartitionChooser::THashPartitionChooser(TKeyedWriteSession* session)
    : Session(session)
{
}

ui32 TKeyedWriteSession::THashPartitionChooser::ChoosePartition(const std::string_view key) {
    ui64 hash = MurmurHash<ui64>(key.data(), key.size());
    return hash % Session->Partitions.size();
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
        NThreading::NWait::WaitAny(futures).Wait(deadline);
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

bool TSimpleBlockingKeyedWriteSession::WaitForAck(std::optional<ui64> seqNo, TDuration timeout) {
    return Wait(timeout, [&]() {
        if (!seqNo.has_value()) {
            if (AckedSeqNos.empty()) {
                return false;
            }

            AckedSeqNos.erase(AckedSeqNos.begin());
            return true;
        }

        if (AckedSeqNos.contains(*seqNo)) {
            AckedSeqNos.erase(*seqNo);
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

    auto seqNo = message.SeqNo_;
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
