#include <format>
#include "write_session.h"

#include <util/system/byteorder.h>
#include <ydb/public/sdk/cpp/src/client/topic/common/log_lazy.h>
#include <ydb/public/sdk/cpp/src/client/topic/common/simple_blocking_helpers.h>
#include <ydb/public/sdk/cpp/src/library/decimal/yql_decimal.h>

#include <library/cpp/threading/future/wait/wait.h>
#include <library/cpp/threading/future/subscription/wait_any.h>
#include <util/digest/murmur.h>
#include <util/string/hex.h>

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

// TKeyedWriteSessionSettings

std::string TKeyedWriteSessionSettings::DefaultPartitioningKeyHasher(const std::string_view key) {
    const std::uint64_t lo = MurmurHash<std::uint64_t>(key.data(), key.size(), std::uint64_t{0});
    const std::uint64_t hi = MurmurHash<std::uint64_t>(key.data(), key.size(), std::uint64_t{0x9E3779B97F4A7C15ull}); // fixed seed

    const std::uint64_t hiBe = InetToHost(hi);
    const std::uint64_t loBe = InetToHost(lo);

    std::string out;
    out.resize(16);
    memcpy(out.data() + 0, &hiBe, 8);
    memcpy(out.data() + 8, &loBe, 8);
    return out; // 16 bytes
}

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
// TKeyedWriteSession::TMessageInfo

TKeyedWriteSession::TMessageInfo::TMessageInfo(const std::string& key, TWriteMessage&& message, std::uint32_t partition, TTransactionBase* tx)
    : Key(key)
    , Data(message.Data)
    , Codec(message.Codec)
    , OriginalSize(message.OriginalSize)
    , SeqNo(message.SeqNo_)
    , CreateTimestamp(message.CreateTimestamp_)
    , TxInMessage(message.Tx_)
    , Tx(tx)
    , Partition(partition)
{
    for (const auto& [key, value] : message.MessageMeta_) {
        MessageMeta.Fields.emplace_back(key, value);
    }
}

TWriteMessage TKeyedWriteSession::TMessageInfo::BuildMessage() const {
    TWriteMessage message(Data);
    message.Codec = Codec;
    message.OriginalSize = OriginalSize;
    message.SeqNo(SeqNo);
    message.CreateTimestamp(CreateTimestamp);
    for (const auto& [key, value] : MessageMeta.Fields) {
        message.MessageMeta_.emplace_back(key, value);
    }
    message.Tx(TxInMessage);
    return message;
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TKeyedWriteSession::TWriteSessionWrapper

TKeyedWriteSession::TWriteSessionWrapper::TWriteSessionWrapper(WriteSessionPtr session, std::uint32_t partition)
    : Session(std::move(session))
    , Partition(partition)
    , QueueSize(0)
{}

bool TKeyedWriteSession::TWriteSessionWrapper::IsQueueEmpty() const {
    return QueueSize == 0;
}

bool TKeyedWriteSession::TWriteSessionWrapper::AddToQueue(std::uint64_t delta) {
    bool idle = QueueSize == 0;
    QueueSize += delta;
    return idle;
}

bool TKeyedWriteSession::TWriteSessionWrapper::RemoveFromQueue(std::uint64_t delta) {
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

TKeyedWriteSession::TSplittedPartitionWorker::TSplittedPartitionWorker(TKeyedWriteSession* session, std::uint32_t partitionId)
    : Session(session)
    , PartitionId(partitionId)
{
    LOG_LAZY(Session->DbDriverState->Log, TLOG_INFO, Session->LogPrefix() << "Creating splitted partition worker for partition " << PartitionId);
}

std::string TKeyedWriteSession::TSplittedPartitionWorker::GetStateName() const {
    switch (State) {
        case EState::Init:
            return "Init";
        case EState::PendingDescribe:
            return "PendingDescribe";
        case EState::GotDescribe:
            return "GotDescribe";
        case EState::PendingMaxSeqNo:
            return "PendingMaxSeqNo";
        case EState::Done:
            return "Done";
        case EState::GotMaxSeqNo:
            return "GotMaxSeqNo";
    }
}

void TKeyedWriteSession::TSplittedPartitionWorker::DoWork() {
    std::unique_lock lock(Lock);
    std::weak_ptr<TKeyedWriteSession> session = Session->shared_from_this();
    switch (State) {
        case EState::Init:
            DescribeTopicFuture = Session->Client->DescribeTopic(Session->Settings.Path_, TDescribeTopicSettings());
            lock.unlock();
            DescribeTopicFuture.Subscribe([this, session](const NThreading::TFuture<TDescribeTopicResult>&) {
                auto sessionPtr = session.lock();
                if (!sessionPtr) {
                    return;
                }

                {
                    std::lock_guard lock(Lock);
                    MoveTo(EState::GotDescribe);
                }

                sessionPtr->RunMainWorker();
            });
            lock.lock();
            if (State == EState::Init) {
                MoveTo(EState::PendingDescribe);
            }
            break;
        case EState::GotDescribe:
            HandleDescribeResult();
            if (State != EState::GotDescribe) {
                break;
            }
            
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
            Session->MessagesWorker->RebuildPendingMessagesIndex(PartitionId);
            Session->MessagesWorker->ScheduleResendMessages(PartitionId, MaxSeqNo);
            for (const auto& child : Session->Partitions[PartitionId].Children_) {
                Session->Partitions[child].Locked(false);
            }
            Session->Partitions[PartitionId].Locked_ = false;
            MoveTo(EState::Done);
            break;
    }
}

void TKeyedWriteSession::TSplittedPartitionWorker::MoveTo(EState state) {
    State = state;
    LOG_LAZY(Session->DbDriverState->Log, TLOG_INFO, Session->LogPrefix() << "Moving splitted partition worker for partition " << PartitionId << " to state " << GetStateName());
}

void TKeyedWriteSession::TSplittedPartitionWorker::UpdateMaxSeqNo(std::uint64_t maxSeqNo) {
    MaxSeqNo = std::max(MaxSeqNo, maxSeqNo);
}

bool TKeyedWriteSession::TSplittedPartitionWorker::IsDone() {
    std::lock_guard lock(Lock);
    return State == EState::Done;
}

bool TKeyedWriteSession::TSplittedPartitionWorker::IsInit() {
    std::lock_guard lock(Lock);
    return State == EState::Init;
}

void TKeyedWriteSession::TSplittedPartitionWorker::HandleDescribeResult() {
    std::vector<std::uint32_t> newPartitionsIds;
    const auto& partitions = DescribeTopicFuture.GetValue().GetTopicDescription().GetPartitions();
    for (const auto& partition : partitions) {
        if (partition.GetPartitionId() != PartitionId) {
            continue;
        }
        
        LOG_LAZY(Session->DbDriverState->Log, TLOG_ERR, Session->LogPrefix() << "Found partition " << partition.GetPartitionId() << " for partition " << PartitionId << " children: " << partition.GetChildPartitionIds().size());
        for (const auto& childPartitionId : partition.GetChildPartitionIds()) {
            newPartitionsIds.push_back(childPartitionId);
        }
        break;
    }

    if (newPartitionsIds.empty()) {
        // describe response is incomplete, we need to resend describe request
        MoveTo(EState::Init);
        Y_ABORT_UNLESS(++Retries < 40, "Too many retries for partition %u", PartitionId);
        LOG_LAZY(Session->DbDriverState->Log, TLOG_ERR, Session->LogPrefix() << "Describe response is incomplete, we need to resend describe request for partition " << PartitionId);
        return;
    }

    std::vector<std::uint32_t> children;
    const auto& splittedPartition = Session->Partitions[PartitionId];
    Session->PartitionsIndex.erase(splittedPartition.FromBound_);

    for (const auto& newPartitionId : newPartitionsIds) {
        auto partitionDescribeInfo = std::find_if(partitions.begin(), partitions.end(), [newPartitionId](const auto& partition) {
            return partition.GetPartitionId() == newPartitionId;
        });
        Y_ABORT_UNLESS(partitionDescribeInfo != partitions.end(), "Partition describe info not found");
        Session->PartitionsIndex[partitionDescribeInfo->GetFromBound().value_or("")] = newPartitionId;
        Session->Partitions[newPartitionId] = TPartitionInfo()
            .PartitionId(newPartitionId)
            .FromBound(partitionDescribeInfo->GetFromBound().value_or(""))
            .ToBound(partitionDescribeInfo->GetToBound())
            .Locked(true);
        children.push_back(newPartitionId);
    }

    Session->Partitions[PartitionId].Children(children);
}

void TKeyedWriteSession::TSplittedPartitionWorker::LaunchGetMaxSeqNoFutures(std::unique_lock<std::mutex>& lock) {
    Y_ABORT_UNLESS(DescribeTopicFuture.IsReady(), "DescribeTopicFuture is not ready yet");

    std::unordered_map<std::uint32_t, std::uint32_t> partitionIdToParentId;
    const auto& partitions = DescribeTopicFuture.GetValue().GetTopicDescription().GetPartitions();
    for (const auto& partition : partitions) {
        auto parentPartitions = partition.GetParentPartitionIds();
        if (parentPartitions.empty()) {
            continue;
        }

        // we consider here that each partition has only one parent partition
        partitionIdToParentId[partition.GetPartitionId()] = parentPartitions.front();
    }

    std::vector<std::uint32_t> ancestors;
    std::uint32_t currentPartitionId = PartitionId;
    while (true) {
        ancestors.push_back(currentPartitionId);

        auto parentPartitionId = partitionIdToParentId.find(currentPartitionId);
        if (parentPartitionId == partitionIdToParentId.end()) {
            break;
        }
        currentPartitionId = parentPartitionId->second;
    }

    NotReadyFutures = ancestors.size();
    for (const auto& ancestor : ancestors) {
        auto wrappedSession = Session->SessionsWorker->GetWriteSession(ancestor, false);
        Y_ABORT_UNLESS(wrappedSession, "Write session not found");
        WriteSessions.push_back(wrappedSession);

        auto future = wrappedSession->Session->GetInitSeqNo();
        std::weak_ptr<TKeyedWriteSession> session = Session->shared_from_this();
        lock.unlock();
        future.Subscribe([this, session, wrappedSession, ancestor](const NThreading::TFuture<uint64_t>& result) {
            auto sessionPtr = session.lock();
            if (!sessionPtr) {
                return;
            }

            if (IsDone()) {
                return;
            }
            
            bool gotMaxSeqNo = false;
            {
                std::lock_guard lock(Lock);
                if (result.HasException()) {
                    LOG_LAZY(sessionPtr->DbDriverState->Log, TLOG_ERR, sessionPtr->LogPrefix() << "Failed to get max seq no for partition " << ancestor << " for splitted partition " << PartitionId);
                    TSessionClosedEvent sessionClosedEvent(EStatus::INTERNAL_ERROR, {});
                    sessionPtr->GetSessionClosedEventAndDie(wrappedSession, std::move(sessionClosedEvent));
                    MoveTo(EState::Done);
                    return;
                }

                UpdateMaxSeqNo(result.GetValue());
                if (--NotReadyFutures == 0) {
                    MoveTo(EState::GotMaxSeqNo);   
                    gotMaxSeqNo = true;
                }
            }

            if (gotMaxSeqNo) {
                sessionPtr->RunMainWorker();
            }
        });
        lock.lock();
        GetMaxSeqNoFutures.push_back(future);
    }
    
    if (ancestors.empty()) {
        LOG_LAZY(Session->DbDriverState->Log, TLOG_INFO, Session->LogPrefix() << "No ancestors found for partition " << PartitionId);
        MoveTo(EState::Init);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TKeyedWriteSession::TEventsWorkerWrapper

TKeyedWriteSession::TEventsWorker::TEventsWorker(TKeyedWriteSession* session)
    : Session(session)
{
    EventsPromise = NThreading::NewPromise();
    EventsFuture = EventsPromise.GetFuture();

    AddReadyToAcceptEvent();
}

void TKeyedWriteSession::TEventsWorker::HandleAcksEvent(std::uint64_t partition, TWriteSessionEvent::TAcksEvent&& event) {
    auto [queueIt, _] = PartitionsEventQueues.try_emplace(partition);
    queueIt->second.push_back(TWriteSessionEvent::TEvent(std::move(event)));
}

void TKeyedWriteSession::TEventsWorker::HandleReadyToAcceptEvent(std::uint32_t partition, TWriteSessionEvent::TReadyToAcceptEvent&& event) {
    Session->MessagesWorker->HandleContinuationToken(partition, std::move(event.ContinuationToken));
}
    
void TKeyedWriteSession::TEventsWorker::HandleSessionClosedEvent(TSessionClosedEvent&& event, std::uint32_t partition) {
    if (event.IsSuccess()) {
        return;
    }

    Session->Partitions[partition].Locked_ = true;

    if (event.GetStatus() == EStatus::OVERLOADED) {
        Session->HandleAutoPartitioning(partition);
        return;
    }

    if (!CloseEvent.has_value()) {
        CloseEvent = std::move(event);
    }
    Session->NonBlockingClose();
}

bool TKeyedWriteSession::TEventsWorker::RunEventLoop(WrappedWriteSessionPtr wrappedSession, std::uint32_t partition) {
    while (true) {
        auto event = wrappedSession->Session->GetEvent(false);
        if (!event) {
            break;
        }

        if (auto sessionClosedEvent = std::get_if<TSessionClosedEvent>(&*event); sessionClosedEvent) {
            HandleSessionClosedEvent(std::move(*sessionClosedEvent), partition);
            return true;
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

    return false;
}

std::optional<NThreading::TPromise<void>> TKeyedWriteSession::TEventsWorker::DoWork() {
    std::unique_lock lock(Lock);

    while (!ReadyFutures.empty()) {
        auto idx = *ReadyFutures.begin();
        ReadyFutures.erase(idx);
        lock.unlock();
        // RunEventLoop without Lock: sub-session's WaitEvent() completion may run the Subscribe
        // callback (ReadyFutures.insert) synchronously; that callback takes Lock -> same-thread deadlock.
        auto isSessionClosed = RunEventLoop(Session->SessionsWorker->GetWriteSession(idx), idx);
        if (!isSessionClosed) {
            SubscribeToPartition(idx);
        } else {
            UnsubscribeFromPartition(idx);
        }
        lock.lock();
    }

    if (!Session->Done.load() && TransferEventsToOutputQueue()) {
        return EventsPromise;
    }

    return std::nullopt;
}

void TKeyedWriteSession::TEventsWorker::SubscribeToPartition(std::uint32_t partition) {
    if (auto it = Session->SplittedPartitionWorkers.find(partition); it != Session->SplittedPartitionWorkers.end()) {
        Session->Partitions[partition].Future(NThreading::MakeFuture());
        return;
    }

    auto wrappedSession = Session->SessionsWorker->GetWriteSession(partition);
    auto newFuture = wrappedSession->Session->WaitEvent();
    std::weak_ptr<TKeyedWriteSession> session = Session->shared_from_this();
    std::weak_ptr<TEventsWorker> self = shared_from_this();

    newFuture.Subscribe([self, session, partition](const NThreading::TFuture<void>&) {
        auto sessionPtr = session.lock();
        if (!sessionPtr) {
            return;
        }

        auto selfPtr = self.lock();
        if (!selfPtr) {
            return;
        }

        {
            std::lock_guard lock(selfPtr->Lock);
            selfPtr->ReadyFutures.insert(partition);
        }
        sessionPtr->RunMainWorker();
    });
    Session->Partitions[partition].Future(newFuture);
}

std::optional<NThreading::TPromise<void>> TKeyedWriteSession::TEventsWorker::HandleNewMessage() {
    std::lock_guard lock(Lock);
    if (Session->MessagesWorker->IsMemoryUsageOK()) {
        AddReadyToAcceptEvent();
        return EventsPromise;
    }

    return std::nullopt;
}

void TKeyedWriteSession::TEventsWorker::AddReadyToAcceptEvent() {
    EventsOutputQueue.push_back(TWriteSessionEvent::TReadyToAcceptEvent(IssueContinuationToken()));
}

bool TKeyedWriteSession::TEventsWorker::AddSessionClosedIfNeeded() {
    if (!Session->Closed.load()) {
        return false;
    }

    if (!CloseEvent.has_value()) {
        CloseEvent = TSessionClosedEvent(EStatus::SUCCESS, {});
    }

    if (EventsOutputQueue.empty() && (Session->MessagesWorker->IsQueueEmpty() || Session->Done.load())) {
        EventsOutputQueue.push_back(*CloseEvent);
        return true;
    }

    return false;
}

bool TKeyedWriteSession::TEventsWorker::TransferEventsToOutputQueue() {
    bool eventsTransferred = false;
    bool shouldAddReadyToAcceptEvent = false;
    std::unordered_map<std::uint32_t, std::deque<TWriteSessionEvent::TWriteAck>> acks;

    auto messagesWorker = Session->MessagesWorker;
    auto buildOutputAckEvent = [&](std::deque<TWriteSessionEvent::TWriteAck>& acksQueue, std::uint64_t partition, std::optional<std::uint64_t> expectedSeqNo) -> TWriteSessionEvent::TAcksEvent {
        TWriteSessionEvent::TAcksEvent ackEvent;

        if (expectedSeqNo.has_value()) {
            if (acksQueue.front().SeqNo != expectedSeqNo.value()) {
                LOG_LAZY(Session->DbDriverState->Log, TLOG_ERR, Session->LogPrefix() << "Expected seqNo=" << expectedSeqNo.value() << " but got " << acksQueue.front().SeqNo << " for partition " << partition);
            }
            Y_ENSURE(acksQueue.front().SeqNo == expectedSeqNo.value(), TStringBuilder() << "Expected seqNo=" << expectedSeqNo.value() << " but got " << acksQueue.front().SeqNo << " for partition " << Session->Partitions[partition].PartitionId_);
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
            EventsOutputQueue.push_back(buildOutputAckEvent(remainingAcks->second, head.Partition, head.SeqNo));
            finishWithAck();
            continue;
        }

        auto eventsQueueIt = PartitionsEventQueues.find(head.Partition);
        if (eventsQueueIt == PartitionsEventQueues.end() || eventsQueueIt->second.empty()) {
            // No events for this message yet, stop processing (preserve order)
            break;
        }

        auto event = std::move(eventsQueueIt->second.front());
        auto acksEvent = std::get_if<TWriteSessionEvent::TAcksEvent>(&event);
        Y_ABORT_UNLESS(acksEvent, "Expected AcksEvent only in PartitionsEventQueues");

        std::deque<TWriteSessionEvent::TWriteAck> acksQueue;
        std::copy(acksEvent->Acks.begin(), acksEvent->Acks.end(), std::back_inserter(acksQueue));
        EventsOutputQueue.push_back(buildOutputAckEvent(acksQueue, head.Partition, head.SeqNo));
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

    return eventsTransferred;
}

std::list<TWriteSessionEvent::TEvent>::iterator TKeyedWriteSession::TEventsWorker::AckQueueBegin(std::uint32_t partition) {
    auto [queueIt, _] = PartitionsEventQueues.try_emplace(partition);
    return queueIt->second.begin();
}

std::list<TWriteSessionEvent::TEvent>::iterator TKeyedWriteSession::TEventsWorker::AckQueueEnd(std::uint32_t partition) {
    auto [queueIt, _] = PartitionsEventQueues.try_emplace(partition);
    return queueIt->second.end();
}

TKeyedWriteSession::TEventsWorker::EEventType TKeyedWriteSession::TEventsWorker::GetEventType(const TWriteSessionEvent::TEvent& event) {
    if (std::holds_alternative<TSessionClosedEvent>(event)) {
        return EEventType::SessionClosed;
    } else if (std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event)) {
        return EEventType::ReadyToAccept;
    } else if (std::holds_alternative<TWriteSessionEvent::TAcksEvent>(event)) {
        return EEventType::Ack;
    }

    Y_ABORT_UNLESS(false, "Unexpected event type");
}

std::optional<TWriteSessionEvent::TEvent> TKeyedWriteSession::TEventsWorker::GetEventImpl(bool block, const std::vector<EEventType>& eventTypes) {
    std::unique_lock lock(Lock);
    if (EventsOutputQueue.empty() && block) {
        lock.unlock();
        WaitEvent().Wait();
        lock.lock();
    }

    if (!EventsOutputQueue.empty()) {
        if (!eventTypes.empty() && std::find(eventTypes.begin(), eventTypes.end(), GetEventType(EventsOutputQueue.front())) == eventTypes.end()) {
            return std::nullopt;
        }

        auto event = std::move(EventsOutputQueue.front());
        EventsOutputQueue.pop_front();
        return event;
    }

    return std::nullopt;
}

std::optional<TWriteSessionEvent::TEvent> TKeyedWriteSession::TEventsWorker::GetEvent(bool block, const std::vector<EEventType>& eventTypes) {
    {
        std::unique_lock lock(Lock);
        AddSessionClosedIfNeeded();
    }
    auto event = GetEventImpl(block, eventTypes);

    return event;
}

std::vector<TWriteSessionEvent::TEvent> TKeyedWriteSession::TEventsWorker::GetEvents(bool block, std::optional<size_t> maxEventsCount, const std::vector<EEventType>& eventTypes) {
    if (maxEventsCount.has_value() && maxEventsCount.value() == 0) {
        return {};
    }

    {
        std::unique_lock lock(Lock);
        AddSessionClosedIfNeeded();
    }

    std::vector<TWriteSessionEvent::TEvent> events;
    while (true) {
        auto event = GetEventImpl(block, eventTypes);
        if (!event) {
            break;
        }

        events.push_back(std::move(*event));
        if (maxEventsCount.has_value() && events.size() >= maxEventsCount.value()) {
            break;
        }
    }

    return events;
}

NThreading::TFuture<void> TKeyedWriteSession::TEventsWorker::WaitEvent() {
    std::unique_lock lock(Lock);

    AddSessionClosedIfNeeded();
    if (!EventsOutputQueue.empty()) {
        return NThreading::MakeFuture();
    }

    if (EventsFuture.IsReady() && !Session->Closed.load()) {
        EventsPromise = NThreading::NewPromise();
        EventsFuture = EventsPromise.GetFuture();
    }

    return EventsFuture;
}

void TKeyedWriteSession::TEventsWorker::UnsubscribeFromPartition(std::uint32_t partition) {
    ReadyFutures.erase(partition);
    Session->Partitions[partition].Future(NThreading::MakeFuture());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TKeyedWriteSession::TSessionsWorker

TKeyedWriteSession::TSessionsWorker::TSessionsWorker(TKeyedWriteSession* session)
    : Session(session)
{}

TKeyedWriteSession::WrappedWriteSessionPtr TKeyedWriteSession::TSessionsWorker::GetWriteSession(std::uint32_t partition, bool directToPartition) {
    auto sessionIter = SessionsIndex.find(partition);
    if (sessionIter == SessionsIndex.end() || !directToPartition) {
        return CreateWriteSession(partition, directToPartition);
    }

    return sessionIter->second;
}

std::string TKeyedWriteSession::TSessionsWorker::GetProducerId(std::uint32_t partitionId) {
    return std::format("{}_{}", Session->Settings.ProducerIdPrefix_, partitionId);
}

TKeyedWriteSession::WrappedWriteSessionPtr TKeyedWriteSession::TSessionsWorker::CreateWriteSession(std::uint32_t partition, bool directToPartition) {
    auto partitionId = Session->Partitions[partition].PartitionId_;
    auto producerId = GetProducerId(partitionId);
    TWriteSessionSettings alteredSettings = Session->Settings;

    alteredSettings
        .ProducerId(producerId)
        .MessageGroupId(producerId)
        .MaxMemoryUsage(std::numeric_limits<std::uint64_t>::max())
        .RetryPolicy(Session->RetryPolicy)
        .EventHandlers(TWriteSessionSettings::TEventHandlers()
        .ReadyToAcceptHandler({})
        .AcksHandler({})
        .SessionClosedHandler({}));
    
    if (directToPartition) {    
        alteredSettings.DirectWriteToPartition(true);
        alteredSettings.PartitionId(partitionId);
    }
    auto writeSession = std::make_shared<TWriteSessionWrapper>(
        Session->Client->CreateWriteSession(alteredSettings),
        partition);

    if (directToPartition) {
        SessionsIndex.emplace(partition, writeSession);
        Session->EventsWorker->SubscribeToPartition(partition);
    }
    return writeSession;
}

void TKeyedWriteSession::TSessionsWorker::DestroyWriteSession(TSessionsIndexIterator& it, TDuration closeTimeout, bool mustBeEmpty) {
    if (it == SessionsIndex.end() || !it->second) {
        return;
    }

    auto closeResult = it->second->Session->Close(closeTimeout);
    Y_ABORT_UNLESS(!mustBeEmpty || closeResult, "There are still messages in flight");
    const auto partition = it->second->Partition;
    it = SessionsIndex.erase(it);
    Session->EventsWorker->UnsubscribeFromPartition(partition);
}

void TKeyedWriteSession::TSessionsWorker::OnReadFromSession(WrappedWriteSessionPtr wrappedSession) {
    if (wrappedSession->RemoveFromQueue(1)) {
        Y_ABORT_UNLESS(!wrappedSession->IdleSession, "IdleSession is already set");
        auto idleSessionPtr = std::make_shared<TIdleSession>(wrappedSession.get(), TInstant::Now(), Session->Settings.SubSessionIdleTimeout_);
        auto [itIdle, inserted] = IdlerSessions.insert(idleSessionPtr);
        Y_ABORT_UNLESS(inserted, "Duplicate idle session for partition");
        IdlerSessionsIndex[wrappedSession->Partition] = itIdle;
        wrappedSession->IdleSession = idleSessionPtr;
    }
}

void TKeyedWriteSession::TSessionsWorker::OnWriteToSession(WrappedWriteSessionPtr wrappedSession) {
    if (wrappedSession->AddToQueue(1) && wrappedSession->IdleSession) {
        auto itIdle = IdlerSessionsIndex.find(wrappedSession->Partition);
        if (itIdle != IdlerSessionsIndex.end()) {
            IdlerSessions.erase(itIdle->second);
            IdlerSessionsIndex.erase(itIdle);
        }
        wrappedSession->IdleSession.reset();
    }
}

void TKeyedWriteSession::TSessionsWorker::DoWork() {
    while (!IdlerSessions.empty()) {
        auto it = IdlerSessions.begin();
        if (!(*it)->IsExpired()) {
            break;
        }

        const auto partition = (*it)->Session->Partition;

        // Remove idle tracking first to keep containers consistent even if the session
        // is already absent from SessionsIndex.
        IdlerSessions.erase(it);
        IdlerSessionsIndex.erase(partition);

        auto sessionIter = SessionsIndex.find(partition);
        if (sessionIter != SessionsIndex.end()) {
            sessionIter->second->IdleSession.reset();
            DestroyWriteSession(sessionIter, TDuration::Zero());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TKeyedWriteSession::TMessagesWorker

TKeyedWriteSession::TMessagesWorker::TMessagesWorker(TKeyedWriteSession* session)
    : Session(session)
{
}

void TKeyedWriteSession::TMessagesWorker::RechoosePartitionIfNeeded(MessageIter message) {
    const auto& partitionInfo = Session->Partitions[message->Partition];
    if (partitionInfo.Children_.empty()) {
        return;
    }

    // this case means that partition was split, so we need to rechoose the partition for the message
    auto newPartition = Session->PartitionChooser->ChoosePartition(message->Key);
    message->Partition = newPartition;
}

void TKeyedWriteSession::TMessagesWorker::DoWork() {
    auto sessionsWorker = Session->SessionsWorker;

    auto iterateMessagesIndex = [&](std::unordered_map<std::uint32_t, std::list<MessageIter>>& messagesIndex, auto stopCondition) {
        std::vector<std::uint32_t> partitionsProcessed;
        for (auto& [partition, messages] : messagesIndex) {
            while (!messages.empty()) {
                auto head = messages.front();
                if (stopCondition(head)) {
                    break;
                }

                auto wrappedSession = sessionsWorker->GetWriteSession(head->Partition);
                if (!SendMessage(wrappedSession, *head)) {
                    break;
                }

                Session->Metrics.AddWriteLag((TInstant::Now() - head->CreateTimestamp.value_or(TInstant::Now())).MilliSeconds());
                head->Sent = true;
                sessionsWorker->OnWriteToSession(wrappedSession);
                messages.pop_front();
            }

            if (messages.empty()) {
                partitionsProcessed.push_back(partition);
            }
        }

        for (const auto& partition : partitionsProcessed) {
            messagesIndex.erase(partition);
        }
    };

    iterateMessagesIndex(
        MessagesToResendIndex,
        [](MessageIter) {
            return false;
        }
    );

    iterateMessagesIndex(
        PendingMessagesIndex,
        [this](MessageIter head) {
        return Session->Partitions[head->Partition].Locked_ ||
            MessagesToResendIndex.contains(head->Partition);
        }
    );
}

bool TKeyedWriteSession::TMessagesWorker::SendMessage(WrappedWriteSessionPtr wrappedSession, const TMessageInfo& message) {    
    if (!wrappedSession) {
        return false;
    }
    
    auto continuationToken = GetContinuationToken(message.Partition);
    if (!continuationToken) {
        return false;
    }
    
    wrappedSession->Session->Write(std::move(*continuationToken), message.BuildMessage(), message.Tx);
    return true;
}

void TKeyedWriteSession::TMessagesWorker::PushInFlightMessage(std::uint32_t partition, TMessageInfo&& message) {
    auto iter = InFlightMessages.insert(InFlightMessages.end(), std::move(message));
    auto [inFlightMessagesIndexIt, _] = InFlightMessagesIndex.try_emplace(partition);
    inFlightMessagesIndexIt->second.push_back(iter);

    auto [pendingMessagesIndexIt, __] = PendingMessagesIndex.try_emplace(partition);
    pendingMessagesIndexIt->second.push_back(iter);
}

void TKeyedWriteSession::TMessagesWorker::HandleAck() {
    PopInFlightMessage();
}

void TKeyedWriteSession::TMessagesWorker::PopInFlightMessage() {
    Y_ABORT_UNLESS(!InFlightMessages.empty());
    const std::uint64_t partition = InFlightMessages.front().Partition;
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

    Y_ABORT_UNLESS(it->Data.size() <= MemoryUsage, "MemoryUsage is less than the size of the message");
    MemoryUsage -= it->Data.size();
    InFlightMessages.pop_front();
}

bool TKeyedWriteSession::TMessagesWorker::IsMemoryUsageOK() const {
    return MemoryUsage <= Session->Settings.MaxMemoryUsage_ / 2;
}

void TKeyedWriteSession::TMessagesWorker::AddMessage(const std::string& key, TWriteMessage&& message, std::uint32_t partition, TTransactionBase* tx) {
    MemoryUsage += message.Data.size();
    PushInFlightMessage(partition, TMessageInfo(key, std::move(message), partition, tx));
}

std::optional<TContinuationToken> TKeyedWriteSession::TMessagesWorker::GetContinuationToken(std::uint32_t partition) {
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

void TKeyedWriteSession::TMessagesWorker::HandleContinuationToken(std::uint32_t partition, TContinuationToken&& continuationToken) {
    auto [it, _] = ContinuationTokens.try_emplace(partition);
    it->second.push_back(std::move(continuationToken));
}

bool TKeyedWriteSession::TMessagesWorker::IsQueueEmpty() const {
    return InFlightMessages.empty();
}

const TKeyedWriteSession::TMessageInfo& TKeyedWriteSession::TMessagesWorker::GetFrontInFlightMessage() const {
    Y_ABORT_UNLESS(!InFlightMessages.empty());
    return InFlightMessages.front();
}

bool TKeyedWriteSession::TMessagesWorker::HasInFlightMessages() const {
    return !InFlightMessages.empty();
}

void TKeyedWriteSession::TMessagesWorker::ScheduleResendMessages(std::uint32_t partition, std::uint64_t afterSeqNo) {
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
        if (!(*resendIt)->SeqNo.has_value() || (*resendIt)->SeqNo.value() > afterSeqNo) {
            break;
        }

        auto seqNo = (*resendIt)->SeqNo.value();
        if (ackQueueIt == ackQueueEnd) {
            // this case can happen if the message was sent, but session was closed before the ack was received
            TWriteSessionEvent::TWriteAck ack;
            ack.SeqNo = seqNo;
            acksToSend.push_back(std::move(ack));
        } else {
            auto acksEvent = std::get_if<TWriteSessionEvent::TAcksEvent>(&*ackQueueIt);
            if (ackIdx == acksEvent->Acks.size()) {
                ++ackQueueIt;
                ackIdx = 0;
                continue;
            }

            if (acksEvent->Acks[ackIdx].SeqNo > seqNo) {
                // this case can happen if the message was sent, but session was closed before the ack was received
                TWriteSessionEvent::TWriteAck ack;
                ack.SeqNo = seqNo;
                acksEvent->Acks.insert(acksEvent->Acks.begin() + ackIdx, std::move(ack));
            }
            ++ackIdx;
        }
        ++resendIt;     
    }

    if (!acksToSend.empty()) {
        TWriteSessionEvent::TAcksEvent event;
        event.Acks = std::move(acksToSend);
        Session->EventsWorker->HandleAcksEvent(partition, std::move(event));
    }

    // IMPORTANT: do not mutate InFlightMessagesIndex while holding references/iterators to its elements.
    // try_emplace()/rehash may invalidate 'it' and 'list' -> use-after-free and segfaults.
    std::vector<std::pair<std::uint64_t, MessageIter>> messagesFromOldPartition;
    messagesFromOldPartition.reserve(std::distance(resendIt, list.end()));
    auto currentSeqNo = resendIt != list.end() ? (*resendIt)->SeqNo.value_or(0) : 0;
    for (auto iter = resendIt; iter != list.end(); ++iter) {
        if (iter != resendIt && currentSeqNo != 0) {
            Y_ABORT_UNLESS((*iter)->SeqNo.value_or(0) > currentSeqNo, "SeqNo is not increasing for partition %d", partition);
        }

        auto newPartition = Session->PartitionChooser->ChoosePartition((*iter)->Key);
        (*iter)->Partition = newPartition;
        messagesFromOldPartition.emplace_back(newPartition, *iter);

        currentSeqNo = (*iter)->SeqNo.value_or(0);
    }
    
    list.erase(resendIt, list.end());
    for (const auto& [newPartition, msgIt] : messagesFromOldPartition) {
        auto [inFlightMessagesIndexChainIt, _] = InFlightMessagesIndex.try_emplace(newPartition);
        inFlightMessagesIndexChainIt->second.push_back(msgIt);

        if (msgIt->Sent) {
            auto [messagesToResendChainIt, __] = MessagesToResendIndex.try_emplace(newPartition);
            messagesToResendChainIt->second.push_back(msgIt);
        }
    }

    InFlightMessagesIndex.erase(partition);
}

void TKeyedWriteSession::TMessagesWorker::RebuildPendingMessagesIndex(std::uint32_t partition) {
    auto [oldPendingMessagesIndexChainIt, __] = PendingMessagesIndex.try_emplace(partition);
    std::unordered_map<std::uint32_t, std::list<MessageIter>> pendingMessagesForNewPartitions;
    for (auto it = oldPendingMessagesIndexChainIt->second.begin(); it != oldPendingMessagesIndexChainIt->second.end(); ++it) {
        auto newPartition = Session->PartitionChooser->ChoosePartition((*it)->Key);
        auto [pendingMessagesForNewPartitionsIt, __] = pendingMessagesForNewPartitions.try_emplace(newPartition);
        pendingMessagesForNewPartitionsIt->second.push_back(*it);
    }

    for (const auto& [newPartition, pendingMessagesForNewPartition] : pendingMessagesForNewPartitions) {
        auto [pendingMessagesIndexChainIt, __] = PendingMessagesIndex.try_emplace(newPartition);
        for (auto reverseIt = pendingMessagesForNewPartition.rbegin(); reverseIt != pendingMessagesForNewPartition.rend(); ++reverseIt) {
            pendingMessagesIndexChainIt->second.push_front(*reverseIt);
        }
    }

    PendingMessagesIndex.erase(partition);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TKeyedWriteSession::TKeyedWriteSessionRetryPolicy

TKeyedWriteSession::TKeyedWriteSessionRetryPolicy::TKeyedWriteSessionRetryPolicy(TKeyedWriteSession* session)
    : Session(session)
{}

typename TKeyedWriteSession::TKeyedWriteSessionRetryPolicy::IRetryState::TPtr TKeyedWriteSession::TKeyedWriteSessionRetryPolicy::CreateRetryState() const {
    struct TRetryState : public IRetryState {
        TRetryState(TKeyedWriteSession* session)
            : Session(session)
        {}
        ~TRetryState() = default;
        TMaybe<TDuration> GetNextRetryDelay(EStatus status) override {
            if (status == EStatus::OVERLOADED) {
                return Nothing();
            }

            if (!UserRetryState) {
                auto policy = Session->Settings.RetryPolicy_ ? Session->Settings.RetryPolicy_ : NYdb::NTopic::IRetryPolicy::GetDefaultPolicy();
                UserRetryState = policy->CreateRetryState();
            }

            return UserRetryState->GetNextRetryDelay(status);
        }

    private:
        TKeyedWriteSession* Session;
        IRetryState::TPtr UserRetryState;
    };
    
    return std::make_unique<TRetryState>(Session);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TKeyedWriteSession::Metrics

void TKeyedWriteSession::TMetricGauge::Add(std::uint64_t value) {
    Sum += value;
    MetricCount++;
}

void TKeyedWriteSession::TMetricGauge::Clear() {
    Sum = 0;
    MetricCount = 0;
}

long double TKeyedWriteSession::TMetricGauge::Average() {
    if (MetricCount == 0) {
        return 0;
    }

    return (long double)Sum / (long double)MetricCount;
}

TKeyedWriteSession::TMetrics::TMetrics(TKeyedWriteSession* session): Session(session) {}

void TKeyedWriteSession::TMetrics::AddMainWorkerTime(std::uint64_t ms) {
    std::lock_guard lock(Lock);
    MainWorkerTimeMs.Add(ms);
}

void TKeyedWriteSession::TMetrics::AddCycleTime(std::uint64_t ms) {
    std::lock_guard lock(Lock);
    CycleTimeMs.Add(ms);
}

void TKeyedWriteSession::TMetrics::AddWriteLag(std::uint64_t lagMs) {
    std::lock_guard lock(Lock);
    WriteLagMs.Add(lagMs);
}

void TKeyedWriteSession::TMetrics::PrintMetrics() {
    std::lock_guard lock(Lock);
    LOG_LAZY(Session->DbDriverState->Log, TLOG_ERR, Session->LogPrefix() << "METRICS: MainWorkerTimeMs: " << MainWorkerTimeMs.Average() << " ms, CycleTimeMs: " << CycleTimeMs.Average() << " ms, WriteLagMs: " << WriteLagMs.Average() << " ms");
    MainWorkerTimeMs.Clear();
    CycleTimeMs.Clear();
    WriteLagMs.Clear();
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TKeyedWriteSession

TKeyedWriteSession::TKeyedWriteSession(
    const TKeyedWriteSessionSettings& settings,
    std::shared_ptr<TTopicClient::TImpl> client,
    std::shared_ptr<TGRpcConnectionsImpl> connections,
    TDbDriverStatePtr dbDriverState)
    : Connections(connections),
    Client(client),
    DbDriverState(dbDriverState),
    Metrics(this),
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
    auto partitions = topicConfig.GetTopicDescription().GetPartitions();
    std::sort(partitions.begin(), partitions.end(), [](const auto& a, const auto& b) -> bool {
        return a.GetPartitionId() < b.GetPartitionId();
    });

    auto partitionChooserStrategy = settings.PartitionChooserStrategy_;
    auto strategy = topicConfig.GetTopicDescription().GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy();
    auto autoPartitioningEnabled = (strategy != EAutoPartitioningStrategy::Disabled &&
                                strategy != EAutoPartitioningStrategy::Unspecified);

    for (const auto& partition : partitions) {
        auto partitionId = partition.GetPartitionId();
        auto fromBound = partition.GetFromBound().value_or("");
        auto toBound = partition.GetToBound();
        LOG_LAZY(DbDriverState->Log, TLOG_ERR, LogPrefix() << "Adding partition " << partitionId << " from bound " << fromBound << " to bound " << (toBound.has_value() ? toBound.value() : "null"));
        Partitions[partitionId] = TPartitionInfo()
            .PartitionId(partitionId)
            .FromBound(fromBound)
            .ToBound(toBound);
    }

    for (const auto& partition : partitions) {
        auto children = partition.GetChildPartitionIds();

        std::vector<std::uint32_t> childrenIndices;
        childrenIndices.reserve(children.size());
        for (auto child : children) {
            childrenIndices.push_back(child);
        }
        Partitions[partition.GetPartitionId()].Children(childrenIndices);
    }

    if (Settings.EventHandlers_.CommonHandler_) {
        EventTypesWithHandlers.push_back(TEventsWorker::EEventType::SessionClosed);
        EventTypesWithHandlers.push_back(TEventsWorker::EEventType::ReadyToAccept);
        EventTypesWithHandlers.push_back(TEventsWorker::EEventType::Ack);
    } else {
        if (Settings.EventHandlers_.SessionClosedHandler_) {
            EventTypesWithHandlers.push_back(TEventsWorker::EEventType::SessionClosed);
        }
        if (Settings.EventHandlers_.ReadyToAcceptHandler_) {
            EventTypesWithHandlers.push_back(TEventsWorker::EEventType::ReadyToAccept);
        }
        if (Settings.EventHandlers_.AcksHandler_) {
            EventTypesWithHandlers.push_back(TEventsWorker::EEventType::Ack);
        }
    }

    switch (partitionChooserStrategy) {
        case TKeyedWriteSessionSettings::EPartitionChooserStrategy::Bound:
            PartitioningKeyHasher = settings.PartitioningKeyHasher_;
            PartitionChooser = std::make_unique<TBoundPartitionChooser>(this);
            for (size_t i = 0; i < Partitions.size(); ++i) {
                if (i > 0 && Partitions[i].FromBound_.empty() && !Partitions[i].ToBound_.has_value()) {
                    ythrow TContractViolation("Unbounded partition is not supported for Bound partition chooser strategy");
                }

                if (!Partitions[i].Children_.empty()) {
                    continue;
                }

                PartitionsIndex[Partitions[i].FromBound_] = Partitions[i].PartitionId_;
            }
            break;
        case TKeyedWriteSessionSettings::EPartitionChooserStrategy::Hash:
            if (autoPartitioningEnabled) {
                ythrow TContractViolation("Hash partition chooser strategy is not supported for topic with auto partitioning");
            }

            std::vector<std::uint32_t> partitionsIds;
            partitionsIds.reserve(partitions.size());
            for (const auto& partition : partitions) {
                partitionsIds.push_back(partition.GetPartitionId());
            }

            PartitionChooser = std::make_unique<THashPartitionChooser>(std::move(partitionsIds));
            break;
    }

    ClosePromise = NThreading::NewPromise();
    CloseFuture = ClosePromise.GetFuture();
    ShutdownPromise = NThreading::NewPromise();
    ShutdownFuture = ShutdownPromise.GetFuture();

    SessionsWorker = std::make_shared<TSessionsWorker>(this);
    MessagesWorker = std::make_shared<TMessagesWorker>(this);
    EventsWorker = std::make_shared<TEventsWorker>(this);
    RetryPolicy = std::make_shared<TKeyedWriteSessionRetryPolicy>(this);

    // Start handlers executor for user callbacks (Acks/ReadyToAccept/SessionClosed/Common).
    Settings.EventHandlers_.HandlersExecutor_->Start();

    CloseFuture.Subscribe([this](const NThreading::TFuture<void>&) {
        RunMainWorker();
    });

    RunMainWorker();

    LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Keyed write session created");
}

std::vector<TKeyedWriteSession::TPartitionInfo> TKeyedWriteSession::GetPartitions() const {
    std::vector<TPartitionInfo> partitions;
    partitions.reserve(Partitions.size());
    for (const auto& [partitionId, partitionInfo] : Partitions) {
        partitions.push_back(partitionInfo);
    }
    return partitions;
}

void TKeyedWriteSession::Write(TContinuationToken&&, const std::string& key, TWriteMessage&& message, TTransactionBase* tx) {
    std::optional<NThreading::TPromise<void>> eventsPromise;
    {
        std::lock_guard lock(GlobalLock);
        if (Closed.load()) {
            return;
        }

        if ((message.SeqNo_.has_value() && SeqNoStrategy == ESeqNoStrategy::WithoutSeqNo)
            || (!message.SeqNo_.has_value() && SeqNoStrategy == ESeqNoStrategy::WithSeqNo)) {
            ythrow TContractViolation("Can not mix messages with and without seqNo");
        }

        if (SeqNoStrategy == ESeqNoStrategy::NotInitialized) {
            SeqNoStrategy = message.SeqNo_.has_value() ? ESeqNoStrategy::WithSeqNo : ESeqNoStrategy::WithoutSeqNo;
        }

        auto partition = PartitionChooser->ChoosePartition(key);
        MessagesWorker->AddMessage(key, std::move(message), partition, tx);
        eventsPromise = EventsWorker->HandleNewMessage();
        RunUserEventLoop();
    }

    RunMainWorker();
    if (eventsPromise) {
        eventsPromise->TrySetValue();
    }
}

bool TKeyedWriteSession::Close(TDuration closeTimeout) {
    if (Closed.exchange(true)) {
        std::lock_guard lock(GlobalLock);
        return MessagesWorker->IsQueueEmpty();
    }

    SetCloseDeadline(closeTimeout);

    ClosePromise.TrySetValue();
    ShutdownFuture.Wait(CloseDeadline);
    RunUserEventLoop();
    Done.store(true);

    // No need to lock here, because we are waiting for the shutdown future and it will block until the main worker is done
    return MessagesWorker->IsQueueEmpty();
}

void TKeyedWriteSession::NonBlockingClose() {
    Closed.store(true);
    Done.store(true);
}

void TKeyedWriteSession::SetCloseDeadline(const TDuration& closeTimeout) {
    std::lock_guard lock(GlobalLock);
    CloseDeadline = TInstant::Now() + closeTimeout;
}

TKeyedWriteSession::~TKeyedWriteSession() {
    Close(TDuration::Zero());
    Settings.EventHandlers_.HandlersExecutor_->Stop();
    ShutdownFuture.Wait();
}

NThreading::TFuture<void> TKeyedWriteSession::WaitEvent() {
    return EventsWorker->WaitEvent();
}

std::optional<TWriteSessionEvent::TEvent> TKeyedWriteSession::GetEvent(bool block) {
    if (Settings.EventHandlers_.CommonHandler_) {
       return std::nullopt;
    }

    return EventsWorker->GetEvent(block);
}

std::vector<TWriteSessionEvent::TEvent> TKeyedWriteSession::GetEvents(bool block, std::optional<size_t> maxEventsCount) {
    if (Settings.EventHandlers_.CommonHandler_) {
        return {};
    }

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

bool TKeyedWriteSession::RunSplittedPartitionWorkers() {
    if (SplittedPartitionWorkers.empty()) {
        return false;
    }

    bool needRerun = false;
    for (const auto& [partition, splittedPartitionWorker] : SplittedPartitionWorkers) {
        if (splittedPartitionWorker->IsDone()) {
            continue;
        }

        splittedPartitionWorker->DoWork();
        needRerun = needRerun || splittedPartitionWorker->IsInit();
        needRerun = needRerun || splittedPartitionWorker->IsDone();
    }

    return needRerun;
}

void TKeyedWriteSession::RunUserEventLoop() {
    if (!Settings.EventHandlers_.AcksHandler_ &&
        !Settings.EventHandlers_.ReadyToAcceptHandler_ &&
        !Settings.EventHandlers_.SessionClosedHandler_ &&
        !Settings.EventHandlers_.CommonHandler_) {
        return;
    }

    auto handlersExecutor = Settings.EventHandlers_.HandlersExecutor_;
    if (!handlersExecutor) {
        return;
    }

    while (true) {
        auto event = EventsWorker->GetEvent(false, EventTypesWithHandlers);
        if (!event) {
            break;
        }

        if (auto* readyToAcceptEvent = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
            if (Settings.EventHandlers_.ReadyToAcceptHandler_) {
                handlersExecutor->Post(
                    [this, ev = std::move(*readyToAcceptEvent)]() mutable {
                        Settings.EventHandlers_.ReadyToAcceptHandler_(ev);
                    });
            } else if (Settings.EventHandlers_.CommonHandler_) {
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
            } else if (Settings.EventHandlers_.CommonHandler_) {
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

void TKeyedWriteSession::GetSessionClosedEventAndDie(WrappedWriteSessionPtr wrappedSession, std::optional<TSessionClosedEvent> sessionClosedEvent) {
    std::optional<TSessionClosedEvent> receivedSessionClosedEvent;
    while (true) {
        auto event = wrappedSession->Session->GetEvent(false);
        if (!event) {
            break;
        }

        if (auto* closedEvent = std::get_if<TSessionClosedEvent>(&*event)) {
            receivedSessionClosedEvent = std::move(*closedEvent);
            break;
        }
    }

    if (!receivedSessionClosedEvent || receivedSessionClosedEvent->GetStatus() == EStatus::SUCCESS || receivedSessionClosedEvent->GetStatus() == EStatus::OVERLOADED) {
        LOG_LAZY(DbDriverState->Log, TLOG_ERR, LogPrefix() << "Failed to get session closed event");
        EventsWorker->HandleSessionClosedEvent(std::move(*sessionClosedEvent), wrappedSession->Partition);
    } else {
        EventsWorker->HandleSessionClosedEvent(std::move(*receivedSessionClosedEvent), wrappedSession->Partition);
    }
}

TStringBuilder TKeyedWriteSession::LogPrefix() {
    return TStringBuilder() << " SessionId: " << Settings.SessionId_ << " Epoch: " << Epoch.load() << " ";
}

void TKeyedWriteSession::NextEpoch() {
    auto maxEpoch = MAX_EPOCH - 1;
    if (Epoch.compare_exchange_weak(maxEpoch, 0)) {
        LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Epoch overflow, resetting to 0");
        return;
    }

    Epoch.fetch_add(1);
}

void TKeyedWriteSession::RunMainWorker() {
    // This function is both "request to run" and the runner itself.
    // We must handle two properties:
    // - TFuture::Subscribe may call back synchronously when future is already ready.
    // - A callback may race with the runner trying to go idle (avoid lost wakeups).
    enum : std::uint8_t {
        Idle = 0,
        Running = 1,
        Rerun = 2,
    };

    // Try to become the runner. If already running, just request a rerun.
    std::uint8_t state = MainWorkerState.load(std::memory_order_acquire);
    for (;;) {
        if (state & Running) {
            if (MainWorkerState.compare_exchange_weak(state, std::uint8_t(state | Rerun),
                                                     std::memory_order_acq_rel,
                                                     std::memory_order_acquire)) {
                return;
            }
            continue;
        } else {
            if (MainWorkerState.compare_exchange_weak(state, Running,
                                                     std::memory_order_acq_rel,
                                                     std::memory_order_acquire)) {
                break; // we are the runner now
            }
            continue;
        }
    }

    NextEpoch();

    auto startWorkerTime = TInstant::Now();
    // Runner loop: process, arm subscription, then either go idle or loop again.
    for (;;) {
        auto startIter = TInstant::Now();
        // Clear rerun request for this iteration.
        MainWorkerState.fetch_and(std::uint8_t(~Rerun), std::memory_order_acq_rel);
        bool needRerun = false;
        std::optional<NThreading::TPromise<void>> eventsPromise;

        {
            std::unique_lock lock(GlobalLock);
            eventsPromise = EventsWorker->DoWork();
            RunUserEventLoop();
            needRerun = RunSplittedPartitionWorkers();
            if (!Done.load()) {
                SessionsWorker->DoWork();
                MessagesWorker->DoWork();
            }
        }

        if (eventsPromise) {
            eventsPromise->TrySetValue();
        }

        const auto isClosed = Closed.load();
        const auto closeTimeout = GetCloseTimeout();
        if (isClosed && (Done.load() || MessagesWorker->IsQueueEmpty() || closeTimeout == TDuration::Zero())) {
            ShutdownPromise.TrySetValue();
            EventsWorker->EventsPromise.TrySetValue();
            ClosePromise.TrySetValue();
            MainWorkerState.store(Idle, std::memory_order_release);
            return;
        }

        if (needRerun) {
            // we need this case to start resending messages if there are any
            Metrics.AddCycleTime((TInstant::Now() - startIter).MilliSeconds());
            continue;
        }

        // Try to go idle. If someone requested rerun concurrently, keep running.
        std::uint8_t cur = MainWorkerState.load(std::memory_order_acquire);
        for (;;) {
            if (cur & Rerun) {
                Metrics.AddCycleTime((TInstant::Now() - startIter).MilliSeconds());
                break; // continue outer loop
            }
            if (MainWorkerState.compare_exchange_weak(cur, Idle,
                                                     std::memory_order_acq_rel,
                                                     std::memory_order_acquire)) {
                auto workerFinished = TInstant::Now();
                Metrics.AddCycleTime((workerFinished - startIter).MilliSeconds());
                Metrics.AddMainWorkerTime((workerFinished - startWorkerTime).MilliSeconds());
                return; // successfully went idle
            }
        }
        // Rerun was requested; continue the loop without recursion.
    }
}

TInstant TKeyedWriteSession::GetCloseDeadline() {
    std::lock_guard lock(GlobalLock);
    return CloseDeadline;
}

void TKeyedWriteSession::HandleAutoPartitioning(std::uint32_t partition) {
    LOG_LAZY(DbDriverState->Log, TLOG_ERR, LogPrefix() << "HandleAutoPartitioning: " << partition);
    auto splittedPartitionWorker = std::make_shared<TSplittedPartitionWorker>(this, partition);
    SplittedPartitionWorkers.try_emplace(partition, splittedPartitionWorker);
}

std::string TKeyedWriteSession::GetProducerId(std::uint32_t partition) {
    return std::format("{}_{}", Settings.ProducerIdPrefix_, partition);
}

TWriterCounters::TPtr TKeyedWriteSession::GetCounters() {
    return nullptr;
}

TKeyedWriteSession::TBoundPartitionChooser::TBoundPartitionChooser(TKeyedWriteSession* session)
    : Session(session)
{}

std::uint32_t TKeyedWriteSession::TBoundPartitionChooser::ChoosePartition(const std::string_view key) {
    auto hashedKey = Session->PartitioningKeyHasher(key);

    auto lowerBound = Session->PartitionsIndex.lower_bound(hashedKey);
    if (lowerBound != Session->PartitionsIndex.end() && lowerBound->first == hashedKey) {
        return lowerBound->second;
    }

    Y_ABORT_IF(lowerBound == Session->PartitionsIndex.begin(), "Lower bound is the first element");
    return std::prev(lowerBound)->second;
}

TKeyedWriteSession::THashPartitionChooser::THashPartitionChooser(std::vector<std::uint32_t>&& partitions)
    : Partitions(std::move(partitions))
{}

std::uint32_t TKeyedWriteSession::THashPartitionChooser::ChoosePartition(const std::string_view key) {
    auto hash = MurmurHash<std::uint64_t>(key.data(), key.size());
    return Partitions[hash % Partitions.size()];
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

bool TSimpleBlockingKeyedWriteSession::WaitForAck(std::optional<std::uint64_t> seqNo, TDuration timeout) {
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