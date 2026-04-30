#include <util/system/byteorder.h>
#include <ydb/public/sdk/cpp/src/client/topic/common/log_lazy.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/producer.h>
#include <library/cpp/string_utils/url/url.h>
#include <util/digest/murmur.h>
#include <util/string/hex.h>
#include <util/generic/guid.h>

#include <format>

namespace NYdb::inline Dev::NTopic {

namespace {

static constexpr auto PARTITION_KEY_META_KEY = "__partition_key";

} // namespace

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TProducer

// TProducerSettings

std::string TProducerSettings::DefaultPartitioningKeyHasher(const std::string_view key) {
    const std::uint64_t lo = MurmurHash<std::uint64_t>(key.data(), key.size(), std::uint64_t{0});
    const std::uint64_t loBe = InetToHost(lo);

    std::string out;
    out.resize(8);
    memcpy(out.data(), &loBe, 8);
    return out; // 8 bytes
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TProducer::TPartitionInfo

bool TProducer::TPartitionInfo::InRange(const std::string_view key) const {
    if (FromBound_ > key) {
        return false;
    }
    if (ToBound_.has_value() && *ToBound_ <= key) {
        return false;
    }
    return true;
}

bool TProducer::TPartitionInfo::IsSplitted() const {
    return !Children_.empty();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TProducer::TMessageInfo

TProducer::TMessageInfo::TMessageInfo(const std::string& key, const std::string& choosePartitionKey, TWriteMessage&& message, std::uint32_t partition)
    : Key(key)
    , Data(message.Data)
    , Codec(message.Codec)
    , OriginalSize(message.OriginalSize)
    , SeqNo(message.SeqNo_)
    , CreateTimestamp(message.CreateTimestamp_)
    , Tx(message.Tx_)
    , Partition(partition)
{
    for (const auto& [key, value] : message.MessageMeta_) {
        MessageMeta.Fields.emplace_back(key, value);
    }

    if (!choosePartitionKey.empty()) {
        MessageMeta.Fields.emplace_back(PARTITION_KEY_META_KEY, choosePartitionKey);
    }
}

TWriteMessage TProducer::TMessageInfo::BuildMessage() const {
    TWriteMessage message(Data);
    message.Codec = Codec;
    message.OriginalSize = OriginalSize;
    message.SeqNo(SeqNo);
    message.CreateTimestamp(CreateTimestamp);
    for (const auto& [key, value] : MessageMeta.Fields) {
        message.MessageMeta_.emplace_back(key, value);
    }
    message.Tx(Tx);
    return message;
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TProducer::TWriteSessionWrapper

TProducer::TWriteSessionWrapper::TWriteSessionWrapper(WriteSessionPtr session, std::uint32_t partition, bool directToPartition)
    : Session(std::move(session))
    , Partition(partition)
    , DirectToPartition(directToPartition)
{}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TProducer::TIdleSession

bool TProducer::TIdleSession::Less(const std::shared_ptr<TIdleSession>& other) const {
    if (EmptySince == other->EmptySince) {
        return Session->Partition < other->Session->Partition;
    }

    return EmptySince < other->EmptySince;
}

bool TProducer::TIdleSession::Comparator::operator()(
    const std::shared_ptr<TIdleSession>& first,
    const std::shared_ptr<TIdleSession>& second) const {
    return first->Less(second);
}

bool TProducer::TIdleSession::IsExpired() const {
    return TInstant::Now() - EmptySince > IdleTimeout;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TProducer::TSplittedPartitionWorker

TProducer::TSplittedPartitionWorker::TSplittedPartitionWorker(TProducer* producer, std::uint32_t partitionId)
    : Producer(producer)
    , PartitionId(partitionId)
{
    LOG_LAZY(Producer->DbDriverState->Log, TLOG_INFO, Producer->LogPrefix() << "Creating splitted partition worker for partition " << PartitionId);
}

std::string TProducer::TSplittedPartitionWorker::GetStateName() const {
    switch (State) {
        case EState::Init:
            return "Init";
        case EState::PendingDescribe:
            return "PendingDescribe";
        case EState::GotDescribe:
            return "GotDescribe";
        case EState::PendingMaxSeqNo:
            return "PendingMaxSeqNo";
        case EState::GotMaxSeqNo:
            return "GotMaxSeqNo";
        case EState::Failed:
            return "Failed";
        case EState::Done:
            return "Done";
    }
}

void TProducer::TSplittedPartitionWorker::DoWork() {
    // Must be called while TProducer::GlobalLock is held. After this worker's
    // lock is released below, we mutate producer-level workers and partitions
    // that are protected by GlobalLock.
    std::vector<WrappedWriteSessionPtr> writeSessionsToCloseOnError;
    std::vector<std::uint32_t> writeSessionPartitionsToDestroy;
    bool handleGotMaxSeqNo = false;
    std::uint64_t maxSeqNo = 0;
    std::unordered_map<std::uint32_t, std::uint64_t> cachedMaxSeqNos;

    std::unique_lock lock(Lock);
    std::weak_ptr<TProducer> producer = Producer->shared_from_this();
    switch (State) {
        case EState::Init: {
            DescribeTopicFuture = Producer->Client->DescribeTopic(Producer->Settings.Path_, TDescribeTopicSettings());
            lock.unlock();
            std::weak_ptr<TSplittedPartitionWorker> self = weak_from_this();
            DescribeTopicFuture.Subscribe([self, producer](const NThreading::TFuture<TDescribeTopicResult>&) {
                auto selfPtr = self.lock();
                if (!selfPtr) {
                    return;
                }

                auto producerPtr = producer.lock();
                if (!producerPtr) {
                    return;
                }

                {
                    std::lock_guard lock(selfPtr->Lock);
                    selfPtr->MoveTo(EState::GotDescribe);
                }

                producerPtr->RunMainWorker(static_cast<std::int64_t>(selfPtr->PartitionId));
            });
            lock.lock();
            if (State == EState::Init) {
                MoveTo(EState::PendingDescribe);
            }
            break;
        }
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
        case EState::Failed:
            writeSessionsToCloseOnError.swap(WriteSessionsToCloseOnError);
            writeSessionPartitionsToDestroy.swap(WriteSessionPartitionsToDestroy);
            MoveTo(EState::Done);
            break;
        case EState::GotMaxSeqNo:
            handleGotMaxSeqNo = true;
            maxSeqNo = MaxSeqNo;
            cachedMaxSeqNos = CachedMaxSeqNos;
            writeSessionPartitionsToDestroy.swap(WriteSessionPartitionsToDestroy);
            MoveTo(EState::Done);
            break;
    }
    lock.unlock();

    for (const auto& writeSession : writeSessionsToCloseOnError) {
        if (!writeSession) {
            continue;
        }
        TSessionClosedEvent sessionClosedEvent(EStatus::INTERNAL_ERROR, {});
        Producer->GetSessionClosedEventAndDie(writeSession, std::move(sessionClosedEvent));
    }

    if (handleGotMaxSeqNo) {
        Producer->MessagesWorker->RebuildPendingMessagesIndex(PartitionId);
        Producer->MessagesWorker->ScheduleResendMessages(PartitionId, maxSeqNo);
        auto partitionIt = Producer->Partitions.find(PartitionId);
        Y_ABORT_UNLESS(partitionIt != Producer->Partitions.end(), "Partition %u not found", PartitionId);
        for (const auto& child : partitionIt->second.Children_) {
            auto childIt = Producer->Partitions.find(child);
            Y_ABORT_UNLESS(childIt != Producer->Partitions.end(), "Child partition %u not found", child);
            childIt->second.Locked(false);
        }
        partitionIt->second.Locked_ = false;

        for (const auto& [partitionId, cachedMaxSeqNo] : cachedMaxSeqNos) {
            auto cachedPartitionIt = Producer->Partitions.find(partitionId);
            Y_ABORT_UNLESS(cachedPartitionIt != Producer->Partitions.end(), "Partition %u not found", partitionId);
            cachedPartitionIt->second.CachedMaxSeqNo = cachedMaxSeqNo;
        }
    }

    for (const auto& partitionId : writeSessionPartitionsToDestroy) {
        Producer->SessionsWorker->DestroyWriteSession(partitionId);
    }
}

void TProducer::TSplittedPartitionWorker::MoveTo(EState state) {
    State = state;
    if (State == EState::Done || State == EState::Failed) {
        DoneAt = TInstant::Now();
    }
    LOG_LAZY(Producer->DbDriverState->Log, TLOG_INFO, Producer->LogPrefix() << "Moving splitted partition worker for partition " << PartitionId << " to state " << GetStateName());
}

void TProducer::TSplittedPartitionWorker::UpdateMaxSeqNo(std::uint32_t partitionId, std::uint64_t maxSeqNo) {
    CachedMaxSeqNos[partitionId] = maxSeqNo;
    MaxSeqNo = std::max(MaxSeqNo, maxSeqNo);
}

bool TProducer::TSplittedPartitionWorker::IsDone() const {
    std::lock_guard lock(Lock);
    return State == EState::Done || State == EState::Failed;
}

bool TProducer::TSplittedPartitionWorker::IsInit() const {
    std::lock_guard lock(Lock);
    return State == EState::Init;
}

void TProducer::TSplittedPartitionWorker::HandleDescribeResult() {
    std::vector<std::uint32_t> newPartitionsIds;
    const auto& partitions = DescribeTopicFuture.GetValue().GetTopicDescription().GetPartitions();
    for (const auto& partition : partitions) {
        if (partition.GetPartitionId() != PartitionId) {
            continue;
        }
        
        LOG_LAZY(Producer->DbDriverState->Log, TLOG_DEBUG, Producer->LogPrefix() << "Found partition " << partition.GetPartitionId() << " for partition " << PartitionId << " children: " << partition.GetChildPartitionIds().size());
        for (const auto& childPartitionId : partition.GetChildPartitionIds()) {
            newPartitionsIds.push_back(childPartitionId);
        }
        break;
    }

    if (newPartitionsIds.empty()) {
        if (++Retries >= 40) {
            // Server keeps returning incomplete describe responses; give up gracefully
            // instead of aborting the whole user process.
            LOG_LAZY(Producer->DbDriverState->Log, TLOG_ERR, Producer->LogPrefix()
                << "Too many retries (" << Retries << ") waiting for complete describe response for partition "
                << PartitionId << "; giving up.");
            MoveTo(EState::Failed);
            return;
        }
        // describe response is incomplete, we need to resend describe request
        MoveTo(EState::Init);
        LOG_LAZY(Producer->DbDriverState->Log, TLOG_ERR, Producer->LogPrefix() << "Describe response is incomplete, we need to resend describe request for partition " << PartitionId);
        return;
    }

    struct TChildPartitionInfo {
        std::uint32_t PartitionId;
        std::string FromBound;
        std::optional<std::string> ToBound;
    };

    std::vector<std::uint32_t> children;
    std::vector<TChildPartitionInfo> childPartitionInfos;
    children.reserve(newPartitionsIds.size());
    childPartitionInfos.reserve(newPartitionsIds.size());

    for (const auto& newPartitionId : newPartitionsIds) {
        auto partitionDescribeInfo = std::find_if(partitions.begin(), partitions.end(), [newPartitionId](const auto& partition) {
            return partition.GetPartitionId() == newPartitionId;
        });
        if (partitionDescribeInfo == partitions.end()) {
            // Server returned a child partition id without a corresponding describe entry.
            // This is a server bug; fail this worker gracefully rather than aborting the process.
            LOG_LAZY(Producer->DbDriverState->Log, TLOG_ERR, Producer->LogPrefix()
                << "Describe response for partition " << PartitionId
                << " references child partition " << newPartitionId
                << " without a describe entry; failing worker.");
            MoveTo(EState::Failed);
            return;
        }
        children.push_back(newPartitionId);
        childPartitionInfos.push_back({
            .PartitionId = newPartitionId,
            .FromBound = partitionDescribeInfo->GetFromBound().value_or(""),
            .ToBound = partitionDescribeInfo->GetToBound(),
        });
    }

    auto splittedPartitionIt = Producer->Partitions.find(PartitionId);
    Y_ABORT_UNLESS(splittedPartitionIt != Producer->Partitions.end(), "Partition %u not found", PartitionId);
    Producer->PartitionsIndex.erase(splittedPartitionIt->second.FromBound_);

    for (const auto& childPartitionInfo : childPartitionInfos) {
        Producer->PartitionsIndex[childPartitionInfo.FromBound] = childPartitionInfo.PartitionId;
        Producer->Partitions[childPartitionInfo.PartitionId] = TPartitionInfo()
            .PartitionId(childPartitionInfo.PartitionId)
            .FromBound(childPartitionInfo.FromBound)
            .ToBound(childPartitionInfo.ToBound)
            .Locked(true);
    }

    splittedPartitionIt->second.Children(children);
}

void TProducer::TSplittedPartitionWorker::LaunchGetMaxSeqNoFutures(std::unique_lock<std::mutex>& lock) {
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
        auto ancestorIt = Producer->Partitions.find(ancestor);
        Y_ABORT_UNLESS(ancestorIt != Producer->Partitions.end(), "Ancestor partition %u not found", ancestor);
        if (ancestorIt->second.CachedMaxSeqNo.has_value()) {
            --NotReadyFutures;
            UpdateMaxSeqNo(ancestor, ancestorIt->second.CachedMaxSeqNo.value());
            continue;
        }
        auto wrappedSession = Producer->SessionsWorker->GetOrCreateWriteSession(ancestor, false);
        Y_ABORT_UNLESS(wrappedSession, "Write session not found");
        WriteSessions.push_back(wrappedSession);

        auto future = wrappedSession->Session->GetInitSeqNo();
        std::weak_ptr<TProducer> producer = Producer->shared_from_this();
        std::weak_ptr<TSplittedPartitionWorker> self = weak_from_this();
        lock.unlock();
        future.Subscribe([self, producer, wrappedSession, ancestor](const NThreading::TFuture<uint64_t>& result) {
            auto selfPtr = self.lock();
            if (!selfPtr) {
                return;
            }

            auto producerPtr = producer.lock();
            if (!producerPtr) {
                return;
            }

            bool needRunMainWorker = false;
            {
                std::lock_guard lock(selfPtr->Lock);
                if (selfPtr->State == EState::Done || selfPtr->State == EState::Failed) {
                    return;
                }

                if (result.HasException()) {
                    LOG_LAZY(producerPtr->DbDriverState->Log, TLOG_ERR, producerPtr->LogPrefix() << "Failed to get max seq no for partition " << ancestor << " for splitted partition " << selfPtr->PartitionId);
                    selfPtr->WriteSessionsToCloseOnError.push_back(wrappedSession);
                    selfPtr->MoveTo(EState::Failed);
                    needRunMainWorker = true;
                } else {
                    selfPtr->UpdateMaxSeqNo(ancestor, result.GetValue());
                    selfPtr->WriteSessionPartitionsToDestroy.push_back(ancestor);
                    if (--selfPtr->NotReadyFutures == 0) {
                        selfPtr->MoveTo(EState::GotMaxSeqNo);
                        needRunMainWorker = true;
                    }
                }
            }

            if (needRunMainWorker) {
                producerPtr->RunMainWorker(static_cast<std::int64_t>(selfPtr->PartitionId));
            }
        });
        lock.lock();
        GetMaxSeqNoFutures.push_back(future);
    }
    
    if (ancestors.empty()) {
        LOG_LAZY(Producer->DbDriverState->Log, TLOG_INFO, Producer->LogPrefix() << "No ancestors found for partition " << PartitionId);
        MoveTo(EState::Init);
        return;
    }

    if (NotReadyFutures == 0) {
        MoveTo(EState::GotMaxSeqNo);
        Producer->RunMainWorker(static_cast<std::int64_t>(PartitionId));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TProducer::TEventsWorkerWrapper

TProducer::TEventsWorker::TEventsWorker(TProducer* producer)
    : Producer(producer)
{
    EventsPromise = NThreading::NewPromise();
    EventsFuture = EventsPromise.GetFuture();
}

void TProducer::TEventsWorker::HandleAcksEvent(std::uint64_t partition, TWriteSessionEvent::TAcksEvent&& event) {
    auto [queueIt, _] = PartitionsEventQueues.try_emplace(partition);
    queueIt->second.push_back(TWriteSessionEvent::TEvent(std::move(event)));
}

void TProducer::TEventsWorker::HandleReadyToAcceptEvent(std::uint32_t partition, TWriteSessionEvent::TReadyToAcceptEvent&& event) {
    Producer->MessagesWorker->HandleContinuationToken(partition, std::move(event.ContinuationToken));
}
    
void TProducer::TEventsWorker::HandleSessionClosedEvent(TSessionClosedEvent&& event, std::uint32_t partition) {
    if (event.IsSuccess()) {
        return;
    }

    auto partitionIt = Producer->Partitions.find(partition);
    if (partitionIt != Producer->Partitions.end()) {
        partitionIt->second.Locked_ = true;
    }

    if (event.GetStatus() == EStatus::OVERLOADED) {
        Producer->HandleAutoPartitioning(partition);
        return;
    }

    if (!CloseEvent.has_value()) {
        CloseEvent = std::move(event);
    }
    Producer->NonBlockingClose();
}

bool TProducer::TEventsWorker::RunEventLoop(WrappedWriteSessionPtr wrappedSession, std::uint32_t partition) {
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
            HandleAcksEvent(partition, std::move(*acksEvent));
            continue;
        }
    }

    return false;
}

std::optional<NThreading::TPromise<void>> TProducer::TEventsWorker::DoWork() {
    std::unique_lock lock(Lock);

    while (!ReadyFutures.empty()) {
        auto partition = *ReadyFutures.begin();
        ReadyFutures.erase(partition);
        auto session = Producer->SessionsWorker->GetWriteSession(partition);
        if (!session) {
            continue;
        }

        lock.unlock();
        // RunEventLoop without Lock: sub-session's WaitEvent() completion may run the Subscribe
        // callback (ReadyFutures.insert) synchronously; that callback takes Lock -> same-thread deadlock.
        auto isSessionClosed = RunEventLoop(session, partition);
        if (!isSessionClosed) {
            SubscribeToPartition(partition);
        } else {
            session->Closed = true;
            Producer->SessionsWorker->DestroyWriteSession(partition);
        }
        lock.lock();
    }

    if (Producer->Done.load() || !TransferEventsToOutputQueue()) {
        return std::nullopt;
    }

    // Atomically rotate EventsPromise/EventsFuture under Lock:
    //   - the returned promise will be fulfilled by the caller outside the lock,
    //   - any concurrent WaitEvent() seeing the lock will get the fresh, not-yet-set future.
    // This avoids racing with WaitEvent re-creating the promise out from under us.
    auto firedPromise = std::move(EventsPromise);
    EventsPromise = NThreading::NewPromise<void>();
    EventsFuture = EventsPromise.GetFuture();
    return firedPromise;
}

NThreading::TPromise<void> TProducer::TEventsWorker::WakeAndRotate() {
    std::lock_guard lock(Lock);
    auto firedPromise = std::move(EventsPromise);
    EventsPromise = NThreading::NewPromise<void>();
    EventsFuture = EventsPromise.GetFuture();
    return firedPromise;
}

void TProducer::TEventsWorker::SubscribeToPartition(std::uint32_t partition) {
    auto partitionIt = Producer->Partitions.find(partition);
    if (partitionIt == Producer->Partitions.end()) {
        return;
    }

    if (partitionIt->second.IsSplitted() || Producer->SplittedPartitionWorkers.contains(partition)) {
        partitionIt->second.Future(NThreading::MakeFuture());
        return;
    }

    auto wrappedSession = Producer->SessionsWorker->GetOrCreateWriteSession(partition);
    auto newFuture = wrappedSession->Session->WaitEvent();
    std::weak_ptr<TProducer> producer = Producer->shared_from_this();
    std::weak_ptr<TEventsWorker> self = shared_from_this();

    newFuture.Subscribe([self, producer, partition](const NThreading::TFuture<void>&) {
        auto producerPtr = producer.lock();
        if (!producerPtr) {
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
        producerPtr->RunMainWorker(static_cast<std::int64_t>(partition));
    });
    partitionIt->second.Future(newFuture);
}

std::optional<TSessionClosedEvent> TProducer::TEventsWorker::GetSessionClosedEvent() {
    std::unique_lock lock(Lock);
    if (CloseEvent.has_value()) {
        return CloseEvent;
    }
    return std::nullopt;
}

std::optional<NThreading::TPromise<void>> TProducer::TEventsWorker::HandleNewMessage() {
    std::lock_guard lock(Lock);
    if (Producer->MessagesWorker->IsMemoryUsageOK()) {
        AddContinuationToken();
        // Rotate atomically under Lock: hand the current promise to the caller (will be
        // fulfilled outside the lock), install a fresh promise/future for future waiters.
        auto firedPromise = std::move(EventsPromise);
        EventsPromise = NThreading::NewPromise<void>();
        EventsFuture = EventsPromise.GetFuture();
        return firedPromise;
    }

    Producer->Metrics.IncBufferFull();
    return std::nullopt;
}

void TProducer::TEventsWorker::AddContinuationToken() {
    auto continuationToken = IssueContinuationToken();
    TokensQueue.push_back(std::move(continuationToken));
    Producer->Metrics.IncContinuationTokensSent();
}

bool TProducer::TEventsWorker::AddSessionClosedIfNeeded() {
    if (!Producer->Closed.load()) {
        return false;
    }

    if (!CloseEvent.has_value()) {
        CloseEvent = TSessionClosedEvent(EStatus::SUCCESS, {});
    }

    if (EventsOutputQueue.empty() && (Producer->MessagesWorker->IsQueueEmpty() || Producer->Done.load())) {
        EventsOutputQueue.push_back(*CloseEvent);
        return true;
    }

    return false;
}

bool TProducer::TEventsWorker::TransferEventsToOutputQueue() {
    bool eventsTransferred = false;
    bool shouldAddContinuationToken = false;
    std::unordered_map<std::uint32_t, std::deque<TWriteSessionEvent::TWriteAck>> acks;

    auto messagesWorker = Producer->MessagesWorker;
    auto buildOutputAckEvent = [&](std::deque<TWriteSessionEvent::TWriteAck>& acksQueue, std::uint64_t partition, std::optional<std::uint64_t> expectedSeqNo) -> TWriteSessionEvent::TAcksEvent {
        TWriteSessionEvent::TAcksEvent ackEvent;

        if (expectedSeqNo.has_value()) {
            Y_ENSURE(acksQueue.front().SeqNo == expectedSeqNo.value(), TStringBuilder() << "Expected seqNo=" << expectedSeqNo.value() << " but got " << acksQueue.front().SeqNo << " for partition " << partition);
        }
    
        auto ack = std::move(acksQueue.front());    
        ackEvent.Acks.push_back(std::move(ack));
        acksQueue.pop_front();
        return ackEvent;
    };
    auto finishWithAck = [this, messagesWorker, &shouldAddContinuationToken](std::uint64_t seqNo) {
        Producer->LastWrittenSeqNo = std::max(Producer->LastWrittenSeqNo, seqNo);
        Producer->MessagesWritten++;
        bool wasMemoryUsageOk = messagesWorker->IsMemoryUsageOK();
        messagesWorker->HandleAck();
        if (messagesWorker->IsMemoryUsageOK() && !wasMemoryUsageOk) {
            shouldAddContinuationToken = true;
        }
    };

    while (messagesWorker->HasInFlightMessages()) {
        const auto& head = messagesWorker->GetFrontInFlightMessage();

        auto remainingAcks = acks.find(head.Partition);
        if (remainingAcks != acks.end() && remainingAcks->second.size() > 0) {
            auto seqNo = remainingAcks->second.front().SeqNo;
            EventsOutputQueue.push_back(buildOutputAckEvent(remainingAcks->second, head.Partition, head.SeqNo));
            finishWithAck(seqNo);
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
        auto seqNo = acksEvent->Acks.front().SeqNo;
        EventsOutputQueue.push_back(buildOutputAckEvent(acksQueue, head.Partition, head.SeqNo));
        acks[head.Partition] = std::move(acksQueue);
        eventsQueueIt->second.pop_front();
        eventsTransferred = true;

        finishWithAck(seqNo);
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

    if (shouldAddContinuationToken) {
        AddContinuationToken();
    }

    return eventsTransferred;
}

std::optional<TContinuationToken> TProducer::TEventsWorker::GetContinuationToken() {
    std::lock_guard lock(Lock);
    if (TokensQueue.empty()) {
        return std::nullopt;
    }

    auto continuationToken = std::move(TokensQueue.front());
    TokensQueue.pop_front();
    return std::move(continuationToken);
}

std::list<TWriteSessionEvent::TEvent>::iterator TProducer::TEventsWorker::AckQueueBegin(std::uint32_t partition) {
    auto [queueIt, _] = PartitionsEventQueues.try_emplace(partition);
    return queueIt->second.begin();
}

std::list<TWriteSessionEvent::TEvent>::iterator TProducer::TEventsWorker::AckQueueEnd(std::uint32_t partition) {
    auto [queueIt, _] = PartitionsEventQueues.try_emplace(partition);
    return queueIt->second.end();
}

TProducer::TEventsWorker::EEventType TProducer::TEventsWorker::GetEventType(const TWriteSessionEvent::TEvent& event) {
    if (std::holds_alternative<TSessionClosedEvent>(event)) {
        return EEventType::SessionClosed;
    } else if (std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event)) {
        return EEventType::ReadyToAccept;
    } else if (std::holds_alternative<TWriteSessionEvent::TAcksEvent>(event)) {
        return EEventType::Ack;
    }

    Y_ABORT_UNLESS(false, "Unexpected event type");
}

std::optional<TWriteSessionEvent::TEvent> TProducer::TEventsWorker::GetEventImpl(bool block, const std::vector<EEventType>& eventTypes) {
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

std::optional<TWriteSessionEvent::TEvent> TProducer::TEventsWorker::GetEvent(bool block, const std::vector<EEventType>& eventTypes) {
    {
        std::unique_lock lock(Lock);
        AddSessionClosedIfNeeded();
    }
    auto event = GetEventImpl(block, eventTypes);

    return event;
}

std::vector<TWriteSessionEvent::TEvent> TProducer::TEventsWorker::GetEvents(bool block, std::optional<size_t> maxEventsCount, const std::vector<EEventType>& eventTypes) {
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

NThreading::TFuture<void> TProducer::TEventsWorker::WaitEvent() {
    std::lock_guard lock(Lock);

    AddSessionClosedIfNeeded();
    if (!EventsOutputQueue.empty()) {
        return NThreading::MakeFuture();
    }

    // Invariant maintained under Lock: EventsPromise has not been set yet, and
    // EventsFuture corresponds to that promise. The promise is rotated atomically
    // by DoWork()/WakeAndRotate() (under the same Lock), so we can simply hand
    // out the current future without any reset logic.
    return EventsFuture;
}

void TProducer::TEventsWorker::UnsubscribeFromPartition(std::uint32_t partition) {
    ReadyFutures.erase(partition);
    auto partitionIt = Producer->Partitions.find(partition);
    if (partitionIt != Producer->Partitions.end()) {
        partitionIt->second.Future(NThreading::MakeFuture());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TProducer::TSessionsWorker

TProducer::TSessionsWorker::TSessionsWorker(TProducer* producer)
    : Producer(producer)
{}

TProducer::WrappedWriteSessionPtr TProducer::TSessionsWorker::GetOrCreateWriteSession(std::uint32_t partition, bool directToPartition) {
    auto sessionIter = SessionsIndex.find(partition);
    if (sessionIter == SessionsIndex.end()) {
        return CreateWriteSession(partition, directToPartition);
    }

    SessionsToRemove.erase(partition);
    if (!directToPartition && sessionIter->second->DirectToPartition) {
        Y_ABORT_UNLESS(sessionIter->second->Closed, "Session is not closed for partition: %u", partition);
        ClosedSessionsToRemove.push_back(sessionIter->second);
        SessionsIndex.erase(sessionIter);
        return CreateWriteSession(partition, directToPartition);
    }

    if (!sessionIter->second->DirectToPartition) {
        sessionIter->second->NonDirectToPartitionOwnership++;
    }

    return sessionIter->second;
}

TProducer::WrappedWriteSessionPtr TProducer::TSessionsWorker::GetWriteSession(std::uint32_t partition, bool directToPartition) {
    auto sessionIter = SessionsIndex.find(partition);
    if (sessionIter == SessionsIndex.end()) {
        return nullptr;
    }

    SessionsToRemove.erase(partition);
    Y_ABORT_UNLESS(directToPartition == sessionIter->second->DirectToPartition, "DirectToPartition mismatch: %s != %s", directToPartition ? "true" : "false", sessionIter->second->DirectToPartition ? "true" : "false");

    return sessionIter->second;
}

std::string TProducer::TSessionsWorker::GetProducerId(std::uint32_t partitionId) {
    return std::format("{}_{}", Producer->Settings.ProducerIdPrefix_, partitionId);
}

TProducer::WrappedWriteSessionPtr TProducer::TSessionsWorker::CreateWriteSession(std::uint32_t partition, bool directToPartition) {
    auto partitionIt = Producer->Partitions.find(partition);
    Y_ABORT_UNLESS(partitionIt != Producer->Partitions.end(), "Partition %u not found", partition);
    auto partitionId = partitionIt->second.PartitionId_;
    auto producerId = GetProducerId(partitionId);
    TWriteSessionSettings alteredSettings = Producer->Settings;

    alteredSettings
        .ProducerId(producerId)
        .MessageGroupId(producerId)
        .MaxMemoryUsage(std::numeric_limits<std::uint64_t>::max())
        .RetryPolicy(Producer->RetryPolicy)
        .EventHandlers(TWriteSessionSettings::TEventHandlers()
        .ReadyToAcceptHandler({})
        .AcksHandler({})
        .SessionClosedHandler({}));
    
    if (directToPartition) {    
        alteredSettings.DirectWriteToPartition(true);
        alteredSettings.PartitionId(partitionId);
    }
    auto writeSession = std::make_shared<TWriteSessionWrapper>(
        Producer->Client->CreateWriteSession(alteredSettings),
        partition,
        directToPartition
    );

    SessionsIndex.emplace(partition, writeSession);
    if (directToPartition) {
        Producer->EventsWorker->SubscribeToPartition(partition);
    }
    return writeSession;
}

void TProducer::TSessionsWorker::DestroyWriteSession(std::uint32_t partition) {
    auto it = SessionsIndex.find(partition);
    if (it == SessionsIndex.end() || !it->second) {
        return;
    }

    if (!it->second->DirectToPartition && --it->second->NonDirectToPartitionOwnership > 0) {
        return;
    }

    if (it->second->DirectToPartition) {
        Producer->EventsWorker->UnsubscribeFromPartition(partition);
    }

    // Remove idle bookkeeping before erasing the session from SessionsIndex so stale
    // idle markers cannot later evict a new session created for the same partition.
    RemoveIdleSession(partition);

    if (static_cast<std::int64_t>(partition) == Producer->MainWorkerOwner) {
        SessionsToRemove.emplace(partition);
    } else {
        SessionsIndex.erase(it);
    }
}

size_t TProducer::TSessionsWorker::GetSessionsCount() const {
    return SessionsIndex.size();
}

size_t TProducer::TSessionsWorker::GetIdleSessionsCount() const {
    return IdlerSessions.size();
}

void TProducer::TSessionsWorker::AddIdleSession(std::uint32_t partition) {
    auto wrappedSession = SessionsIndex.find(partition);
    if (wrappedSession == SessionsIndex.end()) {
        return;
    }

    if (wrappedSession->second->IdleSession) {
        return;
    }

    auto idleSessionPtr = std::make_shared<TIdleSession>(wrappedSession->second.get(), TInstant::Now(), Producer->Settings.SubSessionIdleTimeout_);
    auto [itIdle, inserted] = IdlerSessions.insert(idleSessionPtr);
    Y_ABORT_UNLESS(inserted, "Duplicate idle session for partition");
    IdlerSessionsIndex[partition] = itIdle;
    wrappedSession->second->IdleSession = idleSessionPtr;
}

void TProducer::TSessionsWorker::RemoveIdleSession(std::uint32_t partition) {
    auto itIdle = IdlerSessionsIndex.find(partition);
    if (itIdle == IdlerSessionsIndex.end()) {
        return;
    }

    const auto idleSession = *itIdle->second;
    IdlerSessions.erase(itIdle->second);
    IdlerSessionsIndex.erase(itIdle);

    auto wrappedSession = SessionsIndex.find(partition);
    if (wrappedSession == SessionsIndex.end()) {
        return;
    }

    if (wrappedSession->second.get() == idleSession->Session) {
        wrappedSession->second->IdleSession.reset();
    }
}

void TProducer::TSessionsWorker::DoWork() {
    for (auto it = SessionsToRemove.begin(); it != SessionsToRemove.end();) {
        auto partition = *it;
        if (static_cast<std::int64_t>(partition) == Producer->MainWorkerOwner) {
            ++it;
            continue;
        }

        SessionsIndex.erase(partition);
        it = SessionsToRemove.erase(it);
    }

    for (auto it = ClosedSessionsToRemove.begin(); it != ClosedSessionsToRemove.end();) {
        auto session = *it;
        if (static_cast<std::int64_t>(session->Partition) == Producer->MainWorkerOwner) {
            ++it;
            continue;
        }

        it = ClosedSessionsToRemove.erase(it);
    }

    while (!IdlerSessions.empty()) {
        auto it = IdlerSessions.begin();
        if (!(*it)->IsExpired()) {
            break;
        }

        auto expiredIdleSession = *it;
        const auto partition = expiredIdleSession->Session->Partition;
        auto partitionIt = Producer->Partitions.find(partition);
        if (partitionIt == Producer->Partitions.end()) {
            IdlerSessions.erase(it);
            IdlerSessionsIndex.erase(partition);
            continue;
        }
        if (partitionIt->second.Locked_) {
            break;
        }

        // Remove idle tracking first to keep containers consistent even if the session
        // is already absent from SessionsIndex.
        IdlerSessions.erase(it);
        IdlerSessionsIndex.erase(partition);

        auto sessionIter = SessionsIndex.find(partition);
        if (sessionIter != SessionsIndex.end()) {
            if (sessionIter->second.get() != expiredIdleSession->Session) {
                continue;
            }
            sessionIter->second->IdleSession.reset();
            DestroyWriteSession(partition);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TProducer::TMessagesWorker

TProducer::TMessagesWorker::TMessagesWorker(TProducer* producer)
    : Producer(producer)
{
}

void TProducer::TMessagesWorker::RechoosePartitionIfNeeded(MessageIter message) {
    auto partitionIt = Producer->Partitions.find(message->Partition);
    Y_ABORT_UNLESS(partitionIt != Producer->Partitions.end(), "Partition %u not found", message->Partition);
    if (partitionIt->second.Children_.empty()) {
        return;
    }

    // this case means that partition was split, so we need to rechoose the partition for the message
    auto [newPartition, _] = Producer->PartitionChooser->ChoosePartition(message->Key);
    message->Partition = newPartition;
}

void TProducer::TMessagesWorker::HandleReadyInitSeqNoFutures() {
    std::unique_lock lock(InitLock);
    for (const auto& partition : GotInitSeqNoPartitions) {
        auto it = InitGetMaxSeqNoFutures.find(partition);
        Y_ABORT_UNLESS(it != InitGetMaxSeqNoFutures.end(), "Init get max seq no future not found");
        Y_ABORT_UNLESS(it->second.IsReady(), "Init get max seq no future is not ready");

        auto gotMaxSeqNo = it->second.GetValue();
        CurrentSeqNo = std::max(CurrentSeqNo, gotMaxSeqNo);
        auto partitionIt = Producer->Partitions.find(partition);
        Y_ABORT_UNLESS(partitionIt != Producer->Partitions.end(), "Partition %u not found", partition);
        partitionIt->second.CachedMaxSeqNo = gotMaxSeqNo;
        InitGetMaxSeqNoFutures.erase(it);
    }

    GotInitSeqNoPartitions.clear();
}

void TProducer::TMessagesWorker::FinishInit() {
    for (const auto& partition : Producer->Partitions) {
        if (!partition.second.IsSplitted() && !InFlightMessagesIndex.contains(partition.first)) {
            Producer->SessionsWorker->AddIdleSession(partition.first);
        }

        if (partition.second.IsSplitted()) {
            Producer->SessionsWorker->DestroyWriteSession(partition.first);
        }
    }

    InitWriteSessions.clear();
    InitGetMaxSeqNoFutures.clear();
}

bool TProducer::TMessagesWorker::LazyInit() {
    if (State == EState::Ready) {
        return true;
    }
    
    if (Producer->SeqNoStrategy == ESeqNoStrategy::WithSeqNo) {
        MoveTo(EState::Ready);
        return true;
    }

    if (State == EState::PendingSeqNo) {
        HandleReadyInitSeqNoFutures();
        if (InitGetMaxSeqNoFutures.empty()) {
            FinishInit();
            MoveTo(EState::Ready);
            return true;
        }

        return false;
    }

    std::weak_ptr<TProducer> producer = Producer->shared_from_this();
    std::weak_ptr<TMessagesWorker> self = shared_from_this();
    for (const auto& partition : Producer->Partitions) {
        auto partitionId = partition.first;
        WrappedWriteSessionPtr wrappedSession = nullptr;
        if (partition.second.IsSplitted()) {
            wrappedSession = Producer->SessionsWorker->GetOrCreateWriteSession(partition.first, false);
        } else {
            wrappedSession = Producer->SessionsWorker->GetOrCreateWriteSession(partition.first);
        }

        InitWriteSessions.push_back(wrappedSession);
        auto initGetMaxSeqNoFuture = wrappedSession->Session->GetInitSeqNo();

        initGetMaxSeqNoFuture.Subscribe([self, producer, partitionId](NThreading::TFuture<uint64_t> future) {
            auto selfPtr = self.lock();
            if (!selfPtr) {
                return;
            }

            auto producerPtr = producer.lock();
            if (!producerPtr) {
                return;
            }

            if (!future.IsReady()) {
                return;
            }

            {
                std::lock_guard lock(selfPtr->InitLock);
                selfPtr->GotInitSeqNoPartitions.push_back(partitionId);
            }
            producerPtr->RunMainWorker(partitionId);
        });
        InitGetMaxSeqNoFutures.emplace(partition.first, initGetMaxSeqNoFuture);
    }

    MoveTo(EState::PendingSeqNo);
    return false;
}

void TProducer::TMessagesWorker::MoveTo(EState state) {
    State = state;
}

void TProducer::TMessagesWorker::DoWork() {
    if (MessagesToResendIndex.empty() && PendingMessagesIndex.empty()) {
        return;
    }

    if (!LazyInit()) {
        return;
    }

    auto sessionsWorker = Producer->SessionsWorker;
    auto iterateMessagesIndex = [&](std::unordered_map<std::uint32_t, std::list<MessageIter>>& messagesIndex, auto stopCondition) {
        std::vector<std::uint32_t> partitionsProcessed;
        for (auto& [partition, messages] : messagesIndex) {
            while (!messages.empty()) {
                auto head = messages.front();
                if (stopCondition(head)) {
                    break;
                }

                if (!head->SeqNo.has_value()) {
                    head->SeqNo.emplace(++CurrentSeqNo);
                }

                auto wrappedSession = sessionsWorker->GetOrCreateWriteSession(head->Partition);
                if (!SendMessage(wrappedSession, *head)) {
                    break;
                }

                Producer->Metrics.AddWriteLag((TInstant::Now() - head->CreateTimestamp.value_or(TInstant::Now())).MilliSeconds());
                head->Sent = true;
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
        [this](MessageIter head) {
            auto partitionIt = Producer->Partitions.find(head->Partition);
            Y_ABORT_UNLESS(partitionIt != Producer->Partitions.end(), "Partition %u not found", head->Partition);
            return partitionIt->second.Locked_;
        }
    );

    iterateMessagesIndex(
        PendingMessagesIndex,
        [this](MessageIter head) {
            auto partitionIt = Producer->Partitions.find(head->Partition);
            Y_ABORT_UNLESS(partitionIt != Producer->Partitions.end(), "Partition %u not found", head->Partition);
            return partitionIt->second.Locked_ ||
                MessagesToResendIndex.contains(head->Partition);
        }
    );
}

bool TProducer::TMessagesWorker::SendMessage(WrappedWriteSessionPtr wrappedSession, const TMessageInfo& message) {    
    if (!wrappedSession) {
        return false;
    }
    
    auto continuationToken = GetContinuationToken(message.Partition);
    if (!continuationToken) {
        return false;
    }
    
    Producer->Metrics.IncOutgoingMessages();
    auto builtMessage = message.BuildMessage();
    wrappedSession->Session->Write(std::move(*continuationToken), std::move(builtMessage));
    return true;
}

void TProducer::TMessagesWorker::PushInFlightMessage(std::uint32_t partition, TMessageInfo&& message) {
    auto iter = InFlightMessages.insert(InFlightMessages.end(), std::move(message));
    auto [inFlightMessagesIndexIt, wasInserted] = InFlightMessagesIndex.try_emplace(partition);
    inFlightMessagesIndexIt->second.push_back(iter);

    auto [pendingMessagesIndexIt, __] = PendingMessagesIndex.try_emplace(partition);
    pendingMessagesIndexIt->second.push_back(iter);

    if (wasInserted) {
        Producer->SessionsWorker->RemoveIdleSession(partition);
    }
}

void TProducer::TMessagesWorker::HandleAck() {
    PopInFlightMessage();
}

void TProducer::TMessagesWorker::PopInFlightMessage() {
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
            Producer->SessionsWorker->AddIdleSession(partition);
        }
    }

    Y_ABORT_UNLESS(it->Data.size() <= MemoryUsage, "MemoryUsage is less than the size of the message");
    MemoryUsage -= it->Data.size();

    if (it->FlushPromise.Initialized()) {
        Producer->FlushPromises.push_back(std::make_pair(it->FlushPromise, TFlushResult{
            .Status = EFlushStatus::Success,
            .LastWrittenSeqNo = Producer->LastWrittenSeqNo,
            .ClosedDescription = std::nullopt,
        }));
    }
    InFlightMessages.pop_front();
}

bool TProducer::TMessagesWorker::IsMemoryUsageOK() const {
    return MemoryUsage <= Producer->Settings.MaxMemoryUsage_ / 2;
}

void TProducer::TMessagesWorker::AddMessage(
    const std::string& key,
    const std::string& choosePartitionKey,
    TWriteMessage&& message,
    std::uint32_t partition) {
    MemoryUsage += message.Data.size();
    PushInFlightMessage(partition, TMessageInfo(key, choosePartitionKey, std::move(message), partition));
}

std::optional<TContinuationToken> TProducer::TMessagesWorker::GetContinuationToken(std::uint32_t partition) {
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

void TProducer::TMessagesWorker::HandleContinuationToken(std::uint32_t partition, TContinuationToken&& continuationToken) {
    auto [it, _] = ContinuationTokens.try_emplace(partition);
    it->second.push_back(std::move(continuationToken));
}

bool TProducer::TMessagesWorker::IsQueueEmpty() const {
    return InFlightMessages.empty();
}

const TProducer::TMessageInfo& TProducer::TMessagesWorker::GetFrontInFlightMessage() const {
    Y_ABORT_UNLESS(!InFlightMessages.empty());
    return InFlightMessages.front();
}

bool TProducer::TMessagesWorker::HasInFlightMessages() const {
    return !InFlightMessages.empty();
}

void TProducer::TMessagesWorker::SetClosedStatusToFlushPromises(std::optional<TCloseDescription> closedDescription) {
    for (auto& inFlightMessage : InFlightMessages) {
        if (inFlightMessage.FlushPromise.Initialized()) {
            inFlightMessage.FlushPromise.TrySetValue(TFlushResult{
                .Status = EFlushStatus::ProducerClosed,
                .LastWrittenSeqNo = Producer->LastWrittenSeqNo,
                .ClosedDescription = closedDescription,
            });
        }
    }
}

void TProducer::TMessagesWorker::ScheduleResendMessages(std::uint32_t partition, std::uint64_t afterSeqNo) {
    auto it = InFlightMessagesIndex.find(partition);
    if (it == InFlightMessagesIndex.end()) {
        return;
    }

    auto& list = it->second;
    auto resendIt = list.begin();
    auto ackQueueIt = Producer->EventsWorker->AckQueueBegin(partition);
    size_t ackIdx = 0;
    auto ackQueueEnd = Producer->EventsWorker->AckQueueEnd(partition);
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
        Producer->EventsWorker->HandleAcksEvent(partition, std::move(event));
    }

    // IMPORTANT: do not mutate InFlightMessagesIndex while holding references/iterators to its elements.
    // try_emplace()/rehash may invalidate 'it' and 'list' -> use-after-free and segfaults.
    std::vector<std::pair<std::uint64_t, MessageIter>> messagesFromOldPartition;
    messagesFromOldPartition.reserve(std::distance(resendIt, list.end()));
    auto currentSeqNo = resendIt != list.end() ? (*resendIt)->SeqNo.value_or(0) : 0;
    for (auto iter = resendIt; iter != list.end(); ++iter) {
        if (iter != resendIt && currentSeqNo != 0) {
            Y_ABORT_UNLESS((*iter)->SeqNo.value_or(0) > currentSeqNo, "SeqNo is not increasing for partition %d, currentSeqNo: %llu, iterSeqNo: %llu", partition, currentSeqNo, (*iter)->SeqNo.value_or(0));
        }

        auto [newPartition, _] = Producer->PartitionChooser->ChoosePartition((*iter)->Key);
        (*iter)->Partition = newPartition;
        messagesFromOldPartition.emplace_back(newPartition, *iter);

        currentSeqNo = (*iter)->SeqNo.value_or(0);
    }
    
    list.erase(resendIt, list.end());
    for (auto it = messagesFromOldPartition.rbegin(); it != messagesFromOldPartition.rend(); ++it) {
        auto [newPartition, msgIt] = *it;
        auto [inFlightMessagesIndexChainIt, _] = InFlightMessagesIndex.try_emplace(newPartition);
        inFlightMessagesIndexChainIt->second.push_front(msgIt);

        if (msgIt->Sent) {
            auto [messagesToResendChainIt, __] = MessagesToResendIndex.try_emplace(newPartition);
            messagesToResendChainIt->second.push_front(msgIt);
        }
    }

    InFlightMessagesIndex.erase(partition);
    PendingMessagesIndex.erase(partition);
    MessagesToResendIndex.erase(partition);
    Producer->SessionsWorker->AddIdleSession(partition);
}

void TProducer::TMessagesWorker::RebuildPendingMessagesIndex(std::uint32_t partition) {
    auto [oldPendingMessagesIndexChainIt, __] = PendingMessagesIndex.try_emplace(partition);
    std::unordered_map<std::uint32_t, std::list<MessageIter>> pendingMessagesForNewPartitions;
    for (auto it = oldPendingMessagesIndexChainIt->second.begin(); it != oldPendingMessagesIndexChainIt->second.end(); ++it) {
        auto [newPartition, _] = Producer->PartitionChooser->ChoosePartition((*it)->Key);
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
// TProducer::TProducerRetryPolicy

TProducer::TProducerRetryPolicy::TProducerRetryPolicy(TProducer* producer)
    : Producer(producer)
{}

typename TProducer::TProducerRetryPolicy::IRetryState::TPtr TProducer::TProducerRetryPolicy::CreateRetryState() const {
    struct TRetryState : public IRetryState {
        TRetryState(TProducer* producer)
            : Producer(producer)
        {}
        ~TRetryState() = default;
        TMaybe<TDuration> GetNextRetryDelay(EStatus status) override {
            if (status == EStatus::OVERLOADED) {
                return Nothing();
            }

            if (!UserRetryState) {
                auto policy = Producer->Settings.RetryPolicy_ ? Producer->Settings.RetryPolicy_ : NYdb::NTopic::IRetryPolicy::GetDefaultPolicy();
                UserRetryState = policy->CreateRetryState();
            }

            return UserRetryState->GetNextRetryDelay(status);
        }

    private:
        TProducer* Producer;
        IRetryState::TPtr UserRetryState;
    };
    
    return std::make_unique<TRetryState>(Producer);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TProducer::Metrics

void TProducer::TMetricGauge::Add(std::uint64_t value) {
    Sum += value;
    Max = std::max(Max, value);
    MetricCount++;
}

void TProducer::TMetricGauge::Clear() {
    Sum = 0;
    MetricCount = 0;
    Max = 0;
}

long double TProducer::TMetricGauge::Average() {
    if (MetricCount == 0) {
        return 0;
    }

    return (long double)Sum / (long double)MetricCount;
}

std::uint64_t TProducer::TMetricGauge::GetMax() const {
    return Max;
}

std::uint64_t TProducer::TMetricGauge::GetSum() const {
    return Sum;
}

TProducer::TMetrics::TMetrics(TProducer* producer): Producer(producer) {}

void TProducer::TMetrics::AddMainWorkerTime(std::uint64_t ms) {
    std::lock_guard lock(Lock);
    MainWorkerTimeMs.Add(ms);
}

void TProducer::TMetrics::AddCycleTime(std::uint64_t ms) {
    std::lock_guard lock(Lock);
    CycleTimeMs.Add(ms);
}

void TProducer::TMetrics::AddWriteLag(std::uint64_t lagMs) {
    std::lock_guard lock(Lock);
    WriteLagMs.Add(lagMs);
}

void TProducer::TMetrics::IncContinuationTokensSent() {
    std::lock_guard lock(Lock);
    ContinuationTokensSent.Add(1);
}

void TProducer::TMetrics::IncBufferFull() {
    std::lock_guard lock(Lock);
    BufferFull.Add(1);
}

void TProducer::TMetrics::IncIncomingMessages() {
    std::lock_guard lock(Lock);
    IncomingMessages.Add(1);
}

void TProducer::TMetrics::IncOutgoingMessages() {
    std::lock_guard lock(Lock);
    OutgoingMessages.Add(1);
}

void TProducer::TMetrics::PrintMetrics() {
    std::lock_guard lock(Lock);
    LOG_LAZY(
        Producer->DbDriverState->Log,
        TLOG_DEBUG,
        Producer->LogPrefix() 
            << "METRICS: average MainWorkerTimeMs: " << MainWorkerTimeMs.Average()
            << " ms, average CycleTimeMs: " << CycleTimeMs.Average()
            << " ms, average WriteLagMs: " << WriteLagMs.Average() << " ms, "
            << "max MainWorkerTimeMs: " << MainWorkerTimeMs.GetMax() << " ms, "
            << "max CycleTimeMs: " << CycleTimeMs.GetMax() << " ms, "
            << "max WriteLagMs: " << WriteLagMs.GetMax() << " ms, "
            << "ContinuationTokensSent: " << ContinuationTokensSent.GetSum() << " tokens, "
            << "BufferFull: " << BufferFull.GetSum() << " times, "
            << "IncomingMessages: " << IncomingMessages.GetSum() << " messages, "
            << "OutgoingMessages: " << OutgoingMessages.GetSum() << " messages");
    MainWorkerTimeMs.Clear();
    CycleTimeMs.Clear();
    WriteLagMs.Clear();
    ContinuationTokensSent.Clear();
    BufferFull.Clear();
    IncomingMessages.Clear();
    OutgoingMessages.Clear();
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TProducer

TProducer::TProducer(
    const TProducerSettings& settings,
    std::shared_ptr<TTopicClient::TImpl> client,
    std::shared_ptr<TGRpcConnectionsImpl> connections,
    TDbDriverStatePtr dbDriverState)
    : Id(CreateGuidAsString()),
    Connections(connections),
    Client(client),
    DbDriverState(dbDriverState),
    Metrics(this),
    Settings(settings)
{
    if (settings.ProducerIdPrefix_.empty()) {
        ythrow TContractViolation("ProducerIdPrefix is required for Producer");
    }

    if (!settings.ProducerId_.empty()) {
        ythrow TContractViolation("ProducerId should be empty for Producer, use ProducerIdPrefix instead");
    }

    if (!settings.MessageGroupId_.empty()) {
        ythrow TContractViolation("MessageGroupId should be empty for Producer");
    }

    if (IsFederation(DbDriverState->DiscoveryEndpoint)) {
        ythrow TContractViolation("Producer is not supported for federation");
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
        LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Adding partition " << partitionId << " from bound " << fromBound << " to bound " << (toBound.has_value() ? toBound.value() : "null"));
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
        EventTypesWithHandlers.push_back(TEventsWorker::EEventType::Ack);
    } else {
        if (Settings.EventHandlers_.SessionClosedHandler_) {
            EventTypesWithHandlers.push_back(TEventsWorker::EEventType::SessionClosed);
        }
        if (Settings.EventHandlers_.AcksHandler_) {
            EventTypesWithHandlers.push_back(TEventsWorker::EEventType::Ack);
        }
    }

    switch (partitionChooserStrategy) {
        case TProducerSettings::EPartitionChooserStrategy::Bound:
            PartitioningKeyHasher = settings.PartitioningKeyHasher_;
            PartitionChooser = std::make_unique<TBoundPartitionChooser>(this);
            for (size_t i = 0; i < partitions.size(); ++i) {
                const auto& partition = partitions[i];
                if (i > 0 && !partition.GetFromBound().has_value() && !partition.GetToBound().has_value()) {
                    ythrow TContractViolation("Unbounded partition is not supported for Bound partition chooser strategy");
                }

                if (!partition.GetChildPartitionIds().empty()) {
                    continue;
                }

                PartitionsIndex[partition.GetFromBound().value_or("")] = partition.GetPartitionId();
            }
            break;
        case TProducerSettings::EPartitionChooserStrategy::KafkaHash:
            if (autoPartitioningEnabled) {
                throw TContractViolation("KafkaHash partition chooser strategy is not supported with auto partitioning enabled");
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
    RetryPolicy = std::make_shared<TProducerRetryPolicy>(this);

    EventsWorker->AddContinuationToken();

    // Start handlers executor for user callbacks (Acks/ReadyToAccept/SessionClosed/Common).
    if (auto handlersExecutor = Settings.EventHandlers_.HandlersExecutor_) {
        handlersExecutor->Start();
    }

    CloseFuture.Subscribe([this](const NThreading::TFuture<void>&) {
        RunMainWorker(-1);
    });

    RunMainWorker(-1);
    LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Producer created");
}

std::vector<TProducer::TPartitionInfo> TProducer::GetPartitions() const {
    std::lock_guard lock(GlobalLock);
    std::vector<TPartitionInfo> partitions;
    partitions.reserve(Partitions.size());
    for (const auto& [partitionId, partitionInfo] : Partitions) {
        partitions.push_back(partitionInfo);
    }
    return partitions;
}

std::unordered_map<std::uint32_t, TProducer::TPartitionInfo> TProducer::GetPartitionsMap() const {
    std::lock_guard lock(GlobalLock);
    return Partitions;
}

std::map<std::string, std::uint32_t> TProducer::GetPartitionsIndex() const {
    std::lock_guard lock(GlobalLock);
    return PartitionsIndex;
}

size_t TProducer::GetSessionsCount() {
    RunMainWorker(-1);

    std::lock_guard lock(GlobalLock);
    return SessionsWorker->GetSessionsCount();
}

size_t TProducer::GetIdleSessionsCount() {
    RunMainWorker(-1);
    
    std::lock_guard lock(GlobalLock);
    return SessionsWorker->GetIdleSessionsCount();
}

TCloseResult TProducer::Close(TDuration closeTimeout) {
    if (Closed.exchange(true)) {
        auto sessionClosedEvent = EventsWorker->GetSessionClosedEvent();
        return TCloseResult{
            .Status = ECloseStatus::AlreadyClosed,
            .ClosedDescription = sessionClosedEvent ? std::make_optional<TCloseDescription>(*sessionClosedEvent) : std::nullopt
        };
    }

    SetCloseDeadline(closeTimeout);
    ClosePromise.TrySetValue();

    Flush().Wait(CloseDeadline);
    ShutdownFuture.Wait(CloseDeadline);
    RunUserEventLoop();
    Done.store(true);

    {
        std::lock_guard lock(GlobalLock);
        if (MessagesWorker->IsQueueEmpty()) {
            return TCloseResult{ .Status = ECloseStatus::Success };
        }
    }

    auto sessionClosedEvent = EventsWorker->GetSessionClosedEvent();
    if (sessionClosedEvent && sessionClosedEvent->GetStatus() != EStatus::SUCCESS) {
        return TCloseResult{
            .Status = ECloseStatus::Error,
            .ClosedDescription = std::make_optional<TCloseDescription>(*sessionClosedEvent)
        };
    }

    return TCloseResult{ .Status = ECloseStatus::Timeout };
}

void TProducer::NonBlockingClose() {
    Closed.store(true);
    Done.store(true);
}

void TProducer::SetCloseDeadline(const TDuration& closeTimeout) {
    std::lock_guard lock(GlobalLock);
    CloseDeadline = TInstant::Now() + closeTimeout;
}

TProducer::~TProducer() {
    try {
        auto _ = Close(TDuration::Zero()); // Ignore the result, because we are destroying the producer
        if (auto handlersExecutor = Settings.EventHandlers_.HandlersExecutor_) {
            handlersExecutor->Stop();
        }

        if (MainWorkerState.load() == Idle) {
            ShutdownPromise.TrySetValue();
        }

        // Bounded wait to avoid hanging the destructor indefinitely if
        // ShutdownPromise is never fulfilled (e.g. RunMainWorker is stuck
        // or the state machine never reaches Idle).
        ShutdownFuture.Wait(TDuration::Seconds(30));
    } catch (...) {
        // Destructors must not throw.
    }
}

NThreading::TFuture<void> TProducer::WaitEvent() {
    return EventsWorker->WaitEvent();
}

std::optional<TWriteSessionEvent::TEvent> TProducer::GetEvent(bool block) {
    if (Settings.EventHandlers_.CommonHandler_) {
       return std::nullopt;
    }

    return EventsWorker->GetEvent(block);
}

std::vector<TWriteSessionEvent::TEvent> TProducer::GetEvents(bool block, std::optional<size_t> maxEventsCount) {
    if (Settings.EventHandlers_.CommonHandler_) {
        return {};
    }

    return EventsWorker->GetEvents(block, maxEventsCount);
}

TDuration TProducer::GetCloseTimeout() {
    std::lock_guard lock(GlobalLock);
    auto now = TInstant::Now();
    if (CloseDeadline <= now) {
        return TDuration::Zero();
    }
    return CloseDeadline - now;
}

bool TProducer::RunSplittedPartitionWorkers() {
    if (SplittedPartitionWorkers.empty() && ReadySplittedPartitionWorkers.empty()) {
        return false;
    }

    bool needRerun = false;
    std::unordered_map<std::uint32_t, std::shared_ptr<TSplittedPartitionWorker>> readySplittedPartitionWorkers;
    for (const auto& [partition, splittedPartitionWorker] : SplittedPartitionWorkers) {
        if (splittedPartitionWorker->IsDone()) {
            readySplittedPartitionWorkers[partition] = splittedPartitionWorker;
            continue;
        }

        splittedPartitionWorker->DoWork();
        needRerun = needRerun || splittedPartitionWorker->IsInit();
        needRerun = needRerun || splittedPartitionWorker->IsDone();
    }

    for (const auto& [partition, splittedPartitionWorker] : readySplittedPartitionWorkers) {
        ReadySplittedPartitionWorkers[partition] = splittedPartitionWorker;
        SplittedPartitionWorkers.erase(partition);
    }

    std::vector<std::uint32_t> partitionsToRemove;
    for (const auto& [partition, splittedPartitionWorker] : ReadySplittedPartitionWorkers) {
        if (MainWorkerOwner != partition) {
            partitionsToRemove.push_back(partition);
        }
    }

    for (const auto& partition : partitionsToRemove) {
        ReadySplittedPartitionWorkers.erase(partition);
    }

    return needRerun;
}

void TProducer::RunUserEventLoop() {
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

bool TProducer::IsFederation(const std::string& endpoint) {
    std::string_view host = GetHost(endpoint);
    return host == "logbroker.yandex.net" || host == "logbroker-prestable.yandex.net";
}

void TProducer::GetSessionClosedEventAndDie(WrappedWriteSessionPtr wrappedSession, std::optional<TSessionClosedEvent> sessionClosedEvent) {
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

TStringBuilder TProducer::LogPrefix() {
    return TStringBuilder() << " Id: " << Id << " Epoch: " << Epoch.load() << " ";
}

void TProducer::NextEpoch() {
    auto epoch = Epoch.load(std::memory_order_relaxed);
    for (;;) {
        auto nextEpoch = epoch >= MAX_EPOCH - 1 ? 0 : epoch + 1;
        if (Epoch.compare_exchange_weak(epoch, nextEpoch, std::memory_order_relaxed)) {
            if (nextEpoch == 0) {
                LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Epoch overflow, resetting to 0");
            }
            return;
        }
    }
}

void TProducer::RunMainWorker(std::int64_t owner) {
    // This function is both "request to run" and the runner itself.
    // We must handle two properties:
    // - TFuture::Subscribe may call back synchronously when future is already ready.
    // - A callback may race with the runner trying to go idle (avoid lost wakeups).
    // States Idle/Running/Rerun are defined in the EMainWorkerState enum on TProducer.

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

    MainWorkerOwner = owner;
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

        while (!FlushPromises.empty()) {
            auto& [promise, flushResult] = FlushPromises.front();
            promise.TrySetValue(flushResult);
            FlushPromises.pop_front();
        }

        const auto isClosed = Closed.load();
        const auto closeTimeout = GetCloseTimeout();
        if (isClosed && (Done.load() || MessagesWorker->IsQueueEmpty() || closeTimeout == TDuration::Zero())) {
            // Use the canonical wake path: rotate under EventsWorker's Lock and fire outside.
            auto closeWakeup = EventsWorker->WakeAndRotate();
            closeWakeup.TrySetValue();
            auto sessionClosedEvent = EventsWorker->GetSessionClosedEvent();
            MessagesWorker->SetClosedStatusToFlushPromises(
                sessionClosedEvent ?
                std::make_optional(TCloseDescription(*sessionClosedEvent)) :
                std::nullopt);
            MainWorkerState.store(Idle, std::memory_order_release);
            ShutdownPromise.TrySetValue();
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

TWriteResult TProducer::WriteInternal(TContinuationToken&&, TWriteMessage&& message) {
    std::optional<NThreading::TPromise<void>> eventsPromise;
    {
        std::lock_guard lock(GlobalLock);
        Metrics.IncIncomingMessages();
        if (Closed.load()) {
            auto sessionClosedEvent = EventsWorker->GetSessionClosedEvent();
            return TWriteResult{
                .Status = EWriteStatus::Error,
                .ErrorMessage = "producer is closed",
                .ClosedDescription = sessionClosedEvent ? std::make_optional(TCloseDescription(*sessionClosedEvent)) : std::nullopt,
            };
        }

        if ((message.SeqNo_.has_value() && SeqNoStrategy == ESeqNoStrategy::WithoutSeqNo)
            || (!message.SeqNo_.has_value() && SeqNoStrategy == ESeqNoStrategy::WithSeqNo)) {
            ythrow TContractViolation("Can not mix messages with and without seqNo");
        }

        if (SeqNoStrategy == ESeqNoStrategy::NotInitialized) {
            SeqNoStrategy = message.SeqNo_.has_value() ? ESeqNoStrategy::WithSeqNo : ESeqNoStrategy::WithoutSeqNo;
        }

        std::uint32_t chosenPartition;
        std::string key;
        std::string choosePartitionKey;
        if (message.GetPartition().has_value()) {
            auto partitionIt = Partitions.find(message.GetPartition().value());
            if (partitionIt == Partitions.end()) {
                return TWriteResult{
                    .Status = EWriteStatus::Error,
                    .ErrorMessage = "Unknown partition",
                };
            }
            if (!partitionIt->second.Children_.empty()) {
                return TWriteResult{
                    .Status = EWriteStatus::Error,
                    .ErrorMessage = "Partition was split",
                };
            }

            chosenPartition = message.GetPartition().value();
        } else if (!message.GetKey().has_value()) {
            key = Settings.ProducerIdPrefix_;
            const auto partitionChoice = PartitionChooser->ChoosePartition(Settings.ProducerIdPrefix_);
            chosenPartition = partitionChoice.first;
            choosePartitionKey = partitionChoice.second;
        } else {
            const auto partitionChoice = PartitionChooser->ChoosePartition(*message.GetKey());
            chosenPartition = partitionChoice.first;
            choosePartitionKey = partitionChoice.second;
            key = *message.GetKey();
        }

        MessagesWorker->AddMessage(key, choosePartitionKey, std::move(message), chosenPartition);
        eventsPromise = EventsWorker->HandleNewMessage();
        RunUserEventLoop();
    }

    RunMainWorker(-1);
    if (eventsPromise) {
        eventsPromise->TrySetValue();
    }

    return TWriteResult{
        .Status = EWriteStatus::Queued,
    };
}

TWriteResult TProducer::Write(TWriteMessage&& message) {
    auto remainingTimeout = Settings.MaxBlockTimeout_;
    auto sleepTimeMs = DEFAULT_START_BLOCK_TIMEOUT;
    for (;;) {
        if (Closed.load()) {
            auto sessionClosedEvent = EventsWorker->GetSessionClosedEvent();
            return TWriteResult{
                .Status = EWriteStatus::Error,
                .ErrorMessage = "producer is closed",
                .ClosedDescription = sessionClosedEvent ? std::make_optional(TCloseDescription(*sessionClosedEvent)) : std::nullopt,
            };
        }

        auto continuationToken = EventsWorker->GetContinuationToken();
        if (!continuationToken) {
            if (remainingTimeout > TDuration::Zero()) {
                auto toSleep = Min(sleepTimeMs, remainingTimeout);
                Sleep(toSleep);
                sleepTimeMs *= 2;
                if (remainingTimeout > toSleep) {
                    remainingTimeout -= toSleep;
                    continue;
                }

                return TWriteResult{
                    .Status = EWriteStatus::Timeout,
                };
            }

            return TWriteResult{
                .Status = EWriteStatus::Timeout,
            };
        }

        return WriteInternal(std::move(*continuationToken), std::move(message));
    }
}

void TProducer::Write(TContinuationToken&& continuationToken, TWriteMessage&& message) {
    WriteInternal(std::move(continuationToken), std::move(message));
}

TWriteStats TProducer::GetWriteStats() {
    std::lock_guard lock(GlobalLock);
    return TWriteStats{
        .LastWrittenSeqNo = LastWrittenSeqNo,
        .MessagesWritten = MessagesWritten,
    };
}

NThreading::TFuture<TFlushResult> TProducer::Flush() {
    std::unique_lock lock(GlobalLock);
    if (Closed.load() || MessagesWorker->InFlightMessages.empty()) {
        auto sessionClosedEvent = EventsWorker->GetSessionClosedEvent();
        return NThreading::MakeFuture(TFlushResult{
            .Status = EFlushStatus::Success,
            .LastWrittenSeqNo = LastWrittenSeqNo,
            .ClosedDescription = sessionClosedEvent ? std::make_optional(TCloseDescription(*sessionClosedEvent)) : std::nullopt,
        });
    }

    auto lastInFlightMessage = std::prev(MessagesWorker->InFlightMessages.end());
    if (!lastInFlightMessage->FlushPromise.Initialized()) {
        lastInFlightMessage->FlushPromise = NThreading::NewPromise<TFlushResult>();
    }

    return lastInFlightMessage->FlushPromise.GetFuture();
}

TInstant TProducer::GetCloseDeadline() {
    std::lock_guard lock(GlobalLock);
    return CloseDeadline;
}

void TProducer::HandleAutoPartitioning(std::uint32_t partition) {
    auto splittedPartitionWorker = std::make_shared<TSplittedPartitionWorker>(this, partition);
    SplittedPartitionWorkers.try_emplace(partition, splittedPartitionWorker);
}

TWriterCounters::TPtr TProducer::GetCounters() {
    return nullptr;
}

TProducer::TBoundPartitionChooser::TBoundPartitionChooser(TProducer* producer)
    : Producer(producer)
{}

std::pair<std::uint32_t, std::string> TProducer::TBoundPartitionChooser::ChoosePartition(const std::string_view key) {
    auto hashedKey = Producer->PartitioningKeyHasher(key);

    auto lowerBound = Producer->PartitionsIndex.lower_bound(hashedKey);
    if (lowerBound != Producer->PartitionsIndex.end() && lowerBound->first == hashedKey) {
        return { lowerBound->second, hashedKey };
    }

    Y_ABORT_IF(lowerBound == Producer->PartitionsIndex.begin(), "Lower bound is the first element");
    return { std::prev(lowerBound)->second, hashedKey };
}

TProducer::THashPartitionChooser::THashPartitionChooser(std::vector<std::uint32_t>&& partitions)
    : Partitions(std::move(partitions))
{
    Y_ABORT_UNLESS(!Partitions.empty(), "THashPartitionChooser requires at least one partition");
}

std::pair<std::uint32_t, std::string> TProducer::THashPartitionChooser::ChoosePartition(const std::string_view key) {
    // Partitions is guaranteed non-empty by the constructor invariant.
    auto hash = MurmurHash<std::uint64_t>(key.data(), key.size());
    return {Partitions[hash % Partitions.size()], ""};
}

} // namespace NYdb::inline Dev::NTopic
