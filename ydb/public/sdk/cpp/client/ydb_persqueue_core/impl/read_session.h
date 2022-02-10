#pragma once
 
#include "common.h" 
 
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>

#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>

#include <util/digest/numeric.h>
#include <util/generic/hash.h>
#include <util/system/condvar.h>

#include <atomic>
#include <deque>

namespace NYdb::NPersQueue {

class TPartitionStreamImpl;
class TSingleClusterReadSessionImpl;
class TDeferredActions;
class TReadSession;
using IReadSessionConnectionProcessorFactory = ISessionConnectionProcessorFactory<Ydb::PersQueue::V1::MigrationStreamingReadClientMessage, Ydb::PersQueue::V1::MigrationStreamingReadServerMessage>;
class TReadSessionEventsQueue;

struct IErrorHandler : public TThrRefBase {
    using TPtr = TIntrusivePtr<IErrorHandler>;

    virtual void AbortSession(TSessionClosedEvent&& closeEvent) = 0;

    void AbortSession(EStatus statusCode, NYql::TIssues&& issues) {
        AbortSession(TSessionClosedEvent(statusCode, std::move(issues)));
    }

    void AbortSession(EStatus statusCode, const TString& message) {
        NYql::TIssues issues;
        issues.AddIssue(message);
        AbortSession(statusCode, std::move(issues));
    }

    void AbortSession(TPlainStatus&& status) {
        AbortSession(TSessionClosedEvent(std::move(status)));
    }
};

// Special class that stores actions to be done after lock will be released.
class TDeferredActions {
public:
    using IProcessor = IReadSessionConnectionProcessorFactory::IProcessor;

public:
    ~TDeferredActions() {
        DoActions();
    }

    void DeferReadFromProcessor(const IProcessor::TPtr& processor, Ydb::PersQueue::V1::MigrationStreamingReadServerMessage* dst, IProcessor::TReadCallback callback);
    void DeferStartExecutorTask(const IExecutor::TPtr& executor, IExecutor::TFunction task);
    void DeferAbortSession(const IErrorHandler::TPtr& errorHandler, TSessionClosedEvent&& closeEvent);
    void DeferAbortSession(const IErrorHandler::TPtr& errorHandler, EStatus statusCode, NYql::TIssues&& issues);
    void DeferAbortSession(const IErrorHandler::TPtr& errorHandler, EStatus statusCode, const TString& message);
    void DeferAbortSession(const IErrorHandler::TPtr& errorHandler, TPlainStatus&& status);
    void DeferReconnection(std::shared_ptr<TSingleClusterReadSessionImpl> session, const IErrorHandler::TPtr& errorHandler, TPlainStatus&& status);
    void DeferSignalWaiter(TWaiter&& waiter);

private:
    void DoActions();

    void Read();
    void StartExecutorTasks();
    void AbortSession();
    void Reconnect();
    void SignalWaiters();

private:
    // Read.
    IProcessor::TPtr Processor;
    Ydb::PersQueue::V1::MigrationStreamingReadServerMessage* ReadDst = nullptr;
    IProcessor::TReadCallback ReadCallback;

    // Executor tasks.
    std::vector<std::pair<IExecutor::TPtr, IExecutor::TFunction>> ExecutorsTasks;

    // Abort session.
    IErrorHandler::TPtr ErrorHandler;
    TMaybe<TSessionClosedEvent> SessionClosedEvent;

    // Waiters.
    std::vector<TWaiter> Waiters;

    // Reconnection.
    std::shared_ptr<TSingleClusterReadSessionImpl> Session;
    TPlainStatus ReconnectionStatus;
};

class TDataDecompressionInfo {
public:
    TDataDecompressionInfo(const TDataDecompressionInfo&) = default;
    TDataDecompressionInfo(TDataDecompressionInfo&&) = default;
    TDataDecompressionInfo( 
        Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::PartitionData&& msg, 
        std::weak_ptr<TSingleClusterReadSessionImpl> session, 
        bool doDecompress 
    ); 

    i64 StartDecompressionTasks(const IExecutor::TPtr& executor,
                                i64 availableMemory,
                                double averageCompressionRatio,
                                const TIntrusivePtr<TPartitionStreamImpl>& partitionStream,
                                TDeferredActions& deferred);

    bool IsReady() const {
        return SourceDataNotProcessed == 0;
    }

    bool AllDecompressionTasksStarted() const {
        Y_VERIFY(ServerMessage.batches_size() > 0);
        return CurrentDecompressingMessage.first >= static_cast<size_t>(ServerMessage.batches_size());
    }

    i64 GetCompressedDataSize() const {
        return CompressedDataSize;
    }

    const Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::PartitionData& GetServerMessage() const {
        return ServerMessage;
    }

    Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::PartitionData& GetServerMessage() {
        return ServerMessage;
    }

    TMaybe<std::pair<size_t, size_t>> GetReadyThreshold() const {
        size_t readyCount = 0;
        std::pair<size_t, size_t> ret;
        for (auto i = ReadyThresholds.begin(), end = ReadyThresholds.end(); i != end; ++i) {
            if (i->Ready) {
                ret.first = i->Batch;
                ret.second = i->Message;
                ++readyCount;
            } else {
                break;
            }
        }
        if (!readyCount) {
            return Nothing();
        }
        return ret;
    }

    TWriteSessionMeta::TPtr GetBatchMeta(size_t batchIndex) const {
        Y_ASSERT(batchIndex < BatchesMeta.size());
        return BatchesMeta[batchIndex];
    }

    // Takes data. Returns true if event has more unpacked data.
    bool TakeData(const TIntrusivePtr<TPartitionStreamImpl>& partitionStream, 
                  TVector<TReadSessionEvent::TDataReceivedEvent::TMessage>* messages, 
                  TVector<TReadSessionEvent::TDataReceivedEvent::TCompressedMessage>* compressedMessages, 
                  size_t* maxByteSize); 

    bool HasMoreData() const {
        return CurrentReadingMessage.first < static_cast<size_t>(GetServerMessage().batches_size());
    }

    bool HasReadyUnreadData() const;

    void PutDecompressionError(std::exception_ptr error, size_t batch, size_t message);
    std::exception_ptr GetDecompressionError(size_t batch, size_t message);

private:
    // Special struct for marking (batch/message) as ready.
    struct TReadyMessageThreshold {
        size_t Batch = 0; // Last ready batch with message index.
        size_t Message = 0; // Last ready message index.
        std::atomic<bool> Ready = false;
    };

    struct TDecompressionTask {
        explicit TDecompressionTask(TDataDecompressionInfo* parent, TIntrusivePtr<TPartitionStreamImpl> partitionStream, TReadyMessageThreshold* ready);

        // Decompress and notify about memory consumption changes.
        void operator()();

        void Add(size_t batch, size_t message, size_t sourceDataSize, size_t estimatedDecompressedSize);

        size_t AddedDataSize() const {
            return SourceDataSize;
        }
        size_t AddedMessagesCount() const {
            return Messages.size();
        }

    private:
        TDataDecompressionInfo* Parent;
        TIntrusivePtr<TPartitionStreamImpl> PartitionStream;
        i64 SourceDataSize = 0;
        i64 EstimatedDecompressedSize = 0;
        i64 DecompressedSize = 0;
        struct TMessageRange {
            size_t Batch;
            std::pair<size_t, size_t> MessageRange;
        };
        std::vector<TMessageRange> Messages;
        TReadyMessageThreshold* Ready;
    };

    void BuildBatchesMeta();

private:
    Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::PartitionData ServerMessage;
    std::vector<TWriteSessionMeta::TPtr> BatchesMeta;
    std::weak_ptr<TSingleClusterReadSessionImpl> Session;
    bool DoDecompress; 
    i64 CompressedDataSize = 0;
    std::atomic<i64> SourceDataNotProcessed = 0;
    std::pair<size_t, size_t> CurrentDecompressingMessage = {0, 0}; // (Batch, Message)
    std::deque<TReadyMessageThreshold> ReadyThresholds;
    std::pair<size_t, size_t> CurrentReadingMessage = {0, 0}; // (Batch, Message)

    // Decompression exceptions.
    // Optimization for rare using.
    std::atomic<bool> DecompressionErrorsStructCreated = false;
    TAdaptiveLock DecompressionErrorsStructLock;
    std::vector<std::vector<std::exception_ptr>> DecompressionErrors;
};

struct IUserRetrievedEventCallback {
    virtual ~IUserRetrievedEventCallback() = default;

    virtual void OnUserRetrievedEvent(const TReadSessionEvent::TEvent& event) = 0;
};

struct TReadSessionEventInfo {
    using TEvent = TReadSessionEvent::TEvent;

    // Event with only partition stream ref.
    // Partition stream holds all its events.
    TIntrusivePtr<TPartitionStreamImpl> PartitionStream;
    TMaybe<TEvent> Event;
    std::weak_ptr<IUserRetrievedEventCallback> Session;

    // Close event.
    TReadSessionEventInfo(const TSessionClosedEvent& event, std::weak_ptr<IUserRetrievedEventCallback> session = {})
        : Event(TEvent(event))
        , Session(session)
    {
    }

    // Usual event.
    TReadSessionEventInfo(TIntrusivePtr<TPartitionStreamImpl> partitionStream, std::weak_ptr<IUserRetrievedEventCallback> session, TEvent event);

    // Data event.
    TReadSessionEventInfo(TIntrusivePtr<TPartitionStreamImpl> partitionStream, std::weak_ptr<IUserRetrievedEventCallback> session);

    TReadSessionEventInfo(TIntrusivePtr<TPartitionStreamImpl> partitionStream,
                          std::weak_ptr<IUserRetrievedEventCallback> session,
                          TVector<TReadSessionEvent::TDataReceivedEvent::TMessage> messages, 
                          TVector<TReadSessionEvent::TDataReceivedEvent::TCompressedMessage> compressedMessages); 

    bool IsEmpty() const;
    bool IsDataEvent() const;

    // Takes data. Returns true if event has more unpacked data.
    bool TakeData(TVector<TReadSessionEvent::TDataReceivedEvent::TMessage>* messages, 
                  TVector<TReadSessionEvent::TDataReceivedEvent::TCompressedMessage>* comressedMessages, 
                  size_t* maxByteSize); 

    TEvent& GetEvent() {
        Y_ASSERT(Event);
        return *Event;
    }

    // Move event to partition stream queue.
    void MoveToPartitionStream();

    void ExtractFromPartitionStream();

    void OnUserRetrievedEvent();

    bool HasMoreData() const; // Has unread data.
    bool HasReadyUnreadData() const; // Has ready unread data.

    bool IsSessionClosedEvent() const {
        return Event && std::holds_alternative<TSessionClosedEvent>(*Event);
    }
};

// Raw data with maybe uncompressed parts or other read session event.
struct TRawPartitionStreamEvent {
    std::variant<TDataDecompressionInfo, TReadSessionEvent::TEvent> Event;
    bool Signalled = false;

    TRawPartitionStreamEvent(const TRawPartitionStreamEvent&) = default;
    TRawPartitionStreamEvent(TRawPartitionStreamEvent&&) = default;

    TRawPartitionStreamEvent( 
        Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::PartitionData&& msg, 
        std::weak_ptr<TSingleClusterReadSessionImpl> session, 
        bool doDecompress 
    ) 
        : Event(std::in_place_type_t<TDataDecompressionInfo>(), std::move(msg), std::move(session), doDecompress) 
    {
    }

    template <class T>
    explicit TRawPartitionStreamEvent(T&& event)
        : Event(std::in_place_type_t<TReadSessionEvent::TEvent>(), std::forward<T>(event))
    {
    }

    bool IsDataEvent() const {
        return std::holds_alternative<TDataDecompressionInfo>(Event);
    }

    const TDataDecompressionInfo& GetData() const {
        Y_ASSERT(IsDataEvent());
        return std::get<TDataDecompressionInfo>(Event);
    }

    TDataDecompressionInfo& GetData() {
        Y_ASSERT(IsDataEvent());
        return std::get<TDataDecompressionInfo>(Event);
    }

    TReadSessionEvent::TEvent& GetEvent() {
        Y_ASSERT(!IsDataEvent());
        return std::get<TReadSessionEvent::TEvent>(Event);
    }

    const TReadSessionEvent::TEvent& GetEvent() const {
        Y_ASSERT(!IsDataEvent());
        return std::get<TReadSessionEvent::TEvent>(Event);
    }

    bool IsReady() const {
        return !IsDataEvent() || GetData().IsReady();
    }

    void Signal(TPartitionStreamImpl* partitionStream, TReadSessionEventsQueue* queue, TDeferredActions& deferred);
};



class TPartitionStreamImpl : public TPartitionStream {
public:
    struct TKey { // Hash<TKey> is defined later in this file.
        TString Topic;
        TString Cluster;
        ui64 Partition;

        bool operator==(const TKey& other) const {
            // Compare the most variable fields first.
            return Partition == other.Partition
                && Cluster == other.Cluster
                && Topic == other.Topic;
        }
    };

    TPartitionStreamImpl(ui64 partitionStreamId,
                         TString topicPath,
                         TString cluster,
                         ui64 partitionGroupId,
                         ui64 partitionId,
                         ui64 assignId,
                         ui64 readOffset,
                         std::weak_ptr<TSingleClusterReadSessionImpl> parentSession,
                         IErrorHandler::TPtr errorHandler)
        : Key{topicPath, cluster, partitionId}
        , AssignId(assignId)
        , FirstNotReadOffset(readOffset)
        , Session(std::move(parentSession))
        , ErrorHandler(std::move(errorHandler))
    {
        PartitionStreamId = partitionStreamId;
        TopicPath = std::move(topicPath);
        Cluster = std::move(cluster);
        PartitionGroupId = partitionGroupId;
        PartitionId = partitionId;
        MaxCommittedOffset = readOffset;
    }

    ~TPartitionStreamImpl();

    ui64 GetFirstNotReadOffset() const {
        return FirstNotReadOffset;
    }

    void SetFirstNotReadOffset(const ui64 offset) {
        FirstNotReadOffset = offset;
    }

    void Commit(ui64 startOffset, ui64 endOffset) /*override*/;
    void RequestStatus() override;

    void ConfirmCreate(TMaybe<ui64> readOffset, TMaybe<ui64> commitOffset);
    void ConfirmDestroy();

    void StopReading() /*override*/;
    void ResumeReading() /*override*/;

    ui64 GetAssignId() const {
        return AssignId;
    }

    const TKey& GetKey() const {
        return Key;
    }

    template <class T>
    void InsertEvent(T&& event) {
        EventsQueue.emplace_back(std::forward<T>(event));
    }

    TDataDecompressionInfo& InsertDataEvent( 
        Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::PartitionData&& msg, 
        bool doDecompress 
    ) { 
        ++DataDecompressionEventsCount;
        return EventsQueue.emplace_back(std::move(msg), Session, doDecompress).GetData(); 
    }

    bool IsWaitingForDataDecompression() const {
        return DataDecompressionEventsCount > 0;
    }

    bool HasEvents() const {
        return !EventsQueue.empty();
    }

    TRawPartitionStreamEvent& TopEvent() {
        return EventsQueue.front();
    }

    const TRawPartitionStreamEvent& TopEvent() const {
        return EventsQueue.front();
    }

    void PopEvent() {
        if (EventsQueue.front().IsDataEvent()) {
            --DataDecompressionEventsCount;
        }
        EventsQueue.pop_front();
    }

    std::weak_ptr<TSingleClusterReadSessionImpl> GetSession() const {
        return Session;
    }

    TLog GetLog() const;

    void SignalReadyEvents(TReadSessionEventsQueue* queue, TDeferredActions& deferred);

    const IErrorHandler::TPtr& GetErrorHandler() const {
        return ErrorHandler;
    }

    ui64 GetMaxReadOffset() const {
        return MaxReadOffset;
    }

    ui64 GetMaxCommittedOffset() const {
        return MaxCommittedOffset;
    }

    void UpdateMaxReadOffset(ui64 offset) {
        if (offset > MaxReadOffset) {
            MaxReadOffset = offset;
        }
    }

    void UpdateMaxCommittedOffset(ui64 offset) {
        if (offset > MaxCommittedOffset) {
            ClientCommits.EraseInterval(MaxCommittedOffset, offset);
            MaxCommittedOffset = offset;
        }
    }

    bool HasCommitsInflight() const {
        if (ClientCommits.Empty())
            return false;
        auto range = *ClientCommits.begin();
        if (range.first > MaxCommittedOffset)
            return false;
        // Here we got first range that can be committed by server.
        // If offset to commit is from same position - then nothing is inflight.
        if (!Commits.Empty() && Commits.begin()->first == range.first)
            return false;
        return true;
    }

    bool AddToCommitRanges(const ui64 startOffset, const ui64 endOffset, bool rangesMode) {
        if (ClientCommits.Intersects(startOffset, endOffset) || startOffset < MaxCommittedOffset) {
            ThrowFatalError(TStringBuilder() << "Invalid offset range [" << startOffset << ", " << endOffset << ") : range must start from "
                                             << MaxCommittedOffset << " or has some offsets that are committed already. Partition stream id: " << PartitionStreamId << Endl);
            return false;
        }
        if (rangesMode) { // Otherwise no need to send it to server.
            Y_VERIFY(!Commits.Intersects(startOffset, endOffset));
            Commits.InsertInterval(startOffset, endOffset);
        }
        ClientCommits.InsertInterval(startOffset, endOffset);
        return true;
    }


private:
    const TKey Key;
    ui64 AssignId;
    ui64 FirstNotReadOffset;
    std::weak_ptr<TSingleClusterReadSessionImpl> Session;
    IErrorHandler::TPtr ErrorHandler;
    std::deque<TRawPartitionStreamEvent> EventsQueue;
    size_t DataDecompressionEventsCount = 0;
    ui64 MaxReadOffset = 0;
    ui64 MaxCommittedOffset = 0;

    TDisjointIntervalTree<ui64> Commits;
    TDisjointIntervalTree<ui64> ClientCommits;
};


class TReadSessionEventsQueue : public TBaseSessionEventsQueue<TReadSessionSettings, TReadSessionEvent::TEvent, TReadSessionEventInfo> {
    using TParent = TBaseSessionEventsQueue<TReadSessionSettings, TReadSessionEvent::TEvent, TReadSessionEventInfo>;

public:
    explicit TReadSessionEventsQueue(const TSettings& settings, std::weak_ptr<IUserRetrievedEventCallback> session);

    TMaybe<TEventInfo> GetDataEventImpl(TEventInfo& srcDataEventInfo, size_t* maxByteSize); // Assumes that we're under lock.

    TMaybe<TEventInfo> TryGetEventImpl(size_t* maxByteSize) { // Assumes that we're under lock.
        Y_ASSERT(HasEventsImpl());
        TVector<TReadSessionEvent::TDataReceivedEvent::TMessage> messages;
        if (!Events.empty()) {
            TEventInfo event = std::move(Events.front());
            Events.pop();
            RenewWaiterImpl();
            auto partitionStream = event.PartitionStream;

            if (!partitionStream->HasEvents()) {
                Y_FAIL("can't be here - got events in global queue, but nothing in partition queue");
                return Nothing();
            }

            if (partitionStream->TopEvent().IsDataEvent()) {
                return GetDataEventImpl(event, maxByteSize);
            }

            event = TReadSessionEventInfo(partitionStream.Get(), event.Session, partitionStream->TopEvent().GetEvent());
            partitionStream->PopEvent();
            return event;
        }

        Y_ASSERT(CloseEvent);
        return TEventInfo(*CloseEvent, Session);
    }

    TMaybe<TEventInfo> GetEventImpl(size_t* maxByteSize) { // Assumes that we're under lock and that the event queue has events.
        do {
            TMaybe<TEventInfo> result = TryGetEventImpl(maxByteSize); // We could have read all the data in current message previous time.
            if (result) {
                return result;
            }
        } while (HasEventsImpl());
        return Nothing();
    }

    TVector<TEvent> GetEvents(bool block = false, TMaybe<size_t> maxEventsCount = Nothing(), size_t maxByteSize = std::numeric_limits<size_t>::max()) {
        TVector<TEventInfo> eventInfos;
        const size_t maxCount = maxEventsCount ? *maxEventsCount : std::numeric_limits<size_t>::max();
        TDeferredActions deferred;
        std::vector<TIntrusivePtr<TPartitionStreamImpl>> partitionStreamsForSignalling;
        with_lock (Mutex) {
            eventInfos.reserve(Min(Events.size() + CloseEvent.Defined(), maxCount));
            do {
                if (block) {
                    WaitEventsImpl();
                }

                ApplyCallbacksToReadyEventsImpl(deferred);

                while (HasEventsImpl() && eventInfos.size() < maxCount && maxByteSize > 0) {
                    TMaybe<TEventInfo> event = GetEventImpl(&maxByteSize);
                    if (event) {
                        const TIntrusivePtr<TPartitionStreamImpl> partitionStreamForSignalling = event->IsDataEvent() ? event->PartitionStream : nullptr;
                        eventInfos.emplace_back(std::move(*event));
                        if (eventInfos.back().IsSessionClosedEvent()) {
                            break;
                        }
                        if (partitionStreamForSignalling) {
                            partitionStreamsForSignalling.emplace_back(std::move(partitionStreamForSignalling));
                        }
                    }
                }
            } while (block && (eventInfos.empty() || eventInfos.back().IsSessionClosedEvent()));
            ApplyCallbacksToReadyEventsImpl(deferred);
            for (const auto& partitionStreamForSignalling : partitionStreamsForSignalling) {
                SignalReadyEventsImpl(partitionStreamForSignalling.Get(), deferred);
            }
        }

        TVector<TEvent> result;
        result.reserve(eventInfos.size());
        for (TEventInfo& eventInfo : eventInfos) {
            eventInfo.OnUserRetrievedEvent();
            result.emplace_back(std::move(eventInfo.GetEvent()));
        }
        return result;
    }

    TMaybe<TEvent> GetEvent(bool block = false, size_t maxByteSize = std::numeric_limits<size_t>::max()) {
        TMaybe<TEventInfo> eventInfo;
        TDeferredActions deferred;
        with_lock (Mutex) {
            TIntrusivePtr<TPartitionStreamImpl> partitionStreamForSignalling;
            do {
                if (block) {
                    WaitEventsImpl();
                }

                const bool appliedCallbacks = ApplyCallbacksToReadyEventsImpl(deferred);

                if (HasEventsImpl()) {
                    eventInfo = GetEventImpl(&maxByteSize);
                    if (eventInfo && eventInfo->IsDataEvent()) {
                        partitionStreamForSignalling = eventInfo->PartitionStream;
                    }
                } else if (!appliedCallbacks) {
                    return Nothing();
                }
            } while (block && !eventInfo);
            ApplyCallbacksToReadyEventsImpl(deferred);
            if (partitionStreamForSignalling) {
                SignalReadyEventsImpl(partitionStreamForSignalling.Get(), deferred);
            }
        }
        if (eventInfo) {
            eventInfo->OnUserRetrievedEvent();
            return std::move(eventInfo->Event);
        } else {
            return Nothing();
        }
    }

    void Close(const TSessionClosedEvent& event, TDeferredActions& deferred) {
        TWaiter waiter;
        with_lock (Mutex) {
            CloseEvent = event;
            Closed = true;
            waiter = TWaiter(Waiter.ExtractPromise(), this);
        }

        TEventInfo info(event);
        ApplyHandler(info, deferred);

        waiter.Signal();
    }

    bool HasCallbackForNextEventImpl() const;
    bool ApplyCallbacksToReadyEventsImpl(TDeferredActions& deferred);

    // Push usual event.
    void PushEvent(TReadSessionEventInfo eventInfo, TDeferredActions& deferred);

    // Push data event.
    TDataDecompressionInfo* PushDataEvent(TIntrusivePtr<TPartitionStreamImpl> partitionStream, Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::PartitionData&& msg);

    void SignalEventImpl(TIntrusivePtr<TPartitionStreamImpl> partitionStream, TDeferredActions& deferred); // Assumes that we're under lock.

    void SignalReadyEvents(TPartitionStreamImpl* partitionStream);

    void SignalReadyEventsImpl(TPartitionStreamImpl* partitionStream, TDeferredActions& deferred); // Assumes that we're under lock.

    void SignalWaiterImpl(TDeferredActions& deferred) {
        TWaiter waiter = PopWaiterImpl();
        deferred.DeferSignalWaiter(std::move(waiter)); // No effect if waiter is empty.
    }

    void ClearAllEvents();

private:
    struct THandlersVisitor : public TParent::TBaseHandlersVisitor {
        THandlersVisitor(const TSettings& settings, TEventInfo& eventInfo, TDeferredActions& deferred)
            : TBaseHandlersVisitor(settings, eventInfo)
            , Deferred(deferred)
        {}

#define DECLARE_HANDLER(type, handler, answer)                          \
        bool operator()(type&) {                                        \
            if (PushHandler<type>(                                      \
                std::move(EventInfo),                                   \
                Settings.EventHandlers_.handler,                        \
                Settings.EventHandlers_.CommonHandler_)) {              \
                return answer;                                          \
            }                                                           \
            return false;                                               \
        }                                                               \
        /**/

        DECLARE_HANDLER(TReadSessionEvent::TDataReceivedEvent, DataReceivedHandler_, true);
        DECLARE_HANDLER(TReadSessionEvent::TCommitAcknowledgementEvent, CommitAcknowledgementHandler_, true);
        DECLARE_HANDLER(TReadSessionEvent::TCreatePartitionStreamEvent, CreatePartitionStreamHandler_, true);
        DECLARE_HANDLER(TReadSessionEvent::TDestroyPartitionStreamEvent, DestroyPartitionStreamHandler_, true);
        DECLARE_HANDLER(TReadSessionEvent::TPartitionStreamStatusEvent, PartitionStreamStatusHandler_, true);
        DECLARE_HANDLER(TReadSessionEvent::TPartitionStreamClosedEvent, PartitionStreamClosedHandler_, true);
        DECLARE_HANDLER(TSessionClosedEvent, SessionClosedHandler_, false); // Not applied
#undef DECLARE_HANDLER

        bool Visit() {
            return std::visit(*this, EventInfo.GetEvent());
        }

        void Post(const IExecutor::TPtr& executor, IExecutor::TFunction&& f) {
            Deferred.DeferStartExecutorTask(executor, std::move(f));
        }

        TDeferredActions& Deferred;
    };

    bool ApplyHandler(TEventInfo& eventInfo, TDeferredActions& deferred) {
        THandlersVisitor visitor(Settings, eventInfo, deferred);
        return visitor.Visit();
    }

private:
    bool HasEventCallbacks;
    std::weak_ptr<IUserRetrievedEventCallback> Session;
};



} // namespace NYdb::NPersQueue

template <>
struct THash<NYdb::NPersQueue::TPartitionStreamImpl::TKey> {
    size_t operator()(const NYdb::NPersQueue::TPartitionStreamImpl::TKey& key) const {
        THash<TString> strHash;
        const size_t h1 = strHash(key.Topic);
        const size_t h2 = strHash(key.Cluster);
        const size_t h3 = NumericHash(key.Partition);
        return CombineHashes(h1, CombineHashes(h2, h3));
    }
};

namespace NYdb::NPersQueue {

// Read session for single cluster.
// This class holds only read session logic.
// It is parametrized with output queue for client events
// and connection factory interface to separate logic from transport.
class TSingleClusterReadSessionImpl : public std::enable_shared_from_this<TSingleClusterReadSessionImpl>,
                                      public IUserRetrievedEventCallback {
public:
    using TPtr = std::shared_ptr<TSingleClusterReadSessionImpl>;
    using IProcessor = IReadSessionConnectionProcessorFactory::IProcessor;

    friend class TPartitionStreamImpl;

    TSingleClusterReadSessionImpl(
        const TReadSessionSettings& settings,
        const TString& clusterName,
        const TLog& log,
        std::shared_ptr<IReadSessionConnectionProcessorFactory> connectionFactory,
        std::shared_ptr<TReadSessionEventsQueue> eventsQueue,
        IErrorHandler::TPtr errorHandler,
        NGrpc::IQueueClientContextPtr clientContext,
        ui64 partitionStreamIdStart, ui64 partitionStreamIdStep
    )
        : Settings(settings)
        , ClusterName(clusterName)
        , Log(log)
        , NextPartitionStreamId(partitionStreamIdStart)
        , PartitionStreamIdStep(partitionStreamIdStep)
        , ConnectionFactory(std::move(connectionFactory))
        , EventsQueue(std::move(eventsQueue))
        , ErrorHandler(std::move(errorHandler))
        , ClientContext(std::move(clientContext))
        , CookieMapping(ErrorHandler)
    {
    }

    void Start();
    void ConfirmPartitionStreamCreate(const TPartitionStreamImpl* partitionStream, TMaybe<ui64> readOffset, TMaybe<ui64> commitOffset);
    void ConfirmPartitionStreamDestroy(TPartitionStreamImpl* partitionStream);
    void RequestPartitionStreamStatus(const TPartitionStreamImpl* partitionStream);
    void Commit(const TPartitionStreamImpl* partitionStream, ui64 startOffset, ui64 endOffset);

    void OnCreateNewDecompressionTask();
    void OnDataDecompressed(i64 sourceSize, i64 estimatedDecompressedSize, i64 decompressedSize, size_t messagesCount);

    TReadSessionEventsQueue* GetEventsQueue() {
        return EventsQueue.get();
    }

    void OnUserRetrievedEvent(const TReadSessionEvent::TEvent& event) override;

    void Abort();
    void Close(std::function<void()> callback);

    bool Reconnect(const TPlainStatus& status);

    void StopReadingData();
    void ResumeReadingData();

    void WaitAllDecompressionTasks();

    void DumpStatisticsToLog(TLogElement& log);
    void UpdateMemoryUsageStatistics();

    const TLog& GetLog() const {
        return Log;
    }

private:
    void BreakConnectionAndReconnectImpl(TPlainStatus&& status, TDeferredActions& deferred);

    void BreakConnectionAndReconnectImpl(EStatus statusCode, NYql::TIssues&& issues, TDeferredActions& deferred) {
        BreakConnectionAndReconnectImpl(TPlainStatus(statusCode, std::move(issues)), deferred);
    }

    void BreakConnectionAndReconnectImpl(EStatus statusCode, const TString& message, TDeferredActions& deferred) {
        BreakConnectionAndReconnectImpl(TPlainStatus(statusCode, message), deferred);
    }

    bool HasCommitsInflightImpl() const;

    void OnConnectTimeout(const NGrpc::IQueueClientContextPtr& connectTimeoutContext);
    void OnConnect(TPlainStatus&&, typename IProcessor::TPtr&&, const NGrpc::IQueueClientContextPtr& connectContext);
    void DestroyAllPartitionStreamsImpl(TDeferredActions& deferred); // Destroy all streams before setting new connection // Assumes that we're under lock.

    // Initing.
    void InitImpl(TDeferredActions& deferred); // Assumes that we're under lock.

    // Working logic.
    void ContinueReadingDataImpl(); // Assumes that we're under lock.
    bool IsActualPartitionStreamImpl(const TPartitionStreamImpl* partitionStream); // Assumes that we're under lock.

    // Read/Write.
    void ReadFromProcessorImpl(TDeferredActions& deferred); // Assumes that we're under lock.
    void WriteToProcessorImpl(Ydb::PersQueue::V1::MigrationStreamingReadClientMessage&& req); // Assumes that we're under lock.
    void OnReadDone(NGrpc::TGrpcStatus&& grpcStatus, size_t connectionGeneration);
    void OnReadDoneImpl(Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::InitResponse&& msg, TDeferredActions& deferred); // Assumes that we're under lock.
    void OnReadDoneImpl(Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch&& msg, TDeferredActions& deferred); // Assumes that we're under lock.
    void OnReadDoneImpl(Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::Assigned&& msg, TDeferredActions& deferred); // Assumes that we're under lock.
    void OnReadDoneImpl(Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::Release&& msg, TDeferredActions& deferred); // Assumes that we're under lock.
    void OnReadDoneImpl(Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::Committed&& msg, TDeferredActions& deferred); // Assumes that we're under lock.
    void OnReadDoneImpl(Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::PartitionStatus&& msg, TDeferredActions& deferred); // Assumes that we're under lock.

    void StartDecompressionTasksImpl(TDeferredActions& deferred); // Assumes that we're under lock.

    i64 GetCompressedDataSizeLimit() const {
        const double overallLimit = static_cast<double>(Settings.MaxMemoryUsageBytes_);
        // CompressedDataSize + CompressedDataSize * AverageCompressionRatio <= Settings.MaxMemoryUsageBytes_
        return Max<i64>(1l, static_cast<i64>(overallLimit / (1.0 + AverageCompressionRatio)));
    }

    i64 GetDecompressedDataSizeLimit() const {
        return Max<i64>(1l, static_cast<i64>(Settings.MaxMemoryUsageBytes_) - GetCompressedDataSizeLimit());
    }

    void CallCloseCallbackImpl();

    void UpdateMemoryUsageStatisticsImpl();

private:
    struct TPartitionCookieMapping {
        struct TCookie : public TThrRefBase {
            struct TKey {
                const ui64 AssignId;
                const ui64 CookieId;

                TKey(ui64 assignId, ui64 cookieId)
                    : AssignId(assignId)
                    , CookieId(cookieId)
                {
                }

                bool operator==(const TKey& k) const {
                    return AssignId == k.AssignId && CookieId == k.CookieId;
                }

                struct THash {
                    size_t operator()(const TKey& k) const {
                        ::THash<std::pair<ui64, ui64>> h;
                        return h(std::make_pair(k.AssignId, k.CookieId));
                    }
                };

            };

            using TPtr = TIntrusivePtr<TCookie>;

            explicit TCookie(ui64 cookie, TIntrusivePtr<TPartitionStreamImpl> partitionStream)
                : Cookie(cookie)
                , PartitionStream(std::move(partitionStream))
            {
            }

            // Sets reverse mapping for max offset in this cookie.
            void SetOffsetRange(const std::pair<ui64, ui64>& offsetRange) {
                OffsetRange = offsetRange;
                UncommittedMessagesLeft = offsetRange.second - offsetRange.first;
            }

            TKey GetKey() const {
                return TKey(PartitionStream->GetAssignId(), Cookie);
            }

            ui64 Cookie = 0;
            TIntrusivePtr<TPartitionStreamImpl> PartitionStream;
            std::pair<ui64, ui64> OffsetRange;
            size_t UncommittedMessagesLeft = 0;
        };

        explicit TPartitionCookieMapping(IErrorHandler::TPtr errorHandler)
            : ErrorHandler(std::move(errorHandler))
        {
        }

        bool AddMapping(const TCookie::TPtr& cookie);

        // Removes (partition stream, offset) from mapping.
        // Returns cookie ptr if this was the last message, otherwise nullptr.
        TCookie::TPtr CommitOffset(ui64 partitionStreamId, ui64 offset);

        // Gets and then removes committed cookie from mapping.
        TCookie::TPtr RetrieveCommittedCookie(const Ydb::PersQueue::V1::CommitCookie& cookieProto);

        // Removes mapping on partition stream.
        void RemoveMapping(ui64 partitionStreamId);

        // Clear all mapping before reconnect.
        void ClearMapping();

        bool HasUnacknowledgedCookies() const;

    private:
        THashMap<TCookie::TKey, TCookie::TPtr, TCookie::TKey::THash> Cookies;
        THashMap<std::pair<ui64, ui64>, TCookie::TPtr> UncommittedOffsetToCookie; // (Partition stream id, Offset) -> Cookie.
        THashMultiMap<ui64, TCookie::TPtr> PartitionStreamIdToCookie;
        IErrorHandler::TPtr ErrorHandler;
        size_t CommitInflight = 0; // Commit inflight to server.
    };

    struct TDecompressionQueueItem {
        TDecompressionQueueItem(TDataDecompressionInfo* batchInfo, TIntrusivePtr<TPartitionStreamImpl> partitionStream)
            : BatchInfo(batchInfo)
            , PartitionStream(std::move(partitionStream))
        {
        }

        TDataDecompressionInfo* BatchInfo;
        TIntrusivePtr<TPartitionStreamImpl> PartitionStream;
    };

private:
    const TReadSessionSettings Settings;
    const TString ClusterName;
    TLog Log;
    ui64 NextPartitionStreamId;
    ui64 PartitionStreamIdStep;
    std::shared_ptr<IReadSessionConnectionProcessorFactory> ConnectionFactory;
    std::shared_ptr<TReadSessionEventsQueue> EventsQueue;
    IErrorHandler::TPtr ErrorHandler;
    NGrpc::IQueueClientContextPtr ClientContext; // Common client context.
    NGrpc::IQueueClientContextPtr ConnectContext;
    NGrpc::IQueueClientContextPtr ConnectTimeoutContext;
    NGrpc::IQueueClientContextPtr ConnectDelayContext;
    size_t ConnectionGeneration = 0;
    TAdaptiveLock Lock;
    IProcessor::TPtr Processor;
    IRetryState::TPtr RetryState; // Current retry state (if now we are (re)connecting).
    size_t ConnectionAttemptsDone = 0;

    // Memory usage.
    i64 CompressedDataSize = 0;
    i64 DecompressedDataSize = 0;
    double AverageCompressionRatio = 1.0; // Weighted average for compression memory usage estimate.
    TInstant UsageStatisticsLastUpdateTime = TInstant::Now();

    bool WaitingReadResponse = false;
    std::shared_ptr<Ydb::PersQueue::V1::MigrationStreamingReadServerMessage> ServerMessage; // Server message to write server response to.
    THashMap<ui64, TIntrusivePtr<TPartitionStreamImpl>> PartitionStreams; // assignId -> Partition stream.
    TPartitionCookieMapping CookieMapping;
    std::deque<TDecompressionQueueItem> DecompressionQueue;
    bool DataReadingSuspended = false;

    // Exiting.
    bool Aborting = false;
    bool Closing = false;
    std::function<void()> CloseCallback;
    std::atomic<int> DecompressionTasksInflight = 0;
};

// High level class that manages several read session impls.
// Each one of them works with single cluster.
// This class communicates with cluster discovery service and then creates
// sessions to each cluster.
class TReadSession : public IReadSession,
                     public IUserRetrievedEventCallback,
                     public std::enable_shared_from_this<TReadSession> {
    struct TClusterSessionInfo {
        TClusterSessionInfo(const TString& cluster)
            : ClusterName(cluster)
        {
        }

        TString ClusterName; // In lower case
        TSingleClusterReadSessionImpl::TPtr Session;
        TVector<TTopicReadSettings> Topics;
        TString ClusterEndpoint;
    };

public:
    TReadSession(const TReadSessionSettings& settings,
                 std::shared_ptr<TPersQueueClient::TImpl> client,
                 std::shared_ptr<TGRpcConnectionsImpl> connections,
                 TDbDriverStatePtr dbDriverState);

    ~TReadSession();

    void Start();

    NThreading::TFuture<void> WaitEvent() override;
    TVector<TReadSessionEvent::TEvent> GetEvents(bool block, TMaybe<size_t> maxEventsCount, size_t maxByteSize) override;
    TMaybe<TReadSessionEvent::TEvent> GetEvent(bool block, size_t maxByteSize) override;

    bool Close(TDuration timeout) override;

    TString GetSessionId() const override {
        return SessionId;
    }

    TReaderCounters::TPtr GetCounters() const override {
        return Settings.Counters_; // Always not nullptr.
    }

    void AddTopic(const TTopicReadSettings& topicReadSettings) /*override*/ {
        Y_UNUSED(topicReadSettings);
        // TODO: implement.
        ThrowFatalError("Method \"AddTopic\" is not implemented");
    }

    void RemoveTopic(const TString& path) /*override*/ {
        Y_UNUSED(path);
        // TODO: implement.
        ThrowFatalError("Method \"RemoveTopic\" is not implemented");
    }

    void RemoveTopic(const TString& path, const TVector<ui64>& partitionGruops) /*override*/ {
        Y_UNUSED(path);
        Y_UNUSED(partitionGruops);
        // TODO: implement.
        ThrowFatalError("Method \"RemoveTopic\" is not implemented");
    }

    void StopReadingData() override;
    void ResumeReadingData() override;

    void Abort(TSessionClosedEvent&& closeEvent);

    void WaitAllDecompressionTasks();
    void ClearAllEvents();

private:
    // Start
    bool ValidateSettings();

    // Cluster discovery
    Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest MakeClusterDiscoveryRequest() const;
    void StartClusterDiscovery();
    void OnClusterDiscovery(const TStatus& status, const Ydb::PersQueue::ClusterDiscovery::DiscoverClustersResult& result);
    void ProceedWithoutClusterDiscovery();
    void RestartClusterDiscoveryImpl(TDuration delay, TDeferredActions& deferred);
    void CreateClusterSessionsImpl();


    // Shutdown.
    void Abort(EStatus statusCode, NYql::TIssues&& issues);
    void Abort(EStatus statusCode, const TString& message);

    void AbortImpl(TSessionClosedEvent&& closeEvent, TDeferredActions& deferred);
    void AbortImpl(EStatus statusCode, NYql::TIssues&& issues, TDeferredActions& deferred);
    void AbortImpl(EStatus statusCode, const TString& message, TDeferredActions& deferred);

    void OnUserRetrievedEvent(const TReadSessionEvent::TEvent& event) override;

    void MakeCountersIfNeeded();
    void DumpCountersToLog(size_t timeNumber = 0);
    void ScheduleDumpCountersToLog(size_t timeNumber = 0);

private:
    TReadSessionSettings Settings;
    const TString SessionId;
    const TInstant StartSessionTime = TInstant::Now();
    TLog Log;
    std::shared_ptr<TPersQueueClient::TImpl> Client;
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    IErrorHandler::TPtr ErrorHandler;
    TDbDriverStatePtr DbDriverState;
    TAdaptiveLock Lock;
    std::shared_ptr<TReadSessionEventsQueue> EventsQueue;
    THashMap<TString, TClusterSessionInfo> ClusterSessions; // Cluster name (in lower case) -> TClusterSessionInfo
    NGrpc::IQueueClientContextPtr ClusterDiscoveryDelayContext;
    IRetryState::TPtr ClusterDiscoveryRetryState;
    bool DataReadingSuspended = false;

    NGrpc::IQueueClientContextPtr DumpCountersContext;

    // Exiting.
    bool Aborting = false;
    bool Closing = false;
};

} // namespace NYdb::NPersQueue
