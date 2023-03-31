#pragma once

#include "common.h"

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>

#include <util/digest/numeric.h>
#include <util/generic/hash.h>
#include <util/generic/hash_multi_map.h>
#include <util/system/condvar.h>

#include <atomic>
#include <deque>

namespace NYdb::NPersQueue {

template <bool UseMigrationProtocol>
using TClientMessage = std::conditional_t<UseMigrationProtocol,
    Ydb::PersQueue::V1::MigrationStreamingReadClientMessage,
    Ydb::Topic::StreamReadMessage::FromClient>;

template <bool UseMigrationProtocol>
using TServerMessage = std::conditional_t<UseMigrationProtocol,
    Ydb::PersQueue::V1::MigrationStreamingReadServerMessage,
    Ydb::Topic::StreamReadMessage::FromServer>;

template <bool UseMigrationProtocol>
using IReadSessionConnectionProcessorFactory =
    ISessionConnectionProcessorFactory<TClientMessage<UseMigrationProtocol>, TServerMessage<UseMigrationProtocol>>;

template <bool UseMigrationProtocol>
using IProcessor = typename IReadSessionConnectionProcessorFactory<UseMigrationProtocol>::IProcessor;

template <bool UseMigrationProtocol>
using TPartitionData = std::conditional_t<UseMigrationProtocol,
    Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::PartitionData,
    Ydb::Topic::StreamReadMessage::ReadResponse::PartitionData>;

template <bool UseMigrationProtocol>
using TAWriteSessionMeta = std::conditional_t<UseMigrationProtocol,
    NYdb::NPersQueue::TWriteSessionMeta,
    NYdb::NTopic::TWriteSessionMeta>;

template <bool UseMigrationProtocol>
using TASessionClosedEvent = std::conditional_t<UseMigrationProtocol,
    NYdb::NPersQueue::TSessionClosedEvent,
    NYdb::NTopic::TSessionClosedEvent>;

template <bool UseMigrationProtocol>
using TAPartitionStream = std::conditional_t<UseMigrationProtocol,
    NYdb::NPersQueue::TPartitionStream,
    NYdb::NTopic::TPartitionSession>;

template <bool UseMigrationProtocol>
using TAReadSessionEvent = std::conditional_t<UseMigrationProtocol,
    NYdb::NPersQueue::TReadSessionEvent,
    NYdb::NTopic::TReadSessionEvent>;

template <bool UseMigrationProtocol>
using IARetryPolicy = std::conditional_t<UseMigrationProtocol,
    NYdb::NPersQueue::IRetryPolicy,
    NYdb::NTopic::IRetryPolicy>;

template <bool UseMigrationProtocol>
using IAExecutor = std::conditional_t<UseMigrationProtocol,
    NYdb::NPersQueue::IExecutor,
    NYdb::NTopic::IExecutor>;

template <bool UseMigrationProtocol>
using TAReadSessionSettings = std::conditional_t<UseMigrationProtocol,
    NYdb::NPersQueue::TReadSessionSettings,
    NYdb::NTopic::TReadSessionSettings>;

template <bool UseMigrationProtocol>
using TDataReceivedEvent = typename TAReadSessionEvent<UseMigrationProtocol>::TDataReceivedEvent;

template <bool UseMigrationProtocol>
class TPartitionStreamImpl;

template <bool UseMigrationProtocol>
class TSingleClusterReadSessionImpl;

template <bool UseMigrationProtocol>
class TDeferredActions;

template <bool UseMigrationProtocol>
class TReadSessionEventsQueue;

class TReadSession;

template <bool UseMigrationProtocol>
class TDataDecompressionInfo;

template <bool UseMigrationProtocol>
using TDataDecompressionInfoPtr = typename TDataDecompressionInfo<UseMigrationProtocol>::TPtr;


template <bool UseMigrationProtocol>
struct IErrorHandler : public TThrRefBase {
    using TPtr = TIntrusivePtr<IErrorHandler>;

    virtual void AbortSession(TASessionClosedEvent<UseMigrationProtocol>&& closeEvent) = 0;

    void AbortSession(EStatus statusCode, NYql::TIssues&& issues) {
        AbortSession(TASessionClosedEvent<UseMigrationProtocol>(statusCode, std::move(issues)));
    }

    void AbortSession(EStatus statusCode, const TString& message) {
        NYql::TIssues issues;
        issues.AddIssue(message);
        AbortSession(statusCode, std::move(issues));
    }

    void AbortSession(TPlainStatus&& status) {
        AbortSession(TASessionClosedEvent<UseMigrationProtocol>(std::move(status)));
    }
};

template <bool UseMigrationProtocol>
class TUserRetrievedEventsInfoAccumulator {
public:
    void Add(TDataDecompressionInfoPtr<UseMigrationProtocol> info, i64 decompressedSize);
    void OnUserRetrievedEvent() const;

private:
    struct TCounter {
        i64 DecompressedSize = 0;
        size_t MessagesCount = 0;
    };

    TMap<TDataDecompressionInfoPtr<UseMigrationProtocol>, TCounter> Counters;
};

// Special class that stores actions to be done after lock will be released.
template <bool UseMigrationProtocol>
class TDeferredActions {
public:
    ~TDeferredActions() {
        DoActions();
    }

    void DeferReadFromProcessor(const typename IProcessor<UseMigrationProtocol>::TPtr& processor, TServerMessage<UseMigrationProtocol>* dst, typename IProcessor<UseMigrationProtocol>::TReadCallback callback);
    void DeferStartExecutorTask(const typename IAExecutor<UseMigrationProtocol>::TPtr& executor, typename IAExecutor<UseMigrationProtocol>::TFunction task);
    void DeferAbortSession(const typename IErrorHandler<UseMigrationProtocol>::TPtr& errorHandler, TASessionClosedEvent<UseMigrationProtocol>&& closeEvent);
    void DeferAbortSession(const typename IErrorHandler<UseMigrationProtocol>::TPtr& errorHandler, EStatus statusCode, NYql::TIssues&& issues);
    void DeferAbortSession(const typename IErrorHandler<UseMigrationProtocol>::TPtr& errorHandler, EStatus statusCode, const TString& message);
    void DeferAbortSession(const typename IErrorHandler<UseMigrationProtocol>::TPtr& errorHandler, TPlainStatus&& status);
    void DeferReconnection(std::shared_ptr<TSingleClusterReadSessionImpl<UseMigrationProtocol>> session, const typename IErrorHandler<UseMigrationProtocol>::TPtr& errorHandler, TPlainStatus&& status);
    void DeferStartSession(std::shared_ptr<TSingleClusterReadSessionImpl<UseMigrationProtocol>> session);
    void DeferSignalWaiter(TWaiter&& waiter);
    void DeferDestroyDecompressionInfos(std::vector<TDataDecompressionInfoPtr<UseMigrationProtocol>>&& infos);

private:
    void DoActions();

    void Read();
    void StartExecutorTasks();
    void AbortSession();
    void Reconnect();
    void SignalWaiters();
    void StartSessions();

private:
    // Read.
    typename IProcessor<UseMigrationProtocol>::TPtr Processor;
    TServerMessage<UseMigrationProtocol>* ReadDst = nullptr;
    typename IProcessor<UseMigrationProtocol>::TReadCallback ReadCallback;

    // Executor tasks.
    std::vector<std::pair<typename IAExecutor<UseMigrationProtocol>::TPtr, typename IAExecutor<UseMigrationProtocol>::TFunction>> ExecutorsTasks;

    // Abort session.
    typename IErrorHandler<UseMigrationProtocol>::TPtr ErrorHandler;
    TMaybe<TASessionClosedEvent<UseMigrationProtocol>> SessionClosedEvent;

    // Waiters.
    std::vector<TWaiter> Waiters;

    // Reconnection.
    std::shared_ptr<TSingleClusterReadSessionImpl<UseMigrationProtocol>> Session;
    TPlainStatus ReconnectionStatus;

    // Session to start
    std::vector<std::shared_ptr<TSingleClusterReadSessionImpl<UseMigrationProtocol>>> Sessions;

    std::vector<TDataDecompressionInfoPtr<UseMigrationProtocol>> DecompressionInfos;
};

template <bool UseMigrationProtocol>
class TDataDecompressionInfo : public std::enable_shared_from_this<TDataDecompressionInfo<UseMigrationProtocol>> {
public:
    using TPtr = std::shared_ptr<TDataDecompressionInfo<UseMigrationProtocol>>;

    TDataDecompressionInfo(const TDataDecompressionInfo&) = default;
    TDataDecompressionInfo(TDataDecompressionInfo&&) = default;
    TDataDecompressionInfo(
        TPartitionData<UseMigrationProtocol>&& msg,
        std::weak_ptr<TSingleClusterReadSessionImpl<UseMigrationProtocol>> session,
        bool doDecompress,
        i64 serverBytesSize = 0 // to increment read request bytes size
    );
    ~TDataDecompressionInfo();

    i64 StartDecompressionTasks(const typename IAExecutor<UseMigrationProtocol>::TPtr& executor,
                                i64 availableMemory,
                                TDeferredActions<UseMigrationProtocol>& deferred);
    void PlanDecompressionTasks(double averageCompressionRatio,
                                TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> partitionStream);

    bool IsReady() const {
        return SourceDataNotProcessed == 0;
    }

    bool AllDecompressionTasksStarted() const {
        return Tasks.empty();
    }

    i64 GetCompressedDataSize() const {
        return CompressedDataSize;
    }

    const TPartitionData<UseMigrationProtocol>& GetServerMessage() const {
        return ServerMessage;
    }

    TPartitionData<UseMigrationProtocol>& GetServerMessage() {
        return ServerMessage;
    }

    bool GetDoDecompress() const {
        return DoDecompress;
    }

    i64 GetServerBytesSize() const {
        return ServerBytesSize;
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

    typename TAWriteSessionMeta<UseMigrationProtocol>::TPtr GetBatchMeta(size_t batchIndex) const {
        Y_ASSERT(batchIndex < BatchesMeta.size());
        return BatchesMeta[batchIndex];
    }

    bool HasMoreData() const {
        return CurrentReadingMessage.first < static_cast<size_t>(GetServerMessage().batches_size());
    }

    bool HasReadyUnreadData() const;

    void PutDecompressionError(std::exception_ptr error, size_t batch, size_t message);
    std::exception_ptr GetDecompressionError(size_t batch, size_t message);

    void OnDataDecompressed(i64 sourceSize, i64 estimatedDecompressedSize, i64 decompressedSize, size_t messagesCount);
    void OnUserRetrievedEvent(i64 decompressedDataSize, size_t messagesCount);

private:
    // Special struct for marking (batch/message) as ready.
    struct TReadyMessageThreshold {
        size_t Batch = 0; // Last ready batch with message index.
        size_t Message = 0; // Last ready message index.
        std::atomic<bool> Ready = false;
    };

    struct TDecompressionTask {
        TDecompressionTask(TDataDecompressionInfo::TPtr parent, TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> partitionStream, TReadyMessageThreshold* ready);

        // Decompress and notify about memory consumption changes.
        void operator()();

        void Add(size_t batch, size_t message, size_t sourceDataSize, size_t estimatedDecompressedSize);

        size_t AddedDataSize() const {
            return SourceDataSize;
        }
        size_t AddedMessagesCount() const {
            return Messages.size();
        }

        i64 GetEstimatedDecompressedSize() const {
            return EstimatedDecompressedSize;
        }

    private:
        TDataDecompressionInfo::TPtr Parent;
        TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> PartitionStream;
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
    TPartitionData<UseMigrationProtocol> ServerMessage;
    std::vector<typename TAWriteSessionMeta<UseMigrationProtocol>::TPtr> BatchesMeta;
    std::weak_ptr<TSingleClusterReadSessionImpl<UseMigrationProtocol>> Session;
    bool DoDecompress;
    i64 ServerBytesSize = 0;
    std::atomic<i64> SourceDataNotProcessed = 0;
    std::pair<size_t, size_t> CurrentDecompressingMessage = {0, 0}; // (Batch, Message)
    std::deque<TReadyMessageThreshold> ReadyThresholds;
    std::pair<size_t, size_t> CurrentReadingMessage = {0, 0}; // (Batch, Message)

    // Decompression exceptions.
    // Optimization for rare using.
    std::atomic<bool> DecompressionErrorsStructCreated = false;
    TAdaptiveLock DecompressionErrorsStructLock;
    std::vector<std::vector<std::exception_ptr>> DecompressionErrors;

    std::atomic<i64> MessagesInflight = 0;
    std::atomic<i64> CompressedDataSize = 0;
    std::atomic<i64> DecompressedDataSize = 0;

    std::deque<TDecompressionTask> Tasks;
};

template <bool UseMigrationProtocol>
class TDataDecompressionEvent {
public:
    TDataDecompressionEvent(size_t batch, size_t message, TDataDecompressionInfoPtr<UseMigrationProtocol> parent, std::atomic<bool>& ready) :
        Batch{batch},
        Message{message},
        Parent{std::move(parent)},
        Ready{ready}
    {
    }

    bool IsReady() const {
        return Ready;
    }

    void TakeData(TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> partitionStream,
                  TVector<typename TAReadSessionEvent<UseMigrationProtocol>::TDataReceivedEvent::TMessage>& messages,
                  TVector<typename TAReadSessionEvent<UseMigrationProtocol>::TDataReceivedEvent::TCompressedMessage>& compressedMessages,
                  size_t& maxByteSize,
                  size_t& dataSize) const;

    TDataDecompressionInfoPtr<UseMigrationProtocol> GetParent() const {
        return Parent;
    }

private:
    size_t Batch;
    size_t Message;
    TDataDecompressionInfoPtr<UseMigrationProtocol> Parent;
    std::atomic<bool>& Ready;
};

template <bool UseMigrationProtocol>
struct IUserRetrievedEventCallback {
    virtual ~IUserRetrievedEventCallback() = default;

    virtual void OnUserRetrievedEvent(i64 decompressedSize, size_t messagesCount) = 0;
};

template <bool UseMigrationProtocol>
struct TReadSessionEventInfo {
    using TEvent = typename TAReadSessionEvent<UseMigrationProtocol>::TEvent;
    using TDataReceivedEvent = typename TAReadSessionEvent<UseMigrationProtocol>::TDataReceivedEvent;
    using TMessage = typename TAReadSessionEvent<UseMigrationProtocol>::TDataReceivedEvent::TMessage;
    using TCompressedMessage = typename TAReadSessionEvent<UseMigrationProtocol>::TDataReceivedEvent::TCompressedMessage;

    // Event with only partition stream ref.
    // Partition stream holds all its events.
    TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> PartitionStream;
    bool HasDataEvents = false;
    size_t EventsCount = 0;
    TMaybe<TEvent> Event;
    std::weak_ptr<IUserRetrievedEventCallback<UseMigrationProtocol>> Session;

    // Close event.
    TReadSessionEventInfo(const TASessionClosedEvent<UseMigrationProtocol>& event, std::weak_ptr<IUserRetrievedEventCallback<UseMigrationProtocol>> session = {})
        : Event(TEvent(event))
        , Session(session)
    {
    }

    // Usual event.
    TReadSessionEventInfo(TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> partitionStream, std::weak_ptr<IUserRetrievedEventCallback<UseMigrationProtocol>> session, TEvent event);

    // Data event.
    TReadSessionEventInfo(TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> partitionStream,
                          std::weak_ptr<IUserRetrievedEventCallback<UseMigrationProtocol>> session,
                          bool hasDataEvents);

    bool IsEmpty() const;
    bool IsDataEvent() const;

    TEvent& GetEvent() {
        Y_ASSERT(Event);
        return *Event;
    }

    bool IsSessionClosedEvent() const {
        return Event && std::holds_alternative<TASessionClosedEvent<UseMigrationProtocol>>(*Event);
    }
};

// Raw data with maybe uncompressed parts or other read session event.
template <bool UseMigrationProtocol>
struct TRawPartitionStreamEvent {
    using TEvent = typename TAReadSessionEvent<UseMigrationProtocol>::TEvent;

    std::variant<TDataDecompressionEvent<UseMigrationProtocol>, TEvent> Event;

    TRawPartitionStreamEvent(const TRawPartitionStreamEvent&) = default;
    TRawPartitionStreamEvent(TRawPartitionStreamEvent&&) = default;

    TRawPartitionStreamEvent(size_t batch,
                             size_t message,
                             TDataDecompressionInfoPtr<UseMigrationProtocol> parent,
                             std::atomic<bool> &ready)
        : Event(std::in_place_type_t<TDataDecompressionEvent<UseMigrationProtocol>>(),
                batch,
                message,
                std::move(parent),
                ready)
    {
    }

    template <class T>
    explicit TRawPartitionStreamEvent(T&& event)
        : Event(std::in_place_type_t<TEvent>(), std::forward<T>(event))
    {
    }

    bool IsDataEvent() const {
        return std::holds_alternative<TDataDecompressionEvent<UseMigrationProtocol>>(Event);
    }

    const TDataDecompressionEvent<UseMigrationProtocol>& GetDataEvent() const {
        Y_ASSERT(IsDataEvent());
        return std::get<TDataDecompressionEvent<UseMigrationProtocol>>(Event);
    }

    TEvent& GetEvent() {
        Y_ASSERT(!IsDataEvent());
        return std::get<TEvent>(Event);
    }

    const TEvent& GetEvent() const {
        Y_ASSERT(!IsDataEvent());
        return std::get<TEvent>(Event);
    }

    bool IsReady() const {
        if (!IsDataEvent()) {
            return true;
        }

        return std::get<TDataDecompressionEvent<UseMigrationProtocol>>(Event).IsReady();
    }
};

template <bool UseMigrationProtocol>
class TRawPartitionStreamEventQueue {
public:
    TRawPartitionStreamEventQueue() = default;

    template <class... Ts>
    TRawPartitionStreamEvent<UseMigrationProtocol>& emplace_back(Ts&&... event)
    {
        return NotReady.emplace_back(std::forward<Ts>(event)...);
    }

    bool empty() const
    {
        return Ready.empty() && NotReady.empty();
    }

    TRawPartitionStreamEvent<UseMigrationProtocol>& front()
    {
        Y_VERIFY(!empty());

        return (Ready.empty() ? NotReady : Ready).front();
    }

    void pop_front()
    {
        Y_VERIFY(!empty());

        (Ready.empty() ? NotReady : Ready).pop_front();
    }

    void pop_back()
    {
        Y_VERIFY(!empty());

        (NotReady.empty() ? Ready : NotReady).pop_back();
    }

    void SignalReadyEvents(TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> stream,
                           TReadSessionEventsQueue<UseMigrationProtocol>& queue,
                           TDeferredActions<UseMigrationProtocol>& deferred);
    void DeleteNotReadyTail(TDeferredActions<UseMigrationProtocol>& deferred);

    void GetDataEventImpl(TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> partitionStream,
                          size_t& maxEventsCount,
                          size_t& maxByteSize,
                          TVector<typename TAReadSessionEvent<UseMigrationProtocol>::TDataReceivedEvent::TMessage>& messages,
                          TVector<typename TAReadSessionEvent<UseMigrationProtocol>::TDataReceivedEvent::TCompressedMessage>& compressedMessages,
                          TUserRetrievedEventsInfoAccumulator<UseMigrationProtocol>& accumulator);

private:
    static void GetDataEventImpl(TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> partitionStream,
                                 size_t& maxEventsCount,
                                 size_t& maxByteSize,
                                 TVector<typename TAReadSessionEvent<UseMigrationProtocol>::TDataReceivedEvent::TMessage>& messages,
                                 TVector<typename TAReadSessionEvent<UseMigrationProtocol>::TDataReceivedEvent::TCompressedMessage>& compressedMessages,
                                 TUserRetrievedEventsInfoAccumulator<UseMigrationProtocol>& accumulator,
                                 std::deque<TRawPartitionStreamEvent<UseMigrationProtocol>>& queue);

    std::deque<TRawPartitionStreamEvent<UseMigrationProtocol>> Ready;
    std::deque<TRawPartitionStreamEvent<UseMigrationProtocol>> NotReady;
};

template <bool UseMigrationProtocol>
class TPartitionStreamImpl : public TAPartitionStream<UseMigrationProtocol> {
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

    template <bool V = UseMigrationProtocol, class = std::enable_if_t<V>>
    TPartitionStreamImpl(ui64 partitionStreamId,
                         TString topicPath,
                         TString cluster,
                         ui64 partitionGroupId,
                         ui64 partitionId,
                         ui64 assignId,
                         ui64 readOffset,
                         std::weak_ptr<TSingleClusterReadSessionImpl<UseMigrationProtocol>> parentSession,
                         typename IErrorHandler<UseMigrationProtocol>::TPtr errorHandler)
        : Key{topicPath, cluster, partitionId}
        , AssignId(assignId)
        , FirstNotReadOffset(readOffset)
        , Session(std::move(parentSession))
        , ErrorHandler(std::move(errorHandler))
    {
        TAPartitionStream<true>::PartitionStreamId = partitionStreamId;
        TAPartitionStream<true>::TopicPath = std::move(topicPath);
        TAPartitionStream<true>::Cluster = std::move(cluster);
        TAPartitionStream<true>::PartitionGroupId = partitionGroupId;
        TAPartitionStream<true>::PartitionId = partitionId;
        MaxCommittedOffset = readOffset;
    }

    template <bool V = UseMigrationProtocol, class = std::enable_if_t<!V>>
    TPartitionStreamImpl(ui64 partitionStreamId,
                         TString topicPath,
                         i64 partitionId,
                         i64 assignId,
                         i64 readOffset,
                         std::weak_ptr<TSingleClusterReadSessionImpl<UseMigrationProtocol>> parentSession,
                         typename IErrorHandler<UseMigrationProtocol>::TPtr errorHandler)
        : Key{topicPath, "", static_cast<ui64>(partitionId)}
        , AssignId(static_cast<ui64>(assignId))
        , FirstNotReadOffset(static_cast<ui64>(readOffset))
        , Session(std::move(parentSession))
        , ErrorHandler(std::move(errorHandler))
    {
        TAPartitionStream<false>::PartitionSessionId = partitionStreamId;
        TAPartitionStream<false>::TopicPath = std::move(topicPath);
        TAPartitionStream<false>::PartitionId = static_cast<ui64>(partitionId);
        MaxCommittedOffset = static_cast<ui64>(readOffset);
    }

    ~TPartitionStreamImpl() = default;

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

    void InsertDataEvent(size_t batch,
                         size_t message,
                         TDataDecompressionInfoPtr<UseMigrationProtocol> parent,
                         std::atomic<bool> &ready)
    {
        EventsQueue.emplace_back(batch, message, std::move(parent), ready);
    }

    bool HasEvents() const {
        return !EventsQueue.empty();
    }

    TRawPartitionStreamEvent<UseMigrationProtocol>& TopEvent() {
        return EventsQueue.front();
    }

    void PopEvent() {
        EventsQueue.pop_front();
    }

    std::weak_ptr<TSingleClusterReadSessionImpl<UseMigrationProtocol>> GetSession() const {
        return Session;
    }

    TLog GetLog() const;

    static void SignalReadyEvents(TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> stream,
                                  TReadSessionEventsQueue<UseMigrationProtocol>* queue,
                                  TDeferredActions<UseMigrationProtocol>& deferred);

    const typename IErrorHandler<UseMigrationProtocol>::TPtr& GetErrorHandler() const {
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
            auto id = [this](){
                if constexpr (UseMigrationProtocol) {
                    return this->PartitionStreamId;
                } else {
                    return this->PartitionSessionId;
                }
            }();
            ThrowFatalError(TStringBuilder() << "Invalid offset range [" << startOffset << ", " << endOffset << ") : range must start from "
                                             << MaxCommittedOffset << " or has some offsets that are committed already. Partition stream id: -" << id << Endl);
            return false;
        }
        if (rangesMode) { // Otherwise no need to send it to server.
            Y_VERIFY(!Commits.Intersects(startOffset, endOffset));
            Commits.InsertInterval(startOffset, endOffset);
        }
        ClientCommits.InsertInterval(startOffset, endOffset);
        return true;
    }

    void DeleteNotReadyTail(TDeferredActions<UseMigrationProtocol>& deferred);

    static void GetDataEventImpl(TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> partitionStream,
                                 size_t& maxEventsCount,
                                 size_t& maxByteSize,
                                 TVector<typename TAReadSessionEvent<UseMigrationProtocol>::TDataReceivedEvent::TMessage>& messages,
                                 TVector<typename TAReadSessionEvent<UseMigrationProtocol>::TDataReceivedEvent::TCompressedMessage>& compressedMessages,
                                 TUserRetrievedEventsInfoAccumulator<UseMigrationProtocol>& accumulator);

private:
    const TKey Key;
    ui64 AssignId;
    ui64 FirstNotReadOffset;
    std::weak_ptr<TSingleClusterReadSessionImpl<UseMigrationProtocol>> Session;
    typename IErrorHandler<UseMigrationProtocol>::TPtr ErrorHandler;
    TRawPartitionStreamEventQueue<UseMigrationProtocol> EventsQueue;
    ui64 MaxReadOffset = 0;
    ui64 MaxCommittedOffset = 0;

    TDisjointIntervalTree<ui64> Commits;
    TDisjointIntervalTree<ui64> ClientCommits;
};

template <bool UseMigrationProtocol>
class TReadSessionEventsQueue: public TBaseSessionEventsQueue<TAReadSessionSettings<UseMigrationProtocol>,
                                                              typename TAReadSessionEvent<UseMigrationProtocol>::TEvent,
                                                              TASessionClosedEvent<UseMigrationProtocol>,
                                                              IAExecutor<UseMigrationProtocol>,
                                                              TReadSessionEventInfo<UseMigrationProtocol>> {
    using TParent = TBaseSessionEventsQueue<TAReadSessionSettings<UseMigrationProtocol>,
                                            typename TAReadSessionEvent<UseMigrationProtocol>::TEvent,
                                            TASessionClosedEvent<UseMigrationProtocol>,
                                            IAExecutor<UseMigrationProtocol>,
                                            TReadSessionEventInfo<UseMigrationProtocol>>;

public:
    TReadSessionEventsQueue(const TAReadSessionSettings<UseMigrationProtocol>& settings,
                            std::weak_ptr<IUserRetrievedEventCallback<UseMigrationProtocol>> session);

    TReadSessionEventInfo<UseMigrationProtocol>
        GetEventImpl(size_t& maxByteSize,
                     TUserRetrievedEventsInfoAccumulator<UseMigrationProtocol>& accumulator); // Assumes that we're under lock.

    TVector<typename TAReadSessionEvent<UseMigrationProtocol>::TEvent>
        GetEvents(bool block = false,
                  TMaybe<size_t> maxEventsCount = Nothing(),
                  size_t maxByteSize = std::numeric_limits<size_t>::max());

    TMaybe<typename TAReadSessionEvent<UseMigrationProtocol>::TEvent>
        GetEvent(bool block = false,
                 size_t maxByteSize = std::numeric_limits<size_t>::max());

    void Close(const TASessionClosedEvent<UseMigrationProtocol>& event, TDeferredActions<UseMigrationProtocol>& deferred) {
        TWaiter waiter;
        with_lock (TParent::Mutex) {
            TParent::CloseEvent = event;
            TParent::Closed = true;
            waiter = TWaiter(TParent::Waiter.ExtractPromise(), this);
        }

        TReadSessionEventInfo<UseMigrationProtocol> info(event);
        ApplyHandler(info, deferred);

        waiter.Signal();
    }

    bool TryApplyCallbackToEventImpl(typename TParent::TEvent& event,
                                     TDeferredActions<UseMigrationProtocol>& deferred);
    bool HasDataEventCallback() const;
    void ApplyCallbackToEventImpl(TDataReceivedEvent<UseMigrationProtocol>& event,
                                  TUserRetrievedEventsInfoAccumulator<UseMigrationProtocol>&& eventsInfo,
                                  TDeferredActions<UseMigrationProtocol>& deferred);

    void GetDataEventCallbackSettings(size_t& maxMessagesBytes);

    // Push usual event.
    void PushEvent(TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> partitionStream,
                   std::weak_ptr<IUserRetrievedEventCallback<UseMigrationProtocol>> session,
                   typename TAReadSessionEvent<UseMigrationProtocol>::TEvent event,
                   TDeferredActions<UseMigrationProtocol>& deferred);

    // Push data event.
    void PushDataEvent(TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> stream,
                       size_t batch,
                       size_t message,
                       TDataDecompressionInfoPtr<UseMigrationProtocol> parent,
                       std::atomic<bool> &ready);

    void SignalEventImpl(TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> partitionStream,
                         TDeferredActions<UseMigrationProtocol>& deferred,
                         bool isDataEvent); // Assumes that we're under lock.

    void SignalReadyEvents(TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> partitionStream);

    void SignalReadyEventsImpl(TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> partitionStream,
                               TDeferredActions<UseMigrationProtocol>& deferred); // Assumes that we're under lock.

    void SignalWaiterImpl(TDeferredActions<UseMigrationProtocol>& deferred) {
        TWaiter waiter = TParent::PopWaiterImpl();
        deferred.DeferSignalWaiter(std::move(waiter)); // No effect if waiter is empty.
    }

    void ClearAllEvents();

private:
    struct THandlersVisitor : public TParent::TBaseHandlersVisitor {
        THandlersVisitor(const TAReadSessionSettings<UseMigrationProtocol>& settings, typename TParent::TEvent& event, TDeferredActions<UseMigrationProtocol>& deferred)
            : TParent::TBaseHandlersVisitor(settings, event)
            , Deferred(deferred)
        {}

#define DECLARE_HANDLER(type, handler, answer)                      \
        bool operator()(type&) {                                    \
            if (this->template PushHandler<type>(                   \
                std::move(TParent::TBaseHandlersVisitor::Event),    \
                this->Settings.EventHandlers_.handler,              \
                this->Settings.EventHandlers_.CommonHandler_)) {    \
                return answer;                                      \
            }                                                       \
            return false;                                           \
        }                                                           \
        /**/

        DECLARE_HANDLER(typename TAReadSessionEvent<true>::TDataReceivedEvent, DataReceivedHandler_, true);
        DECLARE_HANDLER(typename TAReadSessionEvent<true>::TCommitAcknowledgementEvent, CommitAcknowledgementHandler_, true);
        DECLARE_HANDLER(typename TAReadSessionEvent<true>::TCreatePartitionStreamEvent, CreatePartitionStreamHandler_, true);
        DECLARE_HANDLER(typename TAReadSessionEvent<true>::TDestroyPartitionStreamEvent, DestroyPartitionStreamHandler_, true);
        DECLARE_HANDLER(typename TAReadSessionEvent<true>::TPartitionStreamStatusEvent, PartitionStreamStatusHandler_, true);
        DECLARE_HANDLER(typename TAReadSessionEvent<true>::TPartitionStreamClosedEvent, PartitionStreamClosedHandler_, true);

        DECLARE_HANDLER(typename TAReadSessionEvent<false>::TDataReceivedEvent, DataReceivedHandler_, true);
        DECLARE_HANDLER(typename TAReadSessionEvent<false>::TCommitOffsetAcknowledgementEvent, CommitOffsetAcknowledgementHandler_, true);
        DECLARE_HANDLER(typename TAReadSessionEvent<false>::TStartPartitionSessionEvent, StartPartitionSessionHandler_, true);
        DECLARE_HANDLER(typename TAReadSessionEvent<false>::TStopPartitionSessionEvent, StopPartitionSessionHandler_, true);
        DECLARE_HANDLER(typename TAReadSessionEvent<false>::TPartitionSessionStatusEvent, PartitionSessionStatusHandler_, true);
        DECLARE_HANDLER(typename TAReadSessionEvent<false>::TPartitionSessionClosedEvent, PartitionSessionClosedHandler_, true);

        DECLARE_HANDLER(TASessionClosedEvent<UseMigrationProtocol>, SessionClosedHandler_, false); // Not applied

#undef DECLARE_HANDLER

        bool Visit() {
            return std::visit(*this, TParent::TBaseHandlersVisitor::Event);
        }

        void Post(const typename IAExecutor<UseMigrationProtocol>::TPtr& executor, typename IAExecutor<UseMigrationProtocol>::TFunction&& f) override {
            Deferred.DeferStartExecutorTask(executor, std::move(f));
        }

        TDeferredActions<UseMigrationProtocol>& Deferred;
    };

    typename TAReadSessionEvent<UseMigrationProtocol>::TDataReceivedEvent
        GetDataEventImpl(TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> stream,
                         size_t& maxByteSize,
                         TUserRetrievedEventsInfoAccumulator<UseMigrationProtocol>& accumulator); // Assumes that we're under lock.

    bool ApplyHandler(TReadSessionEventInfo<UseMigrationProtocol>& eventInfo, TDeferredActions<UseMigrationProtocol>& deferred) {
        THandlersVisitor visitor(this->Settings, eventInfo.GetEvent(), deferred);
        return visitor.Visit();
    }

    bool HasEventCallbacks;
    std::weak_ptr<IUserRetrievedEventCallback<UseMigrationProtocol>> Session;
};

} // namespace NYdb::NPersQueue

template <>
struct THash<NYdb::NPersQueue::TPartitionStreamImpl<false>::TKey> {
    size_t operator()(const NYdb::NPersQueue::TPartitionStreamImpl<false>::TKey& key) const {
        THash<TString> strHash;
        const size_t h1 = strHash(key.Topic);
        const size_t h2 = NumericHash(key.Partition);
        return CombineHashes(h1, h2);
    }
};

template <>
struct THash<NYdb::NPersQueue::TPartitionStreamImpl<true>::TKey> {
    size_t operator()(const NYdb::NPersQueue::TPartitionStreamImpl<true>::TKey& key) const {
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
template <bool UseMigrationProtocol>
class TSingleClusterReadSessionImpl : public std::enable_shared_from_this<TSingleClusterReadSessionImpl<UseMigrationProtocol>>,
                                      public IUserRetrievedEventCallback<UseMigrationProtocol> {
public:
    using TPtr = std::shared_ptr<TSingleClusterReadSessionImpl<UseMigrationProtocol>>;
    using IProcessor = typename IReadSessionConnectionProcessorFactory<UseMigrationProtocol>::IProcessor;


    friend class TPartitionStreamImpl<UseMigrationProtocol>;

    TSingleClusterReadSessionImpl(
        const TAReadSessionSettings<UseMigrationProtocol>& settings,
        const TString& database,
        const TString& sessionId,
        const TString& clusterName,
        const TLog& log,
        std::shared_ptr<IReadSessionConnectionProcessorFactory<UseMigrationProtocol>> connectionFactory,
        std::shared_ptr<TReadSessionEventsQueue<UseMigrationProtocol>> eventsQueue,
        typename IErrorHandler<UseMigrationProtocol>::TPtr errorHandler,
        NGrpc::IQueueClientContextPtr clientContext,
        ui64 partitionStreamIdStart, ui64 partitionStreamIdStep
    )
        : Settings(settings)
        , Database(database)
        , SessionId(sessionId)
        , ClusterName(clusterName)
        , Log(log)
        , NextPartitionStreamId(partitionStreamIdStart)
        , PartitionStreamIdStep(partitionStreamIdStep)
        , ConnectionFactory(std::move(connectionFactory))
        , EventsQueue(std::move(eventsQueue))
        , ErrorHandler(std::move(errorHandler))
        , ClientContext(std::move(clientContext))
        , CookieMapping(ErrorHandler)
        , ReadSizeBudget(GetCompressedDataSizeLimit())
        , ReadSizeServerDelta(0)
    {
    }

    void Start();
    void ConfirmPartitionStreamCreate(const TPartitionStreamImpl<UseMigrationProtocol>* partitionStream, TMaybe<ui64> readOffset, TMaybe<ui64> commitOffset);
    void ConfirmPartitionStreamDestroy(TPartitionStreamImpl<UseMigrationProtocol>* partitionStream);
    void RequestPartitionStreamStatus(const TPartitionStreamImpl<UseMigrationProtocol>* partitionStream);
    void Commit(const TPartitionStreamImpl<UseMigrationProtocol>* partitionStream, ui64 startOffset, ui64 endOffset);

    void OnCreateNewDecompressionTask();
    void OnDecompressionInfoDestroy(i64 compressedSize, i64 decompressedSize, i64 messagesCount, i64 serverBytesSize);

    void OnDataDecompressed(i64 sourceSize, i64 estimatedDecompressedSize, i64 decompressedSize, size_t messagesCount, i64 serverBytesSize = 0);

    TReadSessionEventsQueue<UseMigrationProtocol>* GetEventsQueue() {
        return EventsQueue.get();
    }

    void OnUserRetrievedEvent(i64 decompressedSize, size_t messagesCount) override;

    void Abort();
    void Close(std::function<void()> callback);

    bool Reconnect(const TPlainStatus& status);

    void StopReadingData();
    void ResumeReadingData();

    void DumpStatisticsToLog(TLogElement& log);
    void UpdateMemoryUsageStatistics();

    TStringBuilder GetLogPrefix() const;

    const TLog& GetLog() const {
        return Log;
    }

private:
    void BreakConnectionAndReconnectImpl(TPlainStatus&& status, TDeferredActions<UseMigrationProtocol>& deferred);

    void BreakConnectionAndReconnectImpl(EStatus statusCode, NYql::TIssues&& issues, TDeferredActions<UseMigrationProtocol>& deferred) {
        BreakConnectionAndReconnectImpl(TPlainStatus(statusCode, std::move(issues)), deferred);
    }

    void BreakConnectionAndReconnectImpl(EStatus statusCode, const TString& message, TDeferredActions<UseMigrationProtocol>& deferred) {
        BreakConnectionAndReconnectImpl(TPlainStatus(statusCode, message), deferred);
    }

    bool HasCommitsInflightImpl() const;

    void OnConnectTimeout(const NGrpc::IQueueClientContextPtr& connectTimeoutContext);
    void OnConnect(TPlainStatus&&, typename IProcessor::TPtr&&, const NGrpc::IQueueClientContextPtr& connectContext);
    void DestroyAllPartitionStreamsImpl(TDeferredActions<UseMigrationProtocol>& deferred); // Destroy all streams before setting new connection // Assumes that we're under lock.

    // Initing.
    inline void InitImpl(TDeferredActions<UseMigrationProtocol>& deferred); // Assumes that we're under lock.

    // Working logic.
    void ContinueReadingDataImpl(); // Assumes that we're under lock.
    bool IsActualPartitionStreamImpl(const TPartitionStreamImpl<UseMigrationProtocol>* partitionStream); // Assumes that we're under lock.

    // Read/Write.
    void ReadFromProcessorImpl(TDeferredActions<UseMigrationProtocol>& deferred); // Assumes that we're under lock.
    void WriteToProcessorImpl(TClientMessage<UseMigrationProtocol>&& req); // Assumes that we're under lock.
    void OnReadDone(NGrpc::TGrpcStatus&& grpcStatus, size_t connectionGeneration);

    // Assumes that we're under lock.
    template<typename TMessage>
    inline void OnReadDoneImpl(TMessage&& msg, TDeferredActions<UseMigrationProtocol>& deferred);

    void StartDecompressionTasksImpl(TDeferredActions<UseMigrationProtocol>& deferred); // Assumes that we're under lock.

    i64 GetCompressedDataSizeLimit() const {
        const double overallLimit = static_cast<double>(Settings.MaxMemoryUsageBytes_);
        // CompressedDataSize + CompressedDataSize * AverageCompressionRatio <= Settings.MaxMemoryUsageBytes_
        return Max<i64>(1l, static_cast<i64>(overallLimit / (1.0 + AverageCompressionRatio)));
    }

    i64 GetDecompressedDataSizeLimit() const {
        return Max<i64>(1l, static_cast<i64>(Settings.MaxMemoryUsageBytes_) - GetCompressedDataSizeLimit());
    }

    bool GetRangesMode() const;

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

            explicit TCookie(ui64 cookie, TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> partitionStream)
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
            TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> PartitionStream;
            std::pair<ui64, ui64> OffsetRange;
            size_t UncommittedMessagesLeft = 0;
        };

        explicit TPartitionCookieMapping(typename IErrorHandler<UseMigrationProtocol>::TPtr errorHandler)
            : ErrorHandler(std::move(errorHandler))
        {
        }

        bool AddMapping(const typename TCookie::TPtr& cookie);

        // Removes (partition stream, offset) from mapping.
        // Returns cookie ptr if this was the last message, otherwise nullptr.
        typename TSingleClusterReadSessionImpl<UseMigrationProtocol>::TPartitionCookieMapping::TCookie::TPtr CommitOffset(ui64 partitionStreamId, ui64 offset);

        // Gets and then removes committed cookie from mapping.
        typename TSingleClusterReadSessionImpl<UseMigrationProtocol>::TPartitionCookieMapping::TCookie::TPtr RetrieveCommittedCookie(const Ydb::PersQueue::V1::CommitCookie& cookieProto);

        // Removes mapping on partition stream.
        void RemoveMapping(ui64 partitionStreamId);

        // Clear all mapping before reconnect.
        void ClearMapping();

        bool HasUnacknowledgedCookies() const;

    private:
        THashMap<typename TCookie::TKey, typename TCookie::TPtr, typename TCookie::TKey::THash> Cookies;
        THashMap<std::pair<ui64, ui64>, typename TCookie::TPtr> UncommittedOffsetToCookie; // (Partition stream id, Offset) -> Cookie.
        THashMultiMap<ui64, typename TCookie::TPtr> PartitionStreamIdToCookie;
        typename IErrorHandler<UseMigrationProtocol>::TPtr ErrorHandler;
        size_t CommitInflight = 0; // Commit inflight to server.
    };

    struct TDecompressionQueueItem {
        TDecompressionQueueItem(TDataDecompressionInfoPtr<UseMigrationProtocol> batchInfo,
                                TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> partitionStream)
            : BatchInfo(std::move(batchInfo))
            , PartitionStream(std::move(partitionStream))
        {
        }

        TDataDecompressionInfoPtr<UseMigrationProtocol> BatchInfo;
        TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>> PartitionStream;
    };

private:
    const TAReadSessionSettings<UseMigrationProtocol> Settings;
    const TString Database;
    const TString SessionId;
    const TString ClusterName;
    TLog Log;
    ui64 NextPartitionStreamId;
    ui64 PartitionStreamIdStep;
    std::shared_ptr<IReadSessionConnectionProcessorFactory<UseMigrationProtocol>> ConnectionFactory;
    std::shared_ptr<TReadSessionEventsQueue<UseMigrationProtocol>> EventsQueue;
    typename IErrorHandler<UseMigrationProtocol>::TPtr ErrorHandler;
    NGrpc::IQueueClientContextPtr ClientContext; // Common client context.
    NGrpc::IQueueClientContextPtr ConnectContext;
    NGrpc::IQueueClientContextPtr ConnectTimeoutContext;
    NGrpc::IQueueClientContextPtr ConnectDelayContext;
    size_t ConnectionGeneration = 0;
    TAdaptiveLock Lock;
    typename IProcessor::TPtr Processor;
    typename IARetryPolicy<UseMigrationProtocol>::IRetryState::TPtr RetryState; // Current retry state (if now we are (re)connecting).
    size_t ConnectionAttemptsDone = 0;

    // Memory usage.
    i64 CompressedDataSize = 0;
    i64 DecompressedDataSize = 0;
    double AverageCompressionRatio = 1.0; // Weighted average for compression memory usage estimate.
    TInstant UsageStatisticsLastUpdateTime = TInstant::Now();

    bool WaitingReadResponse = false;
    std::shared_ptr<TServerMessage<UseMigrationProtocol>> ServerMessage; // Server message to write server response to.
    THashMap<ui64, TIntrusivePtr<TPartitionStreamImpl<UseMigrationProtocol>>> PartitionStreams; // assignId -> Partition stream.
    TPartitionCookieMapping CookieMapping;
    std::deque<TDecompressionQueueItem> DecompressionQueue;
    bool DataReadingSuspended = false;

    // Exiting.
    bool Aborting = false;
    bool Closing = false;
    std::function<void()> CloseCallback;
    std::atomic<int> DecompressionTasksInflight = 0;
    i64 ReadSizeBudget;
    i64 ReadSizeServerDelta = 0;
};

// High level class that manages several read session impls.
// Each one of them works with single cluster.
// This class communicates with cluster discovery service and then creates
// sessions to each cluster.
class TReadSession : public IReadSession,
                     public IUserRetrievedEventCallback<true>,
                     public std::enable_shared_from_this<TReadSession> {
    struct TClusterSessionInfo {
        TClusterSessionInfo(const TString& cluster)
            : ClusterName(cluster)
        {
        }

        TString ClusterName; // In lower case
        TSingleClusterReadSessionImpl<true>::TPtr Session;
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

    void ClearAllEvents();

private:
    TStringBuilder GetLogPrefix() const;

    // Start
    bool ValidateSettings();

    // Cluster discovery
    Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest MakeClusterDiscoveryRequest() const;
    void StartClusterDiscovery();
    void OnClusterDiscovery(const TStatus& status, const Ydb::PersQueue::ClusterDiscovery::DiscoverClustersResult& result);
    void ProceedWithoutClusterDiscovery();
    void RestartClusterDiscoveryImpl(TDuration delay, TDeferredActions<true>& deferred);
    void CreateClusterSessionsImpl(TDeferredActions<true>& deferred);


    // Shutdown.
    void Abort(EStatus statusCode, NYql::TIssues&& issues);
    void Abort(EStatus statusCode, const TString& message);

    void AbortImpl(TSessionClosedEvent&& closeEvent, TDeferredActions<true>& deferred);
    void AbortImpl(EStatus statusCode, NYql::TIssues&& issues, TDeferredActions<true>& deferred);
    void AbortImpl(EStatus statusCode, const TString& message, TDeferredActions<true>& deferred);

    void OnUserRetrievedEvent(i64 decompressedSize, size_t messagesCount) override;

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
    typename IErrorHandler<true>::TPtr ErrorHandler;
    TDbDriverStatePtr DbDriverState;
    TAdaptiveLock Lock;
    std::shared_ptr<TReadSessionEventsQueue<true>> EventsQueue;
    THashMap<TString, TClusterSessionInfo> ClusterSessions; // Cluster name (in lower case) -> TClusterSessionInfo
    NGrpc::IQueueClientContextPtr ClusterDiscoveryDelayContext;
    IRetryPolicy::IRetryState::TPtr ClusterDiscoveryRetryState;
    bool DataReadingSuspended = false;

    NGrpc::IQueueClientContextPtr DumpCountersContext;

    // Exiting.
    bool Aborting = false;
    bool Closing = false;
};

} // namespace NYdb::NPersQueue

/////////////////////////////////////////
// Templates implementation
#define READ_SESSION_IMPL
#include "read_session.ipp"
#undef READ_SESSION_IMPL
