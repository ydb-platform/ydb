#pragma once

#include "topic_impl.h"

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/common.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/impl_tracker.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <util/generic/buffer.h>


namespace NYdb::NTopic {

inline const TString& GetCodecId(const ECodec codec) {
    static THashMap<ECodec, TString> idByCodec{
        {ECodec::RAW, TString(1, '\0')},
        {ECodec::GZIP, "\1"},
        {ECodec::LZOP, "\2"},
        {ECodec::ZSTD, "\3"}
    };
    Y_VERIFY(idByCodec.contains(codec));
    return idByCodec[codec];
}

class TWriteSessionEventsQueue: public NPersQueue::TBaseSessionEventsQueue<TWriteSessionSettings, TWriteSessionEvent::TEvent, TSessionClosedEvent, IExecutor> {
    using TParent = TBaseSessionEventsQueue<TWriteSessionSettings, TWriteSessionEvent::TEvent, TSessionClosedEvent, IExecutor>;

public:
    TWriteSessionEventsQueue(const TWriteSessionSettings& settings,
                             std::shared_ptr<NPersQueue::TImplTracker> tracker = std::make_shared<NPersQueue::TImplTracker>())
    : TParent(settings)
    , Tracker(std::move(tracker))
    {}

    void PushEvent(TEventInfo eventInfo) {
        if (Closed || ApplyHandler(eventInfo)) {
            return;
        }

        NPersQueue::TWaiter waiter;
        with_lock (Mutex) {
            Events.emplace(std::move(eventInfo));
            waiter = PopWaiterImpl();
        }
        waiter.Signal(); // Does nothing if waiter is empty.
    }

    TMaybe<TEvent> GetEvent(bool block = false) {
        TMaybe<TEventInfo> eventInfo;
        with_lock (Mutex) {
            if (block) {
                WaitEventsImpl();
            }
            if (HasEventsImpl()) {
                eventInfo = GetEventImpl();
            } else {
                return Nothing();
            }
        }
        eventInfo->OnUserRetrievedEvent();
        return std::move(eventInfo->Event);
    }

    TVector<TEvent> GetEvents(bool block = false, TMaybe<size_t> maxEventsCount = Nothing()) {
        TVector<TEventInfo> eventInfos;
        with_lock (Mutex) {
            if (block) {
                WaitEventsImpl();
            }
            eventInfos.reserve(Min(Events.size() + CloseEvent.Defined(), maxEventsCount ? *maxEventsCount : std::numeric_limits<size_t>::max()));
            while (!Events.empty()) {
                eventInfos.emplace_back(GetEventImpl());
                if (maxEventsCount && eventInfos.size() >= *maxEventsCount) {
                    break;
                }
            }
            if (CloseEvent && Events.empty() && (!maxEventsCount || eventInfos.size() < *maxEventsCount)) {
                eventInfos.push_back({*CloseEvent});
            }
        }

        TVector<TEvent> result;
        result.reserve(eventInfos.size());
        for (TEventInfo& eventInfo : eventInfos) {
            eventInfo.OnUserRetrievedEvent();
            result.emplace_back(std::move(eventInfo.Event));
        }
        return result;
    }

    void Close(const TSessionClosedEvent& event) {
        NPersQueue::TWaiter waiter;
        with_lock (Mutex) {
            CloseEvent = event;
            Closed = true;
            waiter = NPersQueue::TWaiter(Waiter.ExtractPromise(), this);
        }

        TEventInfo info(event);
        ApplyHandler(info);

        waiter.Signal();
    }

private:
    struct THandlersVisitor : public TParent::TBaseHandlersVisitor {
        using TParent::TBaseHandlersVisitor::TBaseHandlersVisitor;
#define DECLARE_HANDLER(type, handler, answer)          \
        bool operator()(type& event) {                  \
            if (Settings.EventHandlers_.handler) {      \
                Settings.EventHandlers_.handler(event); \
                return answer;                          \
            }                                           \
            return false;                               \
        }                                               \
        /**/
        DECLARE_HANDLER(TWriteSessionEvent::TAcksEvent, AcksHandler_, true);
        DECLARE_HANDLER(TWriteSessionEvent::TReadyToAcceptEvent, ReadyToAcceptHander_, true);
        DECLARE_HANDLER(TSessionClosedEvent, SessionClosedHandler_, false); // Not applied

#undef DECLARE_HANDLER
        bool Visit() {
            return std::visit(*this, Event);
        }

    };

    bool ApplyHandler(TEventInfo& eventInfo) {
        THandlersVisitor visitor(Settings, eventInfo.Event, Tracker);
        return visitor.Visit();
    }

    TEventInfo GetEventImpl() { // Assumes that we're under lock and that the event queue has events.
        Y_ASSERT(HasEventsImpl());
        if (!Events.empty()) {
            TEventInfo event = std::move(Events.front());
            Events.pop();
            RenewWaiterImpl();
            return event;
        }
        Y_ASSERT(CloseEvent);
        return {*CloseEvent};
    }

private:
    std::shared_ptr<NPersQueue::TImplTracker> Tracker;
};

struct TMemoryUsageChange {
    bool WasOk; //!< MemoryUsage <= Config.MaxMemoryUsage_ before update
    bool NowOk; //!< Same, only after update
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TWriteSessionImpl

class TWriteSessionImpl : public IWriteSession,
                          public std::enable_shared_from_this<TWriteSessionImpl> {
private:
    friend class TWriteSession;
    friend class TSimpleBlockingWriteSession;

private:
    using TClientMessage = Ydb::Topic::StreamWriteMessage::FromClient;
    using TServerMessage = Ydb::Topic::StreamWriteMessage::FromServer;
    using IWriteSessionConnectionProcessorFactory =
            TTopicClient::TImpl::IWriteSessionConnectionProcessorFactory;
    using IProcessor = IWriteSessionConnectionProcessorFactory::IProcessor;

    struct TMessage {
        ui64 SeqNo;
        TInstant CreatedAt;
        TStringBuf DataRef;
        TMaybe<ECodec> Codec;
        ui32 OriginalSize; // only for coded messages
        TVector<std::pair<TString, TString>> MessageMeta;
        TMessage(ui64 seqNo, const TInstant& createdAt, TStringBuf data, TMaybe<ECodec> codec = {},
                 ui32 originalSize = 0, const TVector<std::pair<TString, TString>>& messageMeta = {})
            : SeqNo(seqNo)
            , CreatedAt(createdAt)
            , DataRef(data)
            , Codec(codec)
            , OriginalSize(originalSize)
            , MessageMeta(messageMeta)
        {}
    };

    struct TMessageBatch {
        TBuffer Data;
        TVector<TMessage> Messages;
        ui64 CurrentSize = 0;
        TInstant StartedAt = TInstant::Zero();
        bool Acquired = false;
        bool FlushRequested = false;
        void Add(ui64 seqNo, const TInstant& createdAt, TStringBuf data, TMaybe<ECodec> codec, ui32 originalSize,
                 const TVector<std::pair<TString, TString>>& messageMeta) {
            if (StartedAt == TInstant::Zero())
                StartedAt = TInstant::Now();
            CurrentSize += codec ? originalSize : data.size();
            Messages.emplace_back(seqNo, createdAt, data, codec, originalSize, messageMeta);
            Acquired = false;
        }

        bool HasCodec() const {
            return Messages.empty() ? false : Messages.front().Codec.Defined();
        }

        bool Acquire() {
            if (Acquired || Messages.empty())
                return false;
            auto currSize = Data.size();
            Data.Append(Messages.back().DataRef.data(), Messages.back().DataRef.size());
            Messages.back().DataRef = TStringBuf(Data.data() + currSize, Data.size() - currSize);
            Acquired = true;
            return true;
        }

        bool Empty() const noexcept {
            return CurrentSize == 0 && Messages.empty();
        }

        void Reset() {
            StartedAt = TInstant::Zero();
            Messages.clear();
            Data.Clear();
            Acquired = false;
            CurrentSize = 0;
            FlushRequested = false;
        }
    };

    struct TBlock {
        size_t Offset = 0; //!< First message sequence number in the block
        size_t MessageCount = 0;
        size_t PartNumber = 0;
        size_t OriginalSize = 0;
        size_t OriginalMemoryUsage = 0;
        ui32 CodecID = static_cast<ui32>(ECodec::RAW);
        mutable TVector<TStringBuf> OriginalDataRefs;
        mutable TBuffer Data;
        bool Compressed = false;
        mutable bool Valid = true;

        TBlock& operator=(TBlock&&) = default;
        TBlock(TBlock&&) = default;
        TBlock() = default;

        //For taking ownership by copying from const object, f.e. lambda -> std::function, priority_queue
        void Move(const TBlock& rhs) {
            Offset = rhs.Offset;
            MessageCount = rhs.MessageCount;
            PartNumber = rhs.PartNumber;
            OriginalSize = rhs.OriginalSize;
            OriginalMemoryUsage = rhs.OriginalMemoryUsage;
            CodecID = rhs.CodecID;
            OriginalDataRefs.swap(rhs.OriginalDataRefs);
            Data.Swap(rhs.Data);
            Compressed = rhs.Compressed;

            rhs.Data.Clear();
            rhs.OriginalDataRefs.clear();
        }
    };

    struct TOriginalMessage {
        ui64 SeqNo;
        TInstant CreatedAt;
        size_t Size;
        TVector<std::pair<TString, TString>> MessageMeta;
        TOriginalMessage(const ui64 sequenceNumber, const TInstant createdAt, const size_t size)
            : SeqNo(sequenceNumber)
            , CreatedAt(createdAt)
            , Size(size)
        {}
        TOriginalMessage(const ui64 sequenceNumber, const TInstant createdAt, const size_t size,
                         TVector<std::pair<TString, TString>>&& messageMeta)
            : SeqNo(sequenceNumber)
            , CreatedAt(createdAt)
            , Size(size)
            , MessageMeta(std::move(messageMeta))
        {}
    };

    //! Block comparer, makes block with smallest offset (first sequence number) appear on top of the PackedMessagesToSend priority queue
    struct Greater {
        bool operator() (const TBlock& lhs, const TBlock& rhs) {
            return lhs.Offset > rhs.Offset;
        }
    };

    struct THandleResult {
        bool DoRestart = false;
        TDuration StartDelay = TDuration::Zero();
        bool DoStop = false;
        bool DoSetSeqNo = false;
    };
    struct TProcessSrvMessageResult {
        THandleResult HandleResult;
        TMaybe<ui64> InitSeqNo;
        TVector<TWriteSessionEvent::TEvent> Events;
        bool Ok = true;
    };

    struct TPartitionLocation {
        TEndpointKey Endpoint;
        i64 Generation;
    };

    THandleResult OnErrorImpl(NYdb::TPlainStatus&& status); // true - should Start(), false - should Close(), empty - no action

public:
    TWriteSessionImpl(const TWriteSessionSettings& settings,
            std::shared_ptr<TTopicClient::TImpl> client,
            std::shared_ptr<TGRpcConnectionsImpl> connections,
            TDbDriverStatePtr dbDriverState,
            std::shared_ptr<NPersQueue::TImplTracker> tracker);

    TMaybe<TWriteSessionEvent::TEvent> GetEvent(bool block = false) override;
    TVector<TWriteSessionEvent::TEvent> GetEvents(bool block = false,
                                                  TMaybe<size_t> maxEventsCount = Nothing()) override;
    NThreading::TFuture<ui64> GetInitSeqNo() override;

    void Write(TContinuationToken&& continuationToken, TWriteMessage&& message) override;

    void Write(TContinuationToken&&, TStringBuf, TMaybe<ui64> seqNo = Nothing(),
               TMaybe<TInstant> createTimestamp = Nothing()) override {
        Y_UNUSED(seqNo);
        Y_UNUSED(createTimestamp);
        Y_FAIL("Do not use this method");
    };

    void WriteEncoded(TContinuationToken&&, TStringBuf, ECodec, ui32,
                      TMaybe<ui64> seqNo = Nothing(), TMaybe<TInstant> createTimestamp = Nothing()) override {
        Y_UNUSED(seqNo);
        Y_UNUSED(createTimestamp);
        Y_FAIL("Do not use this method");
    }


    NThreading::TFuture<void> WaitEvent() override;

    // Empty maybe - block till all work is done. Otherwise block at most at closeTimeout duration.
    bool Close(TDuration closeTimeout = TDuration::Max()) override;

    TWriterCounters::TPtr GetCounters() override {Y_FAIL("Unimplemented"); } //ToDo - unimplemented;

    ~TWriteSessionImpl(); // will not call close - destroy everything without acks

private:

    TStringBuilder LogPrefix() const;

    void UpdateTokenIfNeededImpl();

    void WriteInternal(TContinuationToken&& continuationToken, TWriteMessage&& message);

    void FlushWriteIfRequiredImpl();
    size_t WriteBatchImpl();
    void Start(const TDuration& delay);
    void InitWriter();

    void OnConnect(TPlainStatus&& st, typename IProcessor::TPtr&& processor,
                const NGrpc::IQueueClientContextPtr& connectContext);
    void OnConnectTimeout(const NGrpc::IQueueClientContextPtr& connectTimeoutContext);
    void ResetForRetryImpl();
    THandleResult RestartImpl(const TPlainStatus& status);
    void Connect(const TDuration& delay);
    void InitImpl();
    void ReadFromProcessor(); // Assumes that we're under lock.
    void WriteToProcessorImpl(TClientMessage&& req); // Assumes that we're under lock.
    void OnReadDone(NGrpc::TGrpcStatus&& grpcStatus, size_t connectionGeneration);
    void OnWriteDone(NGrpc::TGrpcStatus&& status, size_t connectionGeneration);
    TProcessSrvMessageResult ProcessServerMessageImpl();
    TMemoryUsageChange OnMemoryUsageChangedImpl(i64 diff);
    void CompressImpl(TBlock&& block);
    void OnCompressed(TBlock&& block, bool isSyncCompression=false);
    TMemoryUsageChange OnCompressedImpl(TBlock&& block);

    //TString GetDebugIdentity() const;
    TClientMessage GetInitClientMessage();
    bool CleanupOnAcknowledged(ui64 sequenceNumber);
    bool IsReadyToSendNextImpl() const;
    ui64 GetNextSeqNoImpl(const TMaybe<ui64>& seqNo);
    void SendImpl();
    void AbortImpl();
    void CloseImpl(EStatus statusCode, NYql::TIssues&& issues);
    void CloseImpl(EStatus statusCode, const TString& message);
    void CloseImpl(TPlainStatus&& status);

    void OnErrorResolved() {
        RetryState = nullptr;
    }
    void CheckHandleResultImpl(THandleResult& result);
    void ProcessHandleResult(THandleResult& result);
    void HandleWakeUpImpl();
    void UpdateTimedCountersImpl();

    void ConnectToPreferredPartitionLocation(const TDuration& delay);
    void OnDescribePartition(const TStatus& status, const Ydb::Topic::DescribePartitionResult& proto, const NGrpc::IQueueClientContextPtr& describePartitionContext);

    TMaybe<TEndpointKey> GetPreferredEndpointImpl(ui32 partitionId, ui64 partitionNodeId);

private:
    TWriteSessionSettings Settings;
    std::shared_ptr<TTopicClient::TImpl> Client;
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    TString TargetCluster;
    TString InitialCluster;
    TString CurrentCluster;
    bool OnSeqNoShift = false;
    TString PreferredClusterByCDS;
    std::shared_ptr<IWriteSessionConnectionProcessorFactory> ConnectionFactory;
    TDbDriverStatePtr DbDriverState;
    TStringType PrevToken;
    bool UpdateTokenInProgress = false;
    TInstant LastTokenUpdate = TInstant::Zero();
    std::shared_ptr<NPersQueue::TImplTracker> Tracker;
    std::shared_ptr<TWriteSessionEventsQueue> EventsQueue;
    NGrpc::IQueueClientContextPtr ClientContext; // Common client context.
    NGrpc::IQueueClientContextPtr ConnectContext;
    NGrpc::IQueueClientContextPtr ConnectTimeoutContext;
    NGrpc::IQueueClientContextPtr ConnectDelayContext;
    NGrpc::IQueueClientContextPtr DescribePartitionContext;
    size_t ConnectionGeneration = 0;
    size_t ConnectionAttemptsDone = 0;
    TAdaptiveLock Lock;
    IProcessor::TPtr Processor;
    IRetryPolicy::IRetryState::TPtr RetryState; // Current retry state (if now we are (re)connecting).
    std::shared_ptr<TServerMessage> ServerMessage; // Server message to write server response to.

    TString SessionId;
    IExecutor::TPtr Executor;
    IExecutor::TPtr CompressionExecutor;
    size_t MemoryUsage = 0; //!< Estimated amount of memory used
    bool FirstTokenSent = false;

    TMessageBatch CurrentBatch;

    std::queue<TOriginalMessage> OriginalMessagesToSend;
    std::priority_queue<TBlock, std::vector<TBlock>, Greater> PackedMessagesToSend;
    //! Messages that are sent but yet not acknowledged
    std::queue<TOriginalMessage> SentOriginalMessages;
    std::queue<TBlock> SentPackedMessage;

    const size_t MaxBlockSize = std::numeric_limits<size_t>::max();
    const size_t MaxBlockMessageCount = 1; //!< Max message count that can be packed into a single block. In block version 0 is equal to 1 for compatibility
    bool Connected = false;
    bool Started = false;
    TAtomic Aborting = 0;
    bool SessionEstablished = false;
    ui32 PartitionId = 0;
    TPartitionLocation PreferredPartitionLocation = {};
    ui64 LastSeqNo = 0;
    ui64 MinUnsentSeqNo = 0;
    ui64 SeqNoShift = 0;
    TMaybe<bool> AutoSeqNoMode;
    bool ValidateSeqNoMode = false;

    NThreading::TPromise<ui64> InitSeqNoPromise;
    bool InitSeqNoSetDone = false;
    TInstant SessionStartedTs;
    TInstant LastCountersUpdateTs = TInstant::Zero();
    TInstant LastCountersLogTs;
    TWriterCounters::TPtr Counters;
    TDuration WakeupInterval;
protected:
    ui64 MessagesAcquired = 0;
};

}; // namespace NYdb::NTopic
