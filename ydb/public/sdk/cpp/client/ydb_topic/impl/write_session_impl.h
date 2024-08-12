#pragma once

#include <ydb/public/sdk/cpp/client/ydb_topic/common/callback_context.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/common.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/topic_impl.h>

#include <util/generic/buffer.h>
#include <util/thread/lfqueue.h>


namespace NYdb::NTopic {

class TWriteSessionEventsQueue: public TBaseSessionEventsQueue<TWriteSessionSettings, TWriteSessionEvent::TEvent, TSessionClosedEvent, IExecutor> {
    using TParent = TBaseSessionEventsQueue<TWriteSessionSettings, TWriteSessionEvent::TEvent, TSessionClosedEvent, IExecutor>;

public:
    TWriteSessionEventsQueue(const TWriteSessionSettings& settings)
    : TParent(settings)
    {}

    void PushEvent(TEventInfo eventInfo) {
        if (Closed || ApplyHandler(eventInfo)) {
            return;
        }

        TWaiter waiter;
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
        TWaiter waiter;
        with_lock (Mutex) {
            CloseEvent = event;
            Closed = true;
            waiter = TWaiter(Waiter.ExtractPromise(), this);
        }

        TEventInfo info(event);
        ApplyHandler(info);

        waiter.Signal();
    }

private:
    struct THandlersVisitor : public TParent::TBaseHandlersVisitor {
        using TParent::TBaseHandlersVisitor::TBaseHandlersVisitor;

#define DECLARE_HANDLER(type, handler, answer)                      \
        bool operator()(type&) {                                    \
            if (this->PushHandler<type>(                            \
                std::move(TParent::TBaseHandlersVisitor::Event),    \
                this->Settings.EventHandlers_.handler,              \
                this->Settings.EventHandlers_.CommonHandler_)) {    \
                return answer;                                      \
            }                                                       \
            return false;                                           \
        }                                                           \
        /**/
        DECLARE_HANDLER(TWriteSessionEvent::TAcksEvent, AcksHandler_, true);
        DECLARE_HANDLER(TWriteSessionEvent::TReadyToAcceptEvent, ReadyToAcceptHandler_, true);
        DECLARE_HANDLER(TSessionClosedEvent, SessionClosedHandler_, false); // Not applied

#undef DECLARE_HANDLER
        bool Visit() {
            return std::visit(*this, Event);
        }
    };

    bool ApplyHandler(TEventInfo& eventInfo) {
        THandlersVisitor visitor(Settings, eventInfo.Event);
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
};

struct TMemoryUsageChange {
    bool WasOk; //!< MemoryUsage <= Config.MaxMemoryUsage_ before update
    bool NowOk; //!< Same, only after update
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TWriteSessionImpl

class TWriteSessionImpl : public TContinuationTokenIssuer,
                          public TEnableSelfContext<TWriteSessionImpl> {
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
        ui64 Id;
        TInstant CreatedAt;
        TStringBuf DataRef;
        TMaybe<ECodec> Codec;
        ui32 OriginalSize; // only for coded messages
        TVector<std::pair<TString, TString>> MessageMeta;
        const NTable::TTransaction* Tx;

        TMessage(ui64 id, const TInstant& createdAt, TStringBuf data, TMaybe<ECodec> codec = {},
                 ui32 originalSize = 0, const TVector<std::pair<TString, TString>>& messageMeta = {},
                 const NTable::TTransaction* tx = nullptr)
            : Id(id)
            , CreatedAt(createdAt)
            , DataRef(data)
            , Codec(codec)
            , OriginalSize(originalSize)
            , MessageMeta(messageMeta)
            , Tx(tx)
        {}
    };

    struct TMessageBatch {
        TBuffer Data;
        TVector<TMessage> Messages;
        ui64 CurrentSize = 0;
        TInstant StartedAt = TInstant::Zero();
        bool Acquired = false;
        bool FlushRequested = false;

        void Add(ui64 id, const TInstant& createdAt, TStringBuf data, TMaybe<ECodec> codec, ui32 originalSize,
                 const TVector<std::pair<TString, TString>>& messageMeta,
                 const NTable::TTransaction* tx) {
            if (StartedAt == TInstant::Zero())
                StartedAt = TInstant::Now();
            CurrentSize += codec ? originalSize : data.size();
            Messages.emplace_back(id, createdAt, data, codec, originalSize, messageMeta, tx);
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
        ui64 Id;
        TInstant CreatedAt;
        size_t Size;
        TVector<std::pair<TString, TString>> MessageMeta;
        const NTable::TTransaction* Tx;

        TOriginalMessage(const ui64 id, const TInstant createdAt, const size_t size,
                         const NTable::TTransaction* tx)
            : Id(id)
            , CreatedAt(createdAt)
            , Size(size)
            , Tx(tx)
        {}

        TOriginalMessage(const ui64 id, const TInstant createdAt, const size_t size,
                         TVector<std::pair<TString, TString>>&& messageMeta,
                         const NTable::TTransaction* tx)
            : Id(id)
            , CreatedAt(createdAt)
            , Size(size)
            , MessageMeta(std::move(messageMeta))
            , Tx(tx)
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
            TDbDriverStatePtr dbDriverState);

    TMaybe<TWriteSessionEvent::TEvent> GetEvent(bool block = false);
    TVector<TWriteSessionEvent::TEvent> GetEvents(bool block = false,
                                                  TMaybe<size_t> maxEventsCount = Nothing());
    NThreading::TFuture<ui64> GetInitSeqNo();

    void Write(TContinuationToken&& continuationToken, TWriteMessage&& message);

    void Write(TContinuationToken&&, TStringBuf, TMaybe<ui64> seqNo = Nothing(),
               TMaybe<TInstant> createTimestamp = Nothing()) {
        Y_UNUSED(seqNo);
        Y_UNUSED(createTimestamp);
        Y_ABORT("Do not use this method");
    };

    void WriteEncoded(TContinuationToken&& continuationToken, TWriteMessage&& message);

    void WriteEncoded(TContinuationToken&&, TStringBuf, ECodec, ui32,
                      TMaybe<ui64> seqNo = Nothing(), TMaybe<TInstant> createTimestamp = Nothing()) {
        Y_UNUSED(seqNo);
        Y_UNUSED(createTimestamp);
        Y_ABORT("Do not use this method");
    }


    NThreading::TFuture<void> WaitEvent();

    // Empty maybe - block till all work is done. Otherwise block at most at closeTimeout duration.
    bool Close(TDuration closeTimeout = TDuration::Max());

    TWriterCounters::TPtr GetCounters() {Y_ABORT("Unimplemented"); } //ToDo - unimplemented;

    const TWriteSessionSettings& GetSettings() const {
        return Settings;
    }

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
                const NYdbGrpc::IQueueClientContextPtr& connectContext);
    void OnConnectTimeout(const NYdbGrpc::IQueueClientContextPtr& connectTimeoutContext);
    void ResetForRetryImpl();
    THandleResult RestartImpl(const TPlainStatus& status);
    void Connect(const TDuration& delay);
    void InitImpl();
    void ReadFromProcessor(); // Assumes that we're under lock.
    void WriteToProcessorImpl(TClientMessage&& req); // Assumes that we're under lock.
    void OnReadDone(NYdbGrpc::TGrpcStatus&& grpcStatus, size_t connectionGeneration);
    void OnWriteDone(NYdbGrpc::TGrpcStatus&& status, size_t connectionGeneration);
    TProcessSrvMessageResult ProcessServerMessageImpl();
    TMemoryUsageChange OnMemoryUsageChangedImpl(i64 diff);
    TBuffer CompressBufferImpl(TVector<TStringBuf>& data, ECodec codec, i32 level);
    void CompressImpl(TBlock&& block);
    void OnCompressed(TBlock&& block, bool isSyncCompression=false);
    TMemoryUsageChange OnCompressedImpl(TBlock&& block);

    //TString GetDebugIdentity() const;
    TClientMessage GetInitClientMessage();
    bool CleanupOnAcknowledged(ui64 id);
    bool IsReadyToSendNextImpl() const;
    ui64 GetNextIdImpl(const TMaybe<ui64>& seqNo);
    ui64 GetSeqNoImpl(ui64 id);
    ui64 GetIdImpl(ui64 seqNo);
    void FormGrpcMessagesImpl();
    void SendGrpcMessages();
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
    void OnDescribePartition(const TStatus& status, const Ydb::Topic::DescribePartitionResult& proto, const NYdbGrpc::IQueueClientContextPtr& describePartitionContext);

    TMaybe<TEndpointKey> GetPreferredEndpointImpl(ui32 partitionId, ui64 partitionNodeId);

    bool TxIsChanged(const Ydb::Topic::StreamWriteMessage_WriteRequest* writeRequest) const;

private:
    TWriteSessionSettings Settings;
    std::shared_ptr<TTopicClient::TImpl> Client;
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    TString TargetCluster;
    TString InitialCluster;
    TString CurrentCluster;
    TString PreferredClusterByCDS;
    std::shared_ptr<IWriteSessionConnectionProcessorFactory> ConnectionFactory;
    TDbDriverStatePtr DbDriverState;
    TStringType PrevToken;
    bool UpdateTokenInProgress = false;
    TInstant LastTokenUpdate = TInstant::Zero();
    std::shared_ptr<TWriteSessionEventsQueue> EventsQueue;
    NYdbGrpc::IQueueClientContextPtr ClientContext; // Common client context.
    NYdbGrpc::IQueueClientContextPtr ConnectContext;
    NYdbGrpc::IQueueClientContextPtr ConnectTimeoutContext;
    NYdbGrpc::IQueueClientContextPtr ConnectDelayContext;
    NYdbGrpc::IQueueClientContextPtr DescribePartitionContext;
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

    TLockFreeQueue<TClientMessage> GrpcMessagesToSend;
    TAdaptiveLock ProcessorLock;

    const size_t MaxBlockSize = std::numeric_limits<size_t>::max();
    const size_t MaxBlockMessageCount = 1; //!< Max message count that can be packed into a single block. In block version 0 is equal to 1 for compatibility
    bool Connected = false;
    bool Started = false;
    TAtomic Aborting = 0;
    bool SessionEstablished = false;
    ui32 PartitionId = 0;
    TPartitionLocation PreferredPartitionLocation = {};
    ui64 NextId = 0;
    ui64 MinUnsentId = 1;
    TMaybe<ui64> InitSeqNo;
    TMaybe<bool> AutoSeqNoMode;
    bool ValidateSeqNoMode = false;

    NThreading::TPromise<ui64> InitSeqNoPromise;
    bool InitSeqNoSetDone = false;
    TInstant SessionStartedTs;
    TInstant LastCountersUpdateTs = TInstant::Zero();
    TInstant LastCountersLogTs;
    TWriterCounters::TPtr Counters;
    TDuration WakeupInterval;

    // Set by the write session, if Settings.DirectWriteToPartition is true and Settings.PartitionId is unset. Otherwise ignored.
    TMaybe<ui64> DirectWriteToPartitionId;
protected:
    ui64 MessagesAcquired = 0;
};

}  // namespace NYdb::NTopic
