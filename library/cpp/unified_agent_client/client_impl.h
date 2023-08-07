#pragma once

#include <library/cpp/unified_agent_client/client.h>
#include <library/cpp/unified_agent_client/client_proto_weighing.h>
#include <library/cpp/unified_agent_client/counters.h>
#include <library/cpp/unified_agent_client/logger.h>
#include <library/cpp/unified_agent_client/variant.h>
#include <library/cpp/unified_agent_client/proto/unified_agent.grpc.pb.h>
#include <library/cpp/unified_agent_client/grpc_io.h>

#include <library/cpp/logger/global/global.h>

#include <util/generic/deque.h>
#include <util/system/mutex.h>

namespace NUnifiedAgent::NPrivate {
    class TClientSession;
    class TGrpcCall;
    class TForkProtector;

    class TClient: public IClient {
    public:
        explicit TClient(const TClientParameters& parameters, std::shared_ptr<TForkProtector> forkProtector);

        ~TClient() override;

        TClientSessionPtr CreateSession(const TSessionParameters& parameters) override;

        void StartTracing(ELogPriority logPriority) override;

        void FinishTracing() override;

        inline const TIntrusivePtr<TClientCounters>& GetCounters() const noexcept {
            return Counters;
        }

        inline NUnifiedAgentProto::UnifiedAgentService::Stub& GetStub() noexcept {
            return *Stub;
        }

        TScopeLogger CreateSessionLogger();

        inline const TClientParameters& GetParameters() const noexcept {
            return Parameters;
        }

        inline grpc::CompletionQueue& GetCompletionQueue() noexcept {
            return ActiveCompletionQueue->GetCompletionQueue();
        }

        void RegisterSession(TClientSession* session);

        void UnregisterSession(TClientSession* session);

        void PreFork();

        void PostForkParent();

        void PostForkChild();

        void EnsureStarted();

    private:
        void EnsureStartedNoLock();

        void EnsureStoppedNoLock();

    private:
        const TClientParameters Parameters;
        std::shared_ptr<TForkProtector> ForkProtector;
        TIntrusivePtr<TClientCounters> Counters;
        TLog Log;
        TLogger MainLogger;
        TScopeLogger Logger;
        std::shared_ptr<grpc::Channel> Channel;
        std::unique_ptr<NUnifiedAgentProto::UnifiedAgentService::Stub> Stub;
        THolder<TGrpcCompletionQueueHost> ActiveCompletionQueue;
        std::atomic<size_t> SessionLogLabel;
        TVector<TClientSession*> ActiveSessions;
        bool Started;
        bool Destroyed;
        TAdaptiveLock Lock;
        static std::atomic<ui64> Id;
    };

    class TForkProtector {
    public:
        TForkProtector();

        void Register(TClient& client);

        void Unregister(TClient& client);

        static std::shared_ptr<TForkProtector> Get(bool createIfNotExists);

    private:
        static void PreFork();

        static void PostForkParent();

        static void PostForkChild();

    private:
        TVector<TClient*> Clients;
        grpc::internal::GrpcLibrary GrpcInitializer;
        bool Enabled;
        TAdaptiveLock Lock;

        static std::weak_ptr<TForkProtector> Instance;
        static TMutex InstanceLock;
        static bool SubscribedToForks;
    };

    class TClientSession: public IClientSession {
    public:
        TClientSession(const TIntrusivePtr<TClient>& client, const TSessionParameters& parameters);

        ~TClientSession();

        void Send(TClientMessage&& message) override;

        NThreading::TFuture<void> CloseAsync(TInstant deadline) override;

        inline TClient& GetClient() noexcept {
            return *Client;
        }

        inline TScopeLogger& GetLogger() noexcept {
            return Logger;
        }

        inline TClientSessionCounters& GetCounters() noexcept {
            return *Counters;
        }

        inline TAsyncJoiner& GetAsyncJoiner() noexcept {
            return AsyncJoiner;
        }

        void PrepareInitializeRequest(NUnifiedAgentProto::Request& target);

        void PrepareWriteBatchRequest(NUnifiedAgentProto::Request& target);

        void Acknowledge(ui64 seqNo);

        void OnGrpcCallInitialized(const TString& sessionId, ui64 lastSeqNo);

        void OnGrpcCallFinished();

        NThreading::TFuture<void> PreFork();

        void PostForkParent();

        void PostForkChild();

        void SetAgentMaxReceiveMessage(size_t);

    private:
        enum class EPollingStatus {
            Active,
            Inactive
        };

        struct TCloseRequestedEvent {
            TInstant Deadline;
        };

        struct TMessageReceivedEvent {
            TClientMessage Message;
            size_t Size;
        };

        struct TPurgeWriteQueueStats {
            size_t PurgedMessages{};
            size_t PurgedBytes{};
        };

        using TEvent = std::variant<TCloseRequestedEvent, TMessageReceivedEvent>;

    public:
        struct TPendingMessage {
            TClientMessage Message;
            size_t Size;
            bool Skipped;
        };

        class TRequestBuilder {
        public:
            struct TAddResult;

        public:
            TRequestBuilder(NUnifiedAgentProto::Request &target, size_t RequestPayloadLimitBytes,
                    TFMaybe<size_t> serializedRequestLimitBytes);

            TAddResult TryAddMessage(const TPendingMessage& message, size_t seqNo);

            void ResetCounters();

            inline size_t GetSerializedRequestSize() const {
                return SerializedRequestSize;
            }

            inline size_t GetRequestPayloadSize() const {
                return RequestPayloadSize;
            }

        public:
            struct TAddResult {
                bool LimitExceeded;
                size_t NewRequestPayloadSize;  // == actual value, if !LimitExceeded
                size_t NewSerializedRequestSize;  // == actual value, if !LimitExceeded
            };

        private:
            struct TMetaItemBuilder {
                size_t ItemIndex;
                size_t ValueIndex{0};
            };

        private:
            NUnifiedAgentProto::Request& Target;
            TFMaybe<NPW::TRequest> PwTarget;
            THashMap<TString, TMetaItemBuilder> MetaItems;
            size_t RequestPayloadSize;
            size_t RequestPayloadLimitBytes;
            size_t SerializedRequestSize;
            TFMaybe<size_t> SerializedRequestLimitBytes;
            bool CountersInvalid;
        };

    private:
        void MakeGrpcCall();

        void DoClose();

        void BeginClose(TInstant deadline);

        void Poll();

        TPurgeWriteQueueStats PurgeWriteQueue();

        void DoStart();

    private:
        TAsyncJoiner AsyncJoiner;
        TIntrusivePtr<TClient> Client;
        TFMaybe<TString> OriginalSessionId;
        TFMaybe<TString> SessionId;
        TFMaybe<THashMap<TString, TString>> Meta;
        TScopeLogger Logger;
        bool CloseStarted;
        bool ForcedCloseStarted;
        bool Closed;
        bool ForkInProgressLocal;
        bool Started;
        NThreading::TPromise<void> ClosePromise;
        TIntrusivePtr<TGrpcCall> ActiveGrpcCall;
        TDeque<TPendingMessage> WriteQueue;
        size_t TrimmedCount;
        size_t NextIndex;
        TFMaybe<ui64> AckSeqNo;
        TInstant PollerLastEventTimestamp;
        TIntrusivePtr<TClientSessionCounters> Counters;
        THolder<TGrpcTimer> MakeGrpcCallTimer;
        THolder<TGrpcTimer> ForceCloseTimer;
        THolder<TGrpcTimer> PollTimer;
        ui64 GrpcInflightMessages;
        ui64 GrpcInflightBytes;

        std::atomic<size_t> InflightBytes;
        bool CloseRequested;
        size_t EventsBatchSize;
        EPollingStatus PollingStatus;
        THolder<TGrpcNotification> EventNotification;
        bool EventNotificationTriggered;
        TVector<TEvent> EventsBatch;
        TVector<TEvent> SecondaryEventsBatch;
        std::atomic<bool> ForkInProgress;
        TAdaptiveLock Lock;
        size_t MaxInflightBytes;
        TFMaybe<size_t> AgentMaxReceiveMessage;
    };

    class TGrpcCall final: public TAtomicRefCount<TGrpcCall> {
    public:
        explicit TGrpcCall(TClientSession& session);

        void Start();

        ~TGrpcCall();

        void BeginClose(bool force);

        void Poison();

        void NotifyMessageAdded();

        inline bool Initialized() const {
            return Initialized_;
        }

        inline bool ReuseSessions() const {
            return ReuseSessions_;
        }

    private:
        void EndAccept(EIOStatus status);

        void EndRead(EIOStatus status);

        void EndWrite(EIOStatus status);

        void EndFinish(EIOStatus status);

        void EndWritesDone(EIOStatus);

        void ScheduleWrite();

        void BeginWritesDone();

        bool CheckHasError(EIOStatus status, const char* method);

        void SetError(const TString& error);

        void EnsureFinishStarted();

        void BeginRead();

        void BeginWrite();

        void ScheduleFinishOnError();

    private:
        TClientSession& Session;
        TAsyncJoinerToken AsyncJoinerToken;
        THolder<IIOCallback> AcceptTag;
        THolder<IIOCallback> ReadTag;
        THolder<IIOCallback> WriteTag;
        THolder<IIOCallback> WritesDoneTag;
        THolder<IIOCallback> FinishTag;
        TScopeLogger Logger;
        bool AcceptPending;
        bool Initialized_;
        bool ReadPending;
        bool ReadsDone;
        bool WritePending;
        bool WritesBlocked;
        bool WritesDonePending;
        bool WritesDone;
        bool ErrorOccured;
        bool FinishRequested;
        bool FinishStarted;
        bool FinishDone;
        bool Cancelled;
        bool Poisoned;
        bool PoisonPillSent;
        bool ReuseSessions_;
        grpc::Status FinishStatus;
        grpc::ClientContext ClientContext;
        std::unique_ptr<grpc::ClientAsyncReaderWriter<NUnifiedAgentProto::Request, NUnifiedAgentProto::Response>> ReaderWriter;
        NUnifiedAgentProto::Request Request;
        NUnifiedAgentProto::Response Response;
    };
}
