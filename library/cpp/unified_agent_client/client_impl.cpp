#include "client_impl.h"
#include "helpers.h"

#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <contrib/libs/grpc/src/core/lib/gpr/string.h>
#include <contrib/libs/grpc/src/core/lib/gprpp/fork.h>
#include <contrib/libs/grpc/src/core/lib/iomgr/executor.h>

#include <util/charset/utf8.h>
#include <util/generic/size_literals.h>
#include <util/system/env.h>

using namespace NThreading;
using namespace NMonitoring;

namespace NUnifiedAgent::NPrivate {
    std::shared_ptr<grpc::Channel> CreateChannel(const grpc::string& target) {
        grpc::ChannelArguments args;
        args.SetCompressionAlgorithm(GRPC_COMPRESS_NONE);
        args.SetMaxReceiveMessageSize(Max<int>());
        args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 60000);
        args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000);
        args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 100);
        args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 200);
        args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
        args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
        args.SetInt(GRPC_ARG_HTTP2_MIN_SENT_PING_INTERVAL_WITHOUT_DATA_MS, 5000);
        args.SetInt(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 5000);
        args.SetInt(GRPC_ARG_TCP_READ_CHUNK_SIZE, 1024*1024);
        return grpc::CreateCustomChannel(target, grpc::InsecureChannelCredentials(), args);
    }

    void AddMeta(NUnifiedAgentProto::Request_Initialize& init, const TString& name, const TString& value) {
        auto* metaItem = init.MutableMeta()->Add();
        metaItem->SetName(name);
        metaItem->SetValue(value);
    }

    std::atomic<ui64> TClient::Id{0};

    TClient::TClient(const TClientParameters& parameters, std::shared_ptr<TForkProtector> forkProtector)
        : Parameters(parameters)
        , ForkProtector(forkProtector)
        , Counters(parameters.Counters ? parameters.Counters : MakeIntrusive<TClientCounters>())
        , Log(parameters.Log)
        , MainLogger(Log, MakeFMaybe(Parameters.LogRateLimitBytes))
        , Logger(MainLogger.Child(Sprintf("ua_%" PRIu64, Id.fetch_add(1))))
        , Channel(nullptr)
        , Stub(nullptr)
        , ActiveCompletionQueue(nullptr)
        , SessionLogLabel(0)
        , ActiveSessions()
        , Started(false)
        , Destroyed(false)
        , Lock()
    {
        MainLogger.SetDroppedBytesCounter(&Counters->ClientLogDroppedBytes);

        if (ForkProtector != nullptr) {
            ForkProtector->Register(*this);
        }

        EnsureStarted();

        YLOG_INFO(Sprintf("created, uri [%s]", Parameters.Uri.c_str()));
    }

    TClient::~TClient() {
        with_lock(Lock) {
            Y_ABORT_UNLESS(ActiveSessions.empty(), "active sessions found");

            EnsureStoppedNoLock();

            Destroyed = true;
        }

        if (ForkProtector != nullptr) {
            ForkProtector->Unregister(*this);
        }

        YLOG_DEBUG(Sprintf("destroyed, uri [%s]", Parameters.Uri.c_str()));
    }

    TClientSessionPtr TClient::CreateSession(const TSessionParameters& parameters) {
        return MakeIntrusive<TClientSession>(this, parameters);
    }

    void TClient::StartTracing(ELogPriority logPriority) {
        MainLogger.StartTracing(logPriority);
        StartGrpcTracing();
        YLOG_INFO("tracing started");
    }

    void TClient::FinishTracing() {
        FinishGrpcTracing();
        MainLogger.FinishTracing();
        YLOG_INFO("tracing finished");
    }

    void TClient::RegisterSession(TClientSession* session) {
        with_lock(Lock) {
            ActiveSessions.push_back(session);
        }
    }

    void TClient::UnregisterSession(TClientSession* session) {
        with_lock(Lock) {
            const auto it = Find(ActiveSessions, session);
            Y_ABORT_UNLESS(it != ActiveSessions.end());
            ActiveSessions.erase(it);
        }
    }

    void TClient::PreFork() {
        YLOG_INFO("pre fork started");

        Lock.Acquire();

        auto futures = TVector<TFuture<void>>(Reserve(ActiveSessions.size()));
        for (auto* s: ActiveSessions) {
            futures.push_back(s->PreFork());
        }
        YLOG_INFO("waiting for sessions");
        WaitAll(futures).Wait();

        EnsureStoppedNoLock();

        YLOG_INFO("shutdown grpc executor");
        grpc_core::Executor::SetThreadingAll(false);

        YLOG_INFO("pre fork finished");
    }

    void TClient::PostForkParent() {
        YLOG_INFO("post fork parent started");

        if (!Destroyed) {
            EnsureStartedNoLock();
        }
        Lock.Release();

        for (auto* s: ActiveSessions) {
            s->PostForkParent();
        }

        YLOG_INFO("post fork parent finished");
    }

    void TClient::PostForkChild() {
        YLOG_INFO("post fork child started");

        Lock.Release();

        for (auto* s: ActiveSessions) {
            s->PostForkChild();
        }

        YLOG_INFO("post fork child finished");
    }

    void TClient::EnsureStarted() {
        with_lock(Lock) {
            EnsureStartedNoLock();
        }
    }

    void TClient::EnsureStartedNoLock() {
        // Lock must be held

        if (Started) {
            return;
        }

        Channel = CreateChannel(Parameters.Uri);
        Stub = NUnifiedAgentProto::UnifiedAgentService::NewStub(Channel);
        ActiveCompletionQueue = MakeHolder<TGrpcCompletionQueueHost>();
        ActiveCompletionQueue->Start();

        Started = true;
    }

    void TClient::EnsureStoppedNoLock() {
        // Lock must be held

        if (!Started) {
            return;
        }

        YLOG_DEBUG("stopping");
        ActiveCompletionQueue->Stop();
        ActiveCompletionQueue = nullptr;
        Stub = nullptr;
        Channel = nullptr;
        YLOG_DEBUG("stopped");

        Started = false;
    }

    TScopeLogger TClient::CreateSessionLogger() {
        return Logger.Child(ToString(SessionLogLabel.fetch_add(1)));
    }

    TForkProtector::TForkProtector()
        : Clients()
        , GrpcInitializer()
        , Enabled(grpc_core::Fork::Enabled())
        , Lock()
    {
    }

    void TForkProtector::Register(TClient& client) {
        if (!Enabled) {
            return;
        }

        Y_ABORT_UNLESS(grpc_is_initialized());
        Y_ABORT_UNLESS(grpc_core::Fork::Enabled());

        with_lock(Lock) {
            Clients.push_back(&client);
        }
    }

    void TForkProtector::Unregister(TClient& client) {
        if (!Enabled) {
            return;
        }

        with_lock(Lock) {
            const auto it = Find(Clients, &client);
            Y_ABORT_UNLESS(it != Clients.end());
            Clients.erase(it);
        }
    }

    std::shared_ptr<TForkProtector> TForkProtector::Get(bool createIfNotExists) {
        with_lock(InstanceLock) {
            auto result = Instance.lock();
            if (!result && createIfNotExists) {
                result = std::make_shared<TForkProtector>();
                if (!result->Enabled) {
                    TLog log("cerr");
                    TLogger logger(log, Nothing());
                    auto scopeLogger = logger.Child("ua client");
                    YLOG(TLOG_WARNING,
                         "Grpc is already initialized, can't enable fork support. "
                         "If forks are possible, please set environment variable GRPC_ENABLE_FORK_SUPPORT to 'true'. "
                         "If not, you can suppress this warning by setting EnableForkSupport "
                         "to false when creating the ua client.",
                         scopeLogger);
                } else if (!SubscribedToForks) {
                    SubscribedToForks = true;
        #ifdef _unix_
                    pthread_atfork(
                        &TForkProtector::PreFork,
                        &TForkProtector::PostForkParent,
                        &TForkProtector::PostForkChild);
        #endif
                }

                Instance = result;
            }
            return result;
        }
    }

    void TForkProtector::PreFork() {
        auto self = Get(false);
        if (!self) {
            return;
        }
        self->Lock.Acquire();
        for (auto* c : self->Clients) {
            c->PreFork();
        }
    }

    void TForkProtector::PostForkParent() {
        auto self = Get(false);
        if (!self) {
            return;
        }
        for (auto* c : self->Clients) {
            c->PostForkParent();
        }
        self->Lock.Release();
    }

    void TForkProtector::PostForkChild() {
        auto self = Get(false);
        if (!self) {
            return;
        }
        for (auto* c : self->Clients) {
            c->PostForkChild();
        }
        self->Lock.Release();
    }

    std::weak_ptr<TForkProtector> TForkProtector::Instance{};
    TMutex TForkProtector::InstanceLock{};
    bool TForkProtector::SubscribedToForks{false};

    TClientSession::TClientSession(const TIntrusivePtr<TClient>& client, const TSessionParameters& parameters)
        : AsyncJoiner()
        , Client(client)
        , OriginalSessionId(MakeFMaybe(parameters.SessionId))
        , SessionId(OriginalSessionId)
        , Meta(MakeFMaybe(parameters.Meta))
        , Logger(Client->CreateSessionLogger())
        , CloseStarted(false)
        , ForcedCloseStarted(false)
        , Closed(false)
        , ForkInProgressLocal(false)
        , Started(false)
        , ClosePromise()
        , ActiveGrpcCall(nullptr)
        , WriteQueue()
        , TrimmedCount(0)
        , NextIndex(0)
        , AckSeqNo(Nothing())
        , PollerLastEventTimestamp()
        , Counters(parameters.Counters ? parameters.Counters : Client->GetCounters()->GetDefaultSessionCounters())
        , MakeGrpcCallTimer(nullptr)
        , ForceCloseTimer(nullptr)
        , PollTimer(nullptr)
        , GrpcInflightMessages(0)
        , GrpcInflightBytes(0)
        , InflightBytes(0)
        , CloseRequested(false)
        , EventsBatchSize(0)
        , PollingStatus(EPollingStatus::Inactive)
        , EventNotification(nullptr)
        , EventNotificationTriggered(false)
        , EventsBatch()
        , SecondaryEventsBatch()
        , ForkInProgress(false)
        , Lock()
        , MaxInflightBytes(
              parameters.MaxInflightBytes.GetOrElse(Client->GetParameters().MaxInflightBytes))
        , AgentMaxReceiveMessage(Nothing()) {
        if (Meta.Defined() && !IsUtf8(*Meta)) {
            throw std::runtime_error("session meta contains non UTF-8 characters");
        }
        Y_ENSURE(!(Client->GetParameters().EnableForkSupport && SessionId.Defined()),
                 "explicit session id is not supported with forks");
        Client->RegisterSession(this);

        with_lock(Lock) {
            DoStart();
        }
    }

    TFuture<void> TClientSession::PreFork() {
        YLOG_INFO("pre fork started");

        Lock.Acquire();

        YLOG_INFO("triggering event notification");
        if (!EventNotificationTriggered) {
            EventNotificationTriggered = true;
            EventNotification->Trigger();
        }

        YLOG_INFO("setting 'fork in progress' flag");
        ForkInProgress.store(true);

        if (!Started) {
            ClosePromise.TrySetValue();
        }
        YLOG_INFO("pre fork finished");
        return ClosePromise.GetFuture();
    }

    void TClientSession::PostForkParent() {
        YLOG_INFO("post fork parent started");
        ForkInProgress.store(false);
        ForkInProgressLocal = false;
        Started = false;

        if (!CloseRequested) {
            DoStart();

            YLOG_INFO("triggering event notification");
            EventNotificationTriggered = true;
            EventNotification->Trigger();
        }

        Lock.Release();

        YLOG_INFO("post fork parent finished");
    }

    void TClientSession::PostForkChild() {
        YLOG_INFO("post fork child started");
        ForkInProgress.store(false);
        ForkInProgressLocal = false;
        Started = false;

        SessionId.Clear();
        TrimmedCount = 0;
        NextIndex = 0;
        AckSeqNo.Clear();
        PurgeWriteQueue();
        EventsBatch.clear();
        SecondaryEventsBatch.clear();
        EventsBatchSize = 0;

        Lock.Release();

        YLOG_INFO("post fork child finished");
    }

    void TClientSession::SetAgentMaxReceiveMessage(size_t newValue) {
        AgentMaxReceiveMessage = newValue;
    }

    void TClientSession::DoStart() {
        // Lock must be held

        Y_ABORT_UNLESS(!Started);
        YLOG_DEBUG("starting");

        Client->EnsureStarted();

        MakeGrpcCallTimer = MakeHolder<TGrpcTimer>(Client->GetCompletionQueue(),
            MakeIOCallback([this](EIOStatus status) {
                if (status == EIOStatus::Error) {
                    return;
                }
                MakeGrpcCall();
            }, &AsyncJoiner));
        ForceCloseTimer = MakeHolder<TGrpcTimer>(Client->GetCompletionQueue(),
            MakeIOCallback([this](EIOStatus status) {
                if (status == EIOStatus::Error) {
                    return;
                }
                YLOG_INFO("ForceCloseTimer");
                BeginClose(TInstant::Zero());
            }, &AsyncJoiner));
        PollTimer = MakeHolder<TGrpcTimer>(Client->GetCompletionQueue(),
            MakeIOCallback([this](EIOStatus status) {
                if (status == EIOStatus::Error) {
                    return;
                }
                Poll();
            }, &AsyncJoiner));
        EventNotification = MakeHolder<TGrpcNotification>(Client->GetCompletionQueue(),
            MakeIOCallback([this](EIOStatus status) {
                Y_ABORT_UNLESS(status == EIOStatus::Ok);
                Poll();
            }, &AsyncJoiner));

        CloseStarted = false;
        ForcedCloseStarted = false;
        Closed = false;
        ClosePromise = NewPromise();
        EventNotificationTriggered = false;
        PollerLastEventTimestamp = Now();
        PollingStatus = EPollingStatus::Inactive;

        ++Client->GetCounters()->ActiveSessionsCount;
        MakeGrpcCallTimer->Set(Now());
        YLOG_DEBUG(Sprintf("started, sessionId [%s]", OriginalSessionId.GetOrElse("").c_str()));

        Started = true;
    }

    void TClientSession::MakeGrpcCall() {
        if (Closed) {
            YLOG_INFO("MakeGrpcCall, session already closed");
            return;
        }
        Y_ABORT_UNLESS(!ForcedCloseStarted);
        Y_ABORT_UNLESS(!ActiveGrpcCall);
        ActiveGrpcCall = MakeIntrusive<TGrpcCall>(*this);
        ActiveGrpcCall->Start();
        ++Counters->GrpcCalls;
        if (CloseStarted) {
            ActiveGrpcCall->BeginClose(false);
        }
    }

    TClientSession::~TClientSession() {
        Close(TInstant::Zero());
        AsyncJoiner.Join().Wait();
        Client->UnregisterSession(this);
        YLOG_DEBUG("destroyed");
    }

    void TClientSession::Send(TClientMessage&& message) {
        const size_t messageSize = SizeOf(message);
        ++Counters->ReceivedMessages;
        Counters->ReceivedBytes += messageSize;
        if (messageSize > Client->GetParameters().GrpcMaxMessageSize) {
            YLOG_ERR(Sprintf("message size [%zu] is greater than max grpc message size [%zu], message dropped",
                              messageSize, Client->GetParameters().GrpcMaxMessageSize));
            ++Counters->DroppedMessages;
            Counters->DroppedBytes += messageSize;
            ++Counters->ErrorsCount;
            return;
        }
        if (message.Meta.Defined() && !IsUtf8(*message.Meta)) {
            YLOG_ERR("message meta contains non UTF-8 characters, message dropped");
            ++Counters->DroppedMessages;
            Counters->DroppedBytes += messageSize;
            ++Counters->ErrorsCount;
            return;
        }
        if (!message.Timestamp.Defined()) {
            message.Timestamp = TInstant::Now();
        }
        ++Counters->InflightMessages;
        Counters->InflightBytes += messageSize;
        {
            auto g = Guard(Lock);

            if (!Started) {
                DoStart();
            }

            if (CloseRequested) {
                g.Release();
                YLOG_ERR(Sprintf("session is closing, message dropped, [%zu] bytes", messageSize));
                --Counters->InflightMessages;
                Counters->InflightBytes -= messageSize;
                ++Counters->DroppedMessages;
                Counters->DroppedBytes += messageSize;
                ++Counters->ErrorsCount;
                return;
            }
            if (InflightBytes.load() + messageSize > MaxInflightBytes) {
                g.Release();
                YLOG_ERR(Sprintf("max inflight of [%zu] bytes reached, [%zu] bytes dropped",
                    MaxInflightBytes, messageSize));
                --Counters->InflightMessages;
                Counters->InflightBytes -= messageSize;
                ++Counters->DroppedMessages;
                Counters->DroppedBytes += messageSize;
                ++Counters->ErrorsCount;
                return;
            }
            InflightBytes.fetch_add(messageSize);
            EventsBatch.push_back(TMessageReceivedEvent{std::move(message), messageSize});
            EventsBatchSize += messageSize;
            if ((PollingStatus == EPollingStatus::Inactive ||
                EventsBatchSize >= Client->GetParameters().GrpcMaxMessageSize) &&
                !EventNotificationTriggered)
            {
                EventNotificationTriggered = true;
                EventNotification->Trigger();
            }
        }
    }

    TFuture<void> TClientSession::CloseAsync(TInstant deadline) {
        YLOG_DEBUG(Sprintf("close, deadline [%s]", ToString(deadline).c_str()));
        if (!ClosePromise.GetFuture().HasValue()) {
            with_lock(Lock) {
                if (!Started) {
                    return MakeFuture();
                }

                CloseRequested = true;

                EventsBatch.push_back(TCloseRequestedEvent{deadline});
                if (!EventNotificationTriggered) {
                    EventNotificationTriggered = true;
                    EventNotification->Trigger();
                }
            }
        }
        return ClosePromise.GetFuture();
    }

    void TClientSession::BeginClose(TInstant deadline) {
        if (Closed) {
            return;
        }
        if (!CloseStarted) {
            CloseStarted = true;
            YLOG_DEBUG("close started");
        }
        const auto force = deadline == TInstant::Zero();
        if (force && !ForcedCloseStarted) {
            ForcedCloseStarted = true;
            YLOG_INFO("forced close started");
        }
        if (!ActiveGrpcCall && (ForcedCloseStarted || WriteQueue.empty())) {
            DoClose();
        } else {
            if (!force) {
                ForceCloseTimer->Set(deadline);
            }
            if (ActiveGrpcCall) {
                ActiveGrpcCall->BeginClose(ForcedCloseStarted);
            }
        }
    }

    void TClientSession::Poll() {
        if (ForkInProgressLocal) {
            return;
        }

        const auto now = Now();
        const auto sendDelay = Client->GetParameters().GrpcSendDelay;
        const auto oldPollingStatus = PollingStatus;

        {
            if (!Lock.TryAcquire()) {
                TSpinWait sw;

                while (Lock.IsLocked() || !Lock.TryAcquire()) {
                    if (ForkInProgress.load()) {
                        YLOG_INFO("poller 'fork in progress' signal received, stopping session");
                        ForkInProgressLocal = true;
                        if (!ActiveGrpcCall || !ActiveGrpcCall->Initialized()) {
                            BeginClose(TInstant::Max());
                        } else if (ActiveGrpcCall->ReuseSessions()) {
                            ActiveGrpcCall->Poison();
                            BeginClose(TInstant::Max());
                        } else {
                            BeginClose(TInstant::Zero());
                        }
                        return;
                    }
                    sw.Sleep();
                }
            }

            if (!EventsBatch.empty()) {
                DoSwap(EventsBatch, SecondaryEventsBatch);
                EventsBatchSize = 0;
                PollerLastEventTimestamp = now;
            }
            const auto needNextPollStep = sendDelay != TDuration::Zero() &&
                                          !CloseRequested &&
                                          (now - PollerLastEventTimestamp) < 10 * sendDelay;
            PollingStatus = needNextPollStep ? EPollingStatus::Active : EPollingStatus::Inactive;
            EventNotificationTriggered = false;

            Lock.Release();
        }

        if (PollingStatus == EPollingStatus::Active) {
            PollTimer->Set(now + sendDelay);
        }
        if (PollingStatus != oldPollingStatus) {
            YLOG_DEBUG(Sprintf("poller %s", PollingStatus == EPollingStatus::Active ? "started" : "stopped"));
        }
        if (auto& batch = SecondaryEventsBatch; !batch.empty()) {
            auto closeIt = FindIf(batch, [](const auto& e) {
                return std::holds_alternative<TCloseRequestedEvent>(e);
            });

            if (auto it = begin(batch); it != closeIt) {
                Y_ABORT_UNLESS(!CloseStarted);
                do {
                    auto& e = std::get<TMessageReceivedEvent>(*it++);
                    WriteQueue.push_back({std::move(e.Message), e.Size, false});
                } while (it != closeIt);
                if (ActiveGrpcCall) {
                    ActiveGrpcCall->NotifyMessageAdded();
                }
            }

            for (auto endIt = end(batch); closeIt != endIt; ++closeIt) {
                const auto& e = std::get<TCloseRequestedEvent>(*closeIt);
                BeginClose(e.Deadline);
            }

            batch.clear();
        }
    };

    void TClientSession::PrepareInitializeRequest(NUnifiedAgentProto::Request& target) {
        auto& initializeMessage = *target.MutableInitialize();
        if (SessionId.Defined()) {
            initializeMessage.SetSessionId(*SessionId);
        }
        if (Client->GetParameters().SharedSecretKey.Defined()) {
            initializeMessage.SetSharedSecretKey(*Client->GetParameters().SharedSecretKey);
        }
        if (Meta.Defined()) {
            for (const auto& p: *Meta) {
                AddMeta(initializeMessage, p.first, p.second);
            }
        }
        if (!Meta.Defined() || Meta->find("_reusable") == Meta->end()) {
            AddMeta(initializeMessage, "_reusable", "true");
        }
    }

    TClientSession::TRequestBuilder::TRequestBuilder(NUnifiedAgentProto::Request& target, size_t RequestPayloadLimitBytes,
        TFMaybe<size_t> serializedRequestLimitBytes)
        : Target(target)
        , PwTarget(MakeFMaybe<NPW::TRequest>())
        , MetaItems()
        , RequestPayloadSize(0)
        , RequestPayloadLimitBytes(RequestPayloadLimitBytes)
        , SerializedRequestSize(0)
        , SerializedRequestLimitBytes(serializedRequestLimitBytes)
        , CountersInvalid(false)
    {
    }

    void TClientSession::TRequestBuilder::ResetCounters() {
        RequestPayloadSize = 0;
        SerializedRequestSize = 0;
        PwTarget.Clear();
        PwTarget.ConstructInPlace();
        CountersInvalid = false;
    }

    TClientSession::TRequestBuilder::TAddResult TClientSession::TRequestBuilder::TryAddMessage(
            const TPendingMessage& message, size_t seqNo) {
        Y_ABORT_UNLESS(!CountersInvalid);
        {
            // add item to pwRequest to increase calculated size
            PwTarget->DataBatch.SeqNo.Add(seqNo);
            PwTarget->DataBatch.Timestamp.Add(message.Message.Timestamp->MicroSeconds());
            PwTarget->DataBatch.Payload.Add().SetValue(message.Message.Payload);
            if (message.Message.Meta.Defined()) {
                for (const auto &m: *message.Message.Meta) {
                    TMetaItemBuilder *metaItemBuilder = nullptr;
                    {
                        auto it = MetaItems.find(m.first);
                        if (it == MetaItems.end()) {
                            PwTarget->DataBatch.Meta.Add().Key.SetValue(m.first);
                        } else {
                            metaItemBuilder = &it->second;
                        }
                    }
                    size_t metaItemIdx = (metaItemBuilder != nullptr) ? metaItemBuilder->ItemIndex :
                            PwTarget->DataBatch.Meta.GetSize() - 1;
                    auto &pwMetaItem = PwTarget->DataBatch.Meta.Get(metaItemIdx);
                    pwMetaItem.Value.Add().SetValue(m.second);
                    const auto index = Target.GetDataBatch().SeqNoSize();
                    if ((metaItemBuilder != nullptr && metaItemBuilder->ValueIndex != index) ||
                            (metaItemBuilder == nullptr && index != 0)) {
                        const auto valueIdx = (metaItemBuilder) ? metaItemBuilder->ValueIndex : 0;
                        pwMetaItem.SkipStart.Add(valueIdx);
                        pwMetaItem.SkipLength.Add(index - valueIdx);
                    }
                }
            }
        }
        const auto newSerializedRequestSize = PwTarget->ByteSizeLong();
        const auto newPayloadSize = RequestPayloadSize + message.Size;
        if ((SerializedRequestLimitBytes.Defined() && newSerializedRequestSize > *SerializedRequestLimitBytes) ||
                newPayloadSize > RequestPayloadLimitBytes) {
            CountersInvalid = true;
            return {true, newPayloadSize, newSerializedRequestSize};
        }

        {
            // add item to the real request
            auto& batch = *Target.MutableDataBatch();
            batch.AddSeqNo(seqNo);
            batch.AddTimestamp(message.Message.Timestamp->MicroSeconds());
            batch.AddPayload(message.Message.Payload);
            if (message.Message.Meta.Defined()) {
                for (const auto &m: *message.Message.Meta) {
                    TMetaItemBuilder *metaItemBuilder;
                    {
                        auto it = MetaItems.find(m.first);
                        if (it == MetaItems.end()) {
                            batch.AddMeta()->SetKey(m.first);
                            auto insertResult = MetaItems.insert({m.first, {batch.MetaSize() - 1}});
                            Y_ABORT_UNLESS(insertResult.second);
                            metaItemBuilder = &insertResult.first->second;
                        } else {
                            metaItemBuilder = &it->second;
                        }
                    }
                    auto *metaItem = batch.MutableMeta(metaItemBuilder->ItemIndex);
                    metaItem->AddValue(m.second);
                    const auto index = batch.SeqNoSize() - 1;
                    if (metaItemBuilder->ValueIndex != index) {
                        metaItem->AddSkipStart(metaItemBuilder->ValueIndex);
                        metaItem->AddSkipLength(index - metaItemBuilder->ValueIndex);
                    }
                    metaItemBuilder->ValueIndex = index + 1;
                }
            }
            SerializedRequestSize = newSerializedRequestSize;
            RequestPayloadSize = newPayloadSize;
        }

        return {false, newPayloadSize, newSerializedRequestSize};
    }

    void TClientSession::PrepareWriteBatchRequest(NUnifiedAgentProto::Request& target) {
        Y_ABORT_UNLESS(AckSeqNo.Defined());
        TRequestBuilder requestBuilder(target, Client->GetParameters().GrpcMaxMessageSize, AgentMaxReceiveMessage);
        const auto startIndex = NextIndex - TrimmedCount;
        for (size_t i = startIndex; i < WriteQueue.size(); ++i) {
            auto& queueItem = WriteQueue[i];
            if (queueItem.Skipped) {
                NextIndex++;
                continue;
            }

            const auto addResult = requestBuilder.TryAddMessage(queueItem, *AckSeqNo + i + 1);
            const size_t serializedLimitToLog = AgentMaxReceiveMessage.Defined() ? *AgentMaxReceiveMessage : 0;
            if (addResult.LimitExceeded && target.GetDataBatch().SeqNoSize() == 0) {
                YLOG_ERR(Sprintf("single serialized message is too large [%zu] > [%zu], dropping it",
                        addResult.NewSerializedRequestSize, serializedLimitToLog));
                queueItem.Skipped = true;
                ++Counters->DroppedMessages;
                Counters->DroppedBytes += queueItem.Size;
                ++Counters->ErrorsCount;
                NextIndex++;
                requestBuilder.ResetCounters();
                continue;
            }
            if (addResult.LimitExceeded) {
                YLOG_DEBUG(Sprintf(
                        "batch limit exceeded: [%zu] > [%zu] (limit for serialized batch)"
                        "OR [%zu] > [%zu] (limit for raw batch)",
                        addResult.NewSerializedRequestSize, serializedLimitToLog,
                        addResult.NewRequestPayloadSize, Client->GetParameters().GrpcMaxMessageSize));
                break;
            }

            NextIndex++;
        }
        const auto messagesCount = target.GetDataBatch().SeqNoSize();
        if (messagesCount == 0) {
            return;
        }
        Y_ABORT_UNLESS(requestBuilder.GetSerializedRequestSize() == target.ByteSizeLong(),
            "failed to calculate size for message [%s]", target.ShortDebugString().c_str());
        GrpcInflightMessages += messagesCount;
        GrpcInflightBytes += requestBuilder.GetRequestPayloadSize();
        YLOG_DEBUG(Sprintf("new write batch, [%zu] messages, [%zu] bytes, first seq_no [%" PRIu64 "], serialized size [%zu]",
                messagesCount, requestBuilder.GetRequestPayloadSize(),
                *target.GetDataBatch().GetSeqNo().begin(), requestBuilder.GetSerializedRequestSize()));
        ++Counters->GrpcWriteBatchRequests;
        Counters->GrpcInflightMessages += messagesCount;
        Counters->GrpcInflightBytes += requestBuilder.GetRequestPayloadSize();
    }

    void TClientSession::Acknowledge(ui64 seqNo) {
        size_t messagesCount = 0;
        size_t bytesCount = 0;
        size_t skippedMessagesCount = 0;
        size_t skippedBytesCount = 0;

        if (AckSeqNo.Defined()) {
            while (!WriteQueue.empty() && ((*AckSeqNo < seqNo) || WriteQueue.front().Skipped)) {
                if (WriteQueue.front().Skipped) {
                    skippedMessagesCount++;
                    skippedBytesCount += WriteQueue.front().Size;
                } else {
                    ++messagesCount;
                    bytesCount += WriteQueue.front().Size;
                }
                ++(*AckSeqNo);
                WriteQueue.pop_front();
                ++TrimmedCount;
            }
        }
        if (!AckSeqNo.Defined() || seqNo > *AckSeqNo) {
            AckSeqNo = seqNo;
        }

        Counters->AcknowledgedMessages += messagesCount;
        Counters->AcknowledgedBytes += bytesCount;
        Counters->InflightMessages -= (messagesCount + skippedMessagesCount);
        Counters->InflightBytes -= (bytesCount + skippedBytesCount);
        InflightBytes.fetch_sub(bytesCount);
        Counters->GrpcInflightMessages -= messagesCount;
        Counters->GrpcInflightBytes -= bytesCount;
        GrpcInflightMessages -= messagesCount;
        GrpcInflightBytes -= bytesCount;

        YLOG_DEBUG(Sprintf("ack [%" PRIu64 "], [%zu] messages, [%zu] bytes", seqNo, messagesCount, bytesCount));
    }

    void TClientSession::OnGrpcCallInitialized(const TString& sessionId, ui64 lastSeqNo) {
        SessionId = sessionId;
        Acknowledge(lastSeqNo);
        NextIndex = TrimmedCount;
        ++Counters->GrpcCallsInitialized;
        Counters->GrpcInflightMessages -= GrpcInflightMessages;
        Counters->GrpcInflightBytes -= GrpcInflightBytes;
        GrpcInflightMessages = 0;
        GrpcInflightBytes = 0;
        YLOG_INFO(Sprintf("grpc call initialized, session_id [%s], last_seq_no [%" PRIu64 "]",
                          sessionId.c_str(), lastSeqNo));
    }

    void TClientSession::OnGrpcCallFinished() {
        Y_ABORT_UNLESS(!Closed);
        Y_ABORT_UNLESS(ActiveGrpcCall);
        ActiveGrpcCall = nullptr;
        if (CloseStarted && (ForcedCloseStarted || WriteQueue.empty())) {
            DoClose();
        } else {
            const auto reconnectTime = TInstant::Now() + Client->GetParameters().GrpcReconnectDelay;
            MakeGrpcCallTimer->Set(reconnectTime);
            YLOG_INFO(Sprintf("grpc call delayed until [%s]", reconnectTime.ToString().c_str()));
        }
    }

    auto TClientSession::PurgeWriteQueue() -> TPurgeWriteQueueStats {
        size_t bytesCount = 0;
        for (const auto& m: WriteQueue) {
            bytesCount += m.Size;
        }
        auto result = TPurgeWriteQueueStats{WriteQueue.size(), bytesCount};

        Counters->DroppedMessages += WriteQueue.size();
        Counters->DroppedBytes += bytesCount;
        Counters->InflightMessages -= WriteQueue.size();
        Counters->InflightBytes -= bytesCount;
        Counters->GrpcInflightMessages -= GrpcInflightMessages;
        Counters->GrpcInflightBytes -= GrpcInflightBytes;

        InflightBytes.fetch_sub(bytesCount);
        GrpcInflightMessages = 0;
        GrpcInflightBytes = 0;
        WriteQueue.clear();

        return result;
    }

    void TClientSession::DoClose() {
        Y_ABORT_UNLESS(CloseStarted);
        Y_ABORT_UNLESS(!Closed);
        Y_ABORT_UNLESS(!ClosePromise.HasValue());
        MakeGrpcCallTimer->Cancel();
        ForceCloseTimer->Cancel();
        PollTimer->Cancel();
        if (!ForkInProgressLocal && WriteQueue.size() > 0) {
            const auto stats = PurgeWriteQueue();
            ++Counters->ErrorsCount;
            YLOG_ERR(Sprintf("DoClose, dropped [%zu] messages, [%zu] bytes",
                             stats.PurgedMessages, stats.PurgedBytes));
        }
        --Client->GetCounters()->ActiveSessionsCount;
        Closed = true;
        ClosePromise.SetValue();
        YLOG_DEBUG("session closed");
    }

    TGrpcCall::TGrpcCall(TClientSession& session)
        : Session(session)
        , AsyncJoinerToken(&Session.GetAsyncJoiner())
        , AcceptTag(MakeIOCallback(this, &TGrpcCall::EndAccept))
        , ReadTag(MakeIOCallback(this, &TGrpcCall::EndRead))
        , WriteTag(MakeIOCallback(this, &TGrpcCall::EndWrite))
        , WritesDoneTag(MakeIOCallback(this, &TGrpcCall::EndWritesDone))
        , FinishTag(MakeIOCallback(this, &TGrpcCall::EndFinish))
        , Logger(session.GetLogger().Child("grpc"))
        , AcceptPending(false)
        , Initialized_(false)
        , ReadPending(false)
        , ReadsDone(false)
        , WritePending(false)
        , WritesBlocked(false)
        , WritesDonePending(false)
        , WritesDone(false)
        , ErrorOccured(false)
        , FinishRequested(false)
        , FinishStarted(false)
        , FinishDone(false)
        , Cancelled(false)
        , Poisoned(false)
        , PoisonPillSent(false)
        , ReuseSessions_(false)
        , FinishStatus()
        , ClientContext()
        , ReaderWriter(nullptr)
        , Request()
        , Response()
    {
    }

    void TGrpcCall::Start() {
        AcceptPending = true;
        auto& client = Session.GetClient();
        ReaderWriter = client.GetStub().AsyncSession(&ClientContext,
                                                     &client.GetCompletionQueue(),
                                                     AcceptTag->Ref());
        YLOG_DEBUG("AsyncSession started");
    }

    TGrpcCall::~TGrpcCall() {
        YLOG_DEBUG("destroyed");
    }

    void TGrpcCall::EnsureFinishStarted() {
        if (!FinishStarted) {
            FinishStarted = true;
            ReaderWriter->Finish(&FinishStatus, FinishTag->Ref());
            YLOG_DEBUG("Finish started");
        }
    }

    bool TGrpcCall::CheckHasError(EIOStatus status, const char* method) {
        if (status == EIOStatus::Error) {
            SetError(Sprintf("%s %s", method, ToString(status).c_str()));
            return true;
        }
        if (ErrorOccured) {
            ScheduleFinishOnError();
            return true;
        }
        return false;
    }

    void TGrpcCall::SetError(const TString& error) {
        if (!Cancelled) {
            YLOG_ERR(error);
            ++Session.GetCounters().ErrorsCount;
        }
        ErrorOccured = true;
        ScheduleFinishOnError();
    }

    void TGrpcCall::ScheduleFinishOnError() {
        if (!AcceptPending && !WritePending && !WritesDonePending) {
            EnsureFinishStarted();
        }
    }

    void TGrpcCall::BeginClose(bool force) {
        if (force) {
            if (!Cancelled) {
                Cancelled = true;
                ClientContext.TryCancel();
                SetError("forced close");
            }
            return;
        }
        YLOG_DEBUG(Sprintf("Close Initialized [%d], AcceptPending [%d], "
                         "WritePending [%d], FinishRequested [%d], "
                         "ErrorOccured [%d]",
                         static_cast<int>(Initialized_),
                         static_cast<int>(AcceptPending),
                         static_cast<int>(WritePending),
                         static_cast<int>(FinishRequested),
                         static_cast<int>(ErrorOccured)));
        if (ErrorOccured || FinishRequested) {
            return;
        }
        FinishRequested = true;
        if (!Initialized_ || WritePending) {
            return;
        }
        WritesBlocked = true;
        BeginWritesDone();
    }

    void TGrpcCall::Poison() {
        Poisoned = true;
        NotifyMessageAdded();
    }

    void TGrpcCall::NotifyMessageAdded() {
        if (WritePending || !Initialized_ || ErrorOccured || FinishRequested) {
            return;
        }
        ScheduleWrite();
    }

    void TGrpcCall::ScheduleWrite() {
        Request.Clear();
        if (!Poisoned) {
            Session.PrepareWriteBatchRequest(Request);
        } else if (!PoisonPillSent) {
            PoisonPillSent = true;
            auto& batch = *Request.mutable_data_batch();
            batch.AddSeqNo(std::numeric_limits<::google::protobuf::uint64>::max());
            batch.AddTimestamp(Now().MicroSeconds());
            batch.AddPayload("");
            YLOG_INFO("poison pill sent");
        }
        if (Request.GetDataBatch().GetSeqNo().empty()) {
            if (FinishRequested) {
                WritesBlocked = true;
                BeginWritesDone();
            }
            return;
        }

        BeginWrite();
    }

    void TGrpcCall::EndAccept(EIOStatus status) {
        Y_ABORT_UNLESS(AcceptPending);
        AcceptPending = false;
        if (CheckHasError(status, "EndAccept")) {
            return;
        }
        BeginRead();
        Request.Clear();
        Session.PrepareInitializeRequest(Request);
        BeginWrite();
    }

    void TGrpcCall::EndRead(EIOStatus status) {
        ReadPending = false;
        if (FinishDone) {
            Session.OnGrpcCallFinished();
            return;
        }
        if (!ErrorOccured && status == EIOStatus::Error && WritesBlocked) {
            Y_ABORT_UNLESS(!WritePending);
            YLOG_DEBUG("EndRead ReadsDone");
            ReadsDone = true;
            if (WritesDone) {
                EnsureFinishStarted();
                return;
            }
            return;
        }
        if (CheckHasError(status, "EndRead")) {
            return;
        }
        if (!Initialized_) {
            const auto metadata = ClientContext.GetServerInitialMetadata();
            {
                const auto it = metadata.find("ua-reuse-sessions");
                if (it != metadata.end() && it->second == "true") {
                    ReuseSessions_ = true;
                }
            }
            {
                const auto it = metadata.find("ua-max-receive-message-size");
                if (it != metadata.end()) {
                    Session.SetAgentMaxReceiveMessage(FromString<size_t>(TString{it->second.begin(), it->second.end()}));
                }
            }

            if (Response.response_case() != NUnifiedAgentProto::Response::kInitialized) {
                SetError(Sprintf("EndRead while initializing, unexpected response_case [%d]",
                                  static_cast<int>(Response.response_case())));
                return;
            }
            Session.OnGrpcCallInitialized(Response.GetInitialized().GetSessionId(),
                                          Response.GetInitialized().GetLastSeqNo());
            Initialized_ = true;
            if (!WritePending) {
                ScheduleWrite();
            }
        } else {
            if (Response.response_case() != NUnifiedAgentProto::Response::kAck) {
                SetError(Sprintf("EndRead unexpected response_case [%d]",
                                 static_cast<int>(Response.response_case())));
                return;
            }
            Session.Acknowledge(Response.GetAck().GetSeqNo());
        }
        BeginRead();
    }

    void TGrpcCall::EndWrite(EIOStatus status) {
        WritePending = false;
        if (CheckHasError(status, "EndWrite")) {
            return;
        }
        if (!Initialized_) {
            return;
        }
        ScheduleWrite();
    }

    void TGrpcCall::EndFinish(EIOStatus status) {
        FinishDone = true;
        const auto finishStatus = status == EIOStatus::Error
            ? grpc::Status(grpc::UNKNOWN, "finish error")
            : FinishStatus;
        YLOG(finishStatus.ok() || Cancelled || Poisoned  ? TLOG_DEBUG : TLOG_ERR,
            Sprintf("EndFinish, code [%s], message [%s]",
                ToString(finishStatus.error_code()).c_str(),
                finishStatus.error_message().c_str()),
             Logger);
        if (!finishStatus.ok() && !Cancelled) {
            ++Session.GetCounters().ErrorsCount;
        }
        if (!ReadPending) {
            Session.OnGrpcCallFinished();
        }
    }

    void TGrpcCall::EndWritesDone(EIOStatus status) {
        YLOG_DEBUG(Sprintf("EndWritesDone [%s]", ToString(status).c_str()));
        Y_ABORT_UNLESS(!WritePending && !WritesDone && WritesDonePending);
        WritesDonePending = false;
        WritesDone = true;
        if (CheckHasError(status, "EndWriteDone")) {
            return;
        }
        if (ReadsDone) {
            EnsureFinishStarted();
        }
    }

    void TGrpcCall::BeginWritesDone() {
        WritesDonePending = true;
        ReaderWriter->WritesDone(WritesDoneTag->Ref());
        YLOG_DEBUG("WritesDone started");
    }

    void TGrpcCall::BeginRead() {
        ReadPending = true;
        Response.Clear();
        ReaderWriter->Read(&Response, ReadTag->Ref());
        YLOG_DEBUG("Read started");
    }

    void TGrpcCall::BeginWrite() {
        WritePending = true;
        ReaderWriter->Write(Request, WriteTag->Ref());
        YLOG_DEBUG("Write started");
    }
}

namespace NUnifiedAgent {
    size_t SizeOf(const TClientMessage& message) {
        auto result = message.Payload.Size() + sizeof(TInstant);
        if (message.Meta.Defined()) {
            for (const auto& m: *message.Meta) {
                result += m.first.Size() + m.second.Size();
            }
        }
        return result;
    }

    TClientParameters::TClientParameters(const TString& uri)
        : Uri(uri)
        , SharedSecretKey(Nothing())
        , MaxInflightBytes(DefaultMaxInflightBytes)
        , Log(TLoggerOperator<TGlobalLog>::Log())
        , LogRateLimitBytes(Nothing())
        , GrpcReconnectDelay(TDuration::MilliSeconds(50))
        , GrpcSendDelay(DefaultGrpcSendDelay)
        , EnableForkSupport(false)
        , GrpcMaxMessageSize(DefaultGrpcMaxMessageSize)
        , Counters(nullptr)
    {
    }

    TSessionParameters::TSessionParameters()
        : SessionId(Nothing())
        , Meta(Nothing())
        , Counters(nullptr)
        , MaxInflightBytes()
    {
    }

    const size_t TClientParameters::DefaultMaxInflightBytes = 10_MB;
    const size_t TClientParameters::DefaultGrpcMaxMessageSize = 1_MB;
    const TDuration TClientParameters::DefaultGrpcSendDelay = TDuration::MilliSeconds(10);

    TClientPtr MakeClient(const TClientParameters& parameters) {

        // Initialization of the Fork in newest grcp core is performed
        // in 'do_basic_init', which is called inside 'grpc_is_initialized'.
        // So the set of the fork env variable has to be done before
        // grpc_is_initialized call.
#ifdef _unix_
        if (parameters.EnableForkSupport) {
            SetEnv("GRPC_ENABLE_FORK_SUPPORT", "true");
        }
#endif
        if (!grpc_is_initialized()) {
            EnsureGrpcConfigured();
        }

        std::shared_ptr<NPrivate::TForkProtector> forkProtector{};
#ifdef _unix_
        if (parameters.EnableForkSupport) {
            forkProtector = NPrivate::TForkProtector::Get(true);
        }
#endif
        return MakeIntrusive<NPrivate::TClient>(parameters, forkProtector);
    }
}
