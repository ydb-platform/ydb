#include "consumer.h"
#include "channel.h"
#include "persqueue_p.h"

#include <kikimr/yndx/persqueue/read_batch_converter/read_batch_converter.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>

#include <util/generic/strbuf.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/string/vector.h>
#include <util/string/builder.h>

#include <grpc++/create_channel.h>

namespace NPersQueue {

class TConsumerDestroyHandler : public IHandler {
public:
    TConsumerDestroyHandler(std::weak_ptr<TConsumer> ptr, TIntrusivePtr<TConsumer::TRpcStuff> rpcStuff,
                            const void* queueTag, TPQLibPrivate* pqLib)
        : Ptr(std::move(ptr))
        , RpcStuff(std::move(rpcStuff))
        , QueueTag(queueTag)
        , PQLib(pqLib)
    {}

    void Destroy(const TError& reason) override {
        auto consumer = Ptr;
        auto handler = [consumer, reason] {
            auto consumerShared = consumer.lock();
            if (consumerShared) {
                consumerShared->Destroy(reason);
            }
        };
        Y_VERIFY(PQLib->GetQueuePool().GetQueue(QueueTag).AddFunc(handler));
    }

    TString ToString() override { return "DestroyHandler"; }

protected:
    std::weak_ptr<TConsumer> Ptr;
    TIntrusivePtr<TConsumer::TRpcStuff> RpcStuff; // must simply live
    const void* QueueTag;
    TPQLibPrivate* PQLib;
};

class TConsumerStreamCreated : public TConsumerDestroyHandler {
public:
    TConsumerStreamCreated(std::weak_ptr<TConsumer> ptr, TIntrusivePtr<TConsumer::TRpcStuff> rpcStuff,
                           const void* queueTag, TPQLibPrivate* pqLib)
        : TConsumerDestroyHandler(std::move(ptr), std::move(rpcStuff), queueTag, pqLib)
    {}

    void Done() override {
        auto consumer = Ptr;
        auto handler = [consumer] {
            auto consumerShared = consumer.lock();
            if (consumerShared) {
                consumerShared->OnStreamCreated();
            }
        };
        Y_VERIFY(PQLib->GetQueuePool().GetQueue(QueueTag).AddFunc(handler));
    }

    TString ToString() override { return "StreamCreated"; }
};

class TConsumerWriteDone : public TConsumerDestroyHandler {
public:
    TConsumerWriteDone(std::weak_ptr<TConsumer> ptr, TIntrusivePtr<TConsumer::TRpcStuff> rpcStuff,
                       const void* queueTag, TPQLibPrivate* pqLib)
        : TConsumerDestroyHandler(std::move(ptr), std::move(rpcStuff), queueTag, pqLib)
    {}

    void Done() override {
        auto consumer = Ptr;
        auto handler = [consumer] {
            auto consumerShared = consumer.lock();
            if (consumerShared) {
                consumerShared->OnWriteDone();
            }
        };
        Y_VERIFY(PQLib->GetQueuePool().GetQueue(QueueTag).AddFunc(handler));
    }

    TString ToString() override { return "WriteDone"; }
};

class TConsumerReadDone : public TConsumerDestroyHandler {
public:
    TConsumerReadDone(std::weak_ptr<TConsumer> ptr, TIntrusivePtr<TConsumer::TRpcStuff> rpcStuff,
                      const void* queueTag, TPQLibPrivate* pqLib)
        : TConsumerDestroyHandler(std::move(ptr), std::move(rpcStuff), queueTag, pqLib)
    {}

    void Done() override {
        auto consumer = Ptr;
        auto handler = [consumer] {
            auto consumerShared = consumer.lock();
            if (consumerShared) {
                consumerShared->OnReadDone();
            }
        };
        Y_VERIFY(PQLib->GetQueuePool().GetQueue(QueueTag).AddFunc(handler));
    }

    TString ToString() override { return "ReadDone"; }
};

void TConsumer::Destroy() noexcept {
    IsDestroying = true;
    RpcStuff->Context.TryCancel();
    ChannelHolder.ChannelPtr = nullptr; //if waiting for channel creation
    Destroy("Destructor called");
}

TConsumer::TConsumer(const TConsumerSettings& settings, std::shared_ptr<grpc::CompletionQueue> cq,
                     NThreading::TPromise<TConsumerCreateResponse> promise, std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib, TIntrusivePtr<ILogger> logger) noexcept
    : IConsumerImpl(std::move(destroyEventRef), std::move(pqLib))
    , RpcStuff(new TRpcStuff())
    , Settings(settings)
    , StartPromise(promise)
    , IsDeadPromise(NThreading::NewPromise<TError>())
    , UncommittedCount(0)
    , UncommittedSize(0)
    , MemoryUsage(0)
    , ReadsOrdered(0)
    , EstimateReadSize(settings.MaxSize)
    , WriteInflight(true)
    , ProxyCookie(0)
    , Logger(std::move(logger))
    , IsDestroyed(false)
    , IsDestroying(false)
{
    RpcStuff->CQ = std::move(cq);
}

void TConsumer::Init()
{
    std::weak_ptr<TConsumer> self(shared_from_this());
    StreamCreatedHandler.Reset(new TConsumerStreamCreated(self, RpcStuff, this, PQLib.Get()));
    ReadDoneHandler.Reset(new TConsumerReadDone(self, RpcStuff, this, PQLib.Get()));
    WriteDoneHandler.Reset(new TConsumerWriteDone(self, RpcStuff, this, PQLib.Get()));
}

void TConsumer::DoStart(TInstant deadline)
{
    if (IsDestroyed)
        return;

    if (deadline != TInstant::Max()) {
        std::weak_ptr<TConsumer> self = shared_from_this();
        auto onStartTimeout = [self] {
            auto selfShared = self.lock();
            if (selfShared) {
                selfShared->OnStartTimeout();
            }
        };
        StartDeadlineCallback =
            PQLib->GetScheduler().Schedule(deadline, this, onStartTimeout);
    }

    FillMetaHeaders(RpcStuff->Context, Settings.Server.Database, Settings.CredentialsProvider.get());
    RpcStuff->Stub = PersQueueService::NewStub(RpcStuff->Channel);
    RpcStuff->Stream = RpcStuff->Stub->AsyncReadSession(&RpcStuff->Context, RpcStuff->CQ.get(), new TQueueEvent(StreamCreatedHandler));
}

NThreading::TFuture<TConsumerCreateResponse> TConsumer::Start(TInstant deadline) noexcept
{
    NThreading::TFuture<TConsumerCreateResponse> ret = StartPromise.GetFuture();
    if (ChannelHolder.ChannelInfo.Initialized()) {
        std::weak_ptr<TConsumer> self = shared_from_this();
        PQLib->Subscribe(ChannelHolder.ChannelInfo,
                         this,
                         [self, deadline](const auto& f) {
                             auto sharedSelf = self.lock();
                             if (sharedSelf) {
                                 sharedSelf->SetChannel(f.GetValue());
                                 sharedSelf->DoStart(deadline);
                             }
                         });
    } else {
        if (RpcStuff->Channel && ProxyCookie) {
            DoStart(deadline);
        } else {
            Destroy("No channel");
        }
    }
    return ret;
}

void TConsumer::SetChannel(const TChannelHolder& channel) noexcept {
    if (IsDestroyed)
        return;

    ChannelHolder = channel;
}

void TConsumer::SetChannel(const TChannelInfo& channel) noexcept {
    if (!channel.Channel || channel.ProxyCookie == 0) {
        Destroy("Channel creation error");
        return;
    }

    if (IsDestroyed)
        return;
    Y_VERIFY(!RpcStuff->Channel);
    RpcStuff->Channel = channel.Channel;
    ProxyCookie = channel.ProxyCookie;
}


TConsumer::~TConsumer() noexcept {
    Destroy();
}


/************************************************read*****************************************************/

void TConsumer::OrderRead() noexcept {
    if (IsDeadPromise.HasValue()) {
        return;
    }

    if (StartPromise.Initialized()) {
        return;
    }

    if (MemoryUsage >= Settings.MaxMemoryUsage || ReadsOrdered >= Settings.MaxInflyRequests) {
        return;
    }
    if (Settings.MaxUncommittedCount > 0 && UncommittedCount >= Settings.MaxUncommittedCount) {
        return;
    }
    if (Settings.MaxUncommittedSize > 0 && UncommittedSize >= Settings.MaxUncommittedSize) {
        return;
    }


    if (IsDeadPromise.HasValue()) {
        return;
    } else {
        while (ReadsOrdered < Settings.MaxInflyRequests && MemoryUsage + ((ui64)ReadsOrdered) * EstimateReadSize <= Settings.MaxMemoryUsage) {
            TReadRequest request;
            Settings.CredentialsProvider->FillAuthInfo(request.MutableCredentials());
            request.MutableRead()->SetMaxCount(Settings.MaxCount);
            request.MutableRead()->SetMaxSize(Settings.MaxSize);
            request.MutableRead()->SetPartitionsAtOnce(Settings.PartsAtOnce);
            request.MutableRead()->SetMaxTimeLagMs(Settings.MaxTimeLagMs);
            request.MutableRead()->SetReadTimestampMs(Settings.ReadTimestampMs);
            Requests.emplace_back(std::move(request));
            ++ReadsOrdered;
            DEBUG_LOG("read ordered memusage " << MemoryUsage << " infly " << ReadsOrdered << " maxmemusage "
                      << Settings.MaxMemoryUsage, "", SessionId);
        }

        DoWrite();
    }
}

void TConsumer::OnWriteDone() {
    WriteInflight = false;
    DoWrite();
}

void TConsumer::DoWrite() {
    if (IsDestroyed) {
        return;
    }

    if (WriteInflight || Requests.empty())
        return;

    WriteInflight = true;
    TReadRequest req = Requests.front();
    Requests.pop_front();
    DEBUG_LOG("sending request " << req.GetRequestCase(), "", SessionId);
    RpcStuff->Stream->Write(req, new TQueueEvent(WriteDoneHandler));
}


void TConsumer::OnStreamCreated() {
    if (IsDestroyed) {
        return;
    }

    TReadRequest req;
    Settings.CredentialsProvider->FillAuthInfo(req.MutableCredentials());

    TReadRequest::TInit* const init = req.MutableInit();
    init->SetClientId(Settings.ClientId);
    for (const auto& t : Settings.Topics) {
        init->AddTopics(t);
    }
    init->SetReadOnlyLocal(!Settings.ReadMirroredPartitions);
    init->SetClientsideLocksAllowed(Settings.UseLockSession);
    init->SetProxyCookie(ProxyCookie);
    init->SetProtocolVersion(TReadRequest::ReadParamsInInit);
    init->SetBalancePartitionRightNow(Settings.BalanceRightNow);
    init->SetCommitsDisabled(Settings.CommitsDisabled);

    for (auto g : Settings.PartitionGroups) {
        init->AddPartitionGroups(g);
    }

    // Read settings
    init->SetMaxReadMessagesCount(Settings.MaxCount);
    init->SetMaxReadSize(Settings.MaxSize);
    init->SetMaxReadPartitionsCount(Settings.PartsAtOnce);
    init->SetMaxTimeLagMs(Settings.MaxTimeLagMs);
    init->SetReadTimestampMs(Settings.ReadTimestampMs);

    WriteInflight = true;
    RpcStuff->Stream->Write(req, new TQueueEvent(WriteDoneHandler));

    RpcStuff->Response.Clear();
    RpcStuff->Stream->Read(&RpcStuff->Response, new TQueueEvent(ReadDoneHandler));
}

void TConsumer::OnReadDone() {
    if (RpcStuff->Response.HasError()) {
        Destroy(RpcStuff->Response.GetError());
        return;
    }

    if (IsDeadPromise.HasValue())
        return;
    if (StartPromise.Initialized()) { //init response
        NThreading::TPromise<TConsumerCreateResponse> tmp;
        tmp.Swap(StartPromise);

        Y_VERIFY(RpcStuff->Response.HasInit());
        auto res(std::move(RpcStuff->Response));
        RpcStuff->Response.Clear();
        RpcStuff->Stream->Read(&RpcStuff->Response, new TQueueEvent(ReadDoneHandler));

        SessionId = res.GetInit().GetSessionId();
        DEBUG_LOG("read stream created", "", SessionId);

        tmp.SetValue(TConsumerCreateResponse{std::move(res)});

        OrderRead();

        return;
    }
    if (RpcStuff->Response.HasBatchedData()) {
        ConvertToOldBatch(RpcStuff->Response); // LOGBROKER-3173
    }
    if (RpcStuff->Response.HasData()) { //read response
        NThreading::TPromise<TConsumerMessage> p(NThreading::NewPromise<TConsumerMessage>());
        MessageResponses.push_back(p.GetFuture());
        const ui32 sz = RpcStuff->Response.ByteSize();
        MemoryUsage += sz;
        Y_VERIFY(ReadsOrdered);
        --ReadsOrdered;

        ui32 cnt = 0;
        for (ui32 i = 0; i < RpcStuff->Response.GetData().MessageBatchSize(); ++i) {
            cnt += RpcStuff->Response.GetData().GetMessageBatch(i).MessageSize();
        }
        if (!Settings.CommitsDisabled) {
            ReadInfo.push_back({RpcStuff->Response.GetData().GetCookie(), {cnt, sz}});
            UncommittedSize += sz;
            UncommittedCount += cnt;
        }

        DEBUG_LOG("read done memusage " << MemoryUsage << " infly " << ReadsOrdered << " size " << sz
                  << " ucs " << UncommittedSize << " ucc " << UncommittedCount, "", SessionId);

        auto res(std::move(RpcStuff->Response));

        RpcStuff->Response.Clear();
        RpcStuff->Stream->Read(&RpcStuff->Response, new TQueueEvent(ReadDoneHandler));

        OrderRead();

        Y_VERIFY(MemoryUsage >= sz);
        EstimateReadSize = res.ByteSize();
        OrderRead();

        p.SetValue(TConsumerMessage{std::move(res)});
    } else if (RpcStuff->Response.HasLock() || RpcStuff->Response.HasRelease() || RpcStuff->Response.HasCommit() || RpcStuff->Response.HasPartitionStatus()) { //lock/release/commit response
        if (!RpcStuff->Response.HasLock()) {
            INFO_LOG("got from server " << RpcStuff->Response, "", SessionId);
            if (RpcStuff->Response.HasCommit() && !Settings.CommitsDisabled) {
                for (ui32 i = 0; i < RpcStuff->Response.GetCommit().CookieSize(); ++i) {
                    ui64 cookie = RpcStuff->Response.GetCommit().GetCookie(i);
                    Y_VERIFY(!ReadInfo.empty() && ReadInfo.front().first == cookie);
                    Y_VERIFY(UncommittedCount >= ReadInfo.front().second.first);
                    Y_VERIFY(UncommittedSize >= ReadInfo.front().second.second);
                    UncommittedCount -= ReadInfo.front().second.first;
                    UncommittedSize -= ReadInfo.front().second.second;
                    ReadInfo.pop_front();
                }
            }
            MessageResponses.push_back(NThreading::MakeFuture<TConsumerMessage>(TConsumerMessage(std::move(RpcStuff->Response))));
            OrderRead();
        } else {
            const auto& p = RpcStuff->Response.GetLock();
            NThreading::TPromise<TLockInfo> promise(NThreading::NewPromise<TLockInfo>());
            //TODO: add subscribe
            auto f = promise.GetFuture();
            std::weak_ptr<TConsumer> ptr = shared_from_this();

            //will take ptr for as long as promises are not set
            PQLib->Subscribe(f,
                             this,
                             [ptr, p](const auto& f) {
                                 auto consumer = ptr.lock();
                                 if (consumer) {
                                     consumer->Lock(p.GetTopic(), p.GetPartition(), p.GetGeneration(), f.GetValue().ReadOffset, f.GetValue().CommitOffset,
                                                    f.GetValue().VerifyReadOffset);
                                 }
                             });

            INFO_LOG("got LOCK from server " << p, "", SessionId);
            MessageResponses.push_back(NThreading::MakeFuture<TConsumerMessage>(TConsumerMessage(std::move(RpcStuff->Response), std::move(promise))));
        }

        RpcStuff->Response.Clear();
        RpcStuff->Stream->Read(&RpcStuff->Response, new TQueueEvent(ReadDoneHandler));
    } else {
        Y_FAIL("unsupported response %s", RpcStuff->Response.DebugString().c_str());
    }

    ProcessResponses();
}


/******************************************commit****************************************************/
void TConsumer::Commit(const TVector<ui64>& cookies) noexcept {
    auto sortedCookies = cookies;
    std::sort(sortedCookies.begin(), sortedCookies.end());

    TReadRequest request;
    for (const auto& cookie : sortedCookies) {
        request.MutableCommit()->AddCookie(cookie);
    }

    DEBUG_LOG("sending COMMIT to server " << request.GetCommit(), "", SessionId);
    if (IsDestroyed || StartPromise.Initialized()) {
        ERR_LOG("trying to commit " << (IsDestroyed ? "after destroy" : "before start"), "", SessionId);
        return;
    }
    Requests.emplace_back(std::move(request));
    DoWrite();
}


void TConsumer::RequestPartitionStatus(const TString& topic, ui64 partition, ui64 generation) noexcept {
    TReadRequest request;
    auto status = request.MutableStatus();
    status->SetTopic(topic);
    status->SetPartition(partition);
    status->SetGeneration(generation);

    DEBUG_LOG("sending GET_STATUS to server " << request.GetStatus(), "", SessionId);
    if (IsDestroyed || StartPromise.Initialized()) {
        ERR_LOG("trying to get status " << (IsDestroyed ? "after destroy" : "before start"), "", SessionId);
        return;
    }
    Requests.emplace_back(std::move(request));
    DoWrite();
}

void TConsumer::GetNextMessage(NThreading::TPromise<TConsumerMessage>& promise) noexcept {
    if (IsDeadPromise.HasValue()) {
        TReadResponse res;
        res.MutableError()->SetDescription("consumer is dead");
        res.MutableError()->SetCode(NErrorCode::ERROR);
        promise.SetValue(TConsumerMessage{std::move(res)});
        return;
    }

    if (StartPromise.Initialized()) {
        TReadResponse res;
        res.MutableError()->SetDescription("consumer is not ready");
        res.MutableError()->SetCode(NErrorCode::ERROR);
        promise.SetValue(TConsumerMessage{std::move(res)});
        return;
    }

    MessagePromises.push_back(promise);
    ProcessResponses(); //if alredy have an answer
}


void TConsumer::Lock(const TString& topic, const ui32 partition, const ui64 generation, const ui64 readOffset,
                     const ui64 commitOffset, const bool verifyReadOffset) noexcept {
    Y_VERIFY(Settings.UseLockSession);

    TReadRequest request;
    request.MutableStartRead()->SetTopic(topic);
    request.MutableStartRead()->SetPartition(partition);
    request.MutableStartRead()->SetReadOffset(readOffset);
    request.MutableStartRead()->SetCommitOffset(commitOffset);
    request.MutableStartRead()->SetVerifyReadOffset(verifyReadOffset);
    request.MutableStartRead()->SetGeneration(generation);
    INFO_LOG("sending START_READ to server " << request.GetStartRead(), "", SessionId);

    Settings.CredentialsProvider->FillAuthInfo(request.MutableCredentials());

    if (IsDestroyed)
        return;
    Requests.emplace_back(std::move(request));
    DoWrite();
}


void TConsumer::ProcessResponses() {
    while(true) {
        if (MessagePromises.empty() || MessageResponses.empty() || !MessageResponses.front().HasValue())
            break;
        auto p(std::move(MessagePromises.front()));
        MessagePromises.pop_front();
        auto res(MessageResponses.front().ExtractValue());
        MessageResponses.pop_front();
        if (res.Type == EMT_DATA) {
            ui32 sz = res.Response.ByteSize();
            Y_VERIFY(MemoryUsage >= sz);
            MemoryUsage -= sz;
        }

        if (res.Type == EMT_DATA) {
            OrderRead();
        }

        p.SetValue(TConsumerMessage{std::move(res)});
    }
}

/***********************************************************************************/

NThreading::TFuture<TError> TConsumer::IsDead() noexcept {
    return IsDeadPromise.GetFuture();
}

void TConsumer::Destroy(const TString& description) {
    TError error;
    error.SetDescription(description);
    error.SetCode(NErrorCode::ERROR);
    Destroy(error);
}

void TConsumer::Destroy(const TError& error) {
    if (!IsDestroying) {
        INFO_LOG("error: " << error, "", SessionId);
    }

    if (IsDestroyed)
        return;
    IsDestroyed = true;

    IsDeadPromise.SetValue(error);

    MessageResponses.clear();

    NThreading::TFuture<TChannelInfo> tmp;
    tmp.Swap(ChannelHolder.ChannelInfo);
    ChannelHolder.ChannelPtr = nullptr;

    Error = error;
    if (StartPromise.Initialized()) {
        NThreading::TPromise<TConsumerCreateResponse> tmp;
        tmp.Swap(StartPromise);
        TReadResponse res;
        res.MutableError()->CopyFrom(Error);
        tmp.SetValue(TConsumerCreateResponse{std::move(res)});
    }

    while (!MessagePromises.empty()) {
        auto p = MessagePromises.front();
        MessagePromises.pop_front();
        TReadResponse res;
        res.MutableError()->CopyFrom(Error);
        p.SetValue(TConsumerMessage{std::move(res)});
    }

    StreamCreatedHandler.Reset();
    ReadDoneHandler.Reset();
    WriteDoneHandler.Reset();

    DestroyPQLibRef();
}

void TConsumer::OnStartTimeout() {
    if (IsDestroyed) {
        return;
    }

    StartDeadlineCallback = nullptr;
    if (!StartPromise.Initialized()) {
        // already replied with successful start
        return;
    }

    TError error;
    error.SetDescription("Start timeout");
    error.SetCode(NErrorCode::CREATE_TIMEOUT);
    Destroy(error);
}

void TConsumer::Cancel() {
    IsDestroying = true;
    RpcStuff->Context.TryCancel();
    ChannelHolder.ChannelPtr = nullptr; //if waiting for channel creation

    Destroy(GetCancelReason());
}

}
