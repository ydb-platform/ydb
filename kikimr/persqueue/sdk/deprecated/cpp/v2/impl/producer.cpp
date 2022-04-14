#include <util/generic/strbuf.h>
#include <util/stream/zlib.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/string/vector.h>
#include <util/string/builder.h>

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/impl/producer.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/impl/persqueue_p.h>

#include <grpc++/create_channel.h>


namespace NPersQueue {

class TProducerDestroyHandler : public IHandler {
public:
    TProducerDestroyHandler(std::weak_ptr<TProducer> ptr, TIntrusivePtr<TProducer::TRpcStuff> rpcStuff,
                            const void* queueTag, TPQLibPrivate* pqLib)
        : Ptr(std::move(ptr))
        , RpcStuff(std::move(rpcStuff))
        , QueueTag(queueTag)
        , PQLib(pqLib)
    {}

    void Destroy(const TError&) override {
        auto producer = Ptr;
        auto handler = [producer] {
            auto producerShared = producer.lock();
            if (producerShared) {
                producerShared->OnFail();
            }
        };
        Y_VERIFY(PQLib->GetQueuePool().GetQueue(QueueTag).AddFunc(handler));
    }

    TString ToString() override { return "TProducerDestroyHandler"; }

protected:
    std::weak_ptr<TProducer> Ptr;
    TIntrusivePtr<TProducer::TRpcStuff> RpcStuff; // must simply live
    const void* QueueTag;
    TPQLibPrivate* PQLib;
};

class TStreamCreated : public TProducerDestroyHandler {
public:
    TStreamCreated(std::weak_ptr<TProducer> ptr, TIntrusivePtr<TProducer::TRpcStuff> rpcStuff,
                   const void* queueTag, TPQLibPrivate* pqLib)
        : TProducerDestroyHandler(std::move(ptr), std::move(rpcStuff), queueTag, pqLib)
    {}

    void Done() override {
        auto producer = Ptr;
        auto UA = PQLib->GetUserAgent();
        auto handler = [producer, UA] {
            auto producerShared = producer.lock();
            if (producerShared) {
                producerShared->OnStreamCreated(UA);
            }
        };
        Y_VERIFY(PQLib->GetQueuePool().GetQueue(QueueTag).AddFunc(handler));
    }

    TString ToString() override { return "StreamCreated"; }
};

class TFinishDone : public TProducerDestroyHandler {
public:
    TFinishDone(std::weak_ptr<TProducer> ptr, TIntrusivePtr<TProducer::TRpcStuff> rpcStuff,
                const void* queueTag, TPQLibPrivate* pqLib)
        : TProducerDestroyHandler(std::move(ptr), std::move(rpcStuff), queueTag, pqLib)
    {}

    void Done() override {
        auto producer = Ptr;
        auto handler = [producer] {
            auto producerShared = producer.lock();
            if (producerShared) {
                producerShared->OnFinishDone();
            }
        };
        Y_VERIFY(PQLib->GetQueuePool().GetQueue(QueueTag).AddFunc(handler));
    }

    void Destroy(const TError&) override {
        Y_FAIL("Finish call failed");
    }

    TString ToString() override { return "Finish"; }
};


class TWriteDone : public TProducerDestroyHandler {
public:
    TWriteDone(std::weak_ptr<TProducer> ptr, TIntrusivePtr<TProducer::TRpcStuff> rpcStuff,
               const void* queueTag, TPQLibPrivate* pqLib)
        : TProducerDestroyHandler(std::move(ptr), std::move(rpcStuff), queueTag, pqLib)
    {}

    void Done() override {
        auto producer = Ptr;
        auto handler = [producer] {
            auto producerShared = producer.lock();
            if (producerShared) {
                producerShared->OnWriteDone();
            }
        };
        Y_VERIFY(PQLib->GetQueuePool().GetQueue(QueueTag).AddFunc(handler));
    }
    TString ToString() override { return "WriteDone"; }
};


class TReadDone : public TProducerDestroyHandler {
public:
    TReadDone(std::weak_ptr<TProducer> ptr, TIntrusivePtr<TProducer::TRpcStuff> rpcStuff,
              const void* queueTag, TPQLibPrivate* pqLib)
        : TProducerDestroyHandler(std::move(ptr), std::move(rpcStuff), queueTag, pqLib)
    {}

    void Done() override {
        auto producer = Ptr;
        auto handler = [producer] {
            auto producerShared = producer.lock();
            if (producerShared) {
                producerShared->OnReadDone();
            }
        };
        Y_VERIFY(PQLib->GetQueuePool().GetQueue(QueueTag).AddFunc(handler));
    }
    TString ToString() override { return "ReadDone"; }
};

TProducer::TProducer(const TProducerSettings& settings, std::shared_ptr<grpc::CompletionQueue> cq,
                     NThreading::TPromise<TProducerCreateResponse> promise, std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib, TIntrusivePtr<ILogger> logger) noexcept
    : IProducerImpl(std::move(destroyEventRef), std::move(pqLib))
    , RpcStuff(new TRpcStuff())
    , Settings(settings)
    , StartPromise(promise)
    , IsDeadPromise(NThreading::NewPromise<TError>())
    , Pos(0)
    , WriteInflight(false)
    , ProxyCookie(0)
    , Logger(std::move(logger))
    , IsDestroyed(false)
    , IsDestroying(false)
    , Failing(false)
{
    RpcStuff->CQ = std::move(cq);
}

TProducer::~TProducer() {
    Destroy();
}

void TProducer::Destroy() noexcept {
    IsDestroying = true;
    RpcStuff->Context.TryCancel();
    ChannelHolder.ChannelPtr = nullptr;

    Destroy("Destructor called");
}

void TProducer::DoStart(TInstant deadline) {
    if (IsDestroyed) {
        return;
    }

    if (deadline != TInstant::Max()) {
        std::weak_ptr<TProducer> self = shared_from_this();
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
    RpcStuff->Stream = RpcStuff->Stub->AsyncWriteSession(&RpcStuff->Context, RpcStuff->CQ.get(), new TQueueEvent(StreamCreatedHandler));
}

void TProducer::Init() {
    std::weak_ptr<TProducer> self = shared_from_this(); // we can't make this object in constructor, because this will be the only reference to us and ~shared_ptr() will destroy us.
    StreamCreatedHandler.Reset(new TStreamCreated(self, RpcStuff, this, PQLib.Get()));
    ReadDoneHandler.Reset(new TReadDone(self, RpcStuff, this, PQLib.Get()));
    WriteDoneHandler.Reset(new TWriteDone(self, RpcStuff, this, PQLib.Get()));
    FinishDoneHandler.Reset(new TFinishDone(self, RpcStuff, this, PQLib.Get()));
}

NThreading::TFuture<TProducerCreateResponse> TProducer::Start(TInstant deadline) noexcept {
    if (IsDestroyed) {
        TWriteResponse res;
        res.MutableError()->SetDescription("Producer is dead");
        res.MutableError()->SetCode(NErrorCode::ERROR);
        return NThreading::MakeFuture<TProducerCreateResponse>(TProducerCreateResponse{std::move(res)});
    }
    NThreading::TFuture<TProducerCreateResponse> ret = StartPromise.GetFuture();
    if (ChannelHolder.ChannelInfo.Initialized()) {
        std::weak_ptr<TProducer> self = shared_from_this();
        PQLib->Subscribe(ChannelHolder.ChannelInfo,
                         this,
                         [self, deadline](const auto& f) {
                             auto selfShared = self.lock();
                             if (selfShared) {
                                 selfShared->SetChannel(f.GetValue());
                                 selfShared->DoStart(deadline);
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

void TProducer::SetChannel(const TChannelHolder& channel) noexcept {
    if (IsDestroyed) {
        return;
    }

    ChannelHolder = channel;
}

void TProducer::SetChannel(const TChannelInfo& channel) noexcept {
    if (IsDestroyed) {
        return;
    }

    Y_VERIFY(!RpcStuff->Channel);
    if (!channel.Channel || channel.ProxyCookie == 0) {
        if (channel.Channel) {
            Destroy("ProxyCookie is zero");
        } else {
            Destroy("Channel creation error");
        }

        return;
    }

    RpcStuff->Channel = channel.Channel;
    ProxyCookie = channel.ProxyCookie;
}


static TProducerCommitResponse MakeErrorCommitResponse(const TString& description, TProducerSeqNo seqNo, const TData& data) {
    TWriteResponse res;
    res.MutableError()->SetDescription(description);
    res.MutableError()->SetCode(NErrorCode::ERROR);
    return TProducerCommitResponse{seqNo, data, std::move(res)};
}

void TProducer::Write(NThreading::TPromise<TProducerCommitResponse>& promise, TData data) noexcept {
    return TProducer::Write(promise, MaxSeqNo + 1, std::move(data));
}

void TProducer::Write(NThreading::TPromise<TProducerCommitResponse>& promise, const TProducerSeqNo seqNo, TData data) noexcept {
    Y_VERIFY(data.IsEncoded());

    MaxSeqNo = Max(seqNo, MaxSeqNo);

    if (IsDestroyed) {
        promise.SetValue(MakeErrorCommitResponse("producer is dead", seqNo, data));
        return;
    }

    if (StartPromise.Initialized()) {
        promise.SetValue(MakeErrorCommitResponse("producer is not ready", seqNo, data));
        return;
    }

    CommitPromises.push_back(promise);
    Data.emplace_back(seqNo, std::move(data));
    DoWrite();
}

void TProducer::OnWriteDone() {
    if (IsDestroyed) {
        return;
    }

    WriteInflight = false;
    DoWrite();
}

void TProducer::DoWrite() {
    if (Failing && !WriteInflight) {
        return;
    }
    if (WriteInflight || Pos == Data.size()) {
        return;
    }
    WriteInflight = true;
    TWriteRequest req;
    Settings.CredentialsProvider->FillAuthInfo(req.MutableCredentials());

    req.MutableData()->SetSeqNo(Data[Pos].SeqNo);
    req.MutableData()->SetData(Data[Pos].Data.GetEncodedData());
    req.MutableData()->SetCodec(Data[Pos].Data.GetCodecType());
    req.MutableData()->SetCreateTimeMs(Data[Pos].Data.GetTimestamp().MilliSeconds());

    ++Pos;
    RpcStuff->Stream->Write(req, new TQueueEvent(WriteDoneHandler));
}

NThreading::TFuture<TError> TProducer::IsDead() noexcept {
    return IsDeadPromise.GetFuture();
}

void TProducer::Destroy(const TString& description) {
    TError error;
    error.SetDescription(description);
    error.SetCode(NErrorCode::ERROR);
    Destroy(error);
}

void TProducer::Destroy(const TError& error) {
    if (IsDestroyed) {
        return;
    }

    if (!IsDestroying) {
        INFO_LOG("Error: " << error, Settings.SourceId, SessionId);
    }

    IsDestroyed = true;
    IsDeadPromise.SetValue(error);

    if (StartDeadlineCallback) {
        StartDeadlineCallback->TryCancel();
    }

    NThreading::TFuture<TChannelInfo> tmp;
    tmp.Swap(ChannelHolder.ChannelInfo);
    ChannelHolder.ChannelPtr = nullptr;

    Error = error;
    if (StartPromise.Initialized()) {
        NThreading::TPromise<TProducerCreateResponse> tmp;
        tmp.Swap(StartPromise);
        TWriteResponse res;
        res.MutableError()->CopyFrom(Error);
        tmp.SetValue(TProducerCreateResponse{std::move(res)});
    }
    while (!CommitPromises.empty()) {
        auto p = CommitPromises.front();
        CommitPromises.pop_front();
        auto pp(std::move(Data.front()));
        Data.pop_front();
        if (Pos > 0) {
            --Pos;
        }
        TWriteResponse res;
        res.MutableError()->CopyFrom(Error);

        p.SetValue(TProducerCommitResponse{pp.SeqNo, std::move(pp.Data), std::move(res)});
    }

    StreamCreatedHandler.Reset();
    ReadDoneHandler.Reset();
    WriteDoneHandler.Reset();
    FinishDoneHandler.Reset();

    DestroyPQLibRef();
}

void TProducer::OnStreamCreated(const TString& userAgent) {
    if (IsDestroyed) {
        return;
    }

    TWriteRequest req;
    Settings.CredentialsProvider->FillAuthInfo(req.MutableCredentials());

    req.MutableInit()->SetSourceId(Settings.SourceId);
    req.MutableInit()->SetTopic(Settings.Topic);
    req.MutableInit()->SetProxyCookie(ProxyCookie);
    req.MutableInit()->SetPartitionGroup(Settings.PartitionGroup);
    req.MutableInit()->SetVersion(userAgent);
    Y_VERIFY(!userAgent.empty());

    for (const auto& attr : Settings.ExtraAttrs) {
        auto item = req.MutableInit()->MutableExtraFields()->AddItems();
        item->SetKey(attr.first);
        item->SetValue(attr.second);
    }

    WriteInflight = true;
    RpcStuff->Stream->Write(req, new TQueueEvent(WriteDoneHandler));

    RpcStuff->Response.Clear();
    RpcStuff->Stream->Read(&RpcStuff->Response, new TQueueEvent(ReadDoneHandler));
}

void TProducer::OnFail() {
    if (Failing)
        return;
    Failing = true;

    if (IsDestroyed) {
        return;
    }

    RpcStuff->Stream->Finish(&RpcStuff->Status, new TQueueEvent(FinishDoneHandler));
}

void TProducer::OnFinishDone() {
    if (IsDestroyed) {
        return;
    }

    TError error;
    const auto& msg = RpcStuff->Status.error_message();
    TString reason(msg.data(), msg.length());
    error.SetDescription(reason);
    error.SetCode(NErrorCode::ERROR);

    Destroy(error);
}

void TProducer::OnReadDone() {
    if (IsDestroyed) {
        return;
    }

    if (RpcStuff->Response.HasError()) {
        Destroy(RpcStuff->Response.GetError());
        return;
    }

    if (StartPromise.Initialized()) { //init response
        NThreading::TPromise<TProducerCreateResponse> tmp;
        tmp.Swap(StartPromise);
        Y_VERIFY(RpcStuff->Response.HasInit());
        auto res(std::move(RpcStuff->Response));
        RpcStuff->Response.Clear();

        SessionId = res.GetInit().GetSessionId();
        const ui32 partition = res.GetInit().GetPartition();
        MaxSeqNo = res.GetInit().GetMaxSeqNo();
        const TProducerSeqNo seqNo = MaxSeqNo;
        const TString topic = res.GetInit().GetTopic();
        tmp.SetValue(TProducerCreateResponse{std::move(res)});

        if (StartDeadlineCallback) {
            StartDeadlineCallback->TryCancel();
        }
        StartDeadlineCallback = nullptr;

        RpcStuff->Stream->Read(&RpcStuff->Response, new TQueueEvent(ReadDoneHandler));

        DEBUG_LOG("Stream created to topic " << topic << " partition " << partition <<  " maxSeqNo " << seqNo, Settings.SourceId, SessionId);
    } else { //write response
        //get first CommitPromise
        Y_VERIFY(!Data.empty());
        auto p = CommitPromises.front();
        CommitPromises.pop_front();
        auto pp(std::move(Data.front()));
        Data.pop_front();
        Y_VERIFY(Pos > 0);
        --Pos;

        Y_VERIFY(RpcStuff->Response.HasAck());
        Y_VERIFY(RpcStuff->Response.GetAck().GetSeqNo() == pp.SeqNo);
        auto res(std::move(RpcStuff->Response));
        RpcStuff->Response.Clear();
        RpcStuff->Stream->Read(&RpcStuff->Response, new TQueueEvent(ReadDoneHandler));

        p.SetValue(TProducerCommitResponse{pp.SeqNo, std::move(pp.Data), std::move(res)});
    }
}

void TProducer::OnStartTimeout() {
    if (IsDestroyed) {
        return;
    }

    StartDeadlineCallback = nullptr;
    if (!StartPromise.Initialized()) {
        // everything is OK, there is no timeout, we have already started.
        return;
    }

    TError error;
    error.SetDescription("Start timeout");
    error.SetCode(NErrorCode::CREATE_TIMEOUT);
    Destroy(error);
}

void TProducer::Cancel() {
    IsDestroying = true;
    RpcStuff->Context.TryCancel();
    ChannelHolder.ChannelPtr = nullptr;

    Destroy(GetCancelReason());
}

}
