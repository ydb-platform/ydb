#pragma once

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/responses.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/impl/channel.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/impl/iproducer_p.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/impl/internals.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/impl/scheduler.h>
#include <library/cpp/threading/future/future.h>
#include <kikimr/yndx/api/grpc/persqueue.grpc.pb.h>
#include <deque>

namespace NPersQueue {

class TProducer : public IProducerImpl, public std::enable_shared_from_this<TProducer> {
public:
    using IProducerImpl::Write;
    void Write(NThreading::TPromise<TProducerCommitResponse>& promise, const TProducerSeqNo seqNo, TData data) noexcept override;
    void Write(NThreading::TPromise<TProducerCommitResponse>& promise, TData data) noexcept override;

    NThreading::TFuture<TError> IsDead() noexcept override;

    NThreading::TFuture<TProducerCreateResponse> Start(TInstant deadline) noexcept override;

    TProducer(const TProducerSettings& settings, std::shared_ptr<grpc::CompletionQueue> cq,
              NThreading::TPromise<TProducerCreateResponse> promise, std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib, TIntrusivePtr<ILogger> logger) noexcept;
    ~TProducer();

    void Destroy() noexcept;
    void SetChannel(const TChannelHolder& channel) noexcept;
    void SetChannel(const TChannelInfo& channel) noexcept;

    void DoStart(TInstant deadline);

    void Init() override;

    const TProducerSettings& GetSettings() const {
        return Settings;
    }

    void Cancel() override;

public:
    using TStream = grpc::ClientAsyncReaderWriterInterface<TWriteRequest, TWriteResponse>;

    // objects that must live after destruction of producer untill all the callbacks arrive at CompletionQueue
    struct TRpcStuff: public TAtomicRefCount<TRpcStuff> {
        TWriteResponse Response;
        std::shared_ptr<grpc::CompletionQueue> CQ;
        std::shared_ptr<grpc::Channel> Channel;
        std::unique_ptr<PersQueueService::Stub> Stub;
        grpc::ClientContext Context;
        std::unique_ptr<TStream> Stream;
        grpc::Status Status;
    };

private:
    friend class TPQLibPrivate;
    friend class TStreamCreated;
    friend class TReadDone;
    friend class TWriteDone;
    friend class TProducerDestroyHandler;
    friend class TFinishDone;

    IHandlerPtr StreamCreatedHandler;
    IHandlerPtr ReadDoneHandler;
    IHandlerPtr WriteDoneHandler;
    IHandlerPtr FinishDoneHandler;

    void Destroy(const TError& error);
    void Destroy(const TString& description); // the same but with Code=ERROR
    void OnStreamCreated(const TString& userAgent);
    void OnReadDone();
    void OnWriteDone();
    void DoWrite();
    void OnFail();
    void OnFinishDone();
    void OnStartTimeout();

protected:
    TIntrusivePtr<TRpcStuff> RpcStuff;

    TChannelHolder ChannelHolder;
    TProducerSettings Settings;

    TString SessionId;

    NThreading::TPromise<TProducerCreateResponse> StartPromise;
    NThreading::TPromise<TError> IsDeadPromise;
    std::deque<NThreading::TPromise<TProducerCommitResponse>> CommitPromises;
    std::deque<TWriteData> Data;
    ui32 Pos = 0;
    bool WriteInflight;
    ui64 ProxyCookie = 0;
    TProducerSeqNo MaxSeqNo = 0;

    TIntrusivePtr<ILogger> Logger;

    TError Error;

    bool IsDestroyed;
    bool IsDestroying;
    bool Failing;
    TIntrusivePtr<TScheduler::TCallbackHandler> StartDeadlineCallback;
};

}
