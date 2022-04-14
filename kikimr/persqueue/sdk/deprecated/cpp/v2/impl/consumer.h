#pragma once

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/responses.h>
#include "channel.h"
#include "scheduler.h"
#include "iconsumer_p.h"
#include "internals.h"
#include <kikimr/yndx/api/grpc/persqueue.grpc.pb.h>

#include <library/cpp/threading/future/future.h>

#include <deque>
#include <memory>

namespace NPersQueue {

class TConsumer: public IConsumerImpl, public std::enable_shared_from_this<TConsumer> {
public:
    friend class TPQLibPrivate;

    NThreading::TFuture<TConsumerCreateResponse> Start(TInstant deadline) noexcept override;

    void Commit(const TVector<ui64>& cookies) noexcept override;

    using IConsumerImpl::GetNextMessage;
    void GetNextMessage(NThreading::TPromise<TConsumerMessage>& promise) noexcept override;

    void RequestPartitionStatus(const TString& topic, ui64 partition, ui64 generation) noexcept override;

    NThreading::TFuture<TError> IsDead() noexcept override;

    TConsumer(const TConsumerSettings& settings, std::shared_ptr<grpc::CompletionQueue> cq,
              NThreading::TPromise<TConsumerCreateResponse> promise, std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib, TIntrusivePtr<ILogger> logger) noexcept;

    void Init() override;

    ~TConsumer() noexcept;

    void Destroy() noexcept;
    void SetChannel(const TChannelHolder& channel) noexcept;
    void SetChannel(const TChannelInfo& channel) noexcept;

    void Lock(const TString& topic, const ui32 partition, const ui64 generation, const ui64 readOffset, const ui64 commitOffset, const bool verifyReadOffset) noexcept;
    void OrderRead() noexcept;

    void Cancel() override;

public:
    using TStream = grpc::ClientAsyncReaderWriterInterface<TReadRequest, TReadResponse>;

    // objects that must live after destruction of producer untill all the callbacks arrive at CompletionQueue
    struct TRpcStuff: public TAtomicRefCount<TRpcStuff> {
        TReadResponse Response;
        std::shared_ptr<grpc::CompletionQueue> CQ;
        std::shared_ptr<grpc::Channel> Channel;
        std::unique_ptr<PersQueueService::Stub> Stub;
        grpc::ClientContext Context;
        std::unique_ptr<TStream> Stream;
    };

protected:
    friend class TConsumerStreamCreated;
    friend class TConsumerReadDone;
    friend class TConsumerWriteDone;
    friend class TConsumerDestroyHandler;

    IHandlerPtr StreamCreatedHandler;
    IHandlerPtr ReadDoneHandler;
    IHandlerPtr WriteDoneHandler;

    void ProcessResponses();
    void Destroy(const TError& reason);
    void Destroy(const TString& description); // the same but with Code=ERROR
    void OnStreamCreated();
    void OnReadDone();
    void OnWriteDone();

    void DoWrite();
    void DoStart(TInstant deadline);
    void OnStartTimeout();

    TIntrusivePtr<TRpcStuff> RpcStuff;

    TChannelHolder ChannelHolder;
    TConsumerSettings Settings;

    TString SessionId;

    NThreading::TPromise<TConsumerCreateResponse> StartPromise;
    NThreading::TPromise<TError> IsDeadPromise;
    std::deque<NThreading::TPromise<TConsumerMessage>> MessagePromises;
    std::deque<NThreading::TFuture<TConsumerMessage>> MessageResponses;

    std::deque<TReadRequest> Requests;


    std::deque<std::pair<ui64, std::pair<ui32, ui64>>> ReadInfo; //cookie -> (count, size)
    ui32 UncommittedCount = 0;
    ui64 UncommittedSize = 0;

    ui64 MemoryUsage = 0;
    ui32 ReadsOrdered = 0;

    ui64 EstimateReadSize = 0;

    bool WriteInflight;

    ui64 ProxyCookie = 0;

    TIntrusivePtr<ILogger> Logger;

    TError Error;

    bool IsDestroyed;
    bool IsDestroying;

    TIntrusivePtr<TScheduler::TCallbackHandler> StartDeadlineCallback;
};

}
