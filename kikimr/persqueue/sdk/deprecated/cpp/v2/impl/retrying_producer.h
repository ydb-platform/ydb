#pragma once

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/responses.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>
#include "scheduler.h"
#include "internals.h"
#include "persqueue_p.h"
#include "producer.h"
#include "iproducer_p.h"
#include <library/cpp/threading/future/future.h>
#include <kikimr/yndx/api/grpc/persqueue.grpc.pb.h>
#include <deque>

namespace NPersQueue {

class TRetryingProducer: public IProducerImpl, public std::enable_shared_from_this<TRetryingProducer> {
public:
    using IProducerImpl::Write;
    // If seqno == 0, we assume it to be autodefined
    void Write(NThreading::TPromise<TProducerCommitResponse>& promise, const TProducerSeqNo seqNo, TData data) noexcept override;
    void Write(NThreading::TPromise<TProducerCommitResponse>& promise, TData data) noexcept override {
        TRetryingProducer::Write(promise, 0, std::move(data));
    }

    TRetryingProducer(const TProducerSettings& settings, std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib,
                TIntrusivePtr<ILogger> logger);

    NThreading::TFuture<TProducerCreateResponse> Start(TInstant deadline) noexcept override;

    NThreading::TFuture<TError> IsDead() noexcept override;

    NThreading::TFuture<void> Destroyed() noexcept override;

    ~TRetryingProducer() noexcept;


    void RecreateProducer(TInstant deadline) noexcept;
    void ProcessStart(const NThreading::TFuture<TProducerCreateResponse>&) noexcept;
    void SendData() noexcept;
    void ProcessFutures() noexcept;
    void DoRecreate(TInstant deadline) noexcept;
    void DelegateWriteAndSubscribe(TProducerSeqNo seqNo, TData&& data) noexcept;
    TDuration UpdateReconnectionDelay();
    void ScheduleRecreation();
    void Destroy(const TError& error);
    void Destroy(const TString& description); // the same but with Code=ERROR
    void SubscribeDestroyed();
    void OnStartDeadline();
    void OnProducerDead(const TError& error);

    void Cancel() override;

protected:
    TProducerSettings Settings;
    TIntrusivePtr<ILogger> Logger;
    std::shared_ptr<IProducerImpl> Producer;
    std::deque<TWriteData> Requests;
    std::deque<TWriteData> ResendRequests;
    std::deque<TWriteData> InFlightRequests;
    std::deque<NThreading::TPromise<TProducerCommitResponse>> Promises;
    std::deque<NThreading::TFuture<TProducerCommitResponse>> Futures;
    NThreading::TFuture<TProducerCreateResponse> StartFuture;
    NThreading::TPromise<TProducerCreateResponse> StartPromise;
    NThreading::TPromise<TError> IsDeadPromise;
    bool NeedRecreation;
    bool Stopping;
    ui32 ToProcess;
    TDuration LastReconnectionDelay;
    unsigned ReconnectionAttemptsDone;
    TIntrusivePtr<TScheduler::TCallbackHandler> ReconnectionCallback;
    NThreading::TPromise<void> ProducersDestroyed = NThreading::NewPromise<void>();
};

}
