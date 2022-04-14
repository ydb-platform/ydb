#pragma once

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/responses.h>
#include "internals.h"
#include "iconsumer_p.h"

#include <util/generic/maybe.h>

#include <deque>
#include <memory>
#include <vector>

namespace NPersQueue {

class TPQLibPrivate;

class TDecompressingConsumer: public IConsumerImpl, public std::enable_shared_from_this<TDecompressingConsumer> {
public:
    using IConsumerImpl::GetNextMessage;
    NThreading::TFuture<TConsumerCreateResponse> Start(TInstant deadline) noexcept override;
    NThreading::TFuture<TError> IsDead() noexcept override;
    NThreading::TFuture<void> Destroyed() noexcept override;
    void GetNextMessage(NThreading::TPromise<TConsumerMessage>& promise) noexcept override;
    void Commit(const TVector<ui64>& cookies) noexcept override;
    void RequestPartitionStatus(const TString& topic, ui64 partition, ui64 generation) noexcept override;
    void Init() override;

    ~TDecompressingConsumer();
    TDecompressingConsumer(std::shared_ptr<IConsumerImpl> subconsumer, const TConsumerSettings& settings, std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib, TIntrusivePtr<ILogger> logger);

    void Cancel() override;

private:
    struct TReadRequestInfo {
        TReadRequestInfo() = default;

        TReadRequestInfo(const NThreading::TPromise<TConsumerMessage>& promise)
            : Promise(promise)
        {
        }

        NThreading::TPromise<TConsumerMessage> Promise;
        NThreading::TFuture<TConsumerMessage> Future;
        TMaybe<TConsumerMessage> Data;

        using TMessageFutures = std::vector<NThreading::TFuture<TString>>;
        using TBatchFutures = std::vector<TMessageFutures>;
        TBatchFutures BatchFutures;
        NThreading::TFuture<void> AllDecompressing;
    };

    void ProcessQueue();
    static void SignalProcessQueue(const void* queueTag, std::weak_ptr<TDecompressingConsumer> self, TPQLibPrivate* pqLib);
    void DestroyQueue(const TString& errorMessage);
    void DestroyQueue(const TError& error);

    template <class T>
    void SubscribeForQueueProcessing(NThreading::TFuture<T>& future);

    void RequestDecompressing(TReadRequestInfo& request);
    void CopyDataToAnswer(TReadRequestInfo& request);
    NThreading::TFuture<TString> RequestDecompressing(const TReadResponse::TData::TMessage& message);
    static void Decompress(const TString& data, ECodec codec, NThreading::TPromise<TString>& promise);
    void AddNewGetNextMessageRequest(NThreading::TPromise<TConsumerMessage>& promise);

protected:
    TIntrusivePtr<ILogger> Logger;
    std::shared_ptr<IConsumerImpl> Subconsumer;
    std::deque<TReadRequestInfo> Queue;
    TConsumerSettings Settings;
    NThreading::TFuture<void> DestroyedFuture; // Subconsumer may be deleted, so store our future here.
    NThreading::TPromise<TError> IsDeadPromise = NThreading::NewPromise<TError>();
    bool IsDestroyed = false;
};

} // namespace NPersQueue
