#pragma once

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/iconsumer.h>
#include "interface_common.h"

namespace NPersQueue {

class TPQLibPrivate;

class IConsumerImpl: public IConsumer, public TSyncDestroyed {
public:
    IConsumerImpl(std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib);

    virtual void Init() {
    }

    using IConsumer::GetNextMessage;
    virtual void GetNextMessage(NThreading::TPromise<TConsumerMessage>& promise) noexcept = 0;
};

// Consumer that is given to client.
class TPublicConsumer: public IConsumer {
public:
    explicit TPublicConsumer(std::shared_ptr<IConsumerImpl> impl);
    ~TPublicConsumer();

    NThreading::TFuture<TConsumerCreateResponse> Start(TInstant deadline) noexcept override {
        return Impl->Start(deadline);
    }

    NThreading::TFuture<TConsumerMessage> GetNextMessage() noexcept override {
        return Impl->GetNextMessage();
    }

    void RequestPartitionStatus(const TString& topic, ui64 partition, ui64 generation) noexcept override {
        Impl->RequestPartitionStatus(topic, partition, generation);
    }

    void Commit(const TVector<ui64>& cookies) noexcept override {
        Impl->Commit(cookies);
    }

    NThreading::TFuture<TError> IsDead() noexcept override {
        return Impl->IsDead();
    }

private:
    std::shared_ptr<IConsumerImpl> Impl;
};

} // namespace NPersQueue
