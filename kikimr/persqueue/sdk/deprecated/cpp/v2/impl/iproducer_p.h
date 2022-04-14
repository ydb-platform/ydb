#pragma once

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/iproducer.h>
#include "interface_common.h"

namespace NPersQueue {

class TPQLibPrivate;

struct IProducerImpl: public IProducer, public TSyncDestroyed {
    using IProducer::Write;

    IProducerImpl(std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib);

    // Initialization after constructor (for example, for correct call of shared_from_this())
    virtual void Init() {
    }
    virtual void Write(NThreading::TPromise<TProducerCommitResponse>& promise, TProducerSeqNo seqNo, TData data) noexcept = 0;
    virtual void Write(NThreading::TPromise<TProducerCommitResponse>& promise, TData data) noexcept = 0;

    NThreading::TFuture<TProducerCommitResponse> Write(TProducerSeqNo, TData) noexcept override {
        Y_FAIL("");
        return NThreading::NewPromise<TProducerCommitResponse>().GetFuture();
    }

    NThreading::TFuture<TProducerCommitResponse> Write(TData) noexcept override {
        Y_FAIL("");
        return NThreading::NewPromise<TProducerCommitResponse>().GetFuture();
    }
};

// Producer that is given to client.
class TPublicProducer: public IProducer {
public:
    explicit TPublicProducer(std::shared_ptr<IProducerImpl> impl);
    ~TPublicProducer();

    NThreading::TFuture<TProducerCreateResponse> Start(TInstant deadline = TInstant::Max()) noexcept {
        return Impl->Start(deadline);
    }

    NThreading::TFuture<TProducerCommitResponse> Write(TProducerSeqNo seqNo, TData data) noexcept {
        return Impl->Write(seqNo, std::move(data));
    }

    NThreading::TFuture<TProducerCommitResponse> Write(TData data) noexcept {
        return Impl->Write(std::move(data));
    }

    NThreading::TFuture<TError> IsDead() noexcept {
        return Impl->IsDead();
    }

private:
    std::shared_ptr<IProducerImpl> Impl;
};

} // namespace NPersQueue
