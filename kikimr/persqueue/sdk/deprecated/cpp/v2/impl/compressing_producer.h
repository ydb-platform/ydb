#pragma once

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/iproducer.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/responses.h>
#include "internals.h"
#include "iproducer_p.h"

#include <deque>
#include <memory>

namespace NPersQueue {

class TPQLibPrivate;

class TCompressingProducer: public IProducerImpl, public std::enable_shared_from_this<TCompressingProducer> {
public:
    NThreading::TFuture<TProducerCreateResponse> Start(TInstant deadline) noexcept override;
    using IProducerImpl::Write;
    void Write(NThreading::TPromise<TProducerCommitResponse>& promise, TProducerSeqNo seqNo, TData data) noexcept override;
    void Write(NThreading::TPromise<TProducerCommitResponse>& promise, TData data) noexcept override;
    NThreading::TFuture<TError> IsDead() noexcept override;
    NThreading::TFuture<void> Destroyed() noexcept override;

    ~TCompressingProducer();

    TCompressingProducer(std::shared_ptr<IProducerImpl> subproducer, ECodec defaultCodec, int quality, std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib, TIntrusivePtr<ILogger> logger);

    void Cancel() override;

private:
    struct TWriteRequestInfo {
        TWriteRequestInfo(TProducerSeqNo seqNo, TData data, const NThreading::TPromise<TProducerCommitResponse>& promise)
            : SeqNo(seqNo)
            , Data(std::move(data))
            , Promise(promise)
        {
        }

        TProducerSeqNo SeqNo;
        TData Data;
        NThreading::TFuture<TData> EncodedData;
        NThreading::TPromise<TProducerCommitResponse> Promise;
        NThreading::TFuture<TProducerCommitResponse> Future;
    };

    void ProcessQueue();
    NThreading::TFuture<TData> Enqueue(TData data);
    // these functions should be static and not lock self in compression thread pool for proper futures ordering
    static void Compress(NThreading::TPromise<TData>& promise, TData&& data, ECodec defaultCodec, int quality,
                         const void* queueTag, std::weak_ptr<TCompressingProducer> self, TPQLibPrivate* pqLib);
    static void SignalProcessQueue(const void* queueTag, std::weak_ptr<TCompressingProducer> self, TPQLibPrivate* pqLib);
    void DestroyQueue(const TString& reason);

protected:
    TIntrusivePtr<ILogger> Logger;
    std::shared_ptr<IProducerImpl> Subproducer;
    ECodec DefaultCodec;
    int Quality;
    std::deque<TWriteRequestInfo> Queue;
    bool DestroyedFlag = false;
};

} // namespace NPersQueue
