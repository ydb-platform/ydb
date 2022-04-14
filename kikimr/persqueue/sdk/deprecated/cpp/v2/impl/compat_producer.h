#pragma once

#include "iproducer_p.h"

#include <kikimr/public/sdk/cpp/client/ydb_persqueue/persqueue.h>

#include <memory>
#include <queue>

namespace NPersQueue {


/**
 * Compatibility producer:
 *      wraps old "interface" around the new one
 */

class TYdbSdkCompatibilityProducer: public IProducerImpl, public std::enable_shared_from_this<TYdbSdkCompatibilityProducer> {

    struct TMsgData {
        NThreading::TPromise<TProducerCommitResponse> Promise;
        TMaybe<TProducerSeqNo> SeqNo;
        TData Data;

        TMsgData() {};
        TMsgData(NThreading::TPromise<TProducerCommitResponse> promise, TMaybe<TProducerSeqNo> seqNo, TData data)
        : Promise(promise)
        , SeqNo(seqNo)
        , Data(data) {}
    };

    std::shared_ptr<NYdb::NPersQueue::IWriteSession> WriteSession;
    NThreading::TPromise<TError> IsDeadPromise;
    NThreading::TFuture<void> NextEvent;

    TMaybe<NYdb::NPersQueue::TContinuationToken> ContToken;
    std::queue<TMsgData> ToWrite;
    std::queue<TMsgData> ToAck;
    TSpinLock Lock;
    bool Closed;

    void DoProcessNextEvent();
    void SubscribeToNextEvent();

    void WriteImpl(NThreading::TPromise<TProducerCommitResponse>& promise, TMaybe<TProducerSeqNo> seqNo, TData data) noexcept;

    void NotifyClient(NErrorCode::EErrorCode code, const TString& reason);

public:
    using IProducerImpl::Write;
    NThreading::TFuture<TProducerCreateResponse> Start(TInstant deadline = TInstant::Max()) noexcept override;
    NThreading::TFuture<TError> IsDead() noexcept override;
    void Write(NThreading::TPromise<TProducerCommitResponse>& promise, TData data) noexcept override;
    void Write(NThreading::TPromise<TProducerCommitResponse>& promise, TProducerSeqNo seqNo, TData data) noexcept override;
    void Cancel() override;
    void Init() noexcept override;

    ~TYdbSdkCompatibilityProducer();

    TYdbSdkCompatibilityProducer(const TProducerSettings& settings, NYdb::NPersQueue::TPersQueueClient& persQueueClient,
                    std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib);

};


}   // namespace NYdb::NPersQueue
