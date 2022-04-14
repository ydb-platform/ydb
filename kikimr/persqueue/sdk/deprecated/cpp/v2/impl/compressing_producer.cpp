#include "compressing_producer.h"
#include "persqueue_p.h"

namespace NPersQueue {

TCompressingProducer::TCompressingProducer(std::shared_ptr<IProducerImpl> subproducer, ECodec defaultCodec, int quality, std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib, TIntrusivePtr<ILogger> logger)
    : IProducerImpl(std::move(destroyEventRef), std::move(pqLib))
    , Logger(std::move(logger))
    , Subproducer(std::move(subproducer))
    , DefaultCodec(defaultCodec)
    , Quality(quality)
{
    if (defaultCodec == ECodec::DEFAULT) {
        ythrow yexception() << "Producer codec can't be ECodec::DEFAULT";
    }
}

TCompressingProducer::~TCompressingProducer() {
    DestroyQueue("Destructor called");
}

NThreading::TFuture<TProducerCreateResponse> TCompressingProducer::Start(TInstant deadline) noexcept {
    return Subproducer->Start(deadline);
}

NThreading::TFuture<TData> TCompressingProducer::Enqueue(TData data) {
    NThreading::TPromise<TData> promise = NThreading::NewPromise<TData>();
    std::weak_ptr<TCompressingProducer> self = shared_from_this();
    const void* queueTag = this;
    const ECodec defaultCodec = DefaultCodec;
    const int quality = Quality;
    auto compress = [promise, self, data, queueTag, defaultCodec, quality, pqLib = PQLib.Get()]() mutable {
        Compress(promise, std::move(data), defaultCodec, quality, queueTag, self, pqLib);
    };
    Y_VERIFY(PQLib->GetCompressionPool().AddFunc(compress));
    return promise.GetFuture();
}

void TCompressingProducer::Compress(NThreading::TPromise<TData>& promise, TData&& data, ECodec defaultCodec, int quality, const void* queueTag, std::weak_ptr<TCompressingProducer> self, TPQLibPrivate* pqLib) {
    if (!self.lock()) {
        return;
    }
    promise.SetValue(TData::Encode(std::move(data), defaultCodec, quality));
    SignalProcessQueue(queueTag, self, pqLib);
}

void TCompressingProducer::SignalProcessQueue(const void* queueTag, std::weak_ptr<TCompressingProducer> self, TPQLibPrivate* pqLib) {
    auto processQueue = [self] {
        auto selfShared = self.lock();
        if (selfShared) {
            selfShared->ProcessQueue();
        }
    };

    //can't use this->Logger here - weak_ptr::lock -> ~TCompressingProducer in compression thread ->
    //race with cancel() in main thread pool
    Y_UNUSED(pqLib->GetQueuePool().GetQueue(queueTag).AddFunc(processQueue));
}

void TCompressingProducer::ProcessQueue() {
    if (DestroyedFlag) {
        return;
    }

    size_t removeCount = 0;
    bool prevSignalled = true; // flag to guarantee correct order of futures signalling.
    for (TWriteRequestInfo& request : Queue) {
        if (!request.EncodedData.HasValue()) {
            // stage 1
            break;
        }
        if (!request.Future.Initialized()) {
            // stage 2
            if (request.SeqNo) {
                request.Future = Subproducer->Write(request.SeqNo, request.EncodedData.GetValue());
            } else {
                request.Future = Subproducer->Write(request.EncodedData.GetValue());
            }
            std::weak_ptr<TCompressingProducer> self = shared_from_this();
            const void* queueTag = this;
            auto signalProcess = [self, queueTag, pqLib = PQLib.Get()](const auto&) {
                SignalProcessQueue(queueTag, self, pqLib);
            };
            request.Future.Subscribe(signalProcess);
        } else {
            // stage 3
            if (prevSignalled && request.Future.HasValue()) {
                request.Promise.SetValue(request.Future.GetValue());
                ++removeCount;
            } else {
                prevSignalled = false;
            }
        }
    }
    while (removeCount--) {
        Queue.pop_front();
    }
}

void TCompressingProducer::DestroyQueue(const TString& reason) {
    if (DestroyedFlag) {
        return;
    }

    DestroyedFlag = true;
    for (TWriteRequestInfo& request : Queue) {
        if (request.Future.Initialized() && request.Future.HasValue()) {
            request.Promise.SetValue(request.Future.GetValue());
        } else {
            TWriteResponse resp;
            TError& err = *resp.MutableError();
            err.SetCode(NErrorCode::ERROR);
            err.SetDescription(reason);
            request.Promise.SetValue(TProducerCommitResponse(request.SeqNo, std::move(request.Data), std::move(resp)));
        }
    }
    Queue.clear();

    DestroyPQLibRef();
}

void TCompressingProducer::Write(NThreading::TPromise<TProducerCommitResponse>& promise, TProducerSeqNo seqNo, TData data) noexcept {
    if (DestroyedFlag) {
        return;
    }

    Queue.emplace_back(seqNo, data, promise);
    if (data.IsEncoded()) {
        Queue.back().EncodedData = NThreading::MakeFuture<TData>(data);
        ProcessQueue();
    } else if (DefaultCodec == ECodec::RAW) {
        Queue.back().EncodedData = NThreading::MakeFuture<TData>(TData::MakeRawIfNotEncoded(data));
        ProcessQueue();
    } else {
        Queue.back().EncodedData = Enqueue(std::move(data));
    }
}

void TCompressingProducer::Write(NThreading::TPromise<TProducerCommitResponse>& promise, TData data) noexcept {
    TCompressingProducer::Write(promise, 0, std::move(data));
}

NThreading::TFuture<TError> TCompressingProducer::IsDead() noexcept {
    return Subproducer->IsDead();
}

NThreading::TFuture<void> TCompressingProducer::Destroyed() noexcept {
    return WaitExceptionOrAll(DestroyedPromise.GetFuture(), Subproducer->Destroyed());
}

void TCompressingProducer::Cancel() {
    DestroyQueue(GetCancelReason());
}

} // namespace NPersQueue
