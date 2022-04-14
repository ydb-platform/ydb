#include "decompressing_consumer.h"
#include "persqueue_p.h"

#include <library/cpp/streams/lzop/lzop.h>
#include <library/cpp/streams/zstd/zstd.h>

#include <util/stream/mem.h>
#include <util/stream/zlib.h>

#include <atomic>

namespace NPersQueue {

TDecompressingConsumer::TDecompressingConsumer(std::shared_ptr<IConsumerImpl> subconsumer, const TConsumerSettings& settings, std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib, TIntrusivePtr<ILogger> logger)
    : IConsumerImpl(std::move(destroyEventRef), std::move(pqLib))
    , Logger(std::move(logger))
    , Subconsumer(std::move(subconsumer))
    , Settings(settings)
{
}

void TDecompressingConsumer::Init() {
    DestroyedFuture = WaitExceptionOrAll(DestroyedPromise.GetFuture(), Subconsumer->Destroyed());
}

TDecompressingConsumer::~TDecompressingConsumer() {
    DestroyQueue("Destructor called");
}

template <class T>
void TDecompressingConsumer::SubscribeForQueueProcessing(NThreading::TFuture<T>& future) {
    const void* queueTag = this;
    std::weak_ptr<TDecompressingConsumer> self = shared_from_this();
    auto signalProcessQueue = [queueTag, self, pqLib = PQLib.Get()](const auto&) mutable {
        SignalProcessQueue(queueTag, std::move(self), pqLib);
    };
    future.Subscribe(signalProcessQueue);
}

NThreading::TFuture<TConsumerCreateResponse> TDecompressingConsumer::Start(TInstant deadline) noexcept {
    auto ret = Subconsumer->Start(deadline);
    SubscribeForQueueProcessing(ret); // when subconsumer starts, we will read ahead some messages

    // subscribe to death
    std::weak_ptr<TDecompressingConsumer> self = shared_from_this();
    auto isDeadHandler = [self](const auto& error) {
        auto selfShared = self.lock();
        if (selfShared && !selfShared->IsDestroyed) {
            selfShared->DestroyQueue(error.GetValue());
        }
    };
    PQLib->Subscribe(Subconsumer->IsDead(), this, isDeadHandler);

    return ret;
}

NThreading::TFuture<TError> TDecompressingConsumer::IsDead() noexcept {
    return IsDeadPromise.GetFuture();
}

void TDecompressingConsumer::Commit(const TVector<ui64>& cookies) noexcept {
    if (IsDestroyed) {
        ERR_LOG("Attempt to commit to dead consumer.", "", "");
        return;
    }
    return Subconsumer->Commit(cookies);
}

void TDecompressingConsumer::RequestPartitionStatus(const TString& topic, ui64 partition, ui64 generation) noexcept {
    if (IsDestroyed) {
        ERR_LOG("Attempt to request status from dead consumer.", "", "");
        return;
    }
    return Subconsumer->RequestPartitionStatus(topic, partition, generation);
}


NThreading::TFuture<void> TDecompressingConsumer::Destroyed() noexcept {
    return DestroyedFuture;
}

void TDecompressingConsumer::AddNewGetNextMessageRequest(NThreading::TPromise<TConsumerMessage>& promise) {
    Queue.emplace_back(promise);
    Queue.back().Future = Subconsumer->GetNextMessage();
    SubscribeForQueueProcessing(Queue.back().Future);
}

void TDecompressingConsumer::GetNextMessage(NThreading::TPromise<TConsumerMessage>& promise) noexcept {
    if (IsDestroyed) {
        TReadResponse response;
        auto* err = response.MutableError();
        err->SetCode(NErrorCode::ERROR);
        err->SetDescription("Consumer is dead.");
        promise.SetValue(TConsumerMessage(std::move(response)));
        return;
    }
    AddNewGetNextMessageRequest(promise);
}

void TDecompressingConsumer::ProcessQueue() {
    try {
        // stage 1: wait subconsumer answer and run decompression tasks
        for (TReadRequestInfo& request : Queue) {
            if (!request.Data && request.Future.HasValue()) {
                request.Data.ConstructInPlace(request.Future.ExtractValue());
                if (request.Data->Type == EMT_DATA) {
                    RequestDecompressing(request);
                }
            }
        }

        // stage 2: answer ready tasks to client
        while (!Queue.empty()) {
            TReadRequestInfo& front = Queue.front();
            if (!front.Data || front.Data->Type == EMT_DATA && !front.AllDecompressing.HasValue()) {
                break;
            }
            if (front.Data->Type == EMT_DATA) {
                CopyDataToAnswer(front);
            }

            front.Promise.SetValue(TConsumerMessage(std::move(*front.Data)));
            Queue.pop_front();
        }
    } catch (const std::exception&) {
        DestroyQueue(TStringBuilder() << "Failed to decompress data: " << CurrentExceptionMessage());
    }
}

void TDecompressingConsumer::SignalProcessQueue(const void* queueTag, std::weak_ptr<TDecompressingConsumer> self, TPQLibPrivate* pqLib) {
    auto processQueue = [self] {
        auto selfShared = self.lock();
        if (selfShared) {
            selfShared->ProcessQueue();
        }
    };
    Y_VERIFY(pqLib->GetQueuePool().GetQueue(queueTag).AddFunc(processQueue));
}

namespace {

struct TWaitAll: public TAtomicRefCount<TWaitAll> {
    TWaitAll(size_t count)
        : Count(count)
    {
    }

    void OnResultReady() {
        if (--Count == 0) {
            Promise.SetValue();
        }
    }

    std::atomic<size_t> Count;
    NThreading::TPromise<void> Promise = NThreading::NewPromise<void>();
};

THolder<IInputStream> CreateDecompressor(const ECodec codec, IInputStream* origin) {
    THolder<IInputStream> result;
    if (codec == ECodec::GZIP) {
        result.Reset(new TZLibDecompress(origin));
    } else if (codec == ECodec::LZOP) {
        result.Reset(new TLzopDecompress(origin));
    } else if (codec == ECodec::ZSTD) {
        result.Reset(new TZstdDecompress(origin));
    }
    return result;
}

} // namespace

void TDecompressingConsumer::RequestDecompressing(TReadRequestInfo& request) {
    size_t futuresCount = 0;
    const TReadResponse::TData& data = request.Data->Response.GetData();
    request.BatchFutures.resize(data.MessageBatchSize());
    for (size_t i = 0; i < data.MessageBatchSize(); ++i) {
        const TReadResponse::TData::TMessageBatch& batch = data.GetMessageBatch(i);
        request.BatchFutures.reserve(batch.MessageSize());
        for (const TReadResponse::TData::TMessage& message : batch.GetMessage()) {
            if (message.GetMeta().GetCodec() != ECodec::RAW) {
                ++futuresCount;
                request.BatchFutures[i].push_back(RequestDecompressing(message));
            }
        }
    }

    if (futuresCount == 0) {
        request.AllDecompressing = NThreading::MakeFuture(); // done
        return;
    } else {
        TIntrusivePtr<TWaitAll> waiter = new TWaitAll(futuresCount);
        auto handler = [waiter](const auto&) {
            waiter->OnResultReady();
        };
        for (auto& batch : request.BatchFutures) {
            for (auto& future : batch) {
                future.Subscribe(handler);
            }
        }
        request.AllDecompressing = waiter->Promise.GetFuture();
    }
    SubscribeForQueueProcessing(request.AllDecompressing);
}

NThreading::TFuture<TString> TDecompressingConsumer::RequestDecompressing(const TReadResponse::TData::TMessage& message) {
    TString data = message.GetData();
    ECodec codec = message.GetMeta().GetCodec();
    NThreading::TPromise<TString> promise = NThreading::NewPromise<TString>();
    auto decompress = [data, codec, promise]() mutable {
        Decompress(data, codec, promise);
    };
    Y_VERIFY(PQLib->GetCompressionPool().AddFunc(decompress));
    return promise.GetFuture();
}

void TDecompressingConsumer::Decompress(const TString& data, ECodec codec, NThreading::TPromise<TString>& promise) {
    try {
        // TODO: Check if decompression was successfull, i.e. if 'data' is valid byte array compressed with 'codec'
        TMemoryInput iss(data.data(), data.size());
        THolder<IInputStream> dec = CreateDecompressor(codec, &iss);
        if (!dec) {
            ythrow yexception() << "Failed to create decompressor";
        }

        TString result;
        TStringOutput oss(result);
        TransferData(dec.Get(), &oss);

        promise.SetValue(result);
    } catch (...) {
        promise.SetException(std::current_exception());
    }
}

void TDecompressingConsumer::CopyDataToAnswer(TReadRequestInfo& request) {
    TReadResponse::TData& data = *request.Data->Response.MutableData();
    Y_VERIFY(request.BatchFutures.size() == data.MessageBatchSize());
    for (size_t i = 0; i < request.BatchFutures.size(); ++i) {
        TReadResponse::TData::TMessageBatch& batch = *data.MutableMessageBatch(i);
        auto currentMessage = request.BatchFutures[i].begin();
        for (TReadResponse::TData::TMessage& message : *batch.MutableMessage()) {
            if (message.GetMeta().GetCodec() != ECodec::RAW) {
                Y_VERIFY(currentMessage != request.BatchFutures[i].end());
                try {
                    message.SetData(currentMessage->GetValue());
                    message.MutableMeta()->SetCodec(ECodec::RAW);
                } catch (const std::exception& ex) {
                    if (Settings.SkipBrokenChunks) {
                        message.SetBrokenPackedData(message.GetData());
                        message.SetData("");
                    } else {
                        throw;
                    }
                }
                ++currentMessage;
            }
        }
        Y_VERIFY(currentMessage == request.BatchFutures[i].end());
    }
}

void TDecompressingConsumer::DestroyQueue(const TString& errorMessage) {
    TError error;
    error.SetDescription(errorMessage);
    error.SetCode(NErrorCode::ERROR);
    DestroyQueue(error);
}

void TDecompressingConsumer::DestroyQueue(const TError& error) {
    if (IsDestroyed) {
        return;
    }

    for (TReadRequestInfo& request : Queue) {
        TReadResponse response;
        *response.MutableError() = error;
        request.Promise.SetValue(TConsumerMessage(std::move(response)));
    }
    Queue.clear();
    IsDeadPromise.SetValue(error);
    IsDestroyed = true;

    DestroyPQLibRef();
}

void TDecompressingConsumer::Cancel() {
    DestroyQueue(GetCancelReason());
}

} // namespace NPersQueue
