#include "processor.h"
#include <util/string/builder.h>

namespace NPersQueue {

template<typename T>
bool IsReady(const NThreading::TFuture<T>& future)
{
    return future.HasValue() || future.HasException();
}

template<typename T>
void Reset(NThreading::TFuture<T>& future)
{
    future = NThreading::TFuture<T>();
}

template<typename T>
void TProcessor::SafeSubscribe(NThreading::TFuture<T>& future)
{
    std::weak_ptr<TProcessor> self(shared_from_this());
    PQLib->Subscribe(
        future,
        this,
        [self](const auto&) {
            auto selfShared = self.lock();
            if (selfShared) {
                selfShared->ProcessFutures();
            }});
}

TProcessor::TProcessor(const TProcessorSettings& settings, std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib, TIntrusivePtr<ILogger> logger) noexcept
    : IProcessorImpl(std::move(destroyEventRef), std::move(pqLib))
    , Settings(settings)
    , Logger(std::move(logger))
{}

TProcessor::~TProcessor() noexcept
{
    SubscribeDestroyed();
    CleanState();
}

void TProcessor::Init()
{
    auto selfShared = shared_from_this();
    auto init = [selfShared] {
        selfShared->InitImpl();
    };
    Y_VERIFY(PQLib->GetQueuePool().GetQueue(this).AddFunc(init));
}

void TProcessor::InitImpl()
{
    ScheduleIdleSessionsCleanup();
    RecreateConsumer();
    SafeSubscribe(ConsumerIsStarted);
    SafeSubscribe(ConsumerIsDead);
}

// Must be called under lock
void TProcessor::CleanState() noexcept
{
    ProcessingData.clear();
    Reset(ConsumerIsDead);
    Reset(ConsumerRequest);
    Consumer.reset();
    Producers.clear();
    Data.clear();
    CurrentOriginDataMemoryUsage = 0;
    CurrentProcessedDataMemoryUsage = 0;
}


bool TProcessor::NextRequest() noexcept
{
    Y_VERIFY(Consumer);
    Y_VERIFY(!ConsumerRequest.Initialized());

    DEBUG_LOG("Current memory usage: "
              << "origin messages " << CurrentOriginDataMemoryUsage << "(" << Settings.MaxOriginDataMemoryUsage << "), "
              << "processed messages " << CurrentProcessedDataMemoryUsage << "(" << Settings.MaxProcessedDataMemoryUsage << ")\n",
              "", "");

    if (CurrentOriginDataMemoryUsage < Settings.MaxOriginDataMemoryUsage && CurrentProcessedDataMemoryUsage < Settings.MaxProcessedDataMemoryUsage) {
        DEBUG_LOG("Issuing consumer request\n", "", "");
        ConsumerRequest = Consumer->GetNextMessage();
        return true;
    } else {
        return false;
    }
}

bool TProcessor::ProcessConsumerResponse(TOriginData& result, TVector<NThreading::TFuture<TProcessedData>>& processedDataFutures) noexcept
{
    Y_VERIFY(ConsumerRequest.HasValue());
    auto message = ConsumerRequest.ExtractValueSync();
    Reset(ConsumerRequest);
    const auto& type = message.Type;
    const auto& resp = message.Response;
    if (type == EMT_ERROR) {
        // Will be handled later via ConsumerIsDead
        ERR_LOG("Got error: " << resp << "\n", "", "");
    } else if (type == EMT_RELEASE) {
        Y_VERIFY(resp.HasRelease());
        auto t = resp.GetRelease().GetTopic();
        auto p = resp.GetRelease().GetPartition();
        auto g = resp.GetRelease().GetGeneration();
        DEBUG_LOG("Got release for " << t << ":" << p << ", generation " << g << "\n", "", "");
    } else if (type == EMT_LOCK) {
        Y_VERIFY(resp.HasLock());
        auto t = resp.GetLock().GetTopic();
        auto p = resp.GetLock().GetPartition();
        auto g = resp.GetLock().GetGeneration();
        DEBUG_LOG("Got lock for " << t << ":" << p << ", generation " << g << "\n", "", "");
        message.ReadyToRead.SetValue(TLockInfo{});
    } else if (type == EMT_DATA) {
        Y_VERIFY(resp.HasData());
        Data.push_back(TDataInfo{resp.GetData().GetCookie()});
        for (auto& batch: resp.GetData().GetMessageBatch()) {
            TPartition partition(batch.GetTopic(), batch.GetPartition());
            TVector<TOriginMessage>& originMessages = result.Messages[partition];
            TDeque<TProcessingData>& processingData = ProcessingData[partition];
            Data.back().TotalOriginMessages += batch.MessageSize();

            for (auto& msg: batch.GetMessage()) {
                CurrentOriginDataMemoryUsage += msg.ByteSizeLong();
                NThreading::TPromise<TProcessedData> promise = NThreading::NewPromise<TProcessedData>();
                NThreading::TFuture<TProcessedData> future = promise.GetFuture();
                TOriginMessage originMessage = TOriginMessage{msg, promise};
                processedDataFutures.push_back(future);
                originMessages.push_back(originMessage);
                processingData.push_back(TProcessingData{future, msg.GetOffset(), msg.ByteSizeLong(), &Data.back()});
            }
        }
        DEBUG_LOG("Processing data message " << resp.GetData().GetCookie() << "\n", "", "");
        return true;
    } else if (type == EMT_COMMIT) {
        DEBUG_LOG("Got commit", "", "");
    } else {
        WARN_LOG("Unknown message type " << int(type) << ", response " << resp << "\n", "", "");
    }

    return false;
}

// Must be called under lock
void TProcessor::RecreateConsumer() noexcept
{
    if (DestroyedFlag) {
        return;
    }

    INFO_LOG("Create consumer\n", "", "");
    CleanState();

    TConsumerSettings consumerSettings = Settings.ConsumerSettings;
    consumerSettings.UseLockSession = true;

    Consumer = PQLib->CreateConsumer(consumerSettings, DestroyEventRef == nullptr, Logger);
    ConsumerIsStarted = Consumer->Start();
    ConsumerIsDead = Consumer->IsDead();
}

void TProcessor::ScheduleIdleSessionsCleanup() noexcept
{
    if (DestroyedFlag) {
        return;
    }

    std::weak_ptr<TProcessor> self(shared_from_this());
    PQLib->GetScheduler().Schedule(Settings.SourceIdIdleTimeout,
                                   this,
                                    [self] {
                                        auto selfShared = self.lock();
                                        if (selfShared) {
                                            selfShared->CloseIdleSessions();
                                        }
                                    });
}

void TProcessor::CloseIdleSessions() noexcept
{
    if (DestroyedFlag) {
        return;
    }

    TInstant now = TInstant::Now();
    for (auto it = Producers.begin(); it != Producers.end();) {
        if (it->second.LastWriteTime + Settings.SourceIdIdleTimeout < now) {
            INFO_LOG("Close producer for sourceid=" << it->first.SourceId, "", "");
            it = Producers.erase(it);
        } else {
            ++it;
        }
    }
    ScheduleIdleSessionsCleanup();
}

void TProcessor::ProcessFutures() noexcept
{
    if (DestroyedFlag) {
        return;
    }

    TMaybe<NThreading::TFuture<TConsumerMessage>> consumerRequestFuture;
    TVector<NThreading::TFuture<TProcessedData>> processedDataFutures;
    TVector<NThreading::TFuture<TProducerCommitResponse>> producerAckFutures;

    if (IsReady(ConsumerIsDead)) {
        ERR_LOG("Consumer died with error" << ConsumerIsDead.ExtractValueSync(), "", "");
        RecreateConsumer();
        SafeSubscribe(ConsumerIsStarted);
        SafeSubscribe(ConsumerIsDead);
        return;
    }

    if (IsReady(ConsumerRequest)) {
        TOriginData data;
        bool hasData = ProcessConsumerResponse(data, processedDataFutures);
        if (hasData) {
            Requests.front().SetValue(data);
            Requests.pop_front();
        }
    }

    TInstant now = TInstant::Now();
    for (auto& partitionData: ProcessingData) {
        TDeque<TProcessingData>& processingData = partitionData.second;

        while (!processingData.empty() && IsReady(processingData.front().Processed)) {
            Y_VERIFY(processingData.front().Processed.HasValue(), "processing future cannot be filled with exception");
            ui64 originMessageOffset = processingData.front().OriginMessageOffset;
            CurrentOriginDataMemoryUsage -= processingData.front().OriginMessageSize;
            TProcessedData processedData = processingData.front().Processed.ExtractValueSync();
            TDataInfo* dataInfo = processingData.front().DataInfo;
            dataInfo->OriginMessagesCounter++;
            dataInfo->TotalProcessedMessages += processedData.Messages.size();
            processingData.pop_front();
            for (auto& processedMessage: processedData.Messages) {
                TString newSourceId = processedMessage.SourceIdPrefix + partitionData.first.Topic + ":" + ToString(partitionData.first.PartitionId) + "_" + processedMessage.Topic;
                TProducerKey producerKey { processedMessage.Topic, newSourceId };
                IProducer* producer = nullptr;

                auto it = Producers.find(producerKey);
                if (it == Producers.end()) {
                    TProducerSettings producerSettings = Settings.ProducerSettings;
                    producerSettings.Topic = processedMessage.Topic;
                    producerSettings.SourceId = newSourceId;
                    producerSettings.PartitionGroup = processedMessage.Group;
                    producerSettings.ReconnectOnFailure = true;
                    Producers[producerKey] = TProducerInfo { PQLib->CreateRetryingProducer(producerSettings, DestroyEventRef == nullptr, Logger), {}, now};
                    producer = Producers[producerKey].Producer.get();
                    producer->Start();
                } else {
                    producer = it->second.Producer.get();
                }

                CurrentProcessedDataMemoryUsage += processedMessage.Data.size();
                DEBUG_LOG("Write message with seqNo=" << originMessageOffset + 1 << " to sourceId=" << newSourceId, "", "");
                NThreading::TFuture<TProducerCommitResponse> ack = producer->Write(originMessageOffset + 1, std::move(processedMessage.Data));
                producerAckFutures.push_back(ack);
                Producers[producerKey].Queue.push_back(TProduceInfo(ack, dataInfo));
            }
        }
    }

    for (auto& producer: Producers) {
        TProducerInfo& producerInfo = producer.second;
        while (!producerInfo.Queue.empty() && IsReady(producerInfo.Queue.front().Ack)) {
            auto ack = producerInfo.Queue.front().Ack.ExtractValueSync();
            DEBUG_LOG("Got ack for message with seqNo=" << ack.SeqNo << " from sourceId=" << producer.first.SourceId << " " << ack.Response, "", "");
            producerInfo.LastWriteTime = now;
            producerInfo.Queue.front().DataInfo->ProcessedMessagesCounter++;
            CurrentProcessedDataMemoryUsage -= ack.Data.GetSourceData().size();
            producerInfo.Queue.pop_front();
        }
    }

    TVector<ui64> toCommit;
    while (!Data.empty() && Data.front().IsReadyForCommit()) {
        toCommit.push_back(Data.front().Cookie);
        Data.pop_front();
    }
    if (!toCommit.empty()) {
        Consumer->Commit(toCommit);
    }

    if (IsReady(ConsumerIsStarted) && !ConsumerRequest.Initialized() && !Requests.empty()) {
        if (NextRequest()) {
            consumerRequestFuture = ConsumerRequest;
        }
    }

    if (consumerRequestFuture.Defined()) {
        SafeSubscribe(*consumerRequestFuture.Get());
    }

    for (auto& future: processedDataFutures) {
        SafeSubscribe(future);
    }

    for (auto& future: producerAckFutures) {
        SafeSubscribe(future);
    }
}

void TProcessor::GetNextData(NThreading::TPromise<TOriginData>& promise) noexcept
{
    Y_VERIFY(!DestroyedFlag);
    TMaybe<NThreading::TFuture<TConsumerMessage>> consumerRequestFuture;
    Requests.push_back(promise);
    if (IsReady(ConsumerIsStarted) && !ConsumerRequest.Initialized()) {
        if (NextRequest()) {
            consumerRequestFuture = ConsumerRequest;
        }
    }

    if (consumerRequestFuture.Defined()) {
        SafeSubscribe(*consumerRequestFuture.Get());
    }
}

void TProcessor::SubscribeDestroyed() {
    NThreading::TPromise<void> promise = ObjectsDestroyed;
    auto handler = [promise](const auto&) mutable {
        promise.SetValue();
    };
    std::vector<NThreading::TFuture<void>> futures;
    futures.reserve(Producers.size() + 2);
    futures.push_back(DestroyedPromise.GetFuture());
    if (Consumer) {
        futures.push_back(Consumer->Destroyed());
    }
    for (const auto& p : Producers) {
        if (p.second.Producer) {
            futures.push_back(p.second.Producer->Destroyed());
        }
    }
    WaitExceptionOrAll(futures).Subscribe(handler);
}

NThreading::TFuture<void> TProcessor::Destroyed() noexcept {
    return ObjectsDestroyed.GetFuture();
}

void TProcessor::Cancel() {
    DestroyedFlag = true;

    for (auto& req : Requests) {
        req.SetValue(TOriginData());
    }
    Requests.clear();

    DestroyPQLibRef();
}

}
