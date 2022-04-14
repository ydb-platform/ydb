#pragma once

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>
#include "persqueue_p.h"

#include <library/cpp/threading/future/future.h>
#include <util/generic/deque.h>
#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NPersQueue {

struct TDataInfo {
    ui64 Cookie;
    ui64 TotalOriginMessages;
    ui64 OriginMessagesCounter;
    ui64 TotalProcessedMessages;
    ui64 ProcessedMessagesCounter;

    TDataInfo(ui64 cookie)
        : Cookie(cookie)
        , TotalOriginMessages(0)
        , OriginMessagesCounter(0)
        , TotalProcessedMessages(0)
        , ProcessedMessagesCounter(0)
    {}

    bool IsReadyForCommit() {
        return TotalOriginMessages == OriginMessagesCounter && TotalProcessedMessages == ProcessedMessagesCounter;
    }
};

struct TProducerKey {
    TString Topic;
    TString SourceId;

    bool operator <(const TProducerKey& other) const {
        return std::tie(Topic, SourceId) < std::tie(other.Topic, other.SourceId);
    }
};

struct TProduceInfo {
    NThreading::TFuture<TProducerCommitResponse> Ack;
    TDataInfo* DataInfo = nullptr;

    explicit TProduceInfo(const NThreading::TFuture<TProducerCommitResponse>& ack, TDataInfo* dataInfo)
        : Ack(ack)
        , DataInfo(dataInfo)
    {}
};

struct TProducerInfo {
    std::shared_ptr<IProducerImpl> Producer;
    TDeque<TProduceInfo> Queue;
    TInstant LastWriteTime;
};

struct TProcessingData {
    NThreading::TFuture<TProcessedData> Processed;
    ui64 OriginMessageOffset;
    ui64 OriginMessageSize;
    TDataInfo* DataInfo;

    TProcessingData(const NThreading::TFuture<TProcessedData>& processed, ui64 originMessageOffset, ui64 originMessageSize, TDataInfo* dataInfo)
        : Processed(processed)
        , OriginMessageOffset(originMessageOffset)
        , OriginMessageSize(originMessageSize)
        , DataInfo(dataInfo)
    {};
};

class TPQLibPrivate;

class TProcessor: public IProcessorImpl, public std::enable_shared_from_this<TProcessor> {
public:
    TProcessor(const TProcessorSettings& settings, std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib, TIntrusivePtr<ILogger> logger) noexcept;
    ~TProcessor() noexcept;
    void Init() override;

    using IProcessorImpl::GetNextData;
    void GetNextData(NThreading::TPromise<TOriginData>& promise) noexcept override;

    NThreading::TFuture<void> Destroyed() noexcept override;

    void Cancel() override;

protected:
    TProcessorSettings Settings;
    TIntrusivePtr<ILogger> Logger;
    std::shared_ptr<IConsumerImpl> Consumer;
    NThreading::TFuture<TConsumerCreateResponse> ConsumerIsStarted;
    NThreading::TFuture<TError> ConsumerIsDead;
    NThreading::TFuture<TConsumerMessage> ConsumerRequest;
    TMap<TProducerKey, TProducerInfo> Producers;
    ui64 CurrentOriginDataMemoryUsage;
    ui64 CurrentProcessedDataMemoryUsage;

    TDeque<NThreading::TPromise<TOriginData>> Requests;
    TDeque<TDataInfo> Data;
    TMap<TPartition, TDeque<TProcessingData>> ProcessingData;
    NThreading::TPromise<void> ObjectsDestroyed = NThreading::NewPromise<void>();
    bool DestroyedFlag = false;

    template<typename T>
    void SafeSubscribe(NThreading::TFuture<T>& future);
    void CleanState() noexcept;
    bool NextRequest() noexcept;
    void RecreateConsumer() noexcept;
    void ScheduleIdleSessionsCleanup() noexcept;
    void CloseIdleSessions() noexcept;
    void ProcessFutures() noexcept;
    bool ProcessConsumerResponse(TOriginData& result, TVector<NThreading::TFuture<TProcessedData>>& processedDataFutures) noexcept;
    void SubscribeDestroyed();
    void InitImpl();
};

}
