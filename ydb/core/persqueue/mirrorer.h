#pragma once

#include "actor_persqueue_client_iface.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/percentile_counter.h>
#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/public/lib/base/msgbus.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/library/persqueue/counter_time_keeper/counter_time_keeper.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>


namespace NKikimr {
namespace NPQ {

class TMirrorer : public TActorBootstrapped<TMirrorer> {
private:
    const ui64 MAX_READ_FUTURES_STORE = 25;
    const ui64 MAX_BYTES_IN_FLIGHT = 16_MB;
    const TDuration WRITE_RETRY_TIMEOUT_MAX = TDuration::Seconds(1);
    const TDuration WRITE_RETRY_TIMEOUT_START = TDuration::MilliSeconds(1);

    const TDuration CONSUMER_INIT_TIMEOUT_MAX = TDuration::Seconds(60);
    const TDuration CONSUMER_INIT_TIMEOUT_START = TDuration::Seconds(5);

    const TDuration CONSUMER_INIT_INTERVAL_MAX = TDuration::Seconds(60);
    const TDuration CONSUMER_INIT_INTERVAL_START = TDuration::Seconds(1);

    const TDuration READ_RETRY_TIMEOUT_MAX = TDuration::Seconds(1);
    const TDuration READ_RETRY_TIMEOUT_START = TDuration::MilliSeconds(1);

    const TDuration UPDATE_COUNTERS_INTERVAL = TDuration::Seconds(5);

    const TDuration LOG_STATE_INTERVAL = TDuration::Minutes(1);
    const TDuration INIT_TIMEOUT = TDuration::Minutes(1);
    const TDuration RECEIVE_READ_EVENT_TIMEOUT = TDuration::Minutes(1);
    const TDuration WRITE_TIMEOUT = TDuration::Minutes(10);


private:
    enum EEventCookie : ui64 {
        WRITE_REQUEST_COOKIE = 1,
        UPDATE_WRITE_TIMESTAMP = 2
    };

private:

    STFUNC(StateInitConsumer)
    {
        NPersQueue::TCounterTimeKeeper keeper(Counters.Cumulative()[COUNTER_PQ_TABLET_CPU_USAGE]);

        TRACE_EVENT(NKikimrServices::PQ_MIRRORER);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvPQ::TEvInitCredentials, HandleInitCredentials);
            HFuncTraced(TEvPQ::TEvCredentialsCreated, HandleCredentialsCreated);
            HFuncTraced(TEvPQ::TEvChangePartitionConfig, HandleChangeConfig);
            HFuncTraced(TEvPQ::TEvCreateConsumer, CreateConsumer);
            HFuncTraced(TEvPQ::TEvRetryWrite, HandleRetryWrite);
            HFuncTraced(TEvPersQueue::TEvResponse, Handle);
            HFuncTraced(TEvPQ::TEvUpdateCounters, Handle);
            HFuncTraced(TEvents::TEvPoisonPill, Handle);
        default:
            break;
        };
    }

    STFUNC(StateWork)
    {
        NPersQueue::TCounterTimeKeeper keeper(Counters.Cumulative()[COUNTER_PQ_TABLET_CPU_USAGE]);

        TRACE_EVENT(NKikimrServices::PQ_MIRRORER);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvPQ::TEvChangePartitionConfig, HandleChangeConfig);
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            HFuncTraced(TEvPQ::TEvRequestPartitionStatus, RequestSourcePartitionStatus);
            HFuncTraced(TEvPQ::TEvRetryWrite, HandleRetryWrite);
            HFuncTraced(TEvPersQueue::TEvResponse, Handle);
            HFuncTraced(TEvPQ::TEvUpdateCounters, Handle);
            HFuncTraced(TEvPQ::TEvReaderEventArrived, ProcessNextReaderEvent);
            HFuncTraced(TEvents::TEvPoisonPill, Handle);
        default:
            break;
        };
    }

private:
    template<class TEvent>
    void ScheduleWithIncreasingTimeout(const TActorId& recipient, TDuration& timeout, const TDuration& maxTimeout, const TActorContext &ctx) {
        ctx.ExecutorThread.ActorSystem->Schedule(timeout, new IEventHandle(recipient, SelfId(), new TEvent()));
        timeout = Min(timeout * 2, maxTimeout);
    }

    bool AddToWriteRequest(
        NKikimrClient::TPersQueuePartitionRequest& request,
        NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TCompressedMessage& message,
        bool& incorrectRequest
    );
    void ProcessError(const TActorContext& ctx, const TString& msg);
    void ProcessError(const TActorContext& ctx, const TString& msg, const NKikimrClient::TResponse& response);
    void AfterSuccesWrite(const TActorContext& ctx);
    void ProcessWriteResponse(
        const TActorContext& ctx,
        const NKikimrClient::TPersQueuePartitionResponse& response
    );
    void ScheduleConsumerCreation(const TActorContext& ctx);
    void StartInit(const TActorContext& ctx);
    void RetryWrite(const TActorContext& ctx);

    void ProcessNextReaderEvent(TEvPQ::TEvReaderEventArrived::TPtr& ev, const TActorContext& ctx);
    void DoProcessNextReaderEvent(const TActorContext& ctx, bool wakeup=false);

    TString MirrorerDescription() const;

    TString GetCurrentState() const;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType();
    TMirrorer(
        TActorId tabletActor,
        TActorId partitionActor,
        const NPersQueue::TTopicConverterPtr& topicConverter,
        ui32 partition,
        bool isLocalDC,
        ui64 endOffset,
        const NKikimrPQ::TMirrorPartitionConfig& config,
        const TTabletCountersBase& counters
    );
    void Bootstrap(const TActorContext& ctx);
    void Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvUpdateCounters::TPtr& ev, const TActorContext& ctx);
    void HandleChangeConfig(TEvPQ::TEvChangePartitionConfig::TPtr& ev, const TActorContext& ctx);
    void TryToRead(const TActorContext& ctx);
    void TryToWrite(const TActorContext& ctx);
    void HandleInitCredentials(TEvPQ::TEvInitCredentials::TPtr& ev, const TActorContext& ctx);
    void HandleCredentialsCreated(TEvPQ::TEvCredentialsCreated::TPtr& ev, const TActorContext& ctx);
    void HandleRetryWrite(TEvPQ::TEvRetryWrite::TPtr& ev, const TActorContext& ctx);
    void HandleWakeup(const TActorContext& ctx);
    void CreateConsumer(TEvPQ::TEvCreateConsumer::TPtr& ev, const TActorContext& ctx);
    void RequestSourcePartitionStatus(TEvPQ::TEvRequestPartitionStatus::TPtr& ev, const TActorContext& ctx);
    void RequestSourcePartitionStatus();
    void TryUpdateWriteTimetsamp(const TActorContext &ctx);
    void AddMessagesToQueue(
        TVector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TCompressedMessage>&& messages
    );
    void StartWaitNextReaderEvent(const TActorContext& ctx);

private:
    TActorId TabletActor;
    TActorId PartitionActor;
    NPersQueue::TTopicConverterPtr TopicConverter;
    ui32 Partition;
    bool IsLocalDC;
    ui64 EndOffset;
    ui64 OffsetToRead;
    NKikimrPQ::TMirrorPartitionConfig Config;

    TDeque<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TCompressedMessage> Queue;
    TDeque<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TCompressedMessage> WriteInFlight;
    ui64 BytesInFlight = 0;
    std::optional<NKikimrClient::TPersQueuePartitionRequest> WriteRequestInFlight;
    TDuration WriteRetryTimeout = WRITE_RETRY_TIMEOUT_START;
    TInstant WriteRequestTimestamp;
    NYdb::TCredentialsProviderFactoryPtr CredentialsProvider;
    std::shared_ptr<NYdb::NTopic::IReadSession> ReadSession;
    ui64 ReaderGeneration = 0;
    NYdb::NTopic::TPartitionSession::TPtr PartitionStream;
    THolder<NYdb::NTopic::TReadSessionEvent::TPartitionSessionStatusEvent> StreamStatus;
    TInstant LastInitStageTimestamp;

    TDuration ConsumerInitTimeout = CONSUMER_INIT_TIMEOUT_START;
    TDuration ConsumerInitInterval = CONSUMER_INIT_INTERVAL_START;
    TDuration ReadRetryTimeout = READ_RETRY_TIMEOUT_START;

    TTabletCountersBase Counters;

    bool WaitNextReaderEventInFlight = false;
    bool CredentialsRequestInFlight = false;

    bool WasSuccessfulRecording = false;
    TInstant LastStateLogTimestamp;

    TMultiCounter MirrorerErrors;
    TMultiCounter InitTimeoutCounter;
    TMultiCounter WriteTimeoutCounter;
    THolder<TPercentileCounter> MirrorerTimeLags;

    TMap<ui64, std::pair<TInstant, NThreading::TFuture<void>>> ReadFeatures;
    ui64 ReadFeatureId = 0;
    ui64 ReadFuturesInFlight = 0;
    TInstant LastReadEventTime;
};

}// NPQ
}// NKikimr
