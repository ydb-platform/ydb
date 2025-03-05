#include "dq_solomon_metrics_queue.h"

#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/dq/actors/common/retry_queue.h>
#include <ydb/library/yql/providers/solomon/events/events.h>
#include <ydb/library/yql/providers/solomon/solomon_accessor/client/solomon_accessor_client.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/utils/yql_panic.h>

#define LOG_E(name, stream) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", queued metrics: " << this->Metrics.size() << ". " << stream)
#define LOG_W(name, stream) \
    LOG_WARN_S (*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", queued metrics: " << this->Metrics.size() << ". " << stream)
#define LOG_I(name, stream) \
    LOG_INFO_S (*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", queued metrics: " << this->Metrics.size() << ". " << stream)
#define LOG_D(name, stream) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", queued metrics: " << this->Metrics.size() << ". " << stream)
#define LOG_T(name, stream) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", queued metrics: " << this->Metrics.size() << ". " << stream)

namespace NYql::NDq {

namespace {

class TDqSolomonMetricsQueueActor : public NActors::TActorBootstrapped<TDqSolomonMetricsQueueActor> {
public:
    static constexpr char ActorName[] = "DQ_SOLOMON_METRICS_QUEUE_ACTOR";

    struct TEvPrivatePrivate {
        enum {
            EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),  // Leave space for RetryQueue events

            EvNextListingChunkReceived = EvBegin,
            EvRoundRobinStageTimeout,
            EvTransitToErrorState,

            EvEnd
        };
        static_assert(
            EvEnd <= EventSpaceEnd(NActors::TEvents::ES_PRIVATE),
            "expected EvEnd <= EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvNextListingChunkReceived : public NActors::TEventLocal<TEvNextListingChunkReceived, EvNextListingChunkReceived> {
            NSo::ISolomonAccessorClient::TListMetricsResult ListingResult;
            TEvNextListingChunkReceived(NSo::ISolomonAccessorClient::TListMetricsResult listingResult)
                : ListingResult(std::move(listingResult)) {};
        };

        struct TEvRoundRobinStageTimeout : public NActors::TEventLocal<TEvRoundRobinStageTimeout, EvRoundRobinStageTimeout> {
        };

        struct TEvTransitToErrorState : public NActors::TEventLocal<TEvTransitToErrorState, EvTransitToErrorState> {
            TString Issues;
            explicit TEvTransitToErrorState(const TString& issues)
                : Issues(issues) {}
        };
    };
    using TBase = TActorBootstrapped<TDqSolomonMetricsQueueActor>;

    TDqSolomonMetricsQueueActor(
        ui64 consumersCount,
        ui64 pageSize,
        ui64 prefetchSize,
        ui64 batchCountLimit,
        TDqSolomonReadParams&& readParams,
        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider)
        : CurrentPage(0)
        , ConsumersCount(consumersCount)
        , PageSize(pageSize)
        , PrefetchSize(prefetchSize)
        , BatchCountLimit(batchCountLimit)
        , ReadParams(std::move(readParams))
        , CredentialsProvider(credentialsProvider)
        , SolomonClient(NSo::ISolomonAccessorClient::Make(ReadParams.Source, CredentialsProvider))
    {}

    void Bootstrap() {
        Schedule(PoisonTimeout, new NActors::TEvents::TEvPoison());

        LOG_I("TDqSolomonMetricsQueueActor", "Bootstrap there are metrics to list, consumersCount=" << ConsumersCount);
        Become(&TDqSolomonMetricsQueueActor::ThereAreMetricsToListState);
    }

    STATEFN(ThereAreMetricsToListState) {
        try {
            switch (const auto etype = ev->GetTypeRewrite()) {
                hFunc(TEvSolomonProvider::TEvGetNextBatch, HandleGetNextBatch);
                hFunc(TEvPrivatePrivate::TEvNextListingChunkReceived, HandleNextListingChunkReceived);
                cFunc(TEvPrivatePrivate::EvRoundRobinStageTimeout, HandleRoundRobinStageTimeout);
                hFunc(TEvPrivatePrivate::TEvTransitToErrorState, HandleTransitToErrorState);
                cFunc(NActors::TEvents::TSystem::Poison, HandlePoison);
                default:
                    MaybeIssues = TStringBuilder{} << "An event with unknown type has been received: '" << etype << "'";
                    TransitToErrorState();
                    break;
            }
        } catch (const std::exception& e) {
            MaybeIssues = TStringBuilder{} << "An unknown exception has occurred: '" << e.what() << "'";
            TransitToErrorState();
        }
    }

    STATEFN(NoMoreMetricsState) {
        try {
            switch (const auto etype = ev->GetTypeRewrite()) {
                hFunc(TEvSolomonProvider::TEvGetNextBatch, HandleGetNextBatchForEmptyState);
                cFunc(TEvPrivatePrivate::EvRoundRobinStageTimeout, HandleRoundRobinStageTimeout);
                cFunc(NActors::TEvents::TSystem::Poison, HandlePoison);
                default:
                    MaybeIssues = TStringBuilder{} << "An event with unknown type has been received: '" << etype << "'";
                    TransitToErrorState();
                    break;
            }
        } catch (const std::exception& e) {
            MaybeIssues = TStringBuilder{} << "An unknown exception has occurred: '" << e.what() << "'";
            TransitToErrorState();
        }
    }

    STATEFN(AnErrorOccurredState) {
        try {
            switch (const auto etype = ev->GetTypeRewrite()) {
                hFunc(TEvSolomonProvider::TEvGetNextBatch, HandleGetNextBatchForErrorState);
                cFunc(TEvPrivatePrivate::EvRoundRobinStageTimeout, HandleRoundRobinStageTimeout);
                cFunc(NActors::TEvents::TSystem::Poison, HandlePoison);
                default:
                    MaybeIssues = TStringBuilder{} << "An event with unknown type has been received: '" << etype << "'";
                    TransitToErrorState();
                    break;
            }
        } catch (const std::exception& e) {
            MaybeIssues = TStringBuilder{} << "An unknown exception has occurred: '" << e.what() << "'";
            TransitToErrorState();
        }
    }

private:
    void HandleGetNextBatch(TEvSolomonProvider::TEvGetNextBatch::TPtr& ev) {
        if (HasEnoughToSend()) {
            LOG_I("TDqSolomonMetricsQueueActor", "HandleGetNextBatch has enough metrics to send, trying to send them");
            TrySendMetrics(ev->Sender, ev->Get()->Record.GetTransportMeta());
            TryPreFetch();
        } else {
            LOG_I("TDqSolomonMetricsQueueActor", "HandleGetNextBatch doesn't have enough to send, trying to fetch");
            ScheduleRequest(ev->Sender, ev->Get()->Record.GetTransportMeta());
            TryFetch();
        }
    }

    void HandleNextListingChunkReceived(TEvPrivatePrivate::TEvNextListingChunkReceived::TPtr& ev) {
        YQL_ENSURE(ListingFuture.Defined());
        ListingFuture = Nothing();
        LOG_D("TDqSolomonMetricsQueueActor", "HandleNextListingChunkReceived");
        if (SaveRetrievedResults(ev->Get()->ListingResult)) {
            AnswerPendingRequests(true);
            if (!HasPendingRequests) {
                LOG_D("TDqSolomonMetricsQueueActor", "HandleNextListingChunkReceived no pending requests. Trying to prefetch");
                TryPreFetch();
            } else {
                LOG_D("TDqSolomonMetricsQueueActor", "HandleNextListingChunkReceived there are pending requests. Fetching more metrics");
                TryFetch();
            }
        } else {
            TransitToErrorState();
        }
    }

    void HandleRoundRobinStageTimeout() {
        LOG_T("TDqSolomonMetricsQueueActor", "Handle round robin stage timeout");
        if (!RoundRobinStageFinished) {
            RoundRobinStageFinished = true;
            AnswerPendingRequests();
        }
    }

    void HandleTransitToErrorState(TEvPrivatePrivate::TEvTransitToErrorState::TPtr& ev) {
        MaybeIssues = ev->Get()->Issues;
        TransitToErrorState();
    }

    void HandlePoison() {
        AnswerPendingRequests();
        PassAway();
    }

    void HandleGetNextBatchForEmptyState(TEvSolomonProvider::TEvGetNextBatch::TPtr& ev) {
        LOG_T("TDqSolomonMetricsQueueActor", "HandleGetNextBatchForEmptyState giving away rest of Objects");
        TrySendMetrics(ev->Sender, ev->Get()->Record.GetTransportMeta());
    }

    void HandleGetNextBatchForErrorState(TEvSolomonProvider::TEvGetNextBatch::TPtr& ev) {
        LOG_D("TDqSolomonMetricsQueueActor", "HandleGetNextBatchForErrorState sending issues");
        Send(ev->Sender, new TEvSolomonProvider::TEvMetricsReadError(*MaybeIssues, ev->Get()->Record.GetTransportMeta()));
        TryFinish(ev->Sender, ev->Get()->Record.GetTransportMeta().GetSeqNo());
    }

    void PassAway() override {
        LOG_I("TDqSolomonMetricsQueueActor", "PassAway, processed " << ProcessedMetrics << " metrics");
        TBase::PassAway();
    }

    void TransitToErrorState() {
        Y_ENSURE(MaybeIssues.Defined());
        LOG_I("TDqSolomonMetricsQueueActor", "TransitToErrorState an error occurred, sending issues");
        AnswerPendingRequests();
        Metrics.clear();
        Become(&TDqSolomonMetricsQueueActor::AnErrorOccurredState);
    }

    bool SaveRetrievedResults(const NSo::ISolomonAccessorClient::TListMetricsResult& listingResult) {
        LOG_T("TDqSolomonMetricsQueueActor", "SaveRetrievedResults");
        if (!listingResult.Success) {
            MaybeIssues = listingResult.ErrorMsg;
            return false;
        }

        if (CurrentPage >= listingResult.PagesCount) {
            LOG_I("TDqSolomonMetricsQueueActor", "SaveRetrievedResults no more metrics to list");
            HasMoreMetrics = false;
            Become(&TDqSolomonMetricsQueueActor::NoMoreMetricsState);
        }

        LOG_D("TDqSolomonMetricsQueueActor", "SaveRetrievedResults saving: " << listingResult.Result.size() << " metrics");
        for (const auto& metric : listingResult.Result) {
            NSo::MetricQueue::TMetricLabels protoMetric;
            protoMetric.SetType(metric.Type);
            protoMetric.MutableLabels()->insert(metric.Labels.begin(), metric.Labels.end());
            Metrics.emplace_back(std::move(protoMetric));
        }
        return true;
    }

    bool TryPreFetch() {
        if (Metrics.size() < PrefetchSize) {
            return TryFetch();
        }
        return false;
    }

    bool TryFetch() {
        if (FetchingInProgress()) {
            LOG_D("TDqSolomonMetricsQueueActor", "TryFetch fetching already in progress");
            return true;
        }

        if (HasMoreMetrics) {
            LOG_D("TDqSolomonMetricsQueueActor", "TryFetch fetching metrics");
            Fetch();
            return true;
        }

        LOG_D("TDqSolomonMetricsQueueActor", "TryFetch couldn't start fetching");
        AnswerPendingRequests();
        return false;
    }

    void Fetch() {
        NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
        ListingFuture = 
            SolomonClient
                ->ListMetrics(ReadParams.Source.GetSelectors(), PageSize, CurrentPage++)
                .Subscribe([actorSystem, selfId = SelfId()](
                                const NThreading::TFuture<NSo::ISolomonAccessorClient::TListMetricsResult>& future) -> void {
                    try {
                        actorSystem->Send(
                            selfId, 
                            new TEvPrivatePrivate::TEvNextListingChunkReceived(
                                std::move(future.GetValue())));
                    } catch (const std::exception& e) {
                        actorSystem->Send(
                            selfId,
                            new TEvPrivatePrivate::TEvTransitToErrorState(TStringBuilder() << "An unknown exception has occurred: '" << e.what() << "'"));
                    }
                });
    }

    bool FetchingInProgress() const {
        return ListingFuture.Defined();
    }

    void AnswerPendingRequests(bool earlyStop = false) {
        bool handledRequest = true;
        while (HasPendingRequests && handledRequest) {
            handledRequest = false;
            HasPendingRequests = false;

            for (auto& [consumer, requests] : PendingRequests) {
                if (!CanSendToConsumer(consumer) || (earlyStop && !HasEnoughToSend())) {
                    continue;
                }

                if (!requests.empty()) {
                    if (MaybeIssues.Defined()) {
                        Send(consumer, new TEvSolomonProvider::TEvMetricsReadError(*MaybeIssues, requests.front()));
                        TryFinish(consumer, requests.front().GetSeqNo());
                    } else {
                        SendMetrics(consumer, requests.front());
                    }

                    requests.pop_front();
                    handledRequest = true;
                }
            }

            for (const auto& [consumer, requests] : PendingRequests) {
                if (!requests.empty()) {
                    HasPendingRequests = true;
                    break;
                }
            }
        }
    }

    void ScheduleRequest(const NActors::TActorId& consumer, const NDqProto::TMessageTransportMeta& transportMeta) {
        PendingRequests[consumer].push_back(transportMeta);
        HasPendingRequests = true;
    }

    bool CanSendToConsumer(const NActors::TActorId& consumer) const {
        return RoundRobinStageFinished ||
               (StartedConsumers.size() < ConsumersCount && !StartedConsumers.contains(consumer));
    }

    bool HasEnoughToSend() const {
        return Metrics.size() >= BatchCountLimit;
    }
    
    bool HasNoMoreItems() const {
        return !HasMoreMetrics && Metrics.empty();
    }

    void TrySendMetrics(const NActors::TActorId& consumer, const NDqProto::TMessageTransportMeta& transportMeta) {
        if (CanSendToConsumer(consumer)) {
            LOG_I("TDqSolomonMetricsQueueActor", "TrySendMetrics can send metrics to consumer");
            SendMetrics(consumer, transportMeta);
        } else {
            LOG_I("TDqSolomonMetricsQueueActor", "TrySendMetrics can't send metrics to consumer, scheduling request");
            ScheduleRequest(consumer, transportMeta);
        }
    }

    void SendMetrics(const NActors::TActorId& consumer, const NDqProto::TMessageTransportMeta& transportMeta) {
        YQL_ENSURE(!MaybeIssues.Defined());
        std::vector<NSo::MetricQueue::TMetricLabels> result;
        while (!Metrics.empty() && result.size() < BatchCountLimit) {
            result.push_back(Metrics.back());
            Metrics.pop_back();
            ProcessedMetrics++;
        }

        LOG_D("TDqSolomonMetricsQueueActor", "SendMetrics Sending " << result.size() << " metrics to consumer with id " << consumer);
        Send(consumer, new TEvSolomonProvider::TEvMetricsBatch(std::move(result), HasNoMoreItems(), transportMeta));

        if (HasNoMoreItems()) {
            TryFinish(consumer, transportMeta.GetSeqNo());
        }

        if (!RoundRobinStageFinished) {
            if (StartedConsumers.empty()) {
                Schedule(RoundRobinStageTimeout, new TEvPrivatePrivate::TEvRoundRobinStageTimeout());
            }
            StartedConsumers.insert(consumer);
            if ((StartedConsumers.size() == ConsumersCount || HasNoMoreItems()) && !IsRoundRobinFinishScheduled) {
                IsRoundRobinFinishScheduled = true;
                Send(SelfId(), new TEvPrivatePrivate::TEvRoundRobinStageTimeout());
            }
        }
    }

    void TryFinish(const NActors::TActorId& consumer, ui64 seqNo) {
        LOG_T("TDqSolomonMetricsQueueActor", "TryFinish from consumer " << consumer << ", " << FinishedConsumers.size() << " consumers already finished, seqNo=" << seqNo);
        if (auto it = FinishingConsumerToLastSeqNo.find(consumer); it != FinishingConsumerToLastSeqNo.end()) {
            LOG_T("TDqSolomonMetricsQueueActor", "TryFinish FinishingConsumerToLastSeqNo=" << FinishingConsumerToLastSeqNo[consumer]);
            if (it->second < seqNo || SelfId().NodeId() == consumer.NodeId()) {
                FinishedConsumers.insert(consumer);
                if (FinishedConsumers.size() == ConsumersCount) {
                    PassAway();
                }
            }
        } else {
            FinishingConsumerToLastSeqNo[consumer] = seqNo;
        }
    }

private:
    ui64 CurrentPage;
    ui64 ProcessedMetrics = 0;
    ui64 ConsumersCount;
    bool HasMoreMetrics = true;
    bool IsRoundRobinFinishScheduled = false;
    bool RoundRobinStageFinished = false;
    THashSet<NActors::TActorId> StartedConsumers;
    THashSet<NActors::TActorId> FinishedConsumers;
    THashMap<NActors::TActorId, ui64> FinishingConsumerToLastSeqNo;

    TMaybe<NThreading::TFuture<NSo::ISolomonAccessorClient::TListMetricsResult>> ListingFuture;
    bool HasPendingRequests;
    THashMap<NActors::TActorId, TDeque<NDqProto::TMessageTransportMeta>> PendingRequests;
    std::vector<NSo::MetricQueue::TMetricLabels> Metrics;
    TMaybe<TString> MaybeIssues;
    
    ui64 PageSize;
    ui64 PrefetchSize;
    ui64 BatchCountLimit;
    const TDqSolomonReadParams ReadParams;
    const std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
    const NSo::ISolomonAccessorClient::TPtr SolomonClient;

    static constexpr TDuration PoisonTimeout = TDuration::Hours(3);
    static constexpr TDuration RoundRobinStageTimeout = TDuration::Seconds(3);
};


} // namespace

NActors::IActor* CreateSolomonMetricsQueueActor(
    ui64 consumersCount,
    TDqSolomonReadParams readParams,
    std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider)
{
    const auto& settings = readParams.Source.settings();

    ui64 pageSize = 0;
    if (auto it = settings.find("metricsQueuePageSize"); it != settings.end()) {
        pageSize = FromString<ui64>(it->second);
    }

    ui64 prefetchSize = 0;
    if (auto it = settings.find("metricsQueuePrefetchSize"); it != settings.end()) {
        prefetchSize = FromString<ui64>(it->second);
    }

    ui64 batchCountLimit = 0;
    if (auto it = settings.find("metricsQueueBatchCountLimit"); it != settings.end()) {
        batchCountLimit = FromString<ui64>(it->second);
    }

    return new TDqSolomonMetricsQueueActor(consumersCount, pageSize, prefetchSize, batchCountLimit, std::move(readParams), credentialsProvider);
}

} // namespace NYql::NDq
