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

#include <util/string/join.h>

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

            EvNextLabelsListingChunkReceived = EvBegin,
            EvNextMetricsListingChunkReceived,
            EvRoundRobinStageTimeout,
            EvTransitToErrorState,

            EvEnd
        };
        static_assert(
            EvEnd <= EventSpaceEnd(NActors::TEvents::ES_PRIVATE),
            "expected EvEnd <= EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvNextLabelsListingChunkReceived : public NActors::TEventLocal<TEvNextLabelsListingChunkReceived, EvNextLabelsListingChunkReceived> {
            NSo::TSelectors Selectors;
            NSo::TListMetricsLabelsResponse Response;
            explicit TEvNextLabelsListingChunkReceived(NSo::TSelectors&& selectors, NSo::TListMetricsLabelsResponse&& response)
                : Selectors(std::move(selectors))
                , Response(std::move(response)) {}
        };

        struct TEvNextMetricsListingChunkReceived : public NActors::TEventLocal<TEvNextMetricsListingChunkReceived, EvNextMetricsListingChunkReceived> {
            NSo::TListMetricsResponse Response;
            explicit TEvNextMetricsListingChunkReceived(NSo::TListMetricsResponse&& response)
                : Response(std::move(response)) {}
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
        TDqSolomonReadParams&& readParams,
        ui64 batchCountLimit,
        TDuration truePointsFindRange,
        ui64 maxListingPageSize,
        ui64 maxApiInflight,
        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider)
        : ConsumersCount(consumersCount)
        , ReadParams(std::move(readParams))
        , BatchCountLimit(batchCountLimit)
        , TrueRangeFrom(TInstant::Seconds(ReadParams.Source.GetFrom()) - truePointsFindRange)
        , TrueRangeTo(TInstant::Seconds(ReadParams.Source.GetTo()) + truePointsFindRange)
        , MaxListingPageSize(maxListingPageSize)
        , MaxApiInflight(maxApiInflight)
        , CredentialsProvider(credentialsProvider)
        , SolomonClient(NSo::ISolomonAccessorClient::Make(ReadParams.Source, CredentialsProvider))
    {}

    void Bootstrap() {
        Schedule(PoisonTimeout, new NActors::TEvents::TEvPoison());

        LOG_I("TDqSolomonMetricsQueueActor", "Bootstrap there are metrics to list, consumersCount=" << ConsumersCount);
        Become(&TDqSolomonMetricsQueueActor::ThereAreMetricsToListState);

        NSo::TSelectors selectors;
        NSo::ProtoToSelectors(ReadParams.Source.GetSelectors(), selectors);
        PendingLabelRequests.push_back(selectors);
        TryFetch();
    }

    STATEFN(ThereAreMetricsToListState) {
        try {
            switch (const auto etype = ev->GetTypeRewrite()) {
                hFunc(TEvSolomonProvider::TEvUpdateConsumersCount, HandleUpdateConsumersCount);
                hFunc(TEvSolomonProvider::TEvGetNextBatch, HandleGetNextBatch);
                hFunc(TEvPrivatePrivate::TEvNextLabelsListingChunkReceived, HandleNextLabelsListingChunkReceived);
                hFunc(TEvPrivatePrivate::TEvNextMetricsListingChunkReceived, HandleNextMetricsListingChunkReceived);
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

    STATEFN(NoMoreMetricsState) {
        try {
            switch (const auto etype = ev->GetTypeRewrite()) {
                hFunc(TEvSolomonProvider::TEvUpdateConsumersCount, HandleUpdateConsumersCount);
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
                hFunc(TEvSolomonProvider::TEvUpdateConsumersCount, HandleUpdateConsumersCount);
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
    void HandleUpdateConsumersCount(TEvSolomonProvider::TEvUpdateConsumersCount::TPtr& ev) {
        if (const auto [it, inserted] = UpdatedConsumers.emplace(ev->Sender); inserted) {
            LOG_D("TDqSolomonMetricsQueueActor",
                "HandleUpdateConsumersCount Reducing ConsumersCount by " << ev->Get()->Record.GetConsumersCountDelta() << ", received from " << ev->Sender);
            ConsumersCount -= ev->Get()->Record.GetConsumersCountDelta();
        }
        Send(ev->Sender, new TEvSolomonProvider::TEvAck(ev->Get()->Record.GetTransportMeta()));
    }

    void HandleGetNextBatch(TEvSolomonProvider::TEvGetNextBatch::TPtr& ev) {
        if (HasEnoughToSend()) {
            LOG_I("TDqSolomonMetricsQueueActor", "HandleGetNextBatch has enough metrics to send, trying to send them");
            TrySendMetrics(ev->Sender, ev->Get()->Record.GetTransportMeta());
        } else {
            LOG_I("TDqSolomonMetricsQueueActor", "HandleGetNextBatch doesn't have enough to send, trying to fetch");
            ScheduleRequest(ev->Sender, ev->Get()->Record.GetTransportMeta());
            TryFetch();
        }
    }

    void HandleNextLabelsListingChunkReceived(TEvPrivatePrivate::TEvNextLabelsListingChunkReceived::TPtr& ev) {
        LOG_D("TDqSolomonMetricsQueueActor", "HandleNextLabelsListingChunkReceived");
        auto& batch = *ev->Get();
        CurrentInflight--;
        DownloadedBytes += batch.Response.DownloadedBytes;

        if (batch.Response.Status != NSo::EStatus::STATUS_OK) {
            MaybeIssues = batch.Response.Error;
            TransitToErrorState();
            return;
        }

        auto listLabelsResult = batch.Response.Result;
        if (listLabelsResult.TotalCount <= MaxListingPageSize) {
            PendingListingRequests.push_back(batch.Selectors);
        } else {
            auto selectors = batch.Selectors;
            auto& labels = listLabelsResult.Labels;
            auto maxSizeLabelIt = std::max_element(labels.begin(), labels.end(),
                [](const NSo::TLabelValues& a, const NSo::TLabelValues& b) {
                    return std::make_pair(!a.Truncated, a.Values.size()) < std::make_pair(!b.Truncated, b.Values.size());
                }
            );

            if (maxSizeLabelIt->Truncated) {
                MaybeIssues = "couldn't list metrics, all label values are too big for listing";
                TransitToErrorState();
                return;
            }

            auto& label = *maxSizeLabelIt;

            if (label.Values.empty()) {
                return;
            }

            ui64 metricsPerLabelValue = std::max<ui64>(1, listLabelsResult.TotalCount / label.Values.size());
            ui64 batchSize = std::max<ui64>(1, MaxListingPageSize * 0.75 / metricsPerLabelValue);

            for (ui64 i = 0; i * batchSize < label.Values.size(); i++) {
                auto batchFromIt = label.Values.begin() + i * batchSize;
                auto batchToIt = label.Values.begin() + std::min<ui64>((i + 1) * batchSize, label.Values.size());

                selectors[label.Name] = { "=", JoinRange("|", batchFromIt, batchToIt) };
                PendingLabelRequests.push_back(selectors);
            }
            if (label.Absent) {
                selectors[label.Name] = { "=", "-" };
                PendingLabelRequests.push_back(selectors);
            }
            
        }

        while (TryFetch()) {}
    }

    void HandleNextMetricsListingChunkReceived(TEvPrivatePrivate::TEvNextMetricsListingChunkReceived::TPtr& ev) {
        LOG_D("TDqSolomonMetricsQueueActor", "HandleNextMetricsListingChunkReceived");
        auto& batch = *ev->Get();
        CurrentInflight--;
        DownloadedBytes += batch.Response.DownloadedBytes;

        if (batch.Response.Status != NSo::EStatus::STATUS_OK) {
            MaybeIssues = batch.Response.Error;
            TransitToErrorState();
            return;
        }

        SaveRetrievedResults(batch.Response);
        AnswerPendingRequests(true);
        while (TryFetch()) {}
    }

    void HandleRoundRobinStageTimeout() {
        LOG_T("TDqSolomonMetricsQueueActor", "Handle round robin stage timeout");
        if (!RoundRobinStageFinished) {
            RoundRobinStageFinished = true;
            AnswerPendingRequests();
        }
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

    void SaveRetrievedResults(const NSo::TListMetricsResponse& response) {
        LOG_T("TDqSolomonMetricsQueueActor", "SaveRetrievedResults");

        LOG_D("TDqSolomonMetricsQueueActor", "SaveRetrievedResults saving: " << response.Result.Metrics.size() << " metrics");
        for (const auto& metric : response.Result.Metrics) {
            NSo::MetricQueue::TMetric protoMetric;
            protoMetric.SetType(metric.Type);
            NSo::SelectorsToProto(metric.Selectors, *protoMetric.MutableSelectors());
            Metrics.emplace_back(std::move(protoMetric));
        }
    }

    bool TryFetch() {
        if (CurrentInflight >= MaxApiInflight) {
            LOG_D("TDqSolomonMetricsQueueActor", "TryFetch can't start fetching, have " << CurrentInflight << " inflight requests, current max: " << MaxApiInflight);
            return false;
        }

        if (PendingLabelRequests.empty() && PendingListingRequests.empty()) {
            LOG_D("TDqSolomonMetricsQueueActor", "TryFetch doesn't have anything to fetch yet, current inflight: " << CurrentInflight);

            if (!CurrentInflight) {
                Become(&TDqSolomonMetricsQueueActor::NoMoreMetricsState);
                AnswerPendingRequests();
            }
            return false;
        }

        LOG_D("TDqSolomonMetricsQueueActor", "TryFetch fetching metrics");
        Fetch();
        return true;
    }

    void Fetch() {
        YQL_ENSURE(!PendingLabelRequests.empty() || !PendingListingRequests.empty());
        NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
        CurrentInflight++;
        
        if (!PendingLabelRequests.empty()) {
            auto selectors = PendingLabelRequests.back();
            PendingLabelRequests.pop_back();
            
            auto labelsListingFuture = SolomonClient->ListMetricsLabels(selectors, TrueRangeFrom, TrueRangeTo);
            labelsListingFuture.Subscribe([actorSystem, selectors = std::move(selectors), selfId = SelfId()]
                (NThreading::TFuture<NSo::TListMetricsLabelsResponse> future) mutable {
                actorSystem->Send(
                    selfId, 
                    new TEvPrivatePrivate::TEvNextLabelsListingChunkReceived(std::move(selectors), future.ExtractValue()));
            });

            return;
        }
        
        if (!PendingListingRequests.empty()) {
            auto selectors = PendingListingRequests.back();
            PendingListingRequests.pop_back();

            auto metricsListingFuture = SolomonClient->ListMetrics(selectors, TrueRangeFrom, TrueRangeTo);
            metricsListingFuture.Subscribe([actorSystem, selfId = SelfId()]
                (NThreading::TFuture<NSo::TListMetricsResponse> future) {
                actorSystem->Send(
                    selfId, 
                    new TEvPrivatePrivate::TEvNextMetricsListingChunkReceived(future.ExtractValue()));
            });

            return;
        }
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
        return CurrentInflight == 0 && PendingLabelRequests.empty() && PendingListingRequests.empty() && Metrics.empty();
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
        std::vector<NSo::MetricQueue::TMetric> result;
        result.reserve(std::min<ui64>(BatchCountLimit, Metrics.size()));
        while (!Metrics.empty() && result.size() < BatchCountLimit) {
            result.push_back(Metrics.back());
            Metrics.pop_back();
            ProcessedMetrics++;
        }

        LOG_D("TDqSolomonMetricsQueueActor", "SendMetrics Sending " << result.size() << " metrics to consumer with id " << consumer);
        Send(consumer, new TEvSolomonProvider::TEvMetricsBatch(std::move(result), HasNoMoreItems(), DownloadedBytes, transportMeta));
        DownloadedBytes = 0;

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
    ui64 ProcessedMetrics = 0;
    ui64 ConsumersCount;
    bool IsRoundRobinFinishScheduled = false;
    bool RoundRobinStageFinished = false;
    ui64 CurrentInflight = 0;
    THashSet<NActors::TActorId> StartedConsumers;
    THashSet<NActors::TActorId> UpdatedConsumers;
    THashSet<NActors::TActorId> FinishedConsumers;
    THashMap<NActors::TActorId, ui64> FinishingConsumerToLastSeqNo;

    bool HasPendingRequests;
    THashMap<NActors::TActorId, TDeque<NDqProto::TMessageTransportMeta>> PendingRequests;
    std::vector<NSo::TSelectors> PendingLabelRequests;
    std::vector<NSo::TSelectors> PendingListingRequests;
    std::vector<NSo::MetricQueue::TMetric> Metrics;
    ui64 DownloadedBytes = 0;
    TMaybe<TString> MaybeIssues;
    
    const TDqSolomonReadParams ReadParams;
    const ui64 BatchCountLimit;
    const TInstant TrueRangeFrom;
    const TInstant TrueRangeTo;
    const ui64 MaxListingPageSize;
    const ui64 MaxApiInflight;
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

    ui64 batchCountLimit = 0;
    if (auto it = settings.find("metricsQueueBatchCountLimit"); it != settings.end()) {
        batchCountLimit = FromString<ui64>(it->second);
    }

    ui64 truePointsFindRange = 301;
    if (auto it = settings.find("truePointsFindRange"); it != settings.end()) {
        truePointsFindRange = FromString<ui64>(it->second);
    }

    ui64 maxListingPageSize = 20000;
    if (auto it = settings.find("maxListingPageSize"); it != settings.end()) {
        maxListingPageSize = FromString<ui64>(it->second);
    }

    ui64 maxInflight = 40;
    if (auto it = settings.find("maxApiInflight"); it != settings.end()) {
        maxInflight = FromString<ui64>(it->second);
    }

    return new TDqSolomonMetricsQueueActor(consumersCount, std::move(readParams), batchCountLimit, TDuration::Seconds(truePointsFindRange), maxListingPageSize, maxInflight, credentialsProvider);
}

} // namespace NYql::NDq
