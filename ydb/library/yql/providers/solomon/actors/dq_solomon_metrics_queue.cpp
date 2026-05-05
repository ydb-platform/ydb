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

#include <util/generic/size_literals.h>
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
        bool enableSolomonClientPostApi,
        ui64 batchCountLimit,
        ui64 prefetchSize,
        TDuration truePointsFindRange,
        ui64 maxListingPageSize,
        ui64 maxApiInflight,
        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider)
        : ConsumersCount(consumersCount)
        , ReadParams(std::move(readParams))
        , EnableSolomonClientPostApi(enableSolomonClientPostApi)
        , BatchCountLimit(batchCountLimit)
        , PrefetchSize(prefetchSize)
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
        ConnectedConsumers.insert(ev->Sender);
        if (const auto [it, inserted] = UpdatedConsumers.emplace(ev->Sender); inserted) {
            const ui64 delta = ev->Get()->Record.GetConsumersCountDelta();
            LOG_D("TDqSolomonMetricsQueueActor",
                "HandleUpdateConsumersCount Reducing ConsumersCount by " << delta << ", received from " << ev->Sender);
            if (delta <= ConsumersCount) {
                ConsumersCount -= delta;
            } else {
                LOG_E("TDqSolomonMetricsQueueActor",
                    "HandleUpdateConsumersCount delta=" << delta << " exceeds ConsumersCount=" << ConsumersCount << ", clamping to 0");
                ConsumersCount = 0;
            }
        }
        Send(ev->Sender, new TEvSolomonProvider::TEvAck(ev->Get()->Record.GetTransportMeta()));
    }

    void HandleGetNextBatch(TEvSolomonProvider::TEvGetNextBatch::TPtr& ev) {
        ConnectedConsumers.insert(ev->Sender);
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

        auto listLabelsResult = std::move(batch.Response.Result);
        if (listLabelsResult.TotalCount <= MaxListingPageSize) {
            PendingListingRequests.push_back(std::move(batch.Selectors));
        } else {
            auto selectors = batch.Selectors;  // intentional copy — will be mutated per-batch below
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

            double metricsPerLabelValue = std::max<double>(1, listLabelsResult.TotalCount * 1.0 / label.Values.size());
            ui64 batchSize = std::max<ui64>(1, MaxListingPageSize * 0.75 / metricsPerLabelValue);

            if (!EnableSolomonClientPostApi) {
                ui64 sumLength = 0;
                for (const auto& value: label.Values) {
                    sumLength += value.size();
                }

                double avgLength = std::max<double>(1.0, sumLength * 1.0 / label.Values.size());
                batchSize = std::min<ui64>(batchSize, MaxHttpGetRequestSize * 0.5 / avgLength);
            }

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
        // PoisonTimeout is a safety net for the case where some read actors are never
        // bootstrapped (e.g. node failure during query startup).  Once we know that all
        // consumers are alive, we can safely ignore the timeout and let the normal
        // shutdown path run.
        if (ConnectedConsumers.size() == ConsumersCount) {
            LOG_D("TDqSolomonMetricsQueueActor", "HandlePoison: consumers are active, ignoring PoisonTimeout");
            return;
        }
        LOG_I("TDqSolomonMetricsQueueActor", "HandlePoison: no consumer messages received, shutting down");
        AnswerPendingRequests();
        PassAway();
    }

    void HandleGetNextBatchForEmptyState(TEvSolomonProvider::TEvGetNextBatch::TPtr& ev) {
        ConnectedConsumers.insert(ev->Sender);
        LOG_T("TDqSolomonMetricsQueueActor", "HandleGetNextBatchForEmptyState giving away rest of Objects");
        TrySendMetrics(ev->Sender, ev->Get()->Record.GetTransportMeta());
    }

    void HandleGetNextBatchForErrorState(TEvSolomonProvider::TEvGetNextBatch::TPtr& ev) {
        ConnectedConsumers.insert(ev->Sender);
        LOG_D("TDqSolomonMetricsQueueActor", "HandleGetNextBatchForErrorState sending issues");
        Send(ev->Sender, new TEvSolomonProvider::TEvMetricsReadError(*MaybeIssues, ev->Get()->Record.GetTransportMeta()));
        TryFinish(ev->Sender, ev->Get()->Record.GetTransportMeta().GetSeqNo());
    }

    void PassAway() override {
        LOG_I("TDqSolomonMetricsQueueActor", "PassAway, processed " << ProcessedMetrics << " metrics");
        // Explicitly cancel all in-flight gRPC requests before the actor dies.
        // ~TSolomonAccessorClient() calls GrpcClient->Stop() which drains the
        // completion queue; doing it here ensures cancellation happens before
        // actor memory is freed.
        SolomonClient.reset();
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
            LOG_D("TDqSolomonMetricsQueueActor", "TryFetch can't start fetching, have " << CurrentInflight << " inflight requests, current limit: " << MaxApiInflight);
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

        if (Metrics.size() >= PrefetchSize) {
            LOG_D("TDqSolomonMetricsQueueActor", "TryFetch can't start fetching, have " << Metrics.size() << " metrics stored, current limit: " << PrefetchSize);
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
            result.push_back(std::move(Metrics.back()));
            Metrics.pop_back();
            ProcessedMetrics++;
        }

        while (TryFetch()) {}

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
    THashSet<NActors::TActorId> ConnectedConsumers;
    THashSet<NActors::TActorId> FinishedConsumers;
    THashMap<NActors::TActorId, ui64> FinishingConsumerToLastSeqNo;

    bool HasPendingRequests = false;
    THashMap<NActors::TActorId, TDeque<NDqProto::TMessageTransportMeta>> PendingRequests;
    std::vector<NSo::TSelectors> PendingLabelRequests;
    std::vector<NSo::TSelectors> PendingListingRequests;
    std::vector<NSo::MetricQueue::TMetric> Metrics;
    ui64 DownloadedBytes = 0;
    TMaybe<TString> MaybeIssues;
    
    const TDqSolomonReadParams ReadParams;
    const bool EnableSolomonClientPostApi;
    const ui64 BatchCountLimit;
    const ui64 PrefetchSize;
    const TInstant TrueRangeFrom;
    const TInstant TrueRangeTo;
    const ui64 MaxListingPageSize;
    const ui64 MaxApiInflight;
    const ui64 MaxHttpGetRequestSize = 4_KB;
    const std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
    NSo::ISolomonAccessorClient::TPtr SolomonClient;

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

    bool enableSolomonClientPostApi = false;
    if (auto it = settings.find("enableSolomonClientPostApi"); it != settings.end()) {
        enableSolomonClientPostApi = FromString<bool>(it->second);
    }

    ui64 batchCountLimit = 0;
    if (auto it = settings.find("metricsQueueBatchCountLimit"); it != settings.end()) {
        batchCountLimit = FromString<ui64>(it->second);
    }
    
    ui64 prefetchSize = 1000;
    if (auto it = settings.find("metricsQueuePrefetchSize"); it != settings.end()) {
        prefetchSize = FromString<ui64>(it->second);
    }

    ui64 truePointsFindRange = 301;
    if (auto it = settings.find("truePointsFindRange"); it != settings.end()) {
        truePointsFindRange = FromString<ui64>(it->second);
    }

    ui64 maxListingPageSize = 20000;
    if (auto it = settings.find("maxListingPageSize"); it != settings.end()) {
        maxListingPageSize = FromString<ui64>(it->second);
    }

    ui64 maxApiInflight = 40;
    if (auto it = settings.find("maxApiInflight"); it != settings.end()) {
        maxApiInflight = FromString<ui64>(it->second);
    }

    return new TDqSolomonMetricsQueueActor(consumersCount, std::move(readParams), enableSolomonClientPostApi, batchCountLimit, prefetchSize, TDuration::Seconds(truePointsFindRange), maxListingPageSize, maxApiInflight, credentialsProvider);
}

} // namespace NYql::NDq
