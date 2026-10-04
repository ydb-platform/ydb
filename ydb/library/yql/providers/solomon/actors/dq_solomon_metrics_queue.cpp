#include "dq_solomon_metrics_queue.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/dq/actors/common/retry_queue.h>
#include <ydb/library/yql/providers/solomon/events/events.h>
#include <ydb/library/yql/providers/solomon/solomon_accessor/client/solomon_accessor_client.h>
#include <ydb/library/yql/providers/solomon/common/constants.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/utils/yql_panic.h>

#include <util/generic/size_literals.h>
#include <util/string/join.h>

#include <algorithm>

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
            EvBegin = TEvRetryQueuePrivate::EvEnd,

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
        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider,
        const NSo::TSolomonReadActorConfig& cfg,
        NSo::ISolomonAccessorClient::TPtr solomonClient)
        : ConsumersCount(consumersCount)
        , ReadParams(std::move(readParams))
        , EnableSolomonClientPostApi(cfg.EnablePostApi)
        , BatchCountLimit(cfg.MetricsQueueBatchCountLimit)
        , PrefetchSize(cfg.MetricsQueuePrefetchSize)
        , TrueRangeFrom(TInstant::Seconds(ReadParams.Source.GetFrom()) - TDuration::Seconds(cfg.TruePointsFindRangeSec))
        , TrueRangeTo(TInstant::Seconds(ReadParams.Source.GetTo()) + TDuration::Seconds(cfg.TruePointsFindRangeSec))
        , MaxListingPageSize(cfg.MaxListingPageSize)
        , MaxApiInflight(cfg.MaxApiInflight)
        , PoisonTimeout(cfg.PoisonTimeout)
        , RoundRobinStageTimeout(cfg.RoundRobinStageTimeout)
        , CredentialsProvider(credentialsProvider)
        , SolomonClient(solomonClient ? std::move(solomonClient) : NSo::ISolomonAccessorClient::Make(ReadParams.Source, CredentialsProvider, cfg))
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
                hFunc(TEvRetryQueuePrivate::TEvRetry, HandleRetry);
                hFunc(TEvRetryQueuePrivate::TEvEvHeartbeat, HandleHeartbeat);
                hFunc(NActors::TEvInterconnect::TEvNodeConnected, HandleConnected);
                hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
                hFunc(NActors::TEvents::TEvUndelivered, HandleUndelivered);
                hFunc(NActors::TEvents::TEvWakeup, HandleDisconnectDeadline);
                hFunc(TEvSolomonProvider::TEvAck, HandleAck);
                hFunc(TEvSolomonProvider::TEvGetNextBatch, HandleGetNextBatch);
                hFunc(TEvSolomonProvider::TEvConsumerFinished, HandleConsumerFinished);
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
                hFunc(TEvRetryQueuePrivate::TEvRetry, HandleRetry);
                hFunc(TEvRetryQueuePrivate::TEvEvHeartbeat, HandleHeartbeat);
                hFunc(NActors::TEvInterconnect::TEvNodeConnected, HandleConnected);
                hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
                hFunc(NActors::TEvents::TEvUndelivered, HandleUndelivered);
                hFunc(NActors::TEvents::TEvWakeup, HandleDisconnectDeadline);
                hFunc(TEvSolomonProvider::TEvAck, HandleAck);
                hFunc(TEvSolomonProvider::TEvGetNextBatch, HandleGetNextBatchForEmptyState);
                hFunc(TEvSolomonProvider::TEvConsumerFinished, HandleConsumerFinished);
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
                hFunc(TEvRetryQueuePrivate::TEvRetry, HandleRetry);
                hFunc(TEvRetryQueuePrivate::TEvEvHeartbeat, HandleHeartbeat);
                hFunc(NActors::TEvInterconnect::TEvNodeConnected, HandleConnected);
                hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
                hFunc(NActors::TEvents::TEvUndelivered, HandleUndelivered);
                hFunc(NActors::TEvents::TEvWakeup, HandleDisconnectDeadline);
                hFunc(TEvSolomonProvider::TEvAck, HandleAck);
                hFunc(TEvSolomonProvider::TEvGetNextBatch, HandleGetNextBatchForErrorState);
                hFunc(TEvSolomonProvider::TEvConsumerFinished, HandleConsumerFinished);
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
    struct TConsumerQueue {
        ui64 Id = 0;
        TRetryEventsQueue Events;
    };

    TRetryEventsQueue& GetConsumerQueue(const NActors::TActorId& consumer) {
        auto [it, inserted] = ConsumerQueues.try_emplace(consumer);
        if (inserted) {
            auto& queue = it->second;
            queue.Id = NextConsumerQueueId++;
            ConsumerByQueueId.emplace(queue.Id, consumer);
            queue.Events.Init("SolomonMetricsQueue", SelfId(), SelfId(), queue.Id, /* keepAlive */ true, /* useConnect */ true, /* ordered */ false);
            queue.Events.OnNewRecipientId(consumer, /* unsubscribe */ false);
        }
        return it->second.Events;
    }

    template <class T>
    bool CheckConsumerEvent(const T& ev) {
        if (!ConsumerQueues.contains(ev->Sender) && FinishedConsumers.contains(ev->Sender)) {
            return false;
        }
        if (!GetConsumerQueue(ev->Sender).OnEventReceived(ev)) {
            // Duplicate requests can still acknowledge retained responses.
            MaybeFinish();
            return false;
        }
        return true;
    }

    void HandleRetry(TEvRetryQueuePrivate::TEvRetry::TPtr& ev) {
        if (auto it = ConsumerByQueueId.find(ev->Get()->EventQueueId); it != ConsumerByQueueId.end()) {
            ConsumerQueues.at(it->second).Events.Retry();
        }
    }

    void HandleHeartbeat(TEvRetryQueuePrivate::TEvEvHeartbeat::TPtr& ev) {
        if (auto it = ConsumerByQueueId.find(ev->Get()->EventQueueId); it != ConsumerByQueueId.end()) {
            auto& queue = ConsumerQueues.at(it->second).Events;
            if (queue.Heartbeat()) {
                queue.Send(new TEvSolomonProvider::TEvAck());
            }
        }
    }

    void HandleConnected(NActors::TEvInterconnect::TEvNodeConnected::TPtr& ev) {
        DisconnectTimers.erase(ev->Get()->NodeId);
        for (auto& [consumer, queue] : ConsumerQueues) {
            queue.Events.HandleNodeConnected(ev->Get()->NodeId);
        }
    }

    void HandleDisconnected(NActors::TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        for (auto& [consumer, queue] : ConsumerQueues) {
            queue.Events.HandleNodeDisconnected(ev->Get()->NodeId);
            if (consumer.NodeId() == ev->Get()->NodeId) {
                StartDisconnectDeadline(consumer.NodeId());
            }
        }
    }

    void HandleUndelivered(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        auto it = ConsumerQueues.find(ev->Sender);
        if (it == ConsumerQueues.end()) {
            return;
        }
        const auto state = it->second.Events.HandleUndelivered(ev);
        if (ev->Get()->Reason == NActors::TEvents::TEvUndelivered::Disconnected) {
            StartDisconnectDeadline(ev->Sender.NodeId());
        }
        if (state == TRetryEventsQueue::ESessionState::SessionClosed) {
            // Interconnect subscriptions belong to the actor, so readers on the same node share one.
            const bool lastConsumerOnNode = std::none_of(ConsumerQueues.begin(), ConsumerQueues.end(), [&](const auto& entry) {
                return entry.first != ev->Sender && entry.first.NodeId() == ev->Sender.NodeId();
            });
            if (lastConsumerOnNode) {
                DisconnectTimers.erase(ev->Sender.NodeId());
                it->second.Events.Unsubscribe();
            }
            ConsumerByQueueId.erase(it->second.Id);
            ConsumerQueues.erase(it);
            PendingRequests.erase(ev->Sender);
            FinishedConsumers.insert(ev->Sender);
            MaybeFinish();
        }
    }

    // A retry interval is not a lifetime bound. Keep one deadline per node,
    // starting at its first disconnect; repeated failures must not postpone it.
    static constexpr TDuration ConsumerDisconnectTimeout = TDuration::Minutes(2);
    THashMap<ui32, ui64> DisconnectTimers;
    ui64 NextDisconnectTimer = 0;

    void StartDisconnectDeadline(ui32 nodeId) {
        if (!DisconnectTimers.contains(nodeId)) {
            const ui64 tag = ++NextDisconnectTimer;
            DisconnectTimers.emplace(nodeId, tag);
            Schedule(ConsumerDisconnectTimeout, new NActors::TEvents::TEvWakeup(tag));
        }
    }

    void HandleDisconnectDeadline(NActors::TEvents::TEvWakeup::TPtr& ev) {
        for (const auto& [nodeId, tag] : DisconnectTimers) {
            if (tag != ev->Get()->Tag) {
                continue;
            }
            const TString message = TStringBuilder()
                << "Source queue consumer node " << nodeId << " disconnected for "
                << ConsumerDisconnectTimeout << "; query cannot complete without losing data";
            // Fail the entire queue: never redistribute or silently discard an
            // unacknowledged batch and let the remaining consumers succeed.
            // Connected consumers receive an error. Disconnected consumers get
            // ActorUnknown on return, which their readers treat as queue loss
            // whenever they still need a response. Actor death stops all retries.
            for (auto& [consumer, queue] : ConsumerQueues) {
                queue.Events.Send(new TEvSolomonProvider::TEvMetricsReadError(message, {}));
            }
            PassAway();
            return;
        }
        // A reconnect or session removal invalidated this timer.
    }

    void HandleAck(TEvSolomonProvider::TEvAck::TPtr& ev) {
        if (CheckConsumerEvent(ev)) {
            MaybeFinish();
        }
    }

    void MaybeFinish() {
        if (FinishedConsumers.size() < ConsumersCount) {
            return;
        }
        for (const auto& [consumer, queue] : ConsumerQueues) {
            if (queue.Events.HasPendingEvents()) {
                return;
            }
        }
        PassAway();
    }

    void HandleUpdateConsumersCount(TEvSolomonProvider::TEvUpdateConsumersCount::TPtr& ev) {
        if (!CheckConsumerEvent(ev)) {
            return;
        }
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
        GetConsumerQueue(ev->Sender).Send(new TEvSolomonProvider::TEvAck(ev->Get()->Record.GetTransportMeta()));
    }

    void HandleGetNextBatch(TEvSolomonProvider::TEvGetNextBatch::TPtr& ev) {
        if (!CheckConsumerEvent(ev)) {
            return;
        }
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
                batchSize = std::min<ui64>(batchSize, NSo::NConstants::MaxHttpGetRequestSize * 0.5 / avgLength);
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
        if (!CheckConsumerEvent(ev)) {
            return;
        }
        ConnectedConsumers.insert(ev->Sender);
        LOG_T("TDqSolomonMetricsQueueActor", "HandleGetNextBatchForEmptyState giving away rest of Objects");
        TrySendMetrics(ev->Sender, ev->Get()->Record.GetTransportMeta());
    }

    void HandleGetNextBatchForErrorState(TEvSolomonProvider::TEvGetNextBatch::TPtr& ev) {
        if (!CheckConsumerEvent(ev)) {
            return;
        }
        ConnectedConsumers.insert(ev->Sender);
        LOG_D("TDqSolomonMetricsQueueActor", "HandleGetNextBatchForErrorState sending issues");
        GetConsumerQueue(ev->Sender).Send(new TEvSolomonProvider::TEvMetricsReadError(*MaybeIssues, ev->Get()->Record.GetTransportMeta()));
        TryFinish(ev->Sender, ev->Get()->Record.GetTransportMeta().GetSeqNo());
    }

    void HandleConsumerFinished(TEvSolomonProvider::TEvConsumerFinished::TPtr& ev) {
        if (!CheckConsumerEvent(ev)) {
            return;
        }
        LOG_I("TDqSolomonMetricsQueueActor",
            "HandleConsumerFinished from " << ev->Sender << ", " << FinishedConsumers.size() + 1
            << "/" << ConsumersCount << " consumers finished");
        ConnectedConsumers.insert(ev->Sender);
        FinishedConsumers.insert(ev->Sender);
        MaybeFinish();
    }

    void PassAway() override {
        for (auto& [consumer, queue] : ConsumerQueues) {
            queue.Events.Unsubscribe();
        }
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
                        GetConsumerQueue(consumer).Send(new TEvSolomonProvider::TEvMetricsReadError(*MaybeIssues, requests.front()));
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
        GetConsumerQueue(consumer).Send(new TEvSolomonProvider::TEvMetricsBatch(std::move(result), HasNoMoreItems(), DownloadedBytes, transportMeta));
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
                MaybeFinish();
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
    ui64 NextConsumerQueueId = 1;
    THashMap<NActors::TActorId, TConsumerQueue> ConsumerQueues;
    THashMap<ui64, NActors::TActorId> ConsumerByQueueId;
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
    const TDuration PoisonTimeout;
    const TDuration RoundRobinStageTimeout;
    const std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
    NSo::ISolomonAccessorClient::TPtr SolomonClient;
};


} // namespace

NActors::IActor* CreateSolomonMetricsQueueActor(
    ui64 consumersCount,
    TDqSolomonReadParams readParams,
    std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider,
    const NSo::TSolomonReadActorConfig& cfg,
    NSo::ISolomonAccessorClient::TPtr solomonClient)
{
    return new TDqSolomonMetricsQueueActor(consumersCount, std::move(readParams), credentialsProvider, cfg, std::move(solomonClient));
}

} // namespace NYql::NDq
