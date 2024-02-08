#pragma once

#include "events.h"
#include "log.h"

#include <ydb/core/base/tablet_pipe.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NSQS {

class TNodeTrackerActor : public TActorBootstrapped<TNodeTrackerActor>{
private:
    static constexpr TDuration CLEANUP_UNUSED_TABLETS_PERIOD = TDuration::Minutes(10);
    static constexpr TDuration DESCRIBE_TABLES_PERIOD_MIN = TDuration::Zero();
    static constexpr TDuration DESCRIBE_TABLES_PERIOD_MAX = TDuration::Seconds(5);

    using TKeyPrefix = std::tuple<ui64, ui64>;

private:
    struct TSubscriberInfo {
        using TPtr = std::unique_ptr<TSubscriberInfo>;

        TSubscriberInfo(
            ui64 queueIdNumber,
            bool isFifo,
            std::optional<ui64> specifiedLeaderTabletId,
            std::optional<ui32> nodeId = std::nullopt
        );

        const ui64 QueueIdNumber;
        const bool IsFifo;
        const std::optional<ui64> SpecifiedLeaderTabletId;

        std::optional<ui32> NodeId;
    };

    struct TTabletInfo {
        TActorId PipeClient;
        TActorId PipeServer;
        THashMap<ui64, TSubscriberInfo::TPtr> Subscribers;
    };

public:
    static const char* GetLogPrefix();

    TNodeTrackerActor(NActors::TActorId schemeCacheActor);
    void Bootstrap(const NActors::TActorContext& ctx);

    void WorkFunc(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvWakeup, HandleWakeup);
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleCacheNavigateResponse);
            HFunc(TSqsEvents::TEvNodeTrackerSubscribeRequest, AddSubscriber);
            HFunc(TSqsEvents::TEvNodeTrackerUnsubscribeRequest, RemoveSubscriber);
            HFunc(TEvTabletPipe::TEvClientDestroyed, HandlePipeClientDisconnected);
            HFunc(TEvTabletPipe::TEvClientConnected, HandlePipeClientConnected);
            HFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);
        default:
            LOG_SQS_ERROR("Unknown type of event came to SQS node tracker actor: " << ev->Type << " (" << ev->ToString() << "), sender: " << ev->Sender);
        }
    }

    void HandleWakeup(TEvWakeup::TPtr&, const NActors::TActorContext& ctx);
    void ScheduleDescribeTables(TDuration runAfter, const NActors::TActorContext& ctx);
    void DescribeTablesFailed(const TString& error, const NActors::TActorContext& ctx);

    void HandlePipeClientConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev, const NActors::TActorContext& ctx);
    void HandlePipeClientDisconnected(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext&);
    void ClosePipeToTablet(TTabletInfo& info);
    TTabletInfo& ConnectToTablet(ui64 tabletId, bool isReconnect = false);
    TTabletInfo& ReconnectToTablet(ui64 tabletId);
    TTabletInfo& GetTabletInfo(ui64 tabletId);


    ui64 GetTabletId(const TMap<TKeyPrefix, ui64>& tabletsPerEndKeyRange, TKeyPrefix keyPrefix) const;
    ui64 GetTabletId(const TSubscriberInfo& subscriber) const;

    void AnswerForSubscriber(ui64 subscriptionId, ui32 nodeId, bool disconnected=false);
    void RemoveSubscriber(TSqsEvents::TEvNodeTrackerUnsubscribeRequest::TPtr& request, const NActors::TActorContext& ctx);
    bool SubscriberMustWait(const TSubscriberInfo& subscriber) const;
    void AddSubscriber(TSqsEvents::TEvNodeTrackerSubscribeRequest::TPtr& request, const NActors::TActorContext& ctx);
    void AddSubscriber(ui64 subscriptionId, std::unique_ptr<TSubscriberInfo> subscriber, const NActors::TActorContext&);
    void MoveSubscribersFromTablet(ui64 tabletId, const NActors::TActorContext& ctx);
    void MoveSubscribersFromTablet(ui64 tabletId, TTabletInfo& info, const NActors::TActorContext& ctx);
    void MoveSubscribersAfterKeyRangeChanged(
        const TMap<TKeyPrefix, ui64>& current,
        const TMap<TKeyPrefix, ui64>& actual,
        const NActors::TActorContext& ctx
    );

    void UpdateKeyRanges(
        TMap<TKeyPrefix, ui64>& currentTabletsPerEndKeyRange,
        const NKikimrSchemeOp::TPathDescription& description,
        const NActors::TActorContext& ctx
    );

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev, const NActors::TActorContext& ctx);

private:
    TDuration DescribeTablesRetyPeriod = DESCRIBE_TABLES_PERIOD_MIN;

    TMap<TKeyPrefix, ui64> TabletsPerEndKeyRangeSTD;
    TMap<TKeyPrefix, ui64> TabletsPerEndKeyRangeFIFO;

    THashMap<ui64, TTabletInfo> TabletsInfo;
    THashMap<ui64, TSubscriberInfo::TPtr> SubscriptionsAwaitingPartitionsUpdates;
    THashMap<ui64, ui64> TabletPerSubscriptionId;
    THashMap<ui64, TInstant> LastAccessOfTabletWithoutSubscribers;

    TActorId ParentActor;
    TActorId SchemeCacheActor;

    const TVector<TString> TablePathSTD;
    const TVector<TString> TablePathFIFO;

    const TDuration UpdatePeriod;
};

} // namespace NKikimr::NSQS
