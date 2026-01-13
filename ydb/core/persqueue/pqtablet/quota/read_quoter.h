#pragma once

#include "quoter_base.h"
#include "account_read_quoter.h"
#include <ydb/core/persqueue/common/microseconds_sliding_window.h>

#include <ydb/core/quoter/public/quoter.h>
#include <ydb/core/persqueue/events/internal.h>

#include <ydb/library/actors/core/hfunc.h>


namespace NKikimr::NPQ {

class TReadQuoter : public TPartitionQuoterBase {

const static ui64 DEFAULT_READ_SPEED_AND_BURST = 1'000'000'000;

public:
    TReadQuoter(
        const NKikimrPQ::TPQConfig& pqConfig,
        const NPersQueue::TTopicConverterPtr& topicConverter,
        const NKikimrPQ::TPQTabletConfig& config,
        const TPartitionId& partition,
        TActorId tabletActor,
        const TActorId& parent,
        ui64 tabletId,
        const std::shared_ptr<TTabletCountersBase>& counters
    )
        : TPartitionQuoterBase(
                topicConverter, config, partition, tabletActor, true, tabletId, counters,
                pqConfig.GetMaxInflightReadRequestsPerPartition()
        )
        , Parent(parent)
    {
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_READ_QUOTER;
    }

    void HandlePoisonPill(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) override;
    void HandleUpdateAccountQuotaCounters(NAccountQuoterEvents::TEvCounters::TPtr& ev, const TActorContext& ctx) override;
    void HandleConsumerRemoved(TEvPQ::TEvConsumerRemoved::TPtr& ev, const TActorContext& ctx);

    void UpdateQuotaConfigImpl(bool totalQuotaUpdated, const TActorContext& ctx) override;
    IEventBase* MakeQuotaApprovedEvent(TRequestContext& context) override;

    TString BuildLogPrefix() const override;

protected:
    void HandleQuotaRequestImpl(TRequestContext& context) override;
    void HandleConsumedImpl(TEvPQ::TEvConsumed::TPtr& ev) override;
    TAccountQuoterHolder* GetAccountQuotaTracker(const THolder<TEvPQ::TEvRequestQuota>& request) override;
    void OnAccountQuotaApproved(TRequestContext&& request) override;
    ui64 GetTotalPartitionSpeed(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const override;
    ui64 GetTotalPartitionSpeedBurst(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const override;
    void UpdateCounters(const TActorContext& ctx) override;
    void HandleWakeUpImpl() override;
    THolder<TAccountQuoterHolder> CreateAccountQuotaTracker(const TString& user, const TActorContext& ctx) const;

    TString Description() const override { return "Read quoter"; }
    STFUNC(ProcessEventImpl) override
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPQ::TEvConsumerRemoved, HandleConsumerRemoved);
        default:
            break;
        };
    }
private:
    TConsumerReadQuota* GetOrCreateConsumerQuota(const TString& consumerStr, const TActorContext& ctx);
    void CheckConsumerPerPartitionQuota(TRequestContext&& context);
    void ApproveQuota(TAutoPtr<TEvPQ::TEvRead>&& ev, const TActorContext& ctx);
    void ProcessPerConsumerQuotaQueue(const TActorContext& ctx);
    TConsumerReadQuota* GetConsumerQuotaIfExists(const TString& consumerStr);
    ui64 GetConsumerReadSpeed(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TString& consumerStr, const TActorContext& ctx) const;
    ui64 GetConsumerReadBurst(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TString& consumerStr, const TActorContext& ctx) const;

private:
    THashMap<TString, TConsumerReadQuota> ConsumerQuotas;
    TActorId Parent;
};

} // namespace NKikimr::NPQ
