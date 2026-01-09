#pragma once

#include "quoter_base.h"

namespace NKikimr::NPQ {

class TWriteQuoter : public TPartitionQuoterBase {
    using TBase = TPartitionQuoterBase;

public:
    TWriteQuoter(
        const NPersQueue::TTopicConverterPtr& topicConverter,
        const NKikimrPQ::TPQTabletConfig& config,
        const NKikimrPQ::TPQConfig& pqConfig,
        const TPartitionId& partition,
        TActorId tabletActor,
        ui64 tabletId,
        const std::shared_ptr<TTabletCountersBase>& counters
    );

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_WRITE_QUOTER;
    }

    void HandlePoisonPill(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) override;
    void HandleUpdateAccountQuotaCounters(NAccountQuoterEvents::TEvCounters::TPtr& ev, const TActorContext& ctx) override;

    void UpdateQuotaConfigImpl(bool totalQuotaUpdated, const TActorContext& ctx) override;
    IEventBase* MakeQuotaApprovedEvent(TRequestContext& context) override;
    void Bootstrap(const TActorContext &ctx) override;
    THolder<TAccountQuoterHolder> CreateAccountQuotaTracker() const;

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
    TString Description() const override { return "Write quoter"; }

    bool CanExaust(TInstant now) override;

    STFUNC(ProcessEventImpl) override
    {
        Y_UNUSED(ev);
    }

private:
    bool GetAccountQuotingEnabled(const NKikimrPQ::TPQConfig& pqConfig) const;
    bool QuotingEnabled;
    THolder<TAccountQuoterHolder> AccountQuotaTracker;

    TQuotaTracker PartitionDeduplicationIdQuotaTracker;
};


}// namespace NKikimr::NPQ
