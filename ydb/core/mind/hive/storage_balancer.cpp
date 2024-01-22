#include <ydb/library/actors/core/actor_bootstrapped.h>
#include "hive_impl.h"
#include "hive_log.h"
#include "balancer.h"

namespace NKikimr {
namespace NHive {

template<>
void BalanceChannels<NKikimrConfig::THiveConfig::HIVE_CHANNEL_BALANCE_STRATEGY_WEIGHTED_RANDOM>(std::vector<TLeaderTabletInfo::TChannel>& channels, NKikimrConfig::THiveConfig::EHiveStorageBalanceStrategy metricToBalance) {
    auto& randGen = *TAppData::RandomProvider.Get();
    std::vector<double> weights;
    std::vector<size_t> order;
    weights.reserve(channels.size());
    order.reserve(channels.size());
    for (size_t i = 0; i < channels.size(); ++i) {
        double weight = channels[i].GetWeight(metricToBalance);
        weights.push_back(weight * randGen());
        order.push_back(i);
    }
    std::sort(order.begin(), order.end(), [&weights](size_t i, size_t j) -> bool {
        return weights[i] > weights[j];
    });
    std::vector<TLeaderTabletInfo::TChannel> result;
    result.reserve(channels.size());
    for (size_t i : order) {
        result.push_back(channels[i]);
    }
    result.swap(channels);
}

template<>
void BalanceChannels<NKikimrConfig::THiveConfig::HIVE_CHANNEL_BALANCE_STRATEGY_HEAVIEST>(std::vector<TLeaderTabletInfo::TChannel>& channels, NKikimrConfig::THiveConfig::EHiveStorageBalanceStrategy metricToBalance) {
    std::sort(channels.begin(), channels.end(), [metricToBalance](const TLeaderTabletInfo::TChannel& a, const TLeaderTabletInfo::TChannel& b) -> bool {
        return a.GetWeight(metricToBalance) > b.GetWeight(metricToBalance);
    });
}

template<>
void BalanceChannels<NKikimrConfig::THiveConfig::HIVE_CHANNEL_BALANCE_STRATEGY_RANDOM>(std::vector<TLeaderTabletInfo::TChannel>& channels, NKikimrConfig::THiveConfig::EHiveStorageBalanceStrategy) {
    auto& randGen = *TAppData::RandomProvider.Get();
    std::shuffle(channels.begin(), channels.end(), randGen);
}

class THiveStorageBalancer : public NActors::TActorBootstrapped<THiveStorageBalancer>, public ISubActor {
protected:
    static constexpr TDuration TIMEOUT = TDuration::Minutes(10);
    THive* Hive;
    TStorageBalancerSettings Settings;
    using TOperations = std::unordered_map<TTabletId, std::unique_ptr<TEvHive::TEvReassignTablet>>;
    TOperations Operations;
    TOperations::iterator NextReassign;
    ui64 ReassignInFlight = 0;
    ui64 Reassigns = 0;
    std::unordered_set<TFullTabletId> SkippedTablets;
    TBalancerStats& Stats;

    TString GetLogPrefix() const {
        return Hive->GetLogPrefix();
    }

    void PassAway() override {
        BLOG_I("StorageBalancer finished");
        Stats.TotalRuns++;
        Stats.TotalMovements += Reassigns;
        Stats.LastRunMovements = Reassigns;
        Stats.IsRunningNow = false;
        Hive->RemoveSubActor(this);
        Send(Hive->SelfId(), new TEvPrivate::TEvStorageBalancerOut());
        return IActor::PassAway();
    }

    void Cleanup() override {
        return PassAway();
    }

    TString GetDescription() const override {
        return TStringBuilder() << "StorageBalancer(" << Settings.StoragePool << ")";
    }

    TSubActorId GetId() const override {
        return SelfId().LocalId();
    }

    void ReassignNextTablet() {
        while (NextReassign != Operations.end() && ReassignInFlight < Settings.MaxInFlight) {
            auto tablet = Hive->FindTablet(NextReassign->first);
            if (!tablet) {
                continue;
            }
            tablet->ActorsToNotifyOnRestart.emplace_back(SelfId());
            BLOG_D("StorageBalancer initiating reassign for tablet " << NextReassign->first);
            Send(Hive->SelfId(), NextReassign->second.release());
            ++NextReassign;
            ++ReassignInFlight;
        }
        if (ReassignInFlight == 0) {
            return PassAway();
        }
    }

    void Handle(TEvPrivate::TEvRestartComplete::TPtr& ev) {
        auto tabletId = ev->Get()->TabletId;
        BLOG_D("StorageBalancer received " << ev->Get()->Status << " for tablet " << tabletId);
        if (SkippedTablets.contains(tabletId)) {
            return;
        }
        --ReassignInFlight;
        Stats.CurrentMovements = ++Reassigns;
        ReassignNextTablet();
    }

    void Handle(TEvPrivate::TEvRestartCancelled::TPtr& ev) {
        auto tabletId = ev->Get()->TabletId;
        BLOG_D("StorageBalancer received RestartCancelled for tablet " << tabletId);
        SkippedTablets.insert(tabletId);
        auto tablet = Hive->FindTablet(tabletId);
        if (tablet) {
            std::erase(tablet->ActorsToNotifyOnRestart, SelfId());
        }
        --ReassignInFlight;
        ReassignNextTablet();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HIVE_BALANCER_ACTOR;
    }

    THiveStorageBalancer(THive* hive, TStorageBalancerSettings&& settings)
        : Hive(hive)
        , Settings(settings)
        , Stats(Hive->BalancerStats[static_cast<size_t>(EBalancerType::Storage)])
    {
        Stats.IsRunningNow = true;
        Stats.CurrentMaxMovements = Settings.NumReassigns;
        Stats.CurrentMovements = 0;
        Stats.LastRunTimestamp = TActivationContext::Now();
    }

    void Bootstrap() {
        Hive->TabletCounters->Cumulative()[NHive::COUNTER_STORAGE_BALANCER_EXECUTED].Increment(1);
        Become(&TThis::StateWork, TIMEOUT, new TEvents::TEvPoison());
        TInstant now = TActivationContext::Now();
        std::vector<TLeaderTabletInfo::TChannel> channels;
        for (const auto& [tabletId, tablet] : Hive->Tablets) {
            if (!tablet.IsReadyToReassignTablet()) {
                continue;
            }
            for (ui32 channelId = 0; channelId < tablet.GetChannelCount(); ++channelId) {
                const auto* channelInfo = tablet.TabletStorageInfo->ChannelInfo(channelId);

                if (channelInfo
                    && (Settings.StoragePool.empty() || channelInfo->StoragePool == Settings.StoragePool)
                    && channelInfo->History.back().Timestamp + Hive->GetMinPeriodBetweenReassign() <= now
                    && channelInfo->History.size() < Hive->GetMaxChannelHistorySize()) {
                    channels.push_back(tablet.GetChannel(channelId));
                }
            }
        }
        BLOG_D("StorageBalancer for pool " << Settings.StoragePool << ": " << channels.size() << " tablet channels suitable for balancing");
        auto metricToBalance = Hive->GetStorageBalanceStrategy();
        switch (Hive->GetChannelBalanceStrategy()) {
        case NKikimrConfig::THiveConfig::HIVE_CHANNEL_BALANCE_STRATEGY_WEIGHTED_RANDOM:
            BalanceChannels<NKikimrConfig::THiveConfig::HIVE_CHANNEL_BALANCE_STRATEGY_WEIGHTED_RANDOM>(channels, metricToBalance);
            break;
        case NKikimrConfig::THiveConfig::HIVE_CHANNEL_BALANCE_STRATEGY_HEAVIEST:
            BalanceChannels<NKikimrConfig::THiveConfig::HIVE_CHANNEL_BALANCE_STRATEGY_HEAVIEST>(channels, metricToBalance);
            break;
        case NKikimrConfig::THiveConfig::HIVE_CHANNEL_BALANCE_STRATEGY_RANDOM:
            BalanceChannels<NKikimrConfig::THiveConfig::HIVE_CHANNEL_BALANCE_STRATEGY_RANDOM>(channels, metricToBalance);
            break;
        }
        for (size_t i = 0; i < channels.size() && Operations.size() < Settings.NumReassigns; ++i) {
            const auto& channel = channels[i];
            auto& ev = Operations[channel.TabletId];
            if (!ev) {
                ev = std::make_unique<TEvHive::TEvReassignTablet>(channel.TabletId);
                ev->Record.SetReassignReason(NKikimrHive::TEvReassignTablet::HIVE_REASSIGN_REASON_BALANCE);
            }
            ev->Record.AddChannels(channel.ChannelId);
        }
        NextReassign = Operations.begin();
        ReassignNextTablet();
    }

    STATEFN(StateWork) {
        switch(ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            hFunc(TEvPrivate::TEvRestartComplete, Handle);
            hFunc(TEvPrivate::TEvRestartCancelled, Handle);
        }
    }
};

void THive::StartHiveStorageBalancer(TStorageBalancerSettings settings) {
    if (IsItPossibleToStartBalancer(EBalancerType::Storage)) {
        auto* balancer = new THiveStorageBalancer(this, std::move(settings));
        SubActors.emplace_back(balancer);
        RegisterWithSameMailbox(balancer);
    }
}

} // NHive
} // NKikimr
