#include <random>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/random_provider/random_provider.h>
#include "hive_impl.h"
#include "hive_log.h"
#include "node_info.h"
#include "balancer.h"

namespace NKikimr {
namespace NHive {

template<>
void BalanceNodes<NKikimrConfig::THiveConfig::HIVE_NODE_BALANCE_STRATEGY_OLD_WEIGHTED_RANDOM>(std::vector<TNodeInfo*>& nodes, EResourceToBalance resourceToBalance) {
    auto& randGen = *TAppData::RandomProvider.Get();
    // weighted random shuffle
    std::vector<double> usages;
    usages.reserve(nodes.size());
    for (auto it = nodes.begin(); it != nodes.end(); ++it) {
        usages.emplace_back((*it)->GetNodeUsage(resourceToBalance));
    }
    auto itN = nodes.begin();
    auto itU = usages.begin();
    while (itN != nodes.end() && itU != usages.end()) {
        auto idx = std::discrete_distribution(itU, usages.end())(randGen);
        if (idx != 0) {
            std::iter_swap(itN, std::next(itN, idx));
            std::iter_swap(itU, std::next(itU, idx));
        }
        ++itN;
        ++itU;
    }
}

template<>
void BalanceNodes<NKikimrConfig::THiveConfig::HIVE_NODE_BALANCE_STRATEGY_WEIGHTED_RANDOM>(std::vector<TNodeInfo*>& nodes, EResourceToBalance resourceToBalance) {
    auto& randGen = *TAppData::RandomProvider.Get();
    std::vector<std::pair<double, TNodeInfo*>> weights;
    weights.reserve(nodes.size());
    for (TNodeInfo* node : nodes) {
        double weight = node->GetNodeUsage(resourceToBalance);
        weights.emplace_back(weight * randGen(), node);
    }
    std::sort(weights.begin(), weights.end(), [](const auto& a, const auto& b) -> bool {
        return a.first > b.first;
    });
    for (size_t n = 0; n < weights.size(); ++n) {
        nodes[n] = weights[n].second;
    }
}

template<>
void BalanceNodes<NKikimrConfig::THiveConfig::HIVE_NODE_BALANCE_STRATEGY_HEAVIEST>(std::vector<TNodeInfo*>& nodes, EResourceToBalance resourceToBalance) {
    std::sort(nodes.begin(), nodes.end(), [resourceToBalance](const TNodeInfo* a, const TNodeInfo* b) -> bool {
        return a->GetNodeUsage(resourceToBalance) > b->GetNodeUsage(resourceToBalance);
    });
}

template<>
void BalanceNodes<NKikimrConfig::THiveConfig::HIVE_NODE_BALANCE_STRATEGY_RANDOM>(std::vector<TNodeInfo*>& nodes, EResourceToBalance) {
    auto& randGen = *TAppData::RandomProvider.Get();
    std::shuffle(nodes.begin(), nodes.end(), randGen);
}

template<>
void BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_OLD_WEIGHTED_RANDOM>(std::vector<TTabletInfo*>& tablets, EResourceToBalance resourceToBalance) {
    auto& randGen = *TAppData::RandomProvider.Get();
    // weighted random shuffle
    std::vector<double> weights;
    weights.reserve(tablets.size());
    for (auto it = tablets.begin(); it != tablets.end(); ++it) {
        weights.emplace_back((*it)->GetWeight(resourceToBalance));
    }
    auto itT = tablets.begin();
    auto itW = weights.begin();
    while (itT != tablets.end() && itW != weights.end()) {
        auto idx = std::discrete_distribution(itW, weights.end())(randGen);
        if (idx != 0) {
            std::iter_swap(itT, std::next(itT, idx));
            std::iter_swap(itW, std::next(itW, idx));
        }
        ++itT;
        ++itW;
    }
}

template<>
void BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_HEAVIEST>(std::vector<TTabletInfo*>& tablets, EResourceToBalance resourceToBalance) {
    std::sort(tablets.begin(), tablets.end(), [resourceToBalance](const TTabletInfo* a, const TTabletInfo* b) -> bool {
        return a->GetWeight(resourceToBalance) > b->GetWeight(resourceToBalance);
    });
}

template<>
void BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_RANDOM>(std::vector<TTabletInfo*>& tablets, EResourceToBalance) {
    auto& randGen = *TAppData::RandomProvider.Get();
    std::shuffle(tablets.begin(), tablets.end(), randGen);
}

template<>
void BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_WEIGHTED_RANDOM>(std::vector<TTabletInfo*>& tablets, EResourceToBalance resourceToBalance) {
    auto& randGen = *TAppData::RandomProvider.Get();
    std::vector<std::pair<double, TTabletInfo*>> weights;
    weights.reserve(tablets.size());
    for (TTabletInfo* tablet : tablets) {
        double weight = tablet->GetWeight(resourceToBalance);
        weights.emplace_back(weight * randGen(), tablet);
    }
    std::sort(weights.begin(), weights.end(), [](const auto& a, const auto& b) -> bool {
        return a.first > b.first;
    });
    for (size_t n = 0; n < weights.size(); ++n) {
        tablets[n] = weights[n].second;
    }
}

class THiveBalancer : public NActors::TActorBootstrapped<THiveBalancer>, public ISubActor {
protected:
    constexpr static TDuration TIMEOUT = TDuration::Minutes(10);
    THive* Hive;
    using TTabletId = TFullTabletId;
    ui64 KickInFlight;
    int Movements;
    TBalancerSettings Settings;
    TBalancerStats& Stats;

    TString GetLogPrefix() const {
        return Hive->GetLogPrefix();
    }

    void PassAway() override {
        BLOG_I("Balancer finished with " << Movements << " movements made");
        Stats.TotalRuns++;
        Stats.TotalMovements += Movements;
        Stats.LastRunMovements = Movements;
        Stats.IsRunningNow = false;
        Hive->RemoveSubActor(this);
        if (Movements == 0) {
            Hive->TabletCounters->Cumulative()[NHive::COUNTER_BALANCER_FAILED].Increment(1);
            // we failed to balance specific nodes
            for (TNodeId nodeId : Settings.FilterNodeIds) {
                TNodeInfo* node = Hive->FindNode(nodeId);
                if (node != nullptr && node->IsOverloaded()) {
                    BLOG_D("Balancer suggests scale-up");
                    Hive->TabletCounters->Cumulative()[NHive::COUNTER_SUGGESTED_SCALE_UP].Increment(1);
                    break;
                }
            }
        }
        if (Settings.RecheckOnFinish && Settings.MaxMovements != 0 && Movements >= Settings.MaxMovements) {
            BLOG_D("Balancer initiated recheck");
            Hive->ProcessTabletBalancer();
        } else {
            Send(Hive->SelfId(), new TEvPrivate::TEvBalancerOut());
        }
        return IActor::PassAway();
    }

    void Cleanup() override {
        PassAway();
    }

    bool CanKickNextTablet() const {
        return KickInFlight < Settings.MaxInFlight;
    }

    void UpdateProgress() {
        Stats.CurrentMovements = Movements;
    }

    void KickNextTablet() {
        if (!CanKickNextTablet()) {
            return;
        }
        if (Settings.MaxMovements != 0 && Movements >= Settings.MaxMovements) {
            if (KickInFlight > 0) {
                return;
            } else {
                return PassAway();
            }
        }

        struct TBalancerNodeInfo {
            const TNodeInfo* Node;
            double Usage;

            TBalancerNodeInfo(const TNodeInfo* node, double usage)
                : Node(node)
                , Usage(usage)
            {}
        };

        TInstant now = TActivationContext::Now();
        std::vector<TNodeInfo*> nodes;
        if (!Settings.FilterNodeIds.empty()) {
            nodes.reserve(Settings.FilterNodeIds.size());
            for (TNodeId nodeId : Settings.FilterNodeIds) {
                TNodeInfo* node = Hive->FindNode(nodeId);
                if (node != nullptr && node->IsAlive()) {
                    nodes.emplace_back(node);
                }
            }
        } else {
            nodes.reserve(Hive->Nodes.size());
            for (auto& [nodeId, nodeInfo] : Hive->Nodes) {
                if (nodeInfo.IsAlive()) {
                    nodes.emplace_back(&nodeInfo);
                }
            }
        }

        switch (Hive->GetNodeBalanceStrategy()) {
        case NKikimrConfig::THiveConfig::HIVE_NODE_BALANCE_STRATEGY_OLD_WEIGHTED_RANDOM:
            BalanceNodes<NKikimrConfig::THiveConfig::HIVE_NODE_BALANCE_STRATEGY_OLD_WEIGHTED_RANDOM>(nodes, Settings.ResourceToBalance);
            break;
        case NKikimrConfig::THiveConfig::HIVE_NODE_BALANCE_STRATEGY_WEIGHTED_RANDOM:
            BalanceNodes<NKikimrConfig::THiveConfig::HIVE_NODE_BALANCE_STRATEGY_WEIGHTED_RANDOM>(nodes, Settings.ResourceToBalance);
            break;
        case NKikimrConfig::THiveConfig::HIVE_NODE_BALANCE_STRATEGY_HEAVIEST:
            BalanceNodes<NKikimrConfig::THiveConfig::HIVE_NODE_BALANCE_STRATEGY_HEAVIEST>(nodes, Settings.ResourceToBalance);
            break;
        case NKikimrConfig::THiveConfig::HIVE_NODE_BALANCE_STRATEGY_RANDOM:
            BalanceNodes<NKikimrConfig::THiveConfig::HIVE_NODE_BALANCE_STRATEGY_RANDOM>(nodes, Settings.ResourceToBalance);
            break;
        }
        for (const TNodeInfo* node : nodes) {
            BLOG_TRACE("Balancer selected node " << node->Id);
            auto itTablets = node->Tablets.find(TTabletInfo::EVolatileState::TABLET_VOLATILE_STATE_RUNNING);
            if (itTablets == node->Tablets.end()) {
                continue;
            }
            const std::unordered_set<TTabletInfo*>& nodeTablets = itTablets->second;
            std::vector<TTabletInfo*> tablets;
            tablets.reserve(nodeTablets.size());
            for (TTabletInfo* tablet : nodeTablets) {
                if (tablet->IsGoodForBalancer(now) && (!Settings.FilterObjectId || tablet->GetObjectId() == *Settings.FilterObjectId)) {
                    tablet->UpdateWeight();
                    tablets.emplace_back(tablet);
                }
            }
            BLOG_TRACE("Balancer on node " << node->Id <<  ": " << tablets.size() << "/" << nodeTablets.size() << " tablets is suitable for balancing");
            if (!tablets.empty()) {
                switch (Hive->GetTabletBalanceStrategy()) {
                case NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_OLD_WEIGHTED_RANDOM:
                    BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_OLD_WEIGHTED_RANDOM>(tablets, Settings.ResourceToBalance);
                    break;
                case NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_WEIGHTED_RANDOM:
                    BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_WEIGHTED_RANDOM>(tablets, Settings.ResourceToBalance);
                    break;
                case NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_HEAVIEST:
                    BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_HEAVIEST>(tablets, Settings.ResourceToBalance);
                    break;
                case NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_RANDOM:
                    BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_RANDOM>(tablets, Settings.ResourceToBalance);
                    break;
                }
                for (TTabletInfo* tablet : tablets) {
                    BLOG_TRACE("Balancer selected tablet " << tablet->ToString());
                    THive::TBestNodeResult result = Hive->FindBestNode(*tablet);
                    if (result.BestNode != nullptr && result.BestNode != tablet->Node) {
                        if (Hive->IsTabletMoveExpedient(*tablet, *result.BestNode)) {
                            tablet->MakeBalancerDecision(now);
                            tablet->ActorsToNotifyOnRestart.emplace_back(SelfId()); // volatile settings, will not persist upon restart
                            ++KickInFlight;
                            ++Movements;
                            BLOG_D("Balancer moving tablet " << tablet->ToString() << " " << tablet->GetResourceValues()
                                   << " from node " << tablet->Node->Id << " " << tablet->Node->ResourceValues
                                   << " to node " << result.BestNode->Id << " " << result.BestNode->ResourceValues);
                            Hive->RecordTabletMove(THive::TTabletMoveInfo(now, *tablet, tablet->Node->Id, result.BestNode->Id));
                            Hive->Execute(Hive->CreateRestartTablet(tablet->GetFullTabletId(), result.BestNode->Id));
                            UpdateProgress();
                            if (!CanKickNextTablet()) {
                                return;
                            }
                        }
                    }
                }
            }
        }
        if (KickInFlight == 0) {
            return PassAway();
        }
    }

    void Handle(TEvPrivate::TEvRestartComplete::TPtr& ev) {
        BLOG_D("Balancer " << SelfId() << " received " << ev->Get()->Status << " for tablet " << ev->Get()->TabletId);
        --KickInFlight;
        KickNextTablet();
    }

    void Timeout() {
        PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HIVE_BALANCER_ACTOR;
    }

    THiveBalancer(THive* hive, TBalancerSettings&& settings)
        : Hive(hive)
        , KickInFlight(0)
        , Movements(0)
        , Settings(std::move(settings))
        , Stats(Hive->BalancerStats[static_cast<std::size_t>(Settings.Type)])
    {
        Stats.IsRunningNow = true;
        Stats.CurrentMaxMovements = Settings.MaxMovements ? Settings.MaxMovements : Hive->TabletsTotal;
        Stats.CurrentMovements = 0;
        Stats.LastRunTimestamp = TActivationContext::Now();
    }

    void Bootstrap() {
        UpdateProgress();
        Hive->TabletCounters->Cumulative()[NHive::COUNTER_BALANCER_EXECUTED].Increment(1);
        Become(&THiveBalancer::StateWork, TIMEOUT, new TEvents::TEvWakeup());
        KickNextTablet();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            hFunc(TEvPrivate::TEvRestartComplete, Handle);
            cFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }
};

void THive::StartHiveBalancer(TBalancerSettings&& settings) {
    if (IsItPossibleToStartBalancer(settings.Type)) {
        LastBalancerTrigger = settings.Type;
        auto* balancer = new THiveBalancer(this, std::move(settings));
        SubActors.emplace_back(balancer);
        RegisterWithSameMailbox(balancer);
    }
}

} // NHive
} // NKikimr
