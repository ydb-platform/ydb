#include <random>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/random_provider/random_provider.h>
#include "hive_impl.h"
#include "hive_log.h"
#include "node_info.h"
#include "balancer.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

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
void BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_OLD_WEIGHTED_RANDOM>(std::vector<TTabletInfo*>::iterator first, std::vector<TTabletInfo*>::iterator last, EResourceToBalance resourceToBalance) {
    auto& randGen = *TAppData::RandomProvider.Get();
    // weighted random shuffle
    std::vector<double> weights;
    weights.reserve(last - first);
    for (auto it = first; it != last; ++it) {
        weights.emplace_back((*it)->GetWeight(resourceToBalance));
    }
    auto itT = first;
    auto itW = weights.begin();
    while (itT != last && itW != weights.end()) {
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
void BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_HEAVIEST>(std::vector<TTabletInfo*>::iterator first, std::vector<TTabletInfo*>::iterator last, EResourceToBalance resourceToBalance) {
    std::sort(first, last, [resourceToBalance](const TTabletInfo* a, const TTabletInfo* b) -> bool {
        return a->GetWeight(resourceToBalance) > b->GetWeight(resourceToBalance);
    });
}

template<>
void BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_RANDOM>(std::vector<TTabletInfo*>::iterator first, std::vector<TTabletInfo*>::iterator last, EResourceToBalance) {
    auto& randGen = *TAppData::RandomProvider.Get();
    std::shuffle(first, last, randGen);
}

template<>
void BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_WEIGHTED_RANDOM>(std::vector<TTabletInfo*>::iterator first, std::vector<TTabletInfo*>::iterator last, EResourceToBalance resourceToBalance) {
    auto& randGen = *TAppData::RandomProvider.Get();
    std::vector<std::pair<double, TTabletInfo*>> weights;
    weights.reserve(last - first);
    for (auto it = first; it != last; ++it) {
        double weight = (*it)->GetWeight(resourceToBalance);
        weights.emplace_back(weight * randGen(), *it);
    }
    std::sort(weights.begin(), weights.end(), [](const auto& a, const auto& b) -> bool {
        return a.first > b.first;
    });
    for (size_t n = 0; n < weights.size(); ++n) {
        first[n] = weights[n].second;
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
    std::vector<TNodeId> Nodes;
    std::vector<TNodeId>::iterator NextNode;
    std::vector<TFullTabletId> Tablets;
    std::vector<TFullTabletId>::iterator NextTablet;

    static constexpr ui64 MAX_TABLETS_PROCESSED = 10;

    TString GetLogPrefix() const {
        return Hive->GetLogPrefix();
    }

    void PassAway() override {
        YDB_LOG_INFO("Balancer finished with movements made",
            {"GetLogPrefix", GetLogPrefix()},
            {"Movements", Movements});
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
                    YDB_LOG_DEBUG("Balancer suggests scale-up",
                        {"GetLogPrefix", GetLogPrefix()});
                    Hive->TabletCounters->Cumulative()[NHive::COUNTER_SUGGESTED_SCALE_UP].Increment(1);
                    break;
                }
            }
        }
        if (Settings.RecheckOnFinish && Settings.MaxMovements != 0 && Movements >= Settings.MaxMovements) {
            YDB_LOG_DEBUG("Balancer initiated recheck",
                {"GetLogPrefix", GetLogPrefix()});
            Hive->ProcessTabletBalancer();
        } else {
            Send(Hive->SelfId(), new TEvPrivate::TEvBalancerOut());
        }
        return IActor::PassAway();
    }

    void Cleanup() override {
        PassAway();
    }

    TString GetDescription() const override {
        return TStringBuilder() << "Balancer(" << EBalancerTypeName(Settings.Type) << ")";
    }

    TSubActorId GetId() const override {
        return SelfId().LocalId();
    }

    bool CanKickNextTablet() const {
        return KickInFlight < Settings.MaxInFlight
               && (Settings.MaxMovements == 0 || Movements < Settings.MaxMovements);
    }

    void UpdateProgress() {
        Stats.CurrentMovements = Movements;
    }

    void BalanceNodes() {
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

        Nodes.reserve(nodes.size());
        for (auto node : nodes) {
            Nodes.push_back(node->Id);
        }

        NextNode = Nodes.begin();
        Tablets.clear();
    }

    std::optional<TFullTabletId> GetNextTablet(TInstant now) {
        for (; Tablets.empty() || NextTablet == Tablets.end(); ++NextNode) {
            if (NextNode == Nodes.end()) {
                return std::nullopt;
            }
            TNodeInfo* node = Hive->FindNode(*NextNode);
            if (node == nullptr) {
                continue;
            }
            YDB_LOG_TRACE("Balancer selected node",
                {"GetLogPrefix", GetLogPrefix()},
                {"Id", node->Id});
            auto itTablets = node->Tablets.find(TTabletInfo::EVolatileState::TABLET_VOLATILE_STATE_RUNNING);
            if (itTablets == node->Tablets.end()) {
                continue;
            }
            const std::unordered_set<TTabletInfo*>& nodeTablets = itTablets->second;
            std::vector<TTabletInfo*> tablets;
            tablets.reserve(nodeTablets.size());
            for (TTabletInfo* tablet : nodeTablets) {
                if (tablet->IsGoodForBalancer(now) && 
                    (!Settings.FilterObjectId || tablet->GetObjectId() == *Settings.FilterObjectId) &&
                    tablet->HasMetric(Settings.ResourceToBalance)) {
                    tablet->UpdateWeight();
                    tablets.emplace_back(tablet);
                }
            }
            YDB_LOG_TRACE("Balancer on node / tablets are suitable for balancing",
                {"GetLogPrefix", GetLogPrefix()},
                {"Id", node->Id},
                {"size", tablets.size()},
                {"#_size", nodeTablets.size()});
            if (!tablets.empty()) {
                // avoid moving system tablets if possible
                std::vector<TTabletInfo*>::iterator partitionIt;
                if (Hive->GetLessSystemTabletsMoves()) {
                    partitionIt = std::partition(tablets.begin(), tablets.end(), [](TTabletInfo* tablet) {
                        return !THive::IsSystemTablet(tablet->GetTabletType());
                    });
                } else {
                    partitionIt = tablets.end();
                }
                switch (Hive->GetTabletBalanceStrategy()) {
                case NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_OLD_WEIGHTED_RANDOM:
                    BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_OLD_WEIGHTED_RANDOM>(tablets.begin(), partitionIt, Settings.ResourceToBalance);
                    BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_OLD_WEIGHTED_RANDOM>(partitionIt, tablets.end(), Settings.ResourceToBalance);
                    break;
                case NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_WEIGHTED_RANDOM:
                    BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_WEIGHTED_RANDOM>(tablets.begin(), partitionIt, Settings.ResourceToBalance);
                    BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_WEIGHTED_RANDOM>(partitionIt, tablets.end(), Settings.ResourceToBalance);
                    break;
                case NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_HEAVIEST:
                    BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_HEAVIEST>(tablets.begin(), partitionIt, Settings.ResourceToBalance);
                    BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_HEAVIEST>(partitionIt, tablets.end(), Settings.ResourceToBalance);
                    break;
                case NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_RANDOM:
                    BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_RANDOM>(tablets.begin(), partitionIt, Settings.ResourceToBalance);
                    BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_RANDOM>(partitionIt, tablets.end(), Settings.ResourceToBalance);
                    break;
                }
                Tablets.clear();
                Tablets.reserve(tablets.size());
                for (auto tablet : tablets) {
                    Tablets.push_back(tablet->GetFullTabletId());
                }
            }
            NextTablet = Tablets.begin();
        }
        return *(NextTablet++);
    }

    void KickNextTablet() {
        if (Settings.MaxMovements != 0 && Movements >= Settings.MaxMovements) {
            if (KickInFlight > 0) {
                return;
            } else {
                return PassAway();
            }
        }

        TInstant now = TActivationContext::Now();
        ui64 tabletsProcessed = 0;

        while (CanKickNextTablet()) {
            if (tabletsProcessed == MAX_TABLETS_PROCESSED) {
                YDB_LOG_TRACE("Balancer - rescheduling",
                    {"GetLogPrefix", GetLogPrefix()});
                Send(SelfId(), new TEvents::TEvWakeup);
                return;
            }
            std::optional<TFullTabletId> tabletId = GetNextTablet(now);
            if (!tabletId) {
                break;
            }
            TTabletInfo* tablet = Hive->FindTablet(*tabletId);
            if (tablet == nullptr || !tablet->IsRunning()) {
                continue;
            }
            YDB_LOG_TRACE("Balancer selected tablet",
                {"GetLogPrefix", GetLogPrefix()},
                {"tablet", tablet->ToString()});
            THive::TBestNodeResult result = Hive->FindBestNode(*tablet);
            if (std::holds_alternative<TNodeInfo*>(result)) {
                TNodeInfo* node = std::get<TNodeInfo*>(result);
                if (node != tablet->Node && Hive->IsTabletMoveExpedient(*tablet, *node)) {
                    tablet->MakeBalancerDecision(now);
                    tablet->ActorsToNotifyOnRestart.emplace_back(SelfId()); // volatile settings, will not persist upon restart
                    ++KickInFlight;
                    ++Movements;
                    YDB_LOG_DEBUG("Balancer moving tablet from node to node",
                        {"GetLogPrefix", GetLogPrefix()},
                        {"tablet", tablet->ToString()},
                        {"#_tablet->Node->Id", tablet->Node->Id},
                        {"Id", node->Id});
                    Hive->RecordTabletMove(THive::TTabletMoveInfo(now, *tablet, tablet->Node->Id, node->Id));
                    Hive->Execute(Hive->CreateRestartTablet(tablet->GetFullTabletId(), node->Id));
                    UpdateProgress();
                }
            }
            ++tabletsProcessed;
        }

        if (KickInFlight == 0) {
            return PassAway();
        }
    }

    void Handle(TEvPrivate::TEvRestartComplete::TPtr& ev) {
        YDB_LOG_DEBUG("Balancer received for tablet",
            {"GetLogPrefix", GetLogPrefix()},
            {"SelfId", SelfId()},
            {"#_ev->Get()->Status", ev->Get()->Status},
            {"#_ev->Get()->TabletId", ev->Get()->TabletId});
        --KickInFlight;
        BalanceNodes();
        KickNextTablet();
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
        Become(&THiveBalancer::StateWork, TIMEOUT, new TEvents::TEvPoison());
        BalanceNodes();
        KickNextTablet();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            hFunc(TEvPrivate::TEvRestartComplete, Handle);
            cFunc(TEvents::TSystem::Wakeup, KickNextTablet);
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
