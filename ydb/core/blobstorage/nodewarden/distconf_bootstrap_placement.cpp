#include "distconf_invoke.h"

#include <ydb/core/blobstorage/base/utility.h>
#include <ydb/library/actors/core/executelater.h>
#include <ydb/library/yaml_config/yaml_config.h>
#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <ydb/library/yaml_json/yaml_to_json.h>

#include <util/generic/is_in.h>
#include <util/string/join.h>

namespace NKikimr::NStorage {

    struct TDistributedConfigKeeper::TBootstrapPlacement {
        NKikimrBlobStorage::TStorageConfig Yaml; // only the two compressed YAML fields
        TMap<ui32, TBridgePileId> Nodes;
        TMap<TBridgePileId, TString> Piles;

        // Only the keeper mailbox accesses the result and waiters.
        std::optional<TBootstrapPlacementIssues> Result;
        THashSet<TActorId> Waiters;

        TBootstrapPlacementIssues Calculate() const {
            using TBootstrap = NKikimrConfig::TBootstrap;
            static constexpr std::array systemTypes{TBootstrap::FLAT_HIVE, TBootstrap::HIVE, TBootstrap::FLAT_BS_CONTROLLER,
                TBootstrap::BS_DOMAINCONTROLLER, TBootstrap::FLAT_SCHEMESHARD, TBootstrap::SCHEMESHARD,
                TBootstrap::FLAT_TX_COORDINATOR, TBootstrap::TX_COORDINATOR, TBootstrap::TX_MEDIATOR, TBootstrap::TX_ALLOCATOR,
                TBootstrap::TX_PROXY, TBootstrap::FLAT_TX_PROXY, TBootstrap::CMS, TBootstrap::CONSOLE,
                TBootstrap::NODE_BROKER, TBootstrap::TENANT_SLOT_BROKER, TBootstrap::DBS_CONTROLLER};
            TString mainYaml;
            Y_ENSURE(!Nodes.empty(), "no static node could be mapped to a bridge pile");
            Y_ENSURE(Yaml.HasConfigComposite(), "main YAML configuration is unavailable");
            if (auto error = DecomposeConfig(Yaml.GetConfigComposite(), &mainYaml, nullptr, nullptr)) {
                ythrow yexception() << *error;
            }
            auto doc = NFyaml::TDocument::Parse(mainYaml);
            doc.Resolve();
            std::optional<TBootstrap> fallback;
            struct TMatches {
                size_t Count = 0;
                size_t Last = 0;
                void Add(size_t projection) {
                    if (std::exchange(Last, projection) != projection) {
                        ++Count;
                    }
                }
            };
            struct TTablet {
                TBootstrap::ETabletType Type;
                THashMap<ui32, TMatches> Eligible;
            };
            TMap<ui64, TTablet> tablets;
            THashMap<ui32, TMatches> compatible;
            TMap<TString, TString> labels{{"node_kind", "static"}, {"dynamic", "false"}};
            auto& nodeId = labels["node_id"];
            size_t projectionId = 0;
            NYamlConfig::EnumerateBootstrapPlacementConfigs(doc,
                [&](const TBootstrap* resolved, const NYamlConfig::TProjectionMatches& matches) {
                    ++projectionId;
                    bool any = false;
                    for (const auto& [id, pile] : Nodes) {
                        nodeId = ToString(id);
                        if (matches(labels)) {
                            compatible[id].Add(projectionId);
                            any = true;
                        }
                    }
                    if (!any) {
                        return;
                    }
                    if (!resolved && !fallback) {
                        auto main = NYaml::Yaml2Json(YAML::Load(mainYaml), true);
                        if (const auto storage = GetStorageYaml(Yaml)) {
                            NYaml::MergeStorageConfig(main, NYaml::Yaml2Json(YAML::Load(*storage), true));
                        }
                        NKikimrConfig::TAppConfig merged;
                        NYaml::Parse(main["config"], NYaml::GetJsonToProtoConfig(true), merged, NYaml::TransformBootstrapPlacement);
                        fallback.emplace(std::move(*merged.MutableBootstrapConfig()));
                    }
                    const auto& bootstrap = resolved ? *resolved : *fallback;
                    for (const auto& tablet : bootstrap.GetTablet()) {
                        if (tablet.GetStandBy() || !tablet.GetInfo().HasTabletID() || !IsIn(systemTypes, tablet.GetType())) {
                            continue;
                        }
                        auto& result = tablets[tablet.GetInfo().GetTabletID()];
                        result.Type = tablet.GetType();
                        for (ui32 id : tablet.GetNode()) {
                            if (auto it = compatible.find(id); it != compatible.end() && it->second.Last == projectionId) {
                                result.Eligible[id].Add(projectionId);
                            }
                        }
                    }
                });
            Y_ENSURE(compatible.size() == Nodes.size(), "some static nodes have no compatible YAML configuration");

            TBootstrapPlacementIssues issues;
            for (const auto& [tabletId, tablet] : tablets) {
                THashSet<TBridgePileId> definite, possible;
                size_t numEligible = 0;
                for (const auto& [id, eligible] : tablet.Eligible) {
                    const auto pile = Nodes.at(id);
                    possible.insert(pile);
                    if (eligible.Count == compatible.at(id).Count) {
                        definite.insert(pile);
                        ++numEligible;
                    }
                }
                for (const auto& [pile, name] : Piles) {
                    if (definite.contains(pile)) {
                        continue;
                    }
                    TStringBuilder msg;
                    msg << TBootstrap::ETabletType_Name(tablet.Type) << " tablet " << tabletId;
                    if (possible.contains(pile)) {
                        msg << " bootstrap placement in pile '" << name << "' depends on unresolved node labels.";
                    } else {
                        msg << " has no eligible bootstrap node in pile '" << name << "'. Eligible nodes elsewhere: "
                            << numEligible << ". Add a node from this pile to its bootstrap node list.";
                    }
                    issues[pile].push_back(msg);
                }
            }
            return issues;
        }
    };

    std::shared_ptr<TDistributedConfigKeeper::TBootstrapPlacement> TDistributedConfigKeeper::GetBootstrapPlacement(
            const NKikimrBlobStorage::TStorageConfig& config) {
        if (!config.HasClusterState()) {
            return {};
        }
        const auto registry = GetConfigPileRegistry(config);
        TMap<TBridgePileId, TString> piles;
        if (registry) {
            for (size_t i = 0; i < registry->PilesSize(); ++i) {
                if (const auto& pile = registry->GetPiles(i); pile.HasName()) {
                    piles.emplace(TBridgePileId::FromPileIndex(i), pile.GetName());
                }
            }
        }
        if (piles.size() < 2) {
            return {};
        }
        TMap<ui32, TBridgePileId> nodes;
        for (const auto& node : config.GetAllNodes()) {
            if (auto pile = NBridge::ResolvePileId(*registry, TNodeLocation(node.GetLocation()).GetBridgePileName())) {
                nodes.emplace(node.GetNodeId(), pile);
            }
        }
        for (auto& entry : BootstrapPlacements) {
            if (entry && config.GetConfigComposite() == entry->Yaml.GetConfigComposite() &&
                    config.GetCompressedStorageYaml() == entry->Yaml.GetCompressedStorageYaml() &&
                    entry->Nodes == nodes && entry->Piles == piles) {
                std::swap(entry, BootstrapPlacements.front());
                return BootstrapPlacements.front();
            }
        }
        auto entry = std::make_shared<TBootstrapPlacement>();
        auto slot = std::find_if(BootstrapPlacements.rbegin(), BootstrapPlacements.rend(),
            [](const auto& cached) { return !cached || cached->Result.has_value(); });
        if (slot == BootstrapPlacements.rend()) {
            entry->Result.emplace()[{}].emplace_back("Could not verify system tablet bootstrap placement: analysis is busy; retry.");
            return entry;
        }
        if (config.HasConfigComposite()) {
            entry->Yaml.SetConfigComposite(config.GetConfigComposite());
        }
        if (config.HasCompressedStorageYaml()) {
            entry->Yaml.SetCompressedStorageYaml(config.GetCompressedStorageYaml());
        }
        entry->Nodes = std::move(nodes);
        entry->Piles = std::move(piles);
        *slot = entry;
        std::swap(*slot, BootstrapPlacements.front());
        RunInBatchPool(TActivationContext::AsActorContext(), NActors::CreateExecuteLaterActor([entry, keeper = SelfId()](const TActorContext& ctx) {
            auto result = std::make_unique<TEvPrivate::TEvBootstrapPlacement>();
            result->Entry = entry;
            try {
                result->Issues = entry->Calculate();
            } catch (const std::exception& ex) {
                result->Issues[{}].emplace_back(TStringBuilder() << "Could not verify system tablet bootstrap placement: " << ex.what());
            }
            ctx.Send(keeper, result.release());
        }, NActors::IActor::EActivityType::ACTORLIB_COMMON));
        return entry;
    }

    void TDistributedConfigKeeper::Handle(TEvPrivate::TEvBootstrapPlacement::TPtr ev) {
        auto& entry = *ev->Get()->Entry;
        entry.Result.emplace(std::move(ev->Get()->Issues));
        for (auto waiter : std::exchange(entry.Waiters, {})) {
            Send(waiter, new TEvPrivate::TEvBootstrapPlacementReady);
        }
        if (InvokeQ.empty() && Scepter && StorageConfig &&
                StorageConfig->GetClusterState().GetPromotedPile() != StorageConfig->GetClusterState().GetPrimaryPile()) {
            CheckForConfigUpdate();
        }
    }

    bool TDistributedConfigKeeper::TInvokeRequestHandlerActor::AwaitBootstrapPlacement(
            const std::shared_ptr<TBootstrapPlacement>& entry, std::function<void()> resume) {
        if (BootstrapPlacementWaitFinished || !entry || entry->Result) {
            return false;
        }
        entry->Waiters.insert(SelfId());
        WaitingForBootstrapPlacement = entry;
        ResumeAfterBootstrapPlacement = std::move(resume);
        // A slow analysis must release the operation queue; its result remains available for a retry.
        Schedule(TDuration::Seconds(1), new TEvPrivate::TEvBootstrapPlacementReady);
        return true;
    }

    void TDistributedConfigKeeper::TInvokeRequestHandlerActor::CancelBootstrapPlacementWait() {
        if (auto entry = std::exchange(WaitingForBootstrapPlacement, {})) {
            entry->Waiters.erase(SelfId());
        }
    }

    void TDistributedConfigKeeper::TInvokeRequestHandlerActor::HandleBootstrapPlacementReady() {
        if (auto resume = std::exchange(ResumeAfterBootstrapPlacement, {})) {
            CancelBootstrapPlacementWait();
            BootstrapPlacementWaitFinished = true;
            resume();
        }
    }

    bool TDistributedConfigKeeper::CheckBootstrapPlacement(const std::shared_ptr<TBootstrapPlacement>& entry,
            TBridgePileId target, std::vector<TString>* warnings) {
        if (!entry) {
            return true;
        }
        if (!entry->Result) {
            if (warnings) {
                warnings->emplace_back("Could not verify system tablet bootstrap placement yet: analysis is in progress; retry.");
            }
            return false;
        }
        bool ok = true;
        for (const auto& [pile, issues] : *entry->Result) {
            if (!target || !pile || pile == target) {
                ok = false;
                if (warnings) {
                    warnings->insert(warnings->end(), issues.begin(), issues.end());
                }
            }
        }
        return ok;
    }

    bool TDistributedConfigKeeper::HasBootstrapPlacementOverride(const NKikimrBlobStorage::TStorageConfig& config) {
        const auto& state = config.GetClusterState();
        return state.GetPromotedPile() != state.GetPrimaryPile() && state.GetGeneration() &&
            config.GetClusterStateDetails().GetBootstrapPlacementOverrideGeneration() == state.GetGeneration();
    }

    bool TDistributedConfigKeeper::IsEmergencyPileDisconnect(const NKikimrBridge::TClusterState& before,
            const NKikimrBridge::TClusterState& after) {
        if (before.PerPileStateSize() != after.PerPileStateSize()) {
            return false;
        }
        const auto oldPrimary = TBridgePileId::FromProto(&before, &NKikimrBridge::TClusterState::GetPrimaryPile);
        if (before.GetPrimaryPile() != after.GetPrimaryPile() &&
                after.GetPerPileState(oldPrimary.GetPileIndex()) != NKikimrBridge::TClusterState::DISCONNECTED) {
            return false;
        }
        bool disconnected = false;
        for (size_t i = 0; i < after.PerPileStateSize(); ++i) {
            if (before.GetPerPileState(i) != after.GetPerPileState(i)) {
                if (after.GetPerPileState(i) != NKikimrBridge::TClusterState::DISCONNECTED) {
                    return false;
                }
                disconnected = true;
            }
        }
        return disconnected;
    }

    void TDistributedConfigKeeper::CheckBootstrapPlacementChange(const NKikimrBlobStorage::TStorageConfig& current,
            NKikimrBlobStorage::TStorageConfig& proposed, bool force, std::vector<TString>& warnings) {
        const auto& before = current.GetClusterState();
        const auto& after = proposed.GetClusterState();
        const auto primary = TBridgePileId::FromProto(&after, &NKikimrBridge::TClusterState::GetPrimaryPile);
        const auto promoted = TBridgePileId::FromProto(&after, &NKikimrBridge::TClusterState::GetPromotedPile);
        const bool continuing = HasBootstrapPlacementOverride(current) && before.GetPromotedPile() == after.GetPromotedPile() &&
            (before.GetPrimaryPile() == after.GetPrimaryPile() || primary == promoted);
        const auto oldPrimary = TBridgePileId::FromProto(&before, &NKikimrBridge::TClusterState::GetPrimaryPile);
        const bool emergency = IsEmergencyPileDisconnect(before, after);
        const bool checkPrimary = primary != oldPrimary || emergency;
        const bool checkPromoted = primary != promoted && (after.GetPromotedPile() != before.GetPromotedPile() || force);
        if (checkPrimary || checkPromoted) {
            const auto entry = GetBootstrapPlacement(proposed);
            const bool primaryOk = !checkPrimary || CheckBootstrapPlacement(entry, primary, &warnings) ||
                emergency || (continuing && primary == promoted);
            const bool promotedOk = !checkPromoted || CheckBootstrapPlacement(entry, promoted, &warnings);
            if (!force && (!primaryOk || !promotedOk)) {
                throw TInvokeRequestHandlerActor::TExPlacement() << JoinSeq("\n", warnings)
                    << " Use --skip-bootstrap-placement-check to override bootstrap placement.";
            }
        }
        if (force) {
            warnings.emplace_back("System tablet bootstrap placement check overridden by the administrator.");
        }
        if (primary != promoted && (force || continuing)) {
            proposed.MutableClusterStateDetails()->SetBootstrapPlacementOverrideGeneration(after.GetGeneration());
        } else if (proposed.GetClusterStateDetails().GetBootstrapPlacementOverrideGeneration()) {
            proposed.MutableClusterStateDetails()->ClearBootstrapPlacementOverrideGeneration();
        }
    }

} // NKikimr::NStorage
