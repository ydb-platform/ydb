#include "distconf_quorum.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

#include <google/protobuf/util/message_differencer.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_NODE

namespace NKikimr::NStorage {

    NKikimrBlobStorage::TStorageConfig MakeDistconfQuorumConfig(const NKikimrBlobStorage::TStorageConfig& config,
            const TNodeWardenConfig& nwConfig) {
        NKikimrBlobStorage::TStorageConfig configured;
        if (nwConfig.NameserviceConfig) {
            for (const auto& node : nwConfig.NameserviceConfig->GetNode()) {
                if (node.HasDistconfState()) {
                    auto *item = configured.AddAllNodes();
                    item->SetNodeId(node.GetNodeId());
                    item->SetDistconfState(node.GetDistconfState());
                }
            }
        }
        if (nwConfig.SelfManagementConfig) {
            configured.MutableSelfManagementConfig()->MutableDataCenterStates()->CopyFrom(
                nwConfig.SelfManagementConfig->GetDataCenterStates());
        }
        auto result = config;
        TDistributedConfigKeeper::MergeDistconfStates(configured, config, &result,
            nwConfig.SelfManagementConfig && nwConfig.SelfManagementConfig->GetOverrideDistconfState());
        return result;
    }

    namespace {
        bool IsExcluded(const NKikimrBlobStorage::TNodeIdentifier& node,
                const NKikimrConfig::TSelfManagementConfig& config) {
            if (node.HasDistconfState() && node.GetDistconfState() == NKikimrBlobStorage::TDistconfState::EXCLUDED) {
                return true;
            }
            for (const auto& item : config.GetDataCenterStates()) {
                if (item.GetName() == node.GetLocation().GetDataCenter()) {
                    return item.HasState() && item.GetState() == NKikimrBlobStorage::TDistconfState::EXCLUDED;
                }
            }
            return false;
        }

        bool HasExclusions(const NKikimrBlobStorage::TStorageConfig& config) {
            return std::ranges::any_of(config.GetAllNodes(), [&](const auto& node) {
                return IsExcluded(node, config.GetSelfManagementConfig());
            });
        }

        bool MayHaveExclusions(const NKikimrBlobStorage::TStorageConfig& config, const TNodeWardenConfig& nwConfig) {
            const auto excludedHost = [](const auto& node) {
                return node.HasDistconfState() && node.GetDistconfState() == NKikimrBlobStorage::TDistconfState::EXCLUDED;
            };
            const auto excludedDataCenter = [](const auto& item) {
                return item.HasState() && item.GetState() == NKikimrBlobStorage::TDistconfState::EXCLUDED;
            };
            return std::ranges::any_of(config.GetAllNodes(), excludedHost)
                || std::ranges::any_of(config.GetSelfManagementConfig().GetDataCenterStates(), excludedDataCenter)
                || (nwConfig.NameserviceConfig && std::ranges::any_of(nwConfig.NameserviceConfig->GetNode(), excludedHost))
                || (nwConfig.SelfManagementConfig && std::ranges::any_of(
                    nwConfig.SelfManagementConfig->GetDataCenterStates(), excludedDataCenter));
        }

        void ClearConfigVersion(NKikimrBlobStorage::TStorageConfig& config) {
            config.ClearGeneration();
            config.ClearFingerprint();
            config.ClearPrevConfig();
            config.ClearForcedDistconfState();
        }

        auto ActiveDisks(const NKikimrBlobStorage::TStorageConfig& config) {
            THashSet<std::tuple<TNodeIdentifier, TString>> disks;
            EnumerateConfigDrives(config, 0, [&](const auto& node, const auto& drive) {
                if (!IsExcluded(node, config.GetSelfManagementConfig())) {
                    disks.emplace(TNodeIdentifier(node), drive.GetPath());
                }
            });
            return disks;
        }

        bool IsStoredForcedTransition(const NKikimrBlobStorage::TStorageConfig& previous,
                const NKikimrBlobStorage::TStorageConfig& proposed) {
            if (previous.HasClusterState() || proposed.HasClusterState()) {
                return false;
            }
            const auto topology = [](const auto& config) {
                auto result = config;
                ClearConfigVersion(result);
                for (auto& node : *result.MutableAllNodes()) {
                    node.ClearDistconfState();
                }
                if (result.HasSelfManagementConfig()) {
                    auto *settings = result.MutableSelfManagementConfig();
                    settings->ClearDataCenterStates();
                    settings->ClearOverrideDistconfState();
                    if (!settings->ByteSizeLong()) {
                        result.ClearSelfManagementConfig();
                    }
                }
                return result;
            };
            if (!google::protobuf::util::MessageDifferencer::Equals(topology(previous), topology(proposed))) {
                return false;
            }
            const auto oldDisks = ActiveDisks(previous);
            const auto newDisks = ActiveDisks(proposed);
            return !newDisks.empty() && newDisks.size() < oldDisks.size()
                && std::ranges::all_of(newDisks, [&](const auto& disk) { return oldDisks.contains(disk); });
        }
    }

    bool HasDistconfExclusions(const NKikimrBlobStorage::TStorageConfig& config, const TNodeWardenConfig& nwConfig) {
        return MayHaveExclusions(config, nwConfig) && HasExclusions(MakeDistconfQuorumConfig(config, nwConfig));
    }

    bool HasDistconfExclusions(const NKikimrBlobStorage::TStorageConfig& config) {
        return HasExclusions(config);
    }

    bool IsForcedDistconfExclusionTransition(const NKikimrBlobStorage::TStorageConfig& previous,
            const NKikimrBlobStorage::TStorageConfig& proposed, const TNodeWardenConfig& nwConfig) {
        if (!nwConfig.SelfManagementConfig || !nwConfig.SelfManagementConfig->GetOverrideDistconfState()
                || previous.HasClusterState() || proposed.HasClusterState() || nwConfig.BridgeConfig) {
            return false;
        }
        auto expected = MakeDistconfQuorumConfig(previous, nwConfig);
        auto candidate = proposed;
        ClearConfigVersion(expected);
        ClearConfigVersion(candidate);
        if (!google::protobuf::util::MessageDifferencer::Equals(expected, candidate)) {
            return false;
        }
        return IsStoredForcedTransition(previous, proposed);
    }

    std::optional<bool> HasStorageQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TSuccessfulDisk> successful,
            TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig, bool allowUnformatted,
            IOutputStream *out, const char *name);

    static bool HasDiskQuorumImpl(const NKikimrBlobStorage::TStorageConfig& config, std::span<TSuccessfulDisk> successful,
        TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig, IOutputStream *out, const char *name,
        bool mindGuids = false);

    // generate set of mandatory pile ids for quorum
    THashSet<TBridgePileId> GetMandatoryPileIds(const NKikimrBlobStorage::TStorageConfig& config,
            const THashSet<TBridgePileId>& pileIdQuorumOverride) {
        THashSet<TBridgePileId> mandatoryPileIds;
        if (pileIdQuorumOverride) { // we are requested to process quorum from specific pile
            mandatoryPileIds = pileIdQuorumOverride;
        } else if (config.HasClusterState()) {
            const auto& clusterState = config.GetClusterState();
            for (size_t i = 0; i < clusterState.PerPileStateSize(); ++i) {
                if (NBridge::PileStateTraits(clusterState.GetPerPileState(i)).RequiresConfigQuorum) {
                    mandatoryPileIds.insert(TBridgePileId::FromPileIndex(i));
                }
            }
        }
        return mandatoryPileIds;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    static THashMap<TBridgePileId, THashMap<TString, std::tuple<ui32, ui32>>> PrepareStatusMap(
            const NKikimrBlobStorage::TStorageConfig& config, TBridgePileId singleBridgePileId) {
        THashMap<TBridgePileId, THashMap<TString, std::tuple<ui32, ui32>>> res;

        if (singleBridgePileId) {
            res.try_emplace(singleBridgePileId);
        } else if (config.HasClusterState()) {
            const auto& cs = config.GetClusterState();
            for (size_t i = 0; i < cs.PerPileStateSize(); ++i) {
                if (NBridge::PileStateTraits(cs.GetPerPileState(i)).RequiresConfigQuorum) {
                    // this pile is part of config quorum, we need it to be connected
                    res.try_emplace(TBridgePileId::FromPileIndex(i));
                }
            }
        } else {
            // default pile for non-bridged installations
            res.try_emplace(TBridgePileId());
        }

        return res;
    }

    static std::optional<bool> HasBootstrapNodeQuorum(const NKikimrBlobStorage::TStorageConfig& config,
                                                      const THashSet<TNodeIdentifier>& successfulNodes,
                                                      TStringStream *out) {
        if (config.GetGeneration() || !config.HasBlobStorageConfig() || config.GetBlobStorageConfig().HasServiceSet()) {
            return std::nullopt;
        }

        size_t numSuccessful = 0;
        for (const auto& node : config.GetAllNodes()) {
            numSuccessful += successfulNodes.contains(TNodeIdentifier(node));
        }
        const size_t numNodes = config.AllNodesSize();
        const bool hasQuorum = numSuccessful > numNodes - numSuccessful;
        if (!hasQuorum && out) {
            *out << " config:no-node-majority:" << numSuccessful << '/' << numNodes;
        }
        return hasQuorum;
    }

    bool HasNodeQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TNodeIdentifier> successful,
            TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig, TStringStream *out,
            bool allowConfigQuorum) {
        if (MayHaveExclusions(config, nwConfig)) {
            const auto effective = MakeDistconfQuorumConfig(config, nwConfig);
            if (HasExclusions(effective)) {
                THashSet<TNodeIdentifier> nodes(successful.begin(), successful.end());
                std::vector<TSuccessfulDisk> disks;
                EnumerateConfigDrives(effective, 0, [&](const auto& node, const auto& drive) {
                    if (nodes.contains(TNodeIdentifier(node))) {
                        disks.emplace_back(node, drive.GetPath(), std::nullopt);
                    }
                });
                return HasDiskQuorumImpl(effective, disks, singleBridgePileId, nwConfig, out, "config");
            }
        }
        if (allowConfigQuorum) {
            THashSet<TNodeIdentifier> successfulNodes(successful.begin(), successful.end());

            if (const auto quorum = HasBootstrapNodeQuorum(config, successfulNodes, out)) {
                return *quorum;
            }

            // calculate pseudo-quorum for all drives in static groups in seen nodes
            std::vector<TSuccessfulDisk> successfulDisks;
            EnumerateConfigDrives(config, 0, [&](const TNodeIdentifier& node, const NKikimrBlobStorage::THostConfigDrive& drive) {
                if (successfulNodes.contains(node)) {
                    successfulDisks.emplace_back(node, drive.GetPath(), std::nullopt /* don't care about guids here */);
                }
            });
            if (auto q = HasStorageQuorum(config, successfulDisks, singleBridgePileId, nwConfig, true, out, "config")) {
                return *q;
            }
        }

        THashSet<TNodeIdentifier> nodes{successful.begin(), successful.end()};
        std::vector<TSuccessfulDisk> disks;
        EnumerateConfigDrives(config, 0, [&](const auto& node, const auto& drive) {
            if (TNodeIdentifier nodeId(node); nodes.contains(nodeId)) {
                disks.emplace_back(nodeId, drive.GetPath(), std::nullopt /*guid*/);
            }
        });
        return HasConfigQuorum(config, disks, singleBridgePileId, nwConfig, false, out);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    bool HasDiskQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TSuccessfulDisk> successful,
            TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig, IOutputStream *out, const char *name) {
        if (MayHaveExclusions(config, nwConfig)) {
            return HasDiskQuorumImpl(MakeDistconfQuorumConfig(config, nwConfig), successful,
                singleBridgePileId, nwConfig, out, name);
        }
        return HasDiskQuorumImpl(config, successful, singleBridgePileId, nwConfig, out, name);
    }

    bool HasDistconfMetadataQuorum(const NKikimrBlobStorage::TStorageConfig& config,
            std::span<TSuccessfulDisk> successful, TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig,
            IOutputStream *out, const char *name, bool mindGuids) {
        return HasDiskQuorumImpl(config, successful, singleBridgePileId, nwConfig, out, name, mindGuids);
    }

    static bool HasDiskQuorumImpl(const NKikimrBlobStorage::TStorageConfig& config, std::span<TSuccessfulDisk> successful,
            TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig, IOutputStream *out, const char *name,
            bool mindGuids) {
        if (HasExclusions(config) && (config.HasClusterState() || nwConfig.BridgeConfig || singleBridgePileId)) {
            if (out) {
                *out << ' ' << name << ":exclusions-not-supported-in-bridge";
            }
            return false;
        }
        // prepare list of piles we want to examine
        auto status = PrepareStatusMap(config, singleBridgePileId);
        // nodes of the config that is not a bridged one (or of the node that is not bridge-capable) are all in default
        // pile
        const auto registry = config.HasClusterState()
            ? GetPileRegistry(config, nwConfig.BridgeConfig.get())
            : std::nullopt;

        // generate set of all required drives
        THashMap<ui32, std::tuple<const NKikimrBlobStorage::TNodeIdentifier&, TBridgePileId, TNodeLocation>> nodeMap;
        THashSet<std::tuple<ui32, TString>> allDrives; // only drives from mandatory piles
        THashMap<std::tuple<ui32, TString>, ui64> expectedGuids;
        if (mindGuids) {
            for (const auto& pdisk : config.GetBlobStorageConfig().GetServiceSet().GetPDisks()) {
                if (pdisk.GetPDiskGuid()) {
                    expectedGuids.emplace(std::make_tuple(pdisk.GetNodeID(), pdisk.GetPath()), pdisk.GetPDiskGuid());
                }
            }
        }
        bool error = false;
        EnumerateConfigDrives(config, 0, [&](const NKikimrBlobStorage::TNodeIdentifier& node, const auto& drive) {
            if (IsExcluded(node, config.GetSelfManagementConfig())) {
                return;
            }
            TNodeLocation location(node.GetLocation());
            const auto bridgePileId = ResolveNodePileId(registry, location);
            if (!bridgePileId) {
                Y_DEBUG_ABORT_S("can't resolve bridge pile Location# " << location.ToString()); // config is inconsistent
                error = true;
            } else if (const auto it = status.find(*bridgePileId); it != status.end()) {
                auto& [ok, err] = it->second[location.GetDataCenterId()];
                if (allDrives.emplace(node.GetNodeId(), drive.GetPath()).second) {
                    ++err;
                }
                nodeMap.try_emplace(node.GetNodeId(), node, *bridgePileId, std::move(location));
            }
        });
        if (error || allDrives.empty()) {
            if (out && allDrives.empty()) {
                *out << ' ' << name << ":no-active-drives";
            }
            return false;
        }

        // process responses
        for (const auto& [node, path, guid] : successful) {
            if (const auto it = nodeMap.find(node.NodeId()); it != nodeMap.end()) {
                const auto& [identifier, bridgePileId, location] = it->second;
                const auto key = std::make_tuple(it->first, path);
                const auto expectedGuid = expectedGuids.find(key);
                const bool validGuid = expectedGuid == expectedGuids.end()
                    || (guid && *guid == expectedGuid->second);
                if (validGuid && node == TNodeIdentifier(identifier) && allDrives.erase(key)) {
                    auto& [ok, err] = status[bridgePileId][location.GetDataCenterId()];
                    Y_ABORT_UNLESS(err);
                    ++ok;
                    --err;
                }
            }
        }

        // calculate number of good and bad datacenters
        for (const auto& [bridgePileId, pileStatus] : status) {
            ui32 ok = 0;
            ui32 err = 0;
            for (const auto& [_, value] : pileStatus) {
                const auto [dcOk, dcErr] = value;
                ++(dcOk > dcErr ? ok : err);
            }

            // strict datacenter majority
            if (ok <= err) {
                if (out) {
                    *out << ' ' << name << ":no-quorum:" << bridgePileId;
                    for (const auto& [_, value] : pileStatus) {
                        const auto [dcOk, dcErr] = value;
                        *out << ':' << dcOk << '/' << dcOk + dcErr;
                    }
                }
                return false;
            }
        }

        return true;
    }

    std::optional<bool> HasStorageQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TSuccessfulDisk> successful,
            TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig, bool allowUnformatted,
            IOutputStream *out, const char *name) {
        auto makeError = [&](TString error) -> bool {
            YDB_LOG_CRIT("Configuration incorrect",
                {"marker", "NWDC41"},
                {"error", error});
            //Y_DEBUG_ABORT("%s", error.c_str());
            if (out) {
                *out << ' ' << name << ':' << error;
            }
            return false;
        };
        if (!config.HasBlobStorageConfig()) { // no storage config at all -- however, this is quite strange
            return makeError("no BlobStorageConfig section in config");
        }
        const auto& bsConfig = config.GetBlobStorageConfig();
        if (!bsConfig.HasServiceSet()) { // maybe this is initial configuration
            return !config.GetGeneration() || makeError("non-initial configuration with missing ServiceSet");
        }
        const auto& ss = bsConfig.GetServiceSet();

        // build map of group infos
        struct TGroupRecord {
            TIntrusivePtr<TBlobStorageGroupInfo> Info;
            TBlobStorageGroupInfo::TGroupVDisks Confirmed; // a set of confirmed group disks

            TGroupRecord(TIntrusivePtr<TBlobStorageGroupInfo>&& info)
                : Info(std::move(info))
                , Confirmed(&Info->GetTopology())
            {}
        };
        THashMap<TGroupId, TGroupRecord> groups;
        for (const auto& group : ss.GetGroups()) {
            const auto groupId = TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID);
            if (TGroupID(groupId).ConfigurationType() != EGroupConfigurationType::Static) {
                return makeError("nonstatic group id in static configuration section");
            }

            TStringStream err;
            TIntrusivePtr<TBlobStorageGroupInfo> info = TBlobStorageGroupInfo::Parse(group, &nwConfig.StaticKey, &err);
            if (!info) {
                return makeError(TStringBuilder() << "failed to parse static group " << groupId << ": " << err.Str());
            }

            if (const auto [it, inserted] = groups.emplace(groupId, std::move(info)); !inserted) {
                return makeError("duplicate group id in static configuration section");
            }
        }

        // fill in pdisk map
        THashMap<std::tuple<ui32, ui32, ui64>, TString> pdiskIdToPath; // (nodeId, pdiskId, pdiskGuid) -> path
        for (const auto& pdisk : ss.GetPDisks()) {
            const auto [it, inserted] = pdiskIdToPath.emplace(std::make_tuple(pdisk.GetNodeID(), pdisk.GetPDiskID(),
                pdisk.GetPDiskGuid()), pdisk.GetPath());
            if (!inserted) {
                return makeError("duplicate pdisk in static configuration section");
            }
        }

        // create confirmation map
        THashMultiMap<std::tuple<ui32, TString, std::optional<ui64>>, TVDiskID> confirm;
        for (const auto& vdisk : ss.GetVDisks()) {
            if (!vdisk.HasVDiskID() || !vdisk.HasVDiskLocation()) {
                return makeError("incorrect TVDisk record");
            }
            if (vdisk.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::DESTROY) {
                continue;
            }
            if (vdisk.HasDonorMode()) {
                continue;
            }
            const auto vdiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
            const auto it = groups.find(vdiskId.GroupID);
            if (it == groups.end()) {
                return makeError(TStringBuilder() << "VDisk " << vdiskId << " does not match any static group");
            }
            const TGroupRecord& group = it->second;
            if (vdiskId.GroupGeneration != group.Info->GroupGeneration) {
                return makeError(TStringBuilder() << "VDisk " << vdiskId << " group generation mismatch");
            }
            const auto& location = vdisk.GetVDiskLocation();
            const auto jt = pdiskIdToPath.find(std::make_tuple(location.GetNodeID(), location.GetPDiskID(),
                location.GetPDiskGuid()));
            if (jt == pdiskIdToPath.end()) {
                return makeError(TStringBuilder() << "VDisk " << vdiskId << " points to incorrect PDisk record");
            }
            confirm.emplace(std::make_tuple(location.GetNodeID(), jt->second, location.GetPDiskGuid()), vdiskId);
            if (allowUnformatted) {
                confirm.emplace(std::make_tuple(location.GetNodeID(), jt->second, std::nullopt), vdiskId);
            }
        }

        // process responded nodes
        for (const auto& [node, path, guid] : successful) {
            const auto key = std::make_tuple(node.NodeId(), path, guid);
            const auto [begin, end] = confirm.equal_range(key);
            for (auto it = begin; it != end; ++it) {
                const TVDiskID& vdiskId = it->second;
                TGroupRecord& group = groups.at(vdiskId.GroupID);
                group.Confirmed |= {&group.Info->GetTopology(), vdiskId};
            }
        }

        // scan all groups and find ones without quorum
        THashMap<TGroupId, bool> bridgedGroups;
        THashSet<TGroupId> badGroups;
        for (const auto& [groupId, group] : groups) {
            const auto& info = group.Info;
            if (info->IsBridged()) {
                if (!config.HasClusterState()) {
                    Y_DEBUG_ABORT();
                    return false;
                }
                const auto& cs = config.GetClusterState();
                Y_ABORT_UNLESS(info->Group);
                const auto& state = info->Group->GetBridgeGroupState();
                if (cs.PerPileStateSize() < state.PileSize()) {
                    Y_DEBUG_ABORT();
                    return false;
                }
                NBridge::ForEachGroupPile(state, [&](TBridgePileId bridgePileId,
                        const NKikimrBridge::TGroupState::TPile& pile) {
                    const auto pileState = cs.GetPerPileState(bridgePileId.GetPileIndex());
                    bridgedGroups[TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId)] =
                        singleBridgePileId
                            ? bridgePileId == singleBridgePileId
                            : NBridge::PileStateTraits(pileState).RequiresConfigQuorum;
                });
            }
            const bool hasOwnVDisks = !info->IsBridged() || info->IsInlinePile(); // bridge proxy group doesn't have them
            if (hasOwnVDisks && !info->GetQuorumChecker().CheckQuorumForGroup(group.Confirmed)) {
                badGroups.insert(groupId);
            }
        }
        for (TGroupId groupId : badGroups) {
            if (const auto it = bridgedGroups.find(groupId); it == bridgedGroups.end() || it->second) {
                if (out) {
                    *out << ' ' << name << ":group:" << groupId;
                }
                return false; // this isn't a bridged group or it is one, but it is required to be working
            }
        }

        return groups.empty()
            ? std::nullopt // we don't know about quorum -- no static groups defined
            : std::make_optional(true); // all groups meet their quorums
    }

    static bool HasConfigQuorumImpl(const NKikimrBlobStorage::TStorageConfig& config, std::span<TSuccessfulDisk> successful,
            TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig, bool mindPrev, TStringStream *out,
            bool useStartup) {
        std::optional<NKikimrBlobStorage::TStorageConfig> overlay;
        if (useStartup && MayHaveExclusions(config, nwConfig)) {
            overlay.emplace(MakeDistconfQuorumConfig(config, nwConfig));
        }
        const auto& effective = overlay ? *overlay : config;
        const bool exclusions = HasExclusions(effective);
        if (!HasDiskQuorumImpl(effective, successful, singleBridgePileId, nwConfig, out, "new")) {
            return false;
        }
        if (!exclusions && !HasStorageQuorum(config, successful, singleBridgePileId,
                nwConfig, true, out, "new").value_or(true)) {
            return false;
        }
        if (!mindPrev || !config.HasPrevConfig()) {
            return true;
        }
        const auto& previous = config.GetPrevConfig();
        if (exclusions || HasExclusions(previous)) {
            const bool forced = config.GetForcedDistconfState()
                && (useStartup ? IsForcedDistconfExclusionTransition(previous, config, nwConfig)
                    : IsStoredForcedTransition(previous, config));
            return forced
                || HasDiskQuorumImpl(previous, successful, singleBridgePileId, nwConfig, out, "prev", true);
        }
        return HasStorageQuorum(previous, successful, singleBridgePileId,
            nwConfig, false, out, "prev").value_or(true);
    }

    bool HasConfigQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TSuccessfulDisk> successful,
            TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig, bool mindPrev, TStringStream *out) {
        return HasConfigQuorumImpl(config, successful, singleBridgePileId, nwConfig, mindPrev, out, true);
    }

    bool HasOriginalConfigQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TSuccessfulDisk> successful,
            TBridgePileId singleBridgePileId, const TNodeWardenConfig& nwConfig, bool mindPrev, TStringStream *out) {
        return HasConfigQuorumImpl(config, successful, singleBridgePileId, nwConfig, mindPrev, out, false);
    }

} // NKikimr::NStorage
