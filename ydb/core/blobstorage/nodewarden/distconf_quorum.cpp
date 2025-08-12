#include "distconf_quorum.h"

namespace NKikimr::NStorage {

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

    static std::optional<TBridgePileId> ResolveBridgePileId(const TNodeLocation& location,
            const THashMap<TString, TBridgePileId>& bridgePileNameMap) {
        const auto& bridgePileName = location.GetBridgePileName(); 
        if (bridgePileName.has_value() != !bridgePileNameMap.empty()) {
            Y_DEBUG_ABORT_S("incorrect node bridge pile name setting Location# " << location.ToString());
            return std::nullopt;
        }
        if (bridgePileNameMap) {
            Y_ABORT_UNLESS(bridgePileName);
            if (const auto it = bridgePileNameMap.find(*bridgePileName); it != bridgePileNameMap.end()) {
                return it->second;
            }
            Y_DEBUG_ABORT_S("incorrect pile name: " << *bridgePileName);
            return std::nullopt;
        } else {
            return TBridgePileId();
        }
    }

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

    bool HasNodeQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TNodeIdentifier> successful,
            const THashMap<TString, TBridgePileId>& bridgePileNameMap, TBridgePileId singleBridgePileId,
            TStringStream *out) {
        // prepare list of piles we want to examine
        auto status = PrepareStatusMap(config, singleBridgePileId);

        // generate set of all nodes
        THashMap<ui32, std::tuple<const NKikimrBlobStorage::TNodeIdentifier&, TBridgePileId, TNodeLocation>> nodeMap;
        for (const NKikimrBlobStorage::TNodeIdentifier& node : config.GetAllNodes()) {
            TNodeLocation location(node.GetLocation());
            const auto& bridgePileId = ResolveBridgePileId(location, bridgePileNameMap);
            if (!bridgePileId) {
                return false;
            } else if (const auto it = status.find(*bridgePileId); it != status.end()) {
                auto& [ok, err] = it->second[location.GetDataCenterId()];
                ++err;
                nodeMap.try_emplace(node.GetNodeId(), node, *bridgePileId, std::move(location));
            }
        }

        // process responses
        THashSet<TNodeIdentifier> seen;
        for (const TNodeIdentifier& node : successful) {
            if (const auto it = nodeMap.find(node.NodeId()); it != nodeMap.end()) {
                const auto& [identifier, bridgePileId, location] = it->second;
                if (node == TNodeIdentifier(identifier) && seen.insert(node).second) {
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
            for (const auto& [dataCenterId, value] : pileStatus) {
                const auto [dcOk, dcErr] = value;
                ++(dcOk > dcErr ? ok : err);
            }

            // strict datacenter majority
            if (ok <= err) {
                if (out) {
                    *out << " no-quorum";
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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    bool HasDiskQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TSuccessfulDisk> successful,
            const THashMap<TString, TBridgePileId>& bridgePileNameMap, TBridgePileId singleBridgePileId,
            IOutputStream *out, const char *name) {
        // prepare list of piles we want to examine
        auto status = PrepareStatusMap(config, singleBridgePileId);

        // generate set of all required drives
        THashMap<ui32, std::tuple<const NKikimrBlobStorage::TNodeIdentifier&, TBridgePileId, TNodeLocation>> nodeMap;
        THashSet<std::tuple<ui32, TString>> allDrives; // only drives from mandatory piles
        bool error = false;
        EnumerateConfigDrives(config, 0, [&](const NKikimrBlobStorage::TNodeIdentifier& node, const auto& drive) {
            TNodeLocation location(node.GetLocation());
            const auto& bridgePileId = ResolveBridgePileId(location, bridgePileNameMap);
            if (!bridgePileId) {
                error = true;
            } else if (const auto it = status.find(*bridgePileId); it != status.end()) {
                auto& [ok, err] = it->second[location.GetDataCenterId()];
                ++err;
                nodeMap.try_emplace(node.GetNodeId(), node, *bridgePileId, std::move(location));
                allDrives.emplace(node.GetNodeId(), drive.GetPath());
            }
        });
        if (error) {
            return false;
        }

        // process responses
        for (const auto& [node, path, guid] : successful) {
            if (const auto it = nodeMap.find(node.NodeId()); it != nodeMap.end()) {
                const auto& [identifier, bridgePileId, location] = it->second;
                if (node == TNodeIdentifier(identifier) && allDrives.erase(std::make_tuple(it->first, path))) {
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

    bool HasStorageQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TSuccessfulDisk> successful,
            const THashMap<TString, TBridgePileId>& /*bridgePileNameMap*/, TBridgePileId singleBridgePileId,
            const TNodeWardenConfig& nwConfig, bool allowUnformatted, IOutputStream *out, const char *name) {
        auto makeError = [&](TString error) -> bool {
            STLOG(PRI_CRIT, BS_NODE, NWDC41, "configuration incorrect", (Error, error));
            Y_DEBUG_ABORT("%s", error.c_str());
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
                const auto& ids = info->GetBridgeGroupIds();
                if (ids.size() != cs.PerPileStateSize()) {
                    Y_DEBUG_ABORT();
                    return false;
                }
                for (size_t i = 0; i < ids.size(); ++i) {
                    bridgedGroups[ids[i]] = singleBridgePileId
                        ? TBridgePileId::FromPileIndex(i) == singleBridgePileId
                        : NBridge::PileStateTraits(cs.GetPerPileState(i)).RequiresConfigQuorum;
                }
            } else if (const auto& checker = info->GetQuorumChecker(); !checker.CheckQuorumForGroup(group.Confirmed)) {
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

        return true; // all group meet their quorums
    }

    bool HasConfigQuorum(const NKikimrBlobStorage::TStorageConfig& config, std::span<TSuccessfulDisk> successful,
            const THashMap<TString, TBridgePileId>& bridgePileNameMap, TBridgePileId singleBridgePileId,
            const TNodeWardenConfig& nwConfig, bool mindPrev, TStringStream *out) {
        return HasDiskQuorum(config, successful, bridgePileNameMap, singleBridgePileId, out, "new") &&
            HasStorageQuorum(config, successful, bridgePileNameMap, singleBridgePileId, nwConfig, true, out, "new") &&
            (!mindPrev || !config.HasPrevConfig() ||
                HasDiskQuorum(config.GetPrevConfig(), successful, bridgePileNameMap, singleBridgePileId, out, "prev") &&
                HasStorageQuorum(config.GetPrevConfig(), successful, bridgePileNameMap, singleBridgePileId, nwConfig, false, out, "prev"));
    }

} // NKikimr::NStorage
