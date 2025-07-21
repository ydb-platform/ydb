#pragma once

namespace NKikimr::NStorage {

    static constexpr TBridgePileId InvalidBridgePileId = TBridgePileId::FromValue(Max<ui32>());

    // generate set of mandatory pile ids for quorum
    THashSet<TBridgePileId> GetMandatoryPileIds(const NKikimrBlobStorage::TStorageConfig& config,
            THashSet<TBridgePileId> specificBridgePileIds) {
        THashSet<TBridgePileId> mandatoryPileIds;
        if (specificBridgePileIds) { // we are requested to process quorum from specific pile
            mandatoryPileIds = std::move(specificBridgePileIds);
        } else if (config.HasClusterState()) {
            const auto& clusterState = config.GetClusterState();
            for (size_t i = 0; i < clusterState.PerPileStateSize(); ++i) {
                if (clusterState.GetPerPileState(i) != NKikimrBridge::TClusterState::DISCONNECTED) {
                    mandatoryPileIds.insert(TBridgePileId::FromValue(i));
                }
            }
        } else {
            mandatoryPileIds.emplace(InvalidBridgePileId); // default pile id for non-bridged mode
        }
        return mandatoryPileIds;
    }

    template<typename T>
    bool HasDiskQuorum(const NKikimrBlobStorage::TStorageConfig& config, T&& generateSuccessful,
            const THashSet<TBridgePileId>& mandatoryPileIds, IOutputStream *out, const char *name) {
        // generate set of all required drives
        THashMap<TBridgePileId, THashMap<TString, std::tuple<ui32, ui32>>> status; // dc -> {ok, err}
        THashMap<ui32, const NKikimrBlobStorage::TNodeIdentifier*> nodeMap;
        THashSet<std::tuple<TNodeIdentifier, TString>> allDrives; // only drives from mandatory piles
        auto cb = [&](const auto& node, const auto& drive) {
            TNodeIdentifier identifier(node);
            const TBridgePileId bridgePileId = identifier.BridgePileId.value_or(InvalidBridgePileId);
            if (!mandatoryPileIds.contains(bridgePileId)) {
                return; // we ignore drives from non-mandatory piles
            }
            const TNodeLocation location(node.GetLocation());
            auto& [ok, err] = status[bridgePileId][location.GetDataCenterId()];
            ++err;
            allDrives.emplace(node, drive.GetPath());
        };
        EnumerateConfigDrives(config, 0, cb, &nodeMap);

        // process responses
        generateSuccessful([&](const TNodeIdentifier& node, const TString& path, std::optional<ui64> /*guid*/) {
            const auto it = nodeMap.find(node.NodeId());
            if (it == nodeMap.end()) {
                return;
            }
            const TNodeIdentifier identifier(*it->second);
            if (identifier != node) { // unexpected node answers
                return;
            }
            if (!allDrives.erase(std::make_tuple(node, path))) { // unexpected drive
                return;
            }
            const TBridgePileId bridgePileId = identifier.BridgePileId.value_or(InvalidBridgePileId);
            const TNodeLocation location(it->second->GetLocation());
            auto& [ok, err] = status[bridgePileId][location.GetDataCenterId()];
            Y_ABORT_UNLESS(err);
            ++ok;
            --err;
        });

        // calculate number of good and bad datacenters
        for (TBridgePileId bridgePileId : mandatoryPileIds) {
            if (!status.contains(bridgePileId)) {
                if (out) {
                    *out << ' ' << name << ":no-pile:" << bridgePileId;
                }
                return false; // not even mention of this mandatory pile
            }

            ui32 ok = 0;
            ui32 err = 0;
            const auto& pileStatus = status.at(bridgePileId);
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

    template<typename T>
    bool HasNodeQuorum(const NKikimrBlobStorage::TStorageConfig& config, T&& generateSuccessful,
            const THashSet<TBridgePileId>& mandatoryPileIds, TStringStream *out) {
        // generate set of all nodes
        THashMap<TBridgePileId, THashMap<TString, std::tuple<ui32, ui32>>> status; // dc -> {ok, err}
        THashMap<ui32, const NKikimrBlobStorage::TNodeIdentifier*> nodeMap;
        for (const auto& node : config.GetAllNodes()) {
            const TNodeIdentifier identifier(node);
            const TBridgePileId bridgePileId = identifier.BridgePileId.value_or(InvalidBridgePileId);
            if (!mandatoryPileIds.contains(bridgePileId)) {
                continue; // skip nodes from non-mandatory piles
            }
            const TNodeLocation location(node.GetLocation());
            auto& [ok, err] = status[bridgePileId][location.GetDataCenterId()];
            ++err;
            nodeMap.emplace(node.GetNodeId(), &node);
        }

        // process responses
        std::set<TNodeIdentifier> seen;
        generateSuccessful([&](const TNodeIdentifier& node) {
            const auto& [_, inserted] = seen.insert(node);
            Y_ABORT_UNLESS(inserted);

            const auto it = nodeMap.find(node.NodeId());
            if (it == nodeMap.end()) {
                return;
            }
            const TNodeIdentifier identifier(*it->second);
            if (identifier != node) { // unexpected node answers
                return;
            }
            const TBridgePileId bridgePileId = identifier.BridgePileId.value_or(InvalidBridgePileId);
            const TNodeLocation location(it->second->GetLocation());
            auto& [ok, err] = status[bridgePileId][location.GetDataCenterId()];
            Y_ABORT_UNLESS(err);
            ++ok;
            --err;
        });

        // calculate number of good and bad datacenters
        for (TBridgePileId bridgePileId : mandatoryPileIds) {
            if (!status.contains(bridgePileId)) {
                if (out) {
                    *out << " no-pile:" << bridgePileId;
                }
                return false;
            }

            ui32 ok = 0;
            ui32 err = 0;
            const auto& pileStatus = status.at(bridgePileId);
            for (const auto& [_, value] : pileStatus) {
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

    template<typename T>
    bool HasNodeQuorum(const NKikimrBlobStorage::TStorageConfig& config, T&& generateSuccessful, TStringStream *out) {
        return HasNodeQuorum(config, std::forward<T>(generateSuccessful), GetMandatoryPileIds(config, {}), out);
    }

    template<typename T>
    bool HasStorageQuorum(const NKikimrBlobStorage::TStorageConfig& config, T&& generateSuccessful,
            const TNodeWardenConfig& nwConfig, bool allowUnformatted, const THashSet<TBridgePileId>& mandatoryPileIds,
            IOutputStream *out, const char *name) {
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
        generateSuccessful([&](const TNodeIdentifier& node, const TString& path, std::optional<ui64> guid) {
            const auto key = std::make_tuple(node.NodeId(), path, guid);
            const auto [begin, end] = confirm.equal_range(key);
            for (auto it = begin; it != end; ++it) {
                const TVDiskID& vdiskId = it->second;
                TGroupRecord& group = groups.at(vdiskId.GroupID);
                group.Confirmed |= {&group.Info->GetTopology(), vdiskId};
            }
        });

        // scan all groups and find ones without quorum
        THashMap<TGroupId, bool> bridgedGroups;
        THashSet<TGroupId> badGroups;
        for (const auto& [groupId, group] : groups) {
            const auto& info = group.Info;
            if (info->IsBridged()) {
                const auto& ids = info->GetBridgeGroupIds();
                for (size_t i = 0; i < ids.size(); ++i) {
                    bridgedGroups[ids[i]] = mandatoryPileIds.contains(TBridgePileId::FromValue(i));
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

    // Ensure configuration has quorum in both disk and storage ways for current and previous configuration.
    template<typename T>
    bool HasConfigQuorum(const NKikimrBlobStorage::TStorageConfig& config, T&& generateSuccessful,
            const TNodeWardenConfig& nwConfig, bool mindPrev = true,
            THashSet<TBridgePileId> specificBridgePileIds = {},
            TStringStream *out = nullptr) {
        const auto& mandatoryPileIds = GetMandatoryPileIds(config, specificBridgePileIds);
        if (!HasDiskQuorum(config, generateSuccessful, mandatoryPileIds, out, "new") ||
                !HasStorageQuorum(config, generateSuccessful, nwConfig, true, mandatoryPileIds, out, "new")) {
            // no quorum in terms of new configuration
            return false;
        }
        if (!mindPrev || !config.HasPrevConfig()) {
            // we don't care about quorum in terms of previous configuration
            return true;
        }
        // calculate quorum in terms of previous configuration
        return HasDiskQuorum(config.GetPrevConfig(), generateSuccessful, mandatoryPileIds, out, "prev") &&
            HasStorageQuorum(config.GetPrevConfig(), generateSuccessful, nwConfig, false, mandatoryPileIds, out, "prev");
    }

} // NKikimr::NStorage
