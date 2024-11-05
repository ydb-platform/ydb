#include "distconf.h"

namespace NKikimr::NStorage {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Inter-config validation

    std::optional<TString> ValidateConfigUpdate(const NKikimrBlobStorage::TNodeWardenServiceSet& current,
            const NKikimrBlobStorage::TNodeWardenServiceSet& proposed, const THashSet<ui32>& invalidatedNodeIds) {
        THashMap<std::tuple<ui32, ui32>, std::tuple<TString, ui64>> pdisks;
        for (const auto& pdisk : current.GetPDisks()) {
            if (const auto [_, inserted] = pdisks.try_emplace(std::tuple(pdisk.GetNodeID(), pdisk.GetPDiskID()),
                    pdisk.GetPath(), pdisk.GetPDiskGuid()); !inserted) {
                return "duplicate NodeID:PDiskID in current config";
            }
        }
        THashSet<std::tuple<ui32, ui32>> invalidatedPDiskIds;
        for (const auto& pdisk : proposed.GetPDisks()) {
            if (invalidatedNodeIds.contains(pdisk.GetNodeID())) { // this node gets invalidated, skip it
                continue;
            }
            if (const auto it = pdisks.find(std::make_tuple(pdisk.GetNodeID(), pdisk.GetPDiskID())); it != pdisks.end()) {
                const auto& [path, pdiskGuid] = it->second;
                if (path != pdisk.GetPath()) {
                    invalidatedPDiskIds.insert(it->first);
                } else if (pdiskGuid != pdisk.GetPDiskGuid()) {
                    return TStringBuilder() << "PDiskGuid gets changed for pdisk " << pdisk.GetNodeID() << ':'
                        << pdisk.GetPDiskID();
                }
            }
        }

        THashMap<ui32, ui32> currentGroupGens;
        for (const auto& group : current.GetGroups()) {
            currentGroupGens.emplace(group.GetGroupID(), group.GetGroupGeneration());
        }

        // make a list of slots in current config
        THashMap<TVDiskID, std::tuple<ui32, ui32, ui32, ui64>> vdisks;
        for (const auto& vslot : current.GetVDisks()) {
            TVDiskID vdiskId = VDiskIDFromVDiskID(vslot.GetVDiskID());
            const auto it = currentGroupGens.find(vdiskId.GroupID.GetRawId());
            if (it == currentGroupGens.end() || it->second != vdiskId.GroupGeneration) {
                continue;
            }
            vdiskId.GroupGeneration = 0;
            const auto& l = vslot.GetVDiskLocation();
            if (const auto [_, inserted] = vdisks.try_emplace(vdiskId, l.GetNodeID(),
                    l.GetPDiskID(), l.GetVDiskSlotID(), l.GetPDiskGuid()); !inserted) {
                return "duplicate VDiskID in current config";
            }
        }

        THashMap<ui32, ui32> proposedGroupGens;
        for (const auto& group : proposed.GetGroups()) {
            proposedGroupGens.emplace(group.GetGroupID(), group.GetGroupGeneration());
        }

        // scan vslots in new config and check if they match
        THashSet<ui32> changedGroups;
        for (const auto& vslot : proposed.GetVDisks()) {
            TVDiskID vdiskId = VDiskIDFromVDiskID(vslot.GetVDiskID());
            const auto groupIt = proposedGroupGens.find(vdiskId.GroupID.GetRawId());
            if (groupIt == proposedGroupGens.end() || groupIt->second != vdiskId.GroupGeneration) {
                continue;
            }
            vdiskId.GroupGeneration = 0;

            const auto& l = vslot.GetVDiskLocation();
            const auto it = vdisks.find(vdiskId);
            if (it == vdisks.end()) { // this is new vslot from a new group
                continue;
            }

            const bool changed = it->second != std::make_tuple(l.GetNodeID(), l.GetPDiskID(), l.GetVDiskSlotID(), l.GetPDiskGuid()) ||
                invalidatedNodeIds.contains(l.GetNodeID()) ||
                invalidatedPDiskIds.contains(std::make_tuple(l.GetNodeID(), l.GetPDiskID()));
            if (changed && !changedGroups.emplace(vdiskId.GroupID.GetRawId()).second) {
                return "more than one slot has changed in group";
            }

            vdisks.erase(it);
        }

        if (!vdisks.empty()) {
            return "vslots disappeared from config";
        }

        // check group generations and species
        THashMap<ui32, const NKikimrBlobStorage::TGroupInfo*> groupInfo;
        for (const auto& group : current.GetGroups()) {
            groupInfo.try_emplace(group.GetGroupID(), &group);
        }
        for (const auto& group : proposed.GetGroups()) {
            if (const auto it = groupInfo.find(group.GetGroupID()); it != groupInfo.end()) {
                const auto& currentGroup = *it->second;
                if (group.GetGroupGeneration() != currentGroup.GetGroupGeneration() &&
                        group.GetGroupGeneration() != currentGroup.GetGroupGeneration() + 1) {
                    return "incorrect group generation change";
                } else if (group.GetErasureSpecies() != currentGroup.GetErasureSpecies()) {
                    return "sudden group ErasureSpecies change";
                } else if (group.RingsSize() != currentGroup.RingsSize()) {
                    return "sudden group geometry change";
                } else if (group.GetRings(0).FailDomainsSize() != currentGroup.GetRings(0).FailDomainsSize()) {
                    return "sudden group geometry change";
                }
                groupInfo.erase(it);
            }
        }

        if (!groupInfo.empty()) {
            return "groups disappeared from config";
        }

        return {};
    }

    std::optional<TString> ValidateConfigUpdate(const NKikimrConfig::TBlobStorageConfig& current,
            const NKikimrConfig::TBlobStorageConfig& proposed, const THashSet<ui32>& invalidatedNodeIds) {
        if (current.HasServiceSet() && proposed.HasServiceSet()) {
            if (auto error = ValidateConfigUpdate(current.GetServiceSet(), proposed.GetServiceSet(), invalidatedNodeIds)) {
                return error;
            }
        } else if (current.HasServiceSet()) {
            return "ServiceSet section disappeared in proposed config";
        }

        return {};
    }

    std::optional<TString> ValidateConfigUpdate(const NKikimrBlobStorage::TStorageConfig& current,
            const NKikimrBlobStorage::TStorageConfig& proposed) {
        if (current.GetGeneration() + 1 != proposed.GetGeneration()) {
            return TStringBuilder() << "invalid proposed config generation current# " << current.GetGeneration()
                << " proposed# " << proposed.GetGeneration();
        }

        if (auto error = ValidateConfig(proposed)) {
            return TStringBuilder() << "proposed config validation failed: " << *error;
        }

        THashMap<ui32, std::tuple<TString, ui16>> currentNodeMap;
        for (const auto& node : current.GetAllNodes()) {
            if (const auto [_, inserted] = currentNodeMap.try_emplace(node.GetNodeId(), node.GetHost(), node.GetPort()); !inserted) {
                return "duplicate NodeId in current config";
            }
        }

        THashSet<ui32> invalidatedNodeIds;
        for (const auto& node : proposed.GetAllNodes()) {
            if (const auto it = currentNodeMap.find(node.GetNodeId()); it != currentNodeMap.end() &&
                    it->second != std::make_tuple(node.GetHost(), node.GetPort())) {
                invalidatedNodeIds.insert(node.GetNodeId());
            }
        }

        if (current.HasBlobStorageConfig() && proposed.HasBlobStorageConfig()) {
            if (auto error = ValidateConfigUpdate(current.GetBlobStorageConfig(), proposed.GetBlobStorageConfig(),
                    invalidatedNodeIds)) {
                return error;
            }
        } else if (current.HasBlobStorageConfig()) {
            return "BlobStorageConfig section disappeared in proposed config";
        }

        return {};
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Intra-config validation code

    std::optional<TString> ValidateConfig(const NKikimrBlobStorage::TNodeWardenServiceSet& config, const THashSet<ui32>& nodeIds) {
        // build a map of pdisks
        THashSet<std::tuple<ui32, TString>> pdisksByPath; // nodeId:path
        THashMap<std::tuple<ui32, ui32>, ui64> pdisksById; // nodeId:pdiskId
        for (const auto& pdisk : config.GetPDisks()) {
            if (!pdisk.HasNodeID()) {
                return "NodeID field missing";
            } else if (!pdisk.HasPDiskID()) {
                return "PDiskID field missing";
            } else if (!pdisk.HasPath()) {
                return "Path field missing";
            } else if (!pdisk.HasPDiskGuid()) {
                return "PDiskGuid field missing";
            } else if (!pdisk.HasPDiskCategory()) {
                return "PDiskCategory field missing";
            }

            const ui32 nodeId = pdisk.GetNodeID();
            if (!nodeIds.contains(nodeId)) {
                return "pdisk NodeID points to nonexistent node";
            }

            const ui32 pdiskId = pdisk.GetPDiskID();
            if (const auto [_, inserted] = pdisksById.emplace(std::make_tuple(nodeId, pdiskId), pdisk.GetPDiskGuid()); !inserted) {
                return "duplicate NodeID:PDiskID for pdisk";
            }

            const TString path = pdisk.GetPath();
            if (const auto [_, inserted] = pdisksByPath.emplace(nodeId, path); !inserted) {
                return "duplicate NodeID:Path for pdisk";
            }
        }

        // scan vslots
        THashSet<std::tuple<ui32, ui32, ui32>> vslots; // nodeId:pdiskId:vslotId
        THashMap<TVDiskID, std::tuple<ui32, ui32, ui32, ui64, bool>> vdisks; // vdiskId -> nodeId:pdiskId:vslotId:guid:donor
        for (const auto& vslot : config.GetVDisks()) {
            if (!vslot.HasVDiskID()) {
                return "VDiskID field missing";
            } else if (!vslot.HasVDiskLocation()) {
                return "VDiskLocation field missing";
            }

            const TVDiskID vdiskId = VDiskIDFromVDiskID(vslot.GetVDiskID());

            const auto& l = vslot.GetVDiskLocation();
            if (!l.HasNodeID() || !l.HasPDiskID() || !l.HasVDiskSlotID() || !l.HasPDiskGuid()) {
                return "VDiskLocation incomplete";
            }

            const ui32 nodeId = l.GetNodeID();
            const ui32 pdiskId = l.GetPDiskID();
            const ui32 vslotId = l.GetVDiskSlotID();
            const ui64 pdiskGuid = l.GetPDiskGuid();

            if (const auto it = pdisksById.find(std::make_tuple(nodeId, pdiskId)); it == pdisksById.end()) {
                return "NodeID:PDiskID not found for vdisk";
            } else if (it->second != pdiskGuid) {
                return "PDiskGuid mismatch in VDiskLocation";
            }

            if (const auto [_, inserted] = vslots.emplace(nodeId, pdiskId, vslotId); !inserted) {
                return "duplicate NodeID:PDiskID:VDiskSlotID for vdisk";
            }

            if (vslot.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::DESTROY) {
                continue;
            }

            if (const auto [_, inserted] = vdisks.try_emplace(vdiskId, nodeId, pdiskId, vslotId, pdiskGuid, vslot.HasDonorMode()); !inserted) {
                return "duplicate VDiskID";
            }
        }

        // scan donors
        for (const auto& vslot : config.GetVDisks()) {
            for (const auto& donor : vslot.GetDonors()) {
                if (!donor.HasVDiskId() || !donor.HasVDiskLocation()) {
                    return "incorrect Donor record";
                }
                const TVDiskID vdiskId = VDiskIDFromVDiskID(donor.GetVDiskId());
                const auto it = vdisks.find(vdiskId);
                if (it == vdisks.end()) {
                    return "incorrect Donor reference";
                }
                const auto& [nodeId, pdiskId, vslotId, pdiskGuid, isDonor] = it->second;
                if (!isDonor) {
                    return "incorrect Donor reference";
                }
                const auto& loc = donor.GetVDiskLocation();
                if (nodeId != loc.GetNodeID() || pdiskId != loc.GetPDiskID() || vslotId != loc.GetVDiskSlotID() ||
                        pdiskGuid != loc.GetPDiskGuid()) {
                    return "incorrect Donor reference";
                }
                vdisks.erase(it);
            }
        }

        // now process groups
        THashSet<ui32> groups;
        for (const auto& group : config.GetGroups()) {
            if (!group.HasGroupID()) {
                return "GroupID field missing";
            } else if (!group.HasGroupGeneration()) {
                return "GroupGeneration field missing";
            } else if (!group.HasErasureSpecies()) {
                return "ErasureSpecies field missing";
            }

            const ui32 groupId = group.GetGroupID();
            const ui32 groupGen = group.GetGroupGeneration();

            if (const auto [_, inserted] = groups.emplace(groupId); !inserted) {
                return "duplicate GroupID";
            }

            const auto e = static_cast<TBlobStorageGroupType::EErasureSpecies>(group.GetErasureSpecies());
            const TBlobStorageGroupType gtype(e);
            std::optional<ui32> numFailDomainsInFailRealm;

            for (ui32 failRealmIdx = 0, count = group.RingsSize(); failRealmIdx < count; ++failRealmIdx) {
                const auto& realm = group.GetRings(failRealmIdx);
                if (!numFailDomainsInFailRealm) {
                    numFailDomainsInFailRealm.emplace(realm.FailDomainsSize());
                } else if (*numFailDomainsInFailRealm != realm.FailDomainsSize()) {
                    return "different number of FailDomains within different fail realms";
                }
                if (failRealmIdx > Max<ui8>()) {
                    return "too many fail realms";
                }
                for (ui32 failDomainIdx = 0; failDomainIdx < *numFailDomainsInFailRealm; ++failDomainIdx) {
                    const auto& domain = realm.GetFailDomains(failDomainIdx);
                    if (domain.VDiskLocationsSize() != 1) {
                        return "VDiskLocations within fail domain must have exact size of 1";
                    }
                    if (failDomainIdx > Max<ui8>()) {
                        return "too many fail domains";
                    }
                    for (ui32 vdiskIdx = 0, count = domain.VDiskLocationsSize(); vdiskIdx < count; ++vdiskIdx) {
                        const auto& l = domain.GetVDiskLocations(vdiskIdx);
                        if (!l.HasNodeID() || !l.HasPDiskID() || !l.HasVDiskSlotID() || !l.HasPDiskGuid()) {
                            return "VDiskLocations item incomplete";
                        }

                        const ui32 nodeId = l.GetNodeID();
                        const ui32 pdiskId = l.GetPDiskID();
                        const ui32 vslotId = l.GetVDiskSlotID();
                        const ui64 pdiskGuid = l.GetPDiskGuid();

                        const TVDiskID vdiskId(TGroupId::FromValue(groupId), groupGen, failRealmIdx, failDomainIdx, vdiskIdx);

                        if (const auto it = vdisks.find(vdiskId); it == vdisks.end()) {
                            return TStringBuilder() << "vslot with specific VDiskID is not found"
                                << " GroupId# " << groupId
                                << " VDiskId# " << vdiskId
                                << " NodeId# " << nodeId
                                << " PDiskId# " << pdiskId
                                << " VSlotId# " << vslotId
                                << " PDiskGuid# " << pdiskGuid;
                        } else if (it->second != std::make_tuple(nodeId, pdiskId, vslotId, pdiskGuid, false)) {
                            return "VDiskLocation mismatch";
                        } else {
                            vdisks.erase(it);
                        }
                    }
                }
            }

            if (e == TBlobStorageGroupType::ErasureMirror3dc) {
                if (group.RingsSize() < 3 || *numFailDomainsInFailRealm < 3) {
                    return "mirror-3-dc group is too small";
                }
            } else {
                if (group.RingsSize() != 1) {
                    return "non-mirror-3-dc group must have exactly one ring";
                } else if (*numFailDomainsInFailRealm < gtype.BlobSubgroupSize()) {
                    return "group size is too small";
                }
            }
        }

        if (!vdisks.empty()) {
            return "unused vslots remaining";
        }

        return {};
    }

    std::optional<TString> ValidateConfig(const NKikimrConfig::TBlobStorageConfig& config, const THashSet<ui32>& nodeIds) {
        if (config.HasServiceSet()) {
            if (auto error = ValidateConfig(config.GetServiceSet(), nodeIds)) {
                return error;
            }
        }

        return {};
    }

    std::optional<TString> ValidateConfig(const NKikimrBlobStorage::TStorageConfig& config) {
        // validate node list
        THashSet<ui32> nodeIds;
        THashSet<std::tuple<TString, ui16>> fqdns;
        for (const auto& node : config.GetAllNodes()) {
            const ui32 port = node.GetPort();
            if (port > Max<ui16>()) {
                return "invalid Port provided";
            } else if (!node.GetNodeId() || node.GetNodeId() >= (1 << 20)) {
                return "invalid NodeId provided";
            } else if (const auto [_, inserted] = nodeIds.insert(node.GetNodeId()); !inserted) {
                return TStringBuilder() << "duplicate NodeId: " << node.GetNodeId();
            } else if (const auto [_, inserted] = fqdns.emplace(node.GetHost(), node.GetPort()); !inserted) {
                return TStringBuilder() << "duplicate FQDN: " << node.GetHost() << ':' << node.GetPort();
            }
        }

        if (config.HasBlobStorageConfig()) {
            if (auto error = ValidateConfig(config.GetBlobStorageConfig(), nodeIds)) {
                return error;
            }
        }

        return {};
    }

} // NKikimr::NStorage
