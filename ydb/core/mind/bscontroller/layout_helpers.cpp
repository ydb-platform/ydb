#include "layout_helpers.h"

namespace NKikimr {

namespace NBsController {

bool CheckLayoutByGroupDefinition(const TGroupMapper::TGroupDefinition& group,
        std::unordered_map<TPDiskId, NLayoutChecker::TPDiskLayoutPosition>& pdisks, const TGroupGeometryInfo& geom,
        bool allowMultipleRealmsOccupation, TString& error) {
    std::unordered_map<ui32, std::vector<TVDiskIdShort>> pRealmOccupants;
    std::unordered_map<ui32, TVDiskIdShort> usedPDomains;
    std::unordered_map<TPDiskId, TVDiskIdShort> usedPDisks;

    if (group.size() != geom.GetNumFailRealms()) {
        error = "Wrong fail realms number";
        return false;
    }
    for (ui32 failRealm = 0; failRealm < geom.GetNumFailRealms(); ++failRealm) {
        if (group[failRealm].size() != geom.GetNumFailDomainsPerFailRealm()) {
            error = TStringBuilder() << "Wrong fail domains number in failRealm# " << failRealm;
            return false;
        }
        for (ui32 failDomain = 0; failDomain < geom.GetNumFailDomainsPerFailRealm(); ++failDomain) {
            if (group[failRealm][failDomain].size() != geom.GetNumVDisksPerFailDomain()) {
                error = TStringBuilder() << "Wrong vdisks number in failRealm# " << failRealm << ", failDomain# " << failDomain;
                return false;
            }
            for (ui32 vdisk = 0; vdisk < geom.GetNumVDisksPerFailDomain(); ++vdisk) {
                const TVDiskIdShort vdiskId(failRealm, failDomain, vdisk);
                const auto& pdiskId = group[failRealm][failDomain][vdisk];
                if (pdiskId == TPDiskId()) {
                    error = TStringBuilder() << "Empty slot, VDisk Id# " << vdiskId.ToString();
                    return false;
                }

                ui32 pRealm = pdisks[pdiskId].Realm.Index();
                auto& occupants = pRealmOccupants[pRealm];
                for (auto occupant : occupants) {
                    if (occupant.FailRealm != failRealm) {
                        error = TStringBuilder() << "Disks from different fail realms occupy the same Realm, "
                                "first VDisk Id# " << occupant.ToString() << " second VDisk Id# " << vdiskId.ToString();
                        return false;
                    }
                }
                occupants.push_back(vdiskId);

                ui32 pDomain = pdisks[pdiskId].Domain.Index();
                if (const auto it = usedPDomains.find(pDomain); it != usedPDomains.end()) {
                    error = TStringBuilder() << "Disks from different fail domains occupy the same Domain, "
                            "first VDisk Id# " << it->second.ToString() << " second VDisk Id# " << vdiskId.ToString();
                    return false;
                }
                usedPDomains[pDomain] = vdiskId;

                Y_ABORT_UNLESS(pdisks.find(pdiskId) != pdisks.end());
                
                if (const auto it = usedPDisks.find(pdiskId); it != usedPDisks.end()) {
                    error = TStringBuilder() << "Two VDisks occupy the same PDisk, PDiskId# " << pdiskId.ToString() << 
                            " first VDisk Id# " << it->second.ToString() << " second VDisk Id# " << vdiskId.ToString();
                    return false;
                }
                usedPDisks[pdiskId] = vdiskId;
            }
        }
    }

    if (!allowMultipleRealmsOccupation && pRealmOccupants.size() > geom.GetNumFailRealms()) {
        error = TStringBuilder() << "VDisks from one fail realm occupy different Realms";
    }

    return true;
}

bool CheckBaseConfigLayout(const TGroupGeometryInfo& geom, const NKikimrBlobStorage::TBaseConfig& cfg,
        bool allowMultipleRealmsOccupation, TString& error) {
    NLayoutChecker::TDomainMapper domainMapper;

    std::unordered_map<ui32, TNodeLocation> nodes;
    std::unordered_map<TPDiskId, NLayoutChecker::TPDiskLayoutPosition> pdisks;
    
    for (ui32 i = 0; i < cfg.NodeSize(); ++i) {
        auto node = cfg.GetNode(i);
        nodes[node.GetNodeId()] = TNodeLocation(node.GetLocation());
    }

    for (ui32 i = 0; i < cfg.PDiskSize(); ++i) {
        auto pdisk = cfg.GetPDisk(i);
        TNodeLocation loc = nodes[pdisk.GetNodeId()];
        TPDiskId pdiskId(pdisk.GetNodeId(), pdisk.GetPDiskId());
        pdisks[pdiskId] = NLayoutChecker::TPDiskLayoutPosition(domainMapper, loc, pdiskId, geom);
    }

    std::unordered_map<ui32, TGroupMapper::TGroupDefinition> groups;
    for (ui32 i = 0; i < cfg.VSlotSize(); ++i) {
        auto vslot = cfg.GetVSlot(i);
        TPDiskId pdiskId(vslot.GetVSlotId().GetNodeId(), vslot.GetVSlotId().GetPDiskId());
        Y_ABORT_UNLESS(pdiskId != TPDiskId());
        Y_ABORT_UNLESS(pdisks.find(pdiskId) != pdisks.end());
        ui32 groupId = vslot.GetGroupId();
        geom.ResizeGroup(groups[groupId]);
        groups[groupId][vslot.GetFailRealmIdx()][vslot.GetFailDomainIdx()][vslot.GetVDiskIdx()] = pdiskId;
    }

    for (auto& [groupId, group] : groups) {
        TString layoutError;
        if (!CheckLayoutByGroupDefinition(group, pdisks, geom, allowMultipleRealmsOccupation, layoutError)) {
            error = TStringBuilder() << "GroupId# " << groupId << " " << layoutError;
            return false;
        }
    }

    return true;
}

TGroupGeometryInfo CreateGroupGeometry(TBlobStorageGroupType type, ui32 numFailRealms, ui32 numFailDomains,
        ui32 numVDisks, ui32 realmBegin, ui32 realmEnd, ui32 domainBegin, ui32 domainEnd) {
    NKikimrBlobStorage::TGroupGeometry g;
    g.SetNumFailRealms(numFailRealms);
    g.SetNumFailDomainsPerFailRealm(numFailDomains);
    g.SetNumVDisksPerFailDomain(numVDisks);
    g.SetRealmLevelBegin(realmBegin);
    g.SetRealmLevelEnd(realmEnd);
    g.SetDomainLevelBegin(domainBegin);
    g.SetDomainLevelEnd(domainEnd);
    return TGroupGeometryInfo(type, g);
}

} // namespace NBsController

} // namespace NKikimr
