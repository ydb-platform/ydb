#include "layout_helpers.h"

namespace NKikimr {

namespace NBsController {

bool CheckLayoutByGroupDefinition(const TGroupMapper::TGroupDefinition& group,
        std::unordered_map<TPDiskId, NLayoutChecker::TPDiskLayoutPosition>& pdisks, 
        const TGroupGeometryInfo& geom, TString& error) {
    std::unordered_set<ui32> usedPRealms;
    std::set<TPDiskId> usedPDisks;
    for (ui32 failRealm = 0; failRealm < geom.GetNumFailRealms(); ++failRealm) {
        ui32 pRealm = pdisks[group[failRealm][0][0]].Realm.Index();
        if (usedPRealms.count(pRealm)) {
            error = TStringBuilder() << "PRealm intersection, PRealm index# " << pRealm;
            return false;
        }
        usedPRealms.insert(pRealm);
        std::unordered_set<ui32> usedPDomains;
        for (ui32 failDomain = 0; failDomain < geom.GetNumFailDomainsPerFailRealm(); ++failDomain) {
            ui32 pDomain = pdisks[group[failRealm][failDomain][0]].Domain.Index();
            if (usedPDomains.count(pDomain)) {
                error = TStringBuilder() << "PDomain intersection, PDomain index# " << pDomain;
                return false;
            }
            usedPDomains.insert(pDomain);
            for (ui32 vdisk = 0; vdisk < geom.GetNumVDisksPerFailDomain(); ++vdisk) {
                const auto& pdiskId = group[failRealm][failDomain][vdisk];
                if (pdiskId == TPDiskId()) {
                    error = TStringBuilder() << "empty slot, VSlotId# {FailRealm# " << failRealm <<
                            ", FailDomain# " << failDomain << ", VDisk# " << vdisk << "}";
                    return false;
                }

                Y_VERIFY(pdisks.find(pdiskId) != pdisks.end());
                if (pdisks[pdiskId].RealmGroup != pdisks[group[0][0][0]].RealmGroup || 
                    pdisks[pdiskId].Realm != pdisks[group[failRealm][0][0]].Realm || 
                    pdisks[pdiskId].Domain != pdisks[group[failRealm][failDomain][0]].Domain) {
                    error = TStringBuilder() << "bad pdisk placement, VSlotId# {FailRealm# " << failRealm <<
                            ", FailDomain# " << failDomain << ", VDisk# " << vdisk << "}, PDiskId# " << pdiskId.ToString();
                    return false;
                } 
                if (usedPDisks.find(pdiskId) != usedPDisks.end()) {
                    error = TStringBuilder() << "PDisk intersection, #id " << pdiskId.ToString();
                    return false;
                }
                usedPDisks.insert(pdiskId);
            }
        }
    }

    return true;
}

bool CheckBaseConfigLayout(const TGroupGeometryInfo& geom, const NKikimrBlobStorage::TBaseConfig& cfg, TString& error) {
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
        Y_VERIFY(pdiskId != TPDiskId());
        Y_VERIFY(pdisks.find(pdiskId) != pdisks.end());
        ui32 groupId = vslot.GetGroupId();
        geom.ResizeGroup(groups[groupId]);
        groups[groupId][vslot.GetFailRealmIdx()][vslot.GetFailDomainIdx()][vslot.GetVDiskIdx()] = pdiskId;
    }

    for (auto& [groupId, group] : groups) {
        TString layoutError;
        if (!CheckLayoutByGroupDefinition(group, pdisks, geom, layoutError)) {
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
