#include "defs.h"

#include <ydb/core/mind/bscontroller/group_layout_checker.h>
#include <ydb/core/mind/bscontroller/group_mapper.h>
#include <ydb/core/mind/bscontroller/group_geometry_info.h>


bool CheckGroupLayout(const TGroupGeometryInfo& geom, const NKikimrBlobStorage::TBaseConfig& cfg, TString& error) {
    static NLayoutChecker::TDomainMapper domainMapper;

    std::map<ui32, TNodeLocation> nodes;
    std::map<TPDiskId, NLayoutChecker::TPDiskLayoutPosition> pdisks;
    
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
    bool status = true;
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
        std::unordered_set<ui32> usedPRealms;
        std::set<TPDiskId> usedPDisks;
        TStringStream g;
        g << "GroupId# " << groupId << ", Group# ";
        for (ui32 failRealm = 0; failRealm < geom.GetNumFailRealms(); ++failRealm) {
            g << "[";
            ui32 pRealm = pdisks[group[failRealm][0][0]].Realm.Index();
            if (usedPRealms.count(pRealm)) {
                if (!status) {
                    error = TStringBuilder() << "PRealm intersection, PRealm index# " << pRealm;
                }
                status = false;
            }
            usedPRealms.insert(pRealm);
            std::unordered_set<ui32> usedPDomains;
            for (ui32 failDomain = 0; failDomain < geom.GetNumFailDomainsPerFailRealm(); ++failDomain) {
                g << "[";
                ui32 pDomain = pdisks[group[failRealm][failDomain][0]].Domain.Index();
                if (usedPDomains.count(pDomain)) {
                    error = TStringBuilder() << "PDomain intersection, PDomain index# " << pDomain;
                    status = false;
                }
                usedPDomains.insert(pDomain);
                for (ui32 vdisk = 0; vdisk < geom.GetNumVDisksPerFailDomain(); ++vdisk) {

                    TString vslot = TStringBuilder() << "VSlot# {FailRealm# " << failRealm <<
                                ", FailDomain# " << failDomain << ", vdisk# " << vdisk << "}";

                    const auto& pdiskId = group[failRealm][failDomain][vdisk];
                    if (pdiskId == TPDiskId()) {
                        error = vslot + ", empty slot";
                        status = false;
                    }
    
                    g << group[failRealm][failDomain][vdisk].ToString() << '=' << pdisks[pdiskId].RealmGroup << '/' << 
                            pdisks[pdiskId].Realm << '/' << pdisks[pdiskId].Domain;

                    Y_VERIFY(pdisks.find(pdiskId) != pdisks.end());
                    if (pdisks[pdiskId].RealmGroup != pdisks[group[0][0][0]].RealmGroup || 
                        pdisks[pdiskId].Realm != pdisks[group[failRealm][0][0]].Realm || 
                        pdisks[pdiskId].Domain != pdisks[group[failRealm][failDomain][0]].Domain) {
                        error = TStringBuilder() << vslot << ", bad pdisk placement, PDiskId# " << pdiskId;
                        status = false;
                    } 
                    if (usedPDisks.find(pdiskId) != usedPDisks.end()) {
                        error = TStringBuilder() << "PDisk intersection, #id " << pdiskId.ToString();
                        status = false;
                    }
                    usedPDisks.insert(pdiskId);
                }
                g << "]";
            }
            g << "]";
        }

        if (!status) {
            error += ", GroupLayout# " + g.Str();
            return false;
        }
    }

    return true;
}

TGroupGeometryInfo CreateGroupGeometry(TBlobStorageGroupType type, ui32 numFailRealms = 0, ui32 numFailDomains = 0,
        ui32 numVDisks = 0, ui32 realmBegin = 0, ui32 realmEnd = 0, ui32 domainBegin = 0, ui32 domainEnd = 0) {
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
