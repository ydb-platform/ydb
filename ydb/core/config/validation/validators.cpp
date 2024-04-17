#include "validators.h"

#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/blobstorage_disk.pb.h>

#include <map>
#include <set>

namespace NKikimr::NConfig {

#define CHECK_ERR(x, err) if (!(x)) { \
    msg = std::vector<TString>{err};      \
    return EValidationResult::Error;  \
} Y_SEMICOLON_GUARD

#define CHECK_WARN(x, warn) if (!(x)) { \
    msg.push_back(warn); \
} Y_SEMICOLON_GUARD

bool IsSame(const NKikimrBlobStorage::TVDiskLocation& lhs, const NKikimrBlobStorage::TVDiskLocation& rhs) {
    return
        lhs.GetNodeID() == rhs.GetNodeID() &&
        lhs.GetPDiskID() == rhs.GetPDiskID() &&
        lhs.GetVDiskSlotID() == rhs.GetVDiskSlotID() &&
        lhs.GetPDiskGuid() == rhs.GetPDiskGuid();
}

bool IsSame(const NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk& lhs, const NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk& rhs) {
    return
        lhs.GetPath() == rhs.GetPath() &&
        lhs.GetPDiskGuid() == rhs.GetPDiskGuid() &&
        lhs.GetPDiskCategory() == rhs.GetPDiskCategory() &&
        lhs.GetNodeID() == rhs.GetNodeID() &&
        lhs.GetPDiskID() == rhs.GetPDiskID();
}

bool IsSame(const NKikimrBlobStorage::TVDiskID& lhs, const NKikimrBlobStorage::TVDiskID& rhs) {
    return
        lhs.GetGroupID() == rhs.GetGroupID() &&
        lhs.GetGroupGeneration() == rhs.GetGroupGeneration() &&
        lhs.GetRing() == rhs.GetRing() &&
        lhs.GetDomain() == rhs.GetDomain() &&
        lhs.GetVDisk() == rhs.GetVDisk();
}

bool IsSame(const NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk& lhs, const NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk& rhs) {
    return
        IsSame(lhs.GetVDiskID(), rhs.GetVDiskID()) && IsSame(lhs.GetVDiskLocation(), rhs.GetVDiskLocation());
}

EValidationResult ValidateStaticGroup(const NKikimrConfig::TAppConfig& current, const NKikimrConfig::TAppConfig& proposed, std::vector<TString>& msg) {
    const auto& currentBsConfig = current.GetBlobStorageConfig();
    const auto& proposedBsConfig = proposed.GetBlobStorageConfig();

    const auto& currentServiceSet = currentBsConfig.GetServiceSet();
    const auto& proposedServiceSet = proposedBsConfig.GetServiceSet();

    size_t locMismatchCount = 0;

    std::map<TPDiskKey, NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk> curPDisks;
    for (const auto& pdisk : currentServiceSet.GetPDisks()) {
        auto [it, assigned] = curPDisks.insert_or_assign(TPDiskKey{pdisk.GetNodeID(), pdisk.GetPDiskID()}, pdisk);
        CHECK_ERR(assigned, "Duplicate pdisk in current config");
    }

    std::map<TPDiskKey, NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk> proposedPDisks;
    for (const auto& pdisk : proposedServiceSet.GetPDisks()) {
        auto [it, assigned] = proposedPDisks.insert_or_assign(TPDiskKey{pdisk.GetNodeID(), pdisk.GetPDiskID()}, pdisk);
        CHECK_ERR(assigned, "Duplicate pdisk in proposed config");
    }

    std::map<TVDiskKey, NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk> curVDisks;
    for (const auto& VDisk : currentServiceSet.GetVDisks()) {
        const auto& loc = VDisk.GetVDiskLocation();
        auto [it, assigned] = curVDisks.insert_or_assign(TVDiskKey{loc.GetNodeID(), loc.GetPDiskID(), loc.GetVDiskSlotID()}, VDisk);
        CHECK_ERR(assigned, "Duplicate VDisk in current config");
    }

    std::map<TVDiskKey, NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk> proposedVDisks;
    for (const auto& VDisk : proposedServiceSet.GetVDisks()) {
        const auto& loc = VDisk.GetVDiskLocation();
        auto [it, assigned] = proposedVDisks.insert_or_assign(TVDiskKey{loc.GetNodeID(), loc.GetPDiskID(), loc.GetVDiskSlotID()}, VDisk);
        CHECK_ERR(assigned, "Duplicate VDisk in proposed config");
    }

    std::set<TPDiskKey> proposedSGPDisks;
    std::set<TVDiskKey> proposedSGVDisks;

    // currently we support adding or removing only static groups at the end
    // more complex cases are ignored intentionally
    // as far as we don't use even these cases now
    // for other complex cases this validation should be improved or ignored.
    CHECK_WARN(currentServiceSet.GroupsSize() == proposedServiceSet.GroupsSize(), "Group either added or removed");
    for (size_t i = 0; i < currentServiceSet.GroupsSize() && i < proposedServiceSet.GroupsSize(); ++i) {
        const auto& curGroup = currentServiceSet.GetGroups(i);
        const auto& proposedGroup = proposedServiceSet.GetGroups(i);

        CHECK_ERR(curGroup.RingsSize() == proposedGroup.RingsSize(), "Ring sizes must be the same");
        for (size_t j = 0; j < curGroup.RingsSize(); ++j) {
            const auto& curRing = curGroup.GetRings(j);
            const auto& proposedRing = proposedGroup.GetRings(j);

            CHECK_ERR(curRing.FailDomainsSize() == proposedRing.FailDomainsSize(), "FailDomain sizes must be the same");
            for (size_t k = 0; k < curRing.FailDomainsSize(); ++k) {
                const auto& curFailDomain = curRing.GetFailDomains(k);
                const auto& proposedFailDomain = proposedRing.GetFailDomains(k);

                CHECK_ERR(curFailDomain.VDiskLocationsSize() == proposedFailDomain.VDiskLocationsSize(), "VDiskLocation sizes must be the same");
                for (size_t l = 0; l < curFailDomain.VDiskLocationsSize(); ++l) {
                    const auto& curLoc = curFailDomain.GetVDiskLocations(l);
                    const auto& proposedLoc = proposedFailDomain.GetVDiskLocations(l);

                    auto pdiskKey = TPDiskKey{proposedLoc.GetNodeID(), proposedLoc.GetPDiskID()};
                    proposedSGPDisks.insert(pdiskKey);
                    auto proposedPDiskIt = proposedPDisks.find(pdiskKey);
                    CHECK_ERR(proposedPDiskIt != proposedPDisks.end(), "PDisk mentioned in FailDomain not present in PDisks section");
                    CHECK_ERR(proposedPDiskIt->second.GetPDiskGuid() == proposedLoc.GetPDiskGuid(), "PDiskGuid mismatch");

                    auto vdiskKey = TVDiskKey{proposedLoc.GetNodeID(), proposedLoc.GetPDiskID(), proposedLoc.GetVDiskSlotID()};
                    proposedSGVDisks.insert(vdiskKey);
                    auto proposedVDiskIt = proposedVDisks.find(vdiskKey);
                    CHECK_ERR(proposedVDiskIt != proposedVDisks.end(), "VDisk mentioned in FailDomain not present in VDisks section");

                    if (!IsSame(curLoc, proposedLoc)) {
                        ++locMismatchCount;
                    }
                }
            }
        }
    }

    CHECK_ERR(locMismatchCount < 2, "Too many VDiskLocation changes");
    CHECK_WARN(locMismatchCount < 1, "VDiskLocation changed");

    for (const auto& key : proposedSGPDisks) {
        auto curPDiskIt = curPDisks.find(key);
        auto proposedPDiskIt = proposedPDisks.find(key);
        CHECK_ERR(proposedPDiskIt != proposedPDisks.end(), "PDisk mentioned in FailDomain not present in PDisks section");
        if (curPDiskIt != curPDisks.end()) {
            // currently can be a little bit overkill, and return false positive
            // but in such cases it is better to validate trice, to check we won't corrupt anything
            CHECK_ERR(IsSame(curPDiskIt->second, proposedPDiskIt->second), "PDisk changed");
        }
    }

    for (const auto& key : proposedSGVDisks) {
        auto curVDiskIt = curVDisks.find(key);
        auto proposedVDiskIt = proposedVDisks.find(key);
        CHECK_ERR(proposedVDiskIt != proposedVDisks.end(), "VDisk mentioned in FailDomain not present in VDisks section");
        if (curVDiskIt != curVDisks.end()) {
            // currently can be a little bit overkill, and return false positive
            // but in such cases it is better to validate trice, to check we won't corrupt anything
            CHECK_ERR(IsSame(curVDiskIt->second, proposedVDiskIt->second), "VDisk changed");
        }
    }

    if (msg.size() > 0) {
        return EValidationResult::Warn;
    }

    return EValidationResult::Ok;
}

} // namespace NKikimr::NConfig
