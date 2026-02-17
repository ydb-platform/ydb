#include "validators.h"

#include <ydb/core/base/statestorage.h>
#include <ydb/core/config/protos/marker.pb.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/blobstorage_base.pb.h>
#include <ydb/core/protos/blobstorage_disk.pb.h>
#include <ydb/core/util/pb.h>

#include <library/cpp/protobuf/json/util.h>

#include <util/generic/xrange.h>
#include <util/string/builder.h>

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


EValidationResult ValidateStateStorageConfig(const NKikimrConfig::TAppConfig& proposed, std::vector<TString>& msg) {
    if (proposed.HasSelfManagementConfig()) {
        const auto& sm = proposed.GetSelfManagementConfig();
        if (sm.GetEnabled()) {
            return EValidationResult::Ok;
        }
    }
    if (!proposed.HasDomainsConfig()) {
        return EValidationResult::Ok;
    }
    const auto& domains = proposed.GetDomainsConfig();
    bool isExplicit = domains.HasExplicitStateStorageConfig() && domains.HasExplicitStateStorageBoardConfig() && domains.HasExplicitSchemeBoardConfig();
    if (!isExplicit) {
        if (domains.DomainSize() < 1) {
            msg.push_back(TStringBuilder() << "Domains is not defined in DomainsConfig");
            return EValidationResult::Error;
        }
        const auto& domain = domains.GetDomain(0);
        bool found = false;
        for (const auto& ss : domains.GetStateStorage()) {
            if (domain.SSIdSize() == 0 || (domain.SSIdSize() == 1 && ss.GetSSId() == domain.GetSSId(0))) {
                found = true;
                if (auto res = ValidateStateStorageConfig("StateStorage", {}, ss); !res.empty()) {
                    msg.push_back(res);
                    return EValidationResult::Error;
                }
            }
        }
        if (!found) {
            msg.push_back(TStringBuilder() << "State storage config is not defined in DomainsConfig section");
            return EValidationResult::Error;
        }
    }
#define VALIDATE_EXPLICIT(NAME) \
        if (domains.HasExplicit##NAME##Config()) { \
            if (auto res = ValidateStateStorageConfig(#NAME, {}, domains.GetExplicit##NAME##Config())) { \
                msg.push_back(res); \
                return EValidationResult::Error; \
            } \
        }
    VALIDATE_EXPLICIT(StateStorage)
    VALIDATE_EXPLICIT(StateStorageBoard)
    VALIDATE_EXPLICIT(SchemeBoard)

    return EValidationResult::Ok;
}

EValidationResult ValidateDatabaseConfig(const NKikimrConfig::TAppConfig& config, std::vector<TString>& msg) {
    const auto* desc = config.GetDescriptor();
    const auto* reflection = config.GetReflection();

    for (int i = 0; i < desc->field_count(); i++) {
        const auto* field = desc->field(i);

        if (field->options().GetExtension(NKikimrConfig::NMarkers::AllowInDatabaseConfig)) {
            continue;
        }

        if (!field->is_repeated()) {
            if (!reflection->HasField(config, field)) {
                continue;
            }
        } else {
            if (!reflection->FieldSize(config, field)) {
                continue;
            }
        }

        auto fieldName = field->name();
        NProtobufJson::ToSnakeCaseDense(&fieldName);
        msg.push_back(TStringBuilder() << "'" << fieldName << "' "
            << "is not allowed to be used in the database configuration");
        return EValidationResult::Error;
    }

    return EValidationResult::Ok;
}

EValidationResult ValidateConfig(const NKikimrConfig::TAppConfig& config, std::vector<TString>& msg) {
    if (config.HasAuthConfig()) {
        NKikimr::NConfig::EValidationResult result = NKikimr::NConfig::ValidateAuthConfig(config.GetAuthConfig(), msg);
        if (result == NKikimr::NConfig::EValidationResult::Error) {
            return EValidationResult::Error;
        }
    }
    if (config.HasColumnShardConfig()) {
        NKikimr::NConfig::EValidationResult result = NKikimr::NConfig::ValidateColumnShardConfig(config.GetColumnShardConfig(), msg);
        if (result == NKikimr::NConfig::EValidationResult::Error) {
            return EValidationResult::Error;
        }
    }
    if (config.HasMonitoringConfig()) {
        NKikimr::NConfig::EValidationResult result = NKikimr::NConfig::ValidateMonitoringConfig(config, msg);
        if (result == NKikimr::NConfig::EValidationResult::Error) {
            return EValidationResult::Error;
        }
    }
    NKikimr::NConfig::EValidationResult result = NKikimr::NConfig::ValidateStateStorageConfig(config, msg);
    if (result == NKikimr::NConfig::EValidationResult::Error) {
        return EValidationResult::Error;
    }
    if (msg.size() > 0) {
        return EValidationResult::Warn;
    }

    return EValidationResult::Ok;
}


TString ValidateStateStorageConfig(const char* name, const NKikimrConfig::TDomainsConfig::TStateStorage& oldSSConfig, const NKikimrConfig::TDomainsConfig::TStateStorage& newSSConfig) {
    if (!newSSConfig.HasRing() && newSSConfig.RingGroupsSize() == 0) {
        return TStringBuilder() << "New " << name << " configuration is not filled in";
    }
    if (!oldSSConfig.HasRing() && oldSSConfig.RingGroupsSize() == 0) {
        auto info = BuildStateStorageInfo(newSSConfig);
        if (info->RingGroups[0].WriteOnly) {
            return TStringBuilder() << "New " << name << " configuration first ring group is WriteOnly";
        }
        for (auto &rg : info->RingGroups) {
            if (rg.NToSelect < 1 || rg.NToSelect > rg.Rings.size()) {
                return TStringBuilder() << "New " << name << " configuration NToSelect has invalid value";
            }

            ui32 disabledCnt = 0;
            for (auto &ring : rg.Rings) {
                if (ring.IsDisabled) {
                    disabledCnt++;
                }
            }
            if (disabledCnt > rg.NToSelect / 2 - 1) { // -1 ring for rolling restart
                return TStringBuilder() << "New " << name << " configuration disabled too many rings";
            }
        }
        return "";
    }

    if ((oldSSConfig.HasRing() || oldSSConfig.RingGroupsSize() == 1) && (newSSConfig.HasRing() || newSSConfig.RingGroupsSize() == 1)) {
        auto toInfo = BuildStateStorageInfo(newSSConfig);
        auto fromInfo = BuildStateStorageInfo(oldSSConfig);
        auto &fromInfoGroup = fromInfo->RingGroups[0];
        auto &toInfoGroup = toInfo->RingGroups[0];
        if (fromInfoGroup.Rings.size() != toInfoGroup.Rings.size()
            || fromInfoGroup.NToSelect != toInfoGroup.NToSelect
            || toInfoGroup.WriteOnly) {
             return TStringBuilder() << name << " NToSelect/rings differs or writeOnly"
                << " from# " << SingleLineProto(oldSSConfig)
                << " to# " << SingleLineProto(newSSConfig);
        }
        ui32 disabledCnt = 0;
        for (ui32 i : xrange(fromInfoGroup.Rings.size())) {
            auto &fromRing = fromInfoGroup.Rings[i];
            auto &toRing = toInfoGroup.Rings[i];
            if (fromRing.IsDisabled || toRing.IsDisabled) {
                disabledCnt++;
            } else if (fromRing != toRing) {
                return TStringBuilder() << name << " ring #" << i << "differs"
                    << " from# " << SingleLineProto(oldSSConfig)
                    << " to# " << SingleLineProto(newSSConfig);
            }
        }
        if (disabledCnt > fromInfoGroup.NToSelect / 2) {
            return TStringBuilder() << name << " configuration disabled too many rings"
                << " from# " << SingleLineProto(oldSSConfig)
                << " to# " << SingleLineProto(newSSConfig);
        }
        return "";
    }
    if (newSSConfig.RingGroupsSize() < 1) {
        return TStringBuilder() << "New " << name << " configuration RingGroups is not filled in";
    }
    if (newSSConfig.GetRingGroups(0).GetWriteOnly()) {
        return TStringBuilder() << "New " << name << " configuration first RingGroup is writeOnly";
    }
    for (auto& rg : newSSConfig.GetRingGroups()) {
        if (rg.RingSize() && rg.NodeSize()) {
            return TStringBuilder() << name << " Ring and Node are defined, use the one of them";
        }
        const size_t numItems = Max(rg.RingSize(), rg.NodeSize());
        if (!rg.HasNToSelect() || numItems < 1 || rg.GetNToSelect() < 1 || rg.GetNToSelect() > numItems) {
            return TStringBuilder() << name << " invalid ring group selection";
        }
        for (auto &ring : rg.GetRing()) {
            if (ring.RingSize() > 0) {
                return TStringBuilder() << name << " too deep nested ring declaration";
            }
            if (ring.HasRingGroupActorIdOffset()) {
                return TStringBuilder() << name << " RingGroupActorIdOffset should be used in ring group level, not ring";
            }
            if (ring.NodeSize() < 1) {
                return TStringBuilder() << name << " empty ring";
            }
        }
    }
    try {
        TIntrusivePtr<TStateStorageInfo> newSSInfo;
        TIntrusivePtr<TStateStorageInfo> oldSSInfo;
        newSSInfo = BuildStateStorageInfo(newSSConfig);
        oldSSInfo = BuildStateStorageInfo(oldSSConfig);
        THashSet<TActorId> replicas;
        for (auto& ringGroup : newSSInfo->RingGroups) {
            for (auto& ring : ringGroup.Rings) {
                for (auto& node : ring.Replicas) {
                    if (!replicas.insert(node).second) {
                        return TStringBuilder() << name << " replicas ActorId intersection, specify"
                            " RingGroupActorIdOffset if you run multiple replicas on one node";
                    }
                }
            }
        }

        Y_ABORT_UNLESS(newSSInfo->RingGroups.size() > 0 && oldSSInfo->RingGroups.size() > 0);

        for (auto& newGroup : newSSInfo->RingGroups) {
            if (newGroup.WriteOnly) {
                continue;
            }
            bool found = false;
            for (auto& rg : oldSSInfo->RingGroups) {
                if (newGroup.SameConfiguration(rg)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return TStringBuilder() << "New introduced ring group should be WriteOnly old: " << oldSSInfo->ToString()
                    << " new: " << newSSInfo->ToString();
            }
        }
        for (auto& oldGroup : oldSSInfo->RingGroups) {
            if (oldGroup.WriteOnly) {
                continue;
            }
            bool found = false;
            for (auto& rg : newSSInfo->RingGroups) {
                if (oldGroup.SameConfiguration(rg)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return TStringBuilder() << "Can not delete not WriteOnly ring group. Make it WriteOnly before deletion old: "
                    << oldSSInfo->ToString() << " new: " << newSSInfo->ToString();
            }
        }
    } catch (const std::exception& e) {
        return TStringBuilder() << "Can not build " << name << " info from config. " << e.what();
    }
    return "";
}


} // namespace NKikimr::NConfig
