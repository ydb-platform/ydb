#pragma once

#include "defs.h"

#include "impl.h"
#include "config.h"
#include "group_mapper.h"

namespace NKikimr::NBsController {

    struct TExFitGroupError : yexception {};

    class TGroupGeometryInfo {
        TBlobStorageGroupType Type;
        ui32 NumFailRealms = 0;
        ui32 NumFailDomainsPerFailRealm = 0;
        ui32 NumVDisksPerFailDomain = 0;
        ui32 RealmLevelBegin = 0;
        ui32 RealmLevelEnd = 0;
        ui32 DomainLevelBegin = 0;
        ui32 DomainLevelEnd = 0;

    public:
        explicit TGroupGeometryInfo() = default;

        TGroupGeometryInfo(TBlobStorageGroupType type, NKikimrBlobStorage::TGroupGeometry g)
            : Type(type)
            , NumFailRealms(g.GetNumFailRealms())
            , NumFailDomainsPerFailRealm(g.GetNumFailDomainsPerFailRealm())
            , NumVDisksPerFailDomain(g.GetNumVDisksPerFailDomain())
            , RealmLevelBegin(g.GetRealmLevelBegin())
            , RealmLevelEnd(g.GetRealmLevelEnd())
            , DomainLevelBegin(g.GetDomainLevelBegin())
            , DomainLevelEnd(g.GetDomainLevelEnd())
        {
            // calculate default minimum values
            const bool isMirror3dc = Type.GetErasure() == TBlobStorageGroupType::ErasureMirror3dc;
            const ui32 minNumFailRealms = isMirror3dc ? 3 : 1;
            const ui32 minNumFailDomainsPerFailRealm = isMirror3dc ? 3 : Type.BlobSubgroupSize();
            const ui32 minNumVDisksPerFailDomain = 1;

            if (!NumFailRealms && !NumFailDomainsPerFailRealm && !NumVDisksPerFailDomain) {
                // no values are set, this means we're going to use the default ones
                NumFailRealms = minNumFailRealms;
                NumFailDomainsPerFailRealm = minNumFailDomainsPerFailRealm;
                NumVDisksPerFailDomain = minNumVDisksPerFailDomain;
            } else if (NumFailRealms < minNumFailRealms ||
                    NumFailDomainsPerFailRealm < minNumFailDomainsPerFailRealm ||
                    NumVDisksPerFailDomain < minNumVDisksPerFailDomain) {
                throw TExFitGroupError() << "not enough fail domains, fail realms, or vdisks for specified erasure";
            }

            if (RealmLevelBegin || RealmLevelEnd || DomainLevelBegin || DomainLevelEnd) {
                TStringStream err;
                bool needComma = false;
                if (RealmLevelEnd < RealmLevelBegin) {
                    needComma = true;
                    err << "RealmLevelBegin = " << RealmLevelBegin << " must be less than or equal to RealmLevelEnd = " << RealmLevelEnd;
                }
                if (DomainLevelEnd < DomainLevelBegin) {
                    if (needComma) {
                        err << ", ";
                    }
                    err << "DomainLevelBegin = " << DomainLevelBegin
                        << " must be less than or equal to DomainLevelEnd = " << DomainLevelEnd;
                }
                if (!err.empty()) {
                    throw TExFitGroupError() << err.Str();
                }
            } else {
                RealmLevelBegin = 10;
                RealmLevelEnd = 20;
                DomainLevelBegin = 10;
                DomainLevelEnd = 40;
            }
        }

        ui32 GetNumFailRealms() const { return NumFailRealms; }
        ui32 GetNumFailDomainsPerFailRealm() const { return NumFailDomainsPerFailRealm; }
        ui32 GetNumVDisksPerFailDomain() const { return NumVDisksPerFailDomain; }
        ui32 GetRealmLevelBegin() const { return RealmLevelBegin; }
        ui32 GetRealmLevelEnd() const { return RealmLevelEnd; }
        ui32 GetDomainLevelBegin() const { return DomainLevelBegin; }
        ui32 GetDomainLevelEnd() const { return DomainLevelEnd; }

        void AllocateGroup(TGroupMapper &mapper, TGroupId groupId, TGroupMapper::TGroupDefinition &group,
                TGroupMapper::TGroupConstraintsDefinition& constrainsts,
                const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks, TGroupMapper::TForbiddenPDisks forbid,
                ui32 groupSizeInUnits, i64 requiredSpace, TBridgePileId bridgePileId) const {
            TString error;
            for (const bool requireOperational : {true, false}) {
                if (mapper.AllocateGroup(groupId.GetRawId(), group, constrainsts, replacedDisks, forbid, groupSizeInUnits, requiredSpace,
                        requireOperational, bridgePileId, error)) {
                    return;
                }
            }
            throw TExFitGroupError() << "failed to allocate group: " << error;
        }

        // returns pair of previous VDisk and PDisk id's
        std::pair<TVDiskIdShort, TPDiskId> SanitizeGroup(TGroupMapper &mapper, TGroupId groupId,
                TGroupMapper::TGroupDefinition &group, TGroupMapper::TGroupConstraintsDefinition&,
                const THashMap<TVDiskIdShort, TPDiskId>& /*replacedDisks*/, TGroupMapper::TForbiddenPDisks forbid,
                ui32 groupSizeInUnits, i64 requiredSpace, TBridgePileId bridgePileId) const {
            TString error;
            auto misplacedVDisks = mapper.FindMisplacedVDisks(group, groupSizeInUnits);
            if (misplacedVDisks.Disks.size() == 0) {
                error = TStringBuilder() << "cannot find misplaced disks, fail level: " << (ui32)misplacedVDisks.FailLevel;
            } else {
                for (const bool requireOperational : {true, false}) {
                    for (const auto& replacedDisk : misplacedVDisks.Disks) {
                        TPDiskId pdiskId = group[replacedDisk.FailRealm][replacedDisk.FailDomain][replacedDisk.VDisk];
                        if (mapper.TargetMisplacedVDisk(groupId, group, replacedDisk, forbid, groupSizeInUnits, requiredSpace,
                                requireOperational, bridgePileId, error)) {
                            return {replacedDisk, pdiskId};
                        }
                    }
                }
            }
            throw TExFitGroupError() << "failed to sanitize group: " << error;
        }

        template<class T>
        bool ResizeGroup(TGroupMapper::TGroupDefinitionBase<T>& group) const {
            if (!group) {
                group.resize(NumFailRealms);
                for (auto &realm : group) {
                    realm.resize(NumFailDomainsPerFailRealm);
                    for (auto &domain : realm) {
                        domain.resize(NumVDisksPerFailDomain);
                    }
                }
            } else {
                bool ok = group.size() == NumFailRealms;
                if (ok) {
                    for (const auto& realm : group) {
                        ok = realm.size() == NumFailDomainsPerFailRealm;
                        if (ok) {
                            for (const auto& domain : realm) {
                                ok = domain.size() == NumVDisksPerFailDomain;
                                if (!ok) {
                                    break;
                                }
                            }
                        }
                        if (!ok) {
                            break;
                        }
                    }
                }
                if (!ok) {
                    return false;
                }
            }
            return true;
        }

        bool CheckGroupSize(const TGroupMapper::TGroupDefinition& group) const {
            if (!group) {
                return false;
            }

            if (group.size() != NumFailRealms)  {
                return false;
            }
            for (const auto& realm : group) {
                if (realm.size() != NumFailDomainsPerFailRealm) {
                    return false;
                }
                for (const auto& domain : realm) {
                    if (domain.size() != NumVDisksPerFailDomain) {
                        return false;
                    }
                }
            }

            return true;
        }

        TBlobStorageGroupType GetType() const {
            return Type;
        }

        TBlobStorageGroupType::EErasureSpecies GetErasure() const {
            return Type.GetErasure();
        }
    };

} // NKikimr::NBsController
