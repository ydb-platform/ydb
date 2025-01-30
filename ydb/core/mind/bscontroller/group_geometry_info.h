#pragma once

#include "defs.h"

#include "impl.h"
#include "config.h"
#include "group_mapper.h"

namespace NKikimr::NBsController {

    struct TExFitGroupError : yexception {};

    class TGroupGeometryInfo {
        const TBlobStorageGroupType Type;
        ui32 NumFailRealms;
        ui32 NumFailDomainsPerFailRealm;
        ui32 NumVDisksPerFailDomain;
        ui32 RealmLevelBegin;
        ui32 RealmLevelEnd;
        ui32 DomainLevelBegin;
        ui32 DomainLevelEnd;

    public:
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

        void AllocateGroup(TGroupMapper &mapper, TGroupId groupId, TGroupMapper::TGroupDefinition &group, TGroupMapper::TGroupConstraintsDefinition& constrainsts,
                const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks, TGroupMapper::TForbiddenPDisks forbid,
                i64 requiredSpace) const {
            TString error;
            for (const bool requireOperational : {true, false}) {
                if (mapper.AllocateGroup(groupId.GetRawId(), group, constrainsts, replacedDisks, forbid, requiredSpace, requireOperational, error)) {
                    return;
                }
            }
            throw TExFitGroupError() << "failed to allocate group: " << error;
        }

        // returns pair of previous VDisk and PDisk id's
        std::pair<TVDiskIdShort, TPDiskId> SanitizeGroup(TGroupMapper &mapper, TGroupId groupId, TGroupMapper::TGroupDefinition &group, TGroupMapper::TGroupConstraintsDefinition&,
                const THashMap<TVDiskIdShort, TPDiskId>& /*replacedDisks*/, TGroupMapper::TForbiddenPDisks forbid, i64 requiredSpace) const {
            TString error;
            auto misplacedVDisks = mapper.FindMisplacedVDisks(group);
            if (misplacedVDisks.Disks.size() == 0) {
                error = TStringBuilder() << "cannot find misplaced disks, fail level: " << (ui32)misplacedVDisks.FailLevel;
            } else {
                for (const bool requireOperational : {true, false}) {
                    for (const auto& replacedDisk : misplacedVDisks.Disks) {
                        TPDiskId pdiskId = group[replacedDisk.FailRealm][replacedDisk.FailDomain][replacedDisk.VDisk];
                        if (mapper.TargetMisplacedVDisk(groupId, group, replacedDisk, forbid, requiredSpace,
                                requireOperational, error)) {
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
