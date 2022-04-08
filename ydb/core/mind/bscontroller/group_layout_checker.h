#pragma once

#include "defs.h"
#include "types.h"
#include "group_geometry_info.h"

namespace NKikimr::NBsController {

    struct TLayoutCheckResult {
        std::vector<TVDiskIdShort> Candidates;

        explicit operator bool() const { // checks whether fail model is correct
            return Candidates.empty();
        }
    };

    TLayoutCheckResult CheckGroupLayout(const TGroupGeometryInfo& geom, const THashMap<TVDiskIdShort, std::pair<TNodeLocation, TPDiskId>>& layout);

    namespace NLayoutChecker {

        class TDomainMapper {
            std::unordered_map<TString, ui32> FailDomainId;

        public:
            ui32 operator ()(TString item) {
                return FailDomainId.emplace(std::move(item), FailDomainId.size()).first->second;
            }

            ui32 GetIdCount() const {
                return FailDomainId.size();
            }
        };

        struct TPDiskLayoutPosition {
            ui32 RealmGroup = 0;
            ui32 Realm = 0;
            ui32 Domain = 0;

            TPDiskLayoutPosition() = default;

            TPDiskLayoutPosition(ui32 realmGroup, ui32 realm, ui32 domain)
                : RealmGroup(realmGroup)
                , Realm(realm)
                , Domain(domain)
            {}

            TPDiskLayoutPosition(TDomainMapper& mapper, const TNodeLocation& location, TPDiskId pdiskId, const TGroupGeometryInfo& geom) {
                TStringStream realmGroup, realm, domain;
                const std::pair<int, TStringStream*> levels[] = {
                    {geom.GetRealmLevelBegin(), &realmGroup},
                    {Max(geom.GetRealmLevelEnd(), geom.GetDomainLevelBegin()), &realm},
                    {Max(geom.GetRealmLevelEnd(), geom.GetDomainLevelEnd()), &domain}
                };
                auto addLevel = [&](int key, const TString& value) {
                    for (const auto& [reference, stream] : levels) {
                        if (key < reference) {
                            Save(stream, std::make_tuple(key, value));
                        }
                    }
                };
                for (const auto& [key, value] : location.GetItems()) {
                    addLevel(key, value);
                }
                addLevel(255, pdiskId.ToString()); // ephemeral level to distinguish between PDisks on the same node
                RealmGroup = mapper(realmGroup.Str());
                Realm = mapper(realm.Str());
                Domain = mapper(domain.Str());
            }

            TString ToString() const {
                return TStringBuilder() << "{" << RealmGroup << "." << Realm << "." << Domain << "}";
            }

            auto AsTuple() const {
                return std::tie(RealmGroup, Realm, Domain);
            }

            friend bool operator ==(const TPDiskLayoutPosition& x, const TPDiskLayoutPosition& y) {
                return x.AsTuple() == y.AsTuple();
            }

            friend bool operator <(const TPDiskLayoutPosition& x, const TPDiskLayoutPosition& y) {
                return x.AsTuple() < y.AsTuple();
            }
        };

        struct TScore {
            ui32 RealmInterlace = 0;
            ui32 DomainInterlace = 0;
            ui32 RealmGroupScatter = 0;
            ui32 RealmScatter = 0;
            ui32 DomainScatter = 0;

            auto AsTuple() const {
                return std::make_tuple(RealmInterlace, DomainInterlace, RealmGroupScatter, RealmScatter, DomainScatter);
            }

            bool BetterThan(const TScore& other) const {
                return AsTuple() < other.AsTuple();
            }

            bool SameAs(const TScore& other) const {
                return AsTuple() == other.AsTuple();
            }

            static TScore Max() {
                return {::Max<ui32>(), ::Max<ui32>(), ::Max<ui32>(), ::Max<ui32>(), ::Max<ui32>()};
            }

            TString ToString() const {
                return TStringBuilder() << "{RealmInterlace# " << RealmInterlace
                    << " DomainInterlace# " << DomainInterlace
                    << " RealmGroupScatter# " << RealmGroupScatter
                    << " RealmScatter# " << RealmScatter
                    << " DomainScatter# " << DomainScatter
                    << "}";
            }
        };

        struct TGroupLayout {
            const ui32 NumFailDomainsPerFailRealm;

            ui32 NumDisks = 0;
            THashMap<ui32, ui32> NumDisksPerRealmGroup;

            TStackVec<ui32, 4> NumDisksInRealm;
            TStackVec<THashMap<ui32, ui32>, 4> NumDisksPerRealm;
            THashMap<ui32, ui32> NumDisksPerRealmTotal;

            TStackVec<ui32, 32> NumDisksInDomain;
            TStackVec<THashMap<ui32, ui32>, 32> NumDisksPerDomain;
            THashMap<ui32, ui32> NumDisksPerDomainTotal;

            TGroupLayout(ui32 numFailRealms, ui32 numFailDomainsPerFailRealm)
                : NumFailDomainsPerFailRealm(numFailDomainsPerFailRealm)
                , NumDisksInRealm(numFailRealms)
                , NumDisksPerRealm(numFailRealms)
                , NumDisksInDomain(numFailRealms * numFailDomainsPerFailRealm)
                , NumDisksPerDomain(numFailRealms * numFailDomainsPerFailRealm)
            {}

            void UpdateDisk(const TPDiskLayoutPosition& pos, ui32 realmIdx, ui32 domainIdx, ui32 value) {
                domainIdx += realmIdx * NumFailDomainsPerFailRealm;
                NumDisks += value;
                NumDisksPerRealmGroup[pos.RealmGroup] += value;
                NumDisksInRealm[realmIdx] += value;
                NumDisksPerRealm[realmIdx][pos.Realm] += value;
                NumDisksPerRealmTotal[pos.Realm] += value;
                NumDisksInDomain[domainIdx] += value;
                NumDisksPerDomain[domainIdx][pos.Domain] += value;
                NumDisksPerDomainTotal[pos.Domain] += value;
            }

            void AddDisk(const TPDiskLayoutPosition& pos, ui32 realmIdx, ui32 domainIdx) {
                UpdateDisk(pos, realmIdx, domainIdx, 1);
            }

            void RemoveDisk(const TPDiskLayoutPosition& pos, ui32 realmIdx, ui32 domainIdx) {
                UpdateDisk(pos, realmIdx, domainIdx, Max<ui32>());
            }

            TScore GetCandidateScore(const TPDiskLayoutPosition& pos, ui32 realmIdx, ui32 domainIdx) {
                domainIdx += realmIdx * NumFailDomainsPerFailRealm;

                return {
                    .RealmInterlace = NumDisksPerRealmTotal[pos.Realm] - NumDisksPerRealm[realmIdx][pos.Realm],
                    .DomainInterlace = NumDisksPerDomainTotal[pos.Domain] - NumDisksPerDomain[domainIdx][pos.Domain],
                    .RealmGroupScatter = NumDisks - NumDisksPerRealmGroup[pos.RealmGroup],
                    .RealmScatter = NumDisksInRealm[realmIdx] - NumDisksPerRealm[realmIdx][pos.Realm],
                    .DomainScatter = NumDisksInDomain[domainIdx] - NumDisksPerDomain[domainIdx][pos.Domain],
                };
            }
        };

    } // NLayoutChecker

} // NKikimr::NBsController
