#pragma once

#include "defs.h"
#include "types.h"
#include "group_geometry_info.h"

namespace NKikimr::NBsController {

    namespace NLayoutChecker {

        struct TEntityId {
            ui32 Value = ::Max<ui32>();

        public:
            bool operator ==(const TEntityId& other) const { return Value == other.Value; }
            bool operator !=(const TEntityId& other) const { return Value != other.Value; }
            bool operator < (const TEntityId& other) const { return Value <  other.Value; }
            bool operator <=(const TEntityId& other) const { return Value <= other.Value; }
            bool operator > (const TEntityId& other) const { return Value >  other.Value; }
            bool operator >=(const TEntityId& other) const { return Value >= other.Value; }

            size_t Index() const {
                return Value;
            }

            size_t Hash() const {
                return THash<ui32>()(Value);
            }

            static constexpr TEntityId Min() { return {.Value = 0}; };
            static constexpr TEntityId Max() { return {.Value = ::Max<ui32>()}; };

            TString ToString() const { return TStringBuilder() << Value; }
            void Output(IOutputStream& s) const { s << Value; }

        private:
            friend class TDomainMapper;

            static TEntityId SequentialValue(size_t index) {
                return TEntityId{static_cast<ui32>(index)};
            }
        };

    } // NLayoutChecker

} // NKikimr::NBsController

template<>
struct THash<NKikimr::NBsController::NLayoutChecker::TEntityId> {
    template<typename T>
    size_t operator()(const T& id) const { return id.Hash(); }
};

namespace NKikimr::NBsController {

    namespace NLayoutChecker {

        class TDomainMapper {
            std::unordered_map<TString, TEntityId> FailDomainId;

        public:
            TEntityId operator ()(TString item) {
                return FailDomainId.emplace(std::move(item), TEntityId::SequentialValue(FailDomainId.size())).first->second;
            }

            size_t GetIdCount() const {
                return FailDomainId.size();
            }
        };

        struct TPDiskLayoutPosition {
            TEntityId RealmGroup;
            TEntityId Realm;
            TEntityId Domain;
            TEntityId Device;

            TPDiskLayoutPosition() = default;

            TPDiskLayoutPosition(TEntityId realmGroup, TEntityId realm, TEntityId domain, TEntityId device)
                : RealmGroup(realmGroup)
                , Realm(realm)
                , Domain(domain)
                , Device(device)
            {}

            TPDiskLayoutPosition(TDomainMapper& mapper, const TNodeLocation& location, TPDiskId pdiskId, const TGroupGeometryInfo& geom) {
                TStringStream realmGroup, realm, domain, device;
                ui32 deviceLevelEnd = TNodeLocation::TKeys::E::Unit + 1;
                const std::pair<int, TStringStream*> levels[] = {
                    {geom.GetRealmLevelBegin(), &realmGroup},
                    {Max(geom.GetRealmLevelEnd(), geom.GetDomainLevelBegin()), &realm},
                    {Max(geom.GetRealmLevelEnd(), geom.GetDomainLevelEnd()), &domain},
                    {Max(geom.GetRealmLevelEnd(), geom.GetDomainLevelEnd(), deviceLevelEnd), &device}
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
                Device = mapper(device.Str());
            }

            TString ToString() const {
                return TStringBuilder() << "{" << RealmGroup << "." << Realm << "." << Domain << "." << Device << "}";
            }

            auto AsTuple() const {
                return std::tie(RealmGroup, Realm, Domain, Device);
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
            ui32 DeviceInterlace = 0;
            ui32 RealmGroupScatter = 0;
            ui32 RealmScatter = 0;
            ui32 DomainScatter = 0;

            auto AsTuple() const {
                return std::make_tuple(RealmInterlace, DomainInterlace, DeviceInterlace, RealmGroupScatter, RealmScatter, DomainScatter);
            }

            bool BetterThan(const TScore& other) const {
                return AsTuple() < other.AsTuple();
            }

            bool SameAs(const TScore& other) const {
                return AsTuple() == other.AsTuple();
            }

            static TScore Max() {
                return {::Max<ui32>(), ::Max<ui32>(), ::Max<ui32>(), ::Max<ui32>(), ::Max<ui32>(), ::Max<ui32>()};
            }

            TString ToString() const {
                return TStringBuilder() << "{RealmInterlace# " << RealmInterlace
                    << " DomainInterlace# " << DomainInterlace
                    << " DeviceInterlace# " << DeviceInterlace
                    << " RealmGroupScatter# " << RealmGroupScatter
                    << " RealmScatter# " << RealmScatter
                    << " DomainScatter# " << DomainScatter
                    << "}";
            }
        };

        struct TGroupLayout {
            const TBlobStorageGroupInfo::TTopology& Topology;

            ui32 NumDisks = 0;
            THashMap<TEntityId, ui32> NumDisksPerRealmGroup;

            TStackVec<ui32, 4> NumDisksInRealm;
            TStackVec<THashMap<TEntityId, ui32>, 4> NumDisksPerRealm;
            THashMap<TEntityId, ui32> NumDisksPerRealmTotal;

            TStackVec<ui32, 32> NumDisksInDomain;
            TStackVec<THashMap<TEntityId, ui32>, 32> NumDisksPerDomain;
            THashMap<TEntityId, ui32> NumDisksPerDomainTotal;

            THashMap<TEntityId, ui32> NumDisksPerDevice;

            bool Correct = true;

            TGroupLayout(const TBlobStorageGroupInfo::TTopology& topology)
                : Topology(topology)
                , NumDisksInRealm(Topology.GetTotalFailRealmsNum())
                , NumDisksPerRealm(Topology.GetTotalFailRealmsNum())
                , NumDisksInDomain(Topology.GetTotalFailDomainsNum())
                , NumDisksPerDomain(Topology.GetTotalFailDomainsNum())
            {}

            void UpdateDisk(const TPDiskLayoutPosition& pos, ui32 orderNumber, ui32 value) {
                NumDisks += value;
                const ui32 z = NumDisksPerRealmGroup[pos.RealmGroup] += value;
                const TVDiskIdShort vdisk = Topology.GetVDiskId(orderNumber);
                const ui32 x1 = NumDisksInRealm[vdisk.FailRealm] += value;
                const ui32 x2 = NumDisksPerRealm[vdisk.FailRealm][pos.Realm] += value;
                const ui32 x3 = NumDisksPerRealmTotal[pos.Realm] += value;
                const ui32 domainIdx = Topology.GetFailDomainOrderNumber(vdisk);
                const ui32 y1 = NumDisksInDomain[domainIdx] += value;
                const ui32 y2 = NumDisksPerDomain[domainIdx][pos.Domain] += value;
                const ui32 y3 = NumDisksPerDomainTotal[pos.Domain] += value;

                NumDisksPerDevice[pos.Device] += value;

                Correct = Correct && x1 == x2 && x2 == x3 && y1 == y2 && y2 == y3 && z == NumDisks;
            }

            void AddDisk(const TPDiskLayoutPosition& pos, ui32 orderNumber) {
                UpdateDisk(pos, orderNumber, 1);
            }

            void RemoveDisk(const TPDiskLayoutPosition& pos, ui32 orderNumber) {
                UpdateDisk(pos, orderNumber, Max<ui32>());
            }

            TScore GetCandidateScore(const TPDiskLayoutPosition& pos, ui32 orderNumber) {
                const TVDiskIdShort vdisk = Topology.GetVDiskId(orderNumber);
                const ui32 domainIdx = Topology.GetFailDomainOrderNumber(vdisk);

                const auto& disksPerRealm = NumDisksPerRealm[vdisk.FailRealm][pos.Realm];
                const auto& disksPerDomain = NumDisksPerDomain[domainIdx][pos.Domain];

                const ui32 disksOnDevice = NumDisksPerDevice[pos.Device];

                return {
                    .RealmInterlace = NumDisksPerRealmTotal[pos.Realm] - disksPerRealm,
                    .DomainInterlace = NumDisksPerDomainTotal[pos.Domain] - disksPerDomain,
                    .DeviceInterlace = disksOnDevice,
                    .RealmGroupScatter = NumDisks - NumDisksPerRealmGroup[pos.RealmGroup],
                    .RealmScatter = NumDisksInRealm[vdisk.FailRealm] - disksPerRealm,
                    .DomainScatter = NumDisksInDomain[domainIdx] - disksPerDomain,
                };
            }

            TScore GetExcludedDiskScore(const TPDiskLayoutPosition& pos, ui32 orderNumber) {
                RemoveDisk(pos, orderNumber);
                const TScore score = GetCandidateScore(pos, orderNumber);
                AddDisk(pos, orderNumber);
                return score;
            }

            bool IsCorrect() const {
#ifdef NDEBUG
                return Correct;
#endif

                if (NumDisksPerRealmGroup.size() != 1) { // all disks must reside in the same realm group
                    Y_DEBUG_ABORT_UNLESS(!Correct);
                    return false;
                }

                for (size_t i = 0, num = NumDisksInRealm.size(); i < num; ++i) {
                    for (const auto& [entityId, numDisks] : NumDisksPerRealm[i]) {
                        Y_DEBUG_ABORT_UNLESS(NumDisksPerRealmTotal.contains(entityId));
                        if (numDisks != NumDisksInRealm[i] || numDisks != NumDisksPerRealmTotal.at(entityId)) {
                            // the first case is when group realm contains disks from different real-world realms (DC's)
                            // -- this is not as bad as it seems, but breaks strict failure model; the second one is a bit
                            // worse, it means that disks from this real-world realm (DC) are in several realms, which
                            // may lead to unavailability when DC goes down
                            Y_DEBUG_ABORT_UNLESS(!Correct);
                            return false;
                        }
                    }
                }

                // the same code goes for domains
                for (size_t j = 0, num = NumDisksInDomain.size(); j < num; ++j) {
                    for (const auto& [entityId, numDisks] : NumDisksPerDomain[j]) {
                        Y_DEBUG_ABORT_UNLESS(NumDisksPerDomainTotal.contains(entityId));
                        if (numDisks != NumDisksInDomain[j] || numDisks != NumDisksPerDomainTotal.at(entityId)) {
                            Y_DEBUG_ABORT_UNLESS(!Correct);
                            return false;
                        }

                    }
                }

                Y_DEBUG_ABORT_UNLESS(Correct);
                return true;
            }
        };

    } // NLayoutChecker

    struct TLayoutCheckResult {
        std::vector<TVDiskIdShort> Candidates;

        explicit operator bool() const { // checks whether fail model is correct
            return Candidates.empty();
        }
    };

} // NKikimr::NBsController
