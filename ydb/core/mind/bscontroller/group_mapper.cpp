#include "group_mapper.h"
#include "group_geometry_info.h"
#include "group_layout_checker.h"

#include <ydb/core/control/lib/immediate_control_board_impl.h>

#include <util/generic/scope.h>

namespace NKikimr::NBsController {

    using namespace NLayoutChecker;

    class TGroupMapper::TImpl : TNonCopyable {
        // Note: absolute scores do not matter, only their relations greater / less.
        static inline TControlWrapper GroupSizeInUnitsLargerThanPDiskPenalty{10, -1000, 1000};
        static inline TControlWrapper GroupSizeInUnitsSmallerThanPDiskPenalty{20, -1000, 1000};

        struct TPDiskInfo : TPDiskRecord {
            TPDiskLayoutPosition Position;
            TBridgePileId BridgePileId;

            TPDiskInfo(const TPDiskRecord& pdisk, TPDiskLayoutPosition position, TBridgePileId bridgePileId)
                : TPDiskRecord(pdisk)
                , Position(std::move(position))
                , BridgePileId(bridgePileId)
            {
                std::sort(Groups.begin(), Groups.end());
            }

            void InsertGroup(ui32 groupId) {
                if (const auto it = std::lower_bound(Groups.begin(), Groups.end(), groupId); it == Groups.end() || *it != groupId) {
                    Groups.insert(it, groupId);
                }
            }

            void EraseGroup(ui32 groupId) {
                if (const auto it = std::lower_bound(Groups.begin(), Groups.end(), groupId); it != Groups.end() && *it == groupId) {
                    Groups.erase(it);
                }
            }

            // can be negative
            i32 FreeSlots() const {
                return i32(ExpectedSlotCount) - NumActiveSlots;
            }

            bool HasFixedSlotSize() const {
                return SlotSizeInBytes != 0;
            }

            ui32 GetOwnerWeight(ui32 groupSizeInUnits) const {
                return TPDiskConfig::GetOwnerWeight(groupSizeInUnits, SlotSizeInUnits, SlotSizeInBytes);
            }

            // the less the better
            double GetPickerScore(ui32 groupSizeInUnits) const {
                double penalty = 0;
                if (!HasFixedSlotSize()) {
                    ui32 vu = groupSizeInUnits ?: 1;
                    ui32 pu = SlotSizeInUnits ?: 1;
                    if (vu > pu) {
                        // double-unit vdisk occupies two single pdisk slots
                        penalty += TImpl::GroupSizeInUnitsLargerThanPDiskPenalty;
                    } else if (vu < pu) {
                        // single-unit vdisk occupies double-unit pdisk slot (storage waste)
                        penalty += TImpl::GroupSizeInUnitsSmallerThanPDiskPenalty;
                    }
                }
                if (!ExpectedSlotCount) {
                    return NumActiveSlots + penalty;
                } else {
                    return double(NumActiveSlots) / ExpectedSlotCount + penalty;
                }
            }
        };

        using TPDisks = THashMap<TPDiskId, TPDiskInfo>;
        using TPDiskByPosition = std::vector<std::pair<TPDiskLayoutPosition, const TPDiskInfo*>>;

        using TGroup = std::vector<const TPDiskInfo*>;
        using TGroupConstraints = std::vector<TTargetDiskConstraints>;

        struct TDiskCandidate {
            const TPDiskInfo* PDisk;
            ui32 SkipToNextRealmGroup = 0;
            ui32 SkipToNextRealm = 0;
            ui32 SkipToNextDomain = 0;
            bool Tried = false;
        };

        using TDiskCandidates = std::vector<TDiskCandidate>;

        struct TCompareCandidatePosition {
            bool operator ()(const TDiskCandidate& x, const TPDiskLayoutPosition& y) const {
                return x.PDisk->Position < y;
            }

            bool operator ()(const TPDiskLayoutPosition& x, const TDiskCandidate& y) const {
                return x < y.PDisk->Position;
            }
        };

        // A search owns tentative placements; it cannot change PDisk reservations.
        struct TPlacementSearch {
            const TImpl& Self;
            const TBlobStorageGroupInfo::TTopology Topology;
            THashSet<TPDiskId> OldGroupContent; // set of all existing disks in the group, inclusing ones which are replaced
            THashSet<TPDiskId> ReplacedDisks; // set of pdisks whose vdisks are being replaced
            const i64 RequiredSpace;
            const bool RequireOperational;
            TForbiddenPDisks ForbiddenDisks;
            const TBridgePileId BridgePileId;
            THashMap<ui32, unsigned> LocalityFactor;
            TGroupLayout GroupLayout;
            const ui32 GroupSizeInUnits;
            std::optional<TScore> WorstScore;
            TGroup Group;
            std::vector<ui32> AssignedSlots;
            std::vector<ui32> NumDomainCandidates;

            TPlacementSearch(const TImpl& self, const TGroupGeometryInfo& geom, i64 requiredSpace, bool requireOperational,
                    TForbiddenPDisks forbiddenDisks, const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks, ui32 groupSizeInUnits,
                    TBridgePileId bridgePileId)
                : Self(self)
                , Topology(geom.GetType(), geom.GetNumFailRealms(), geom.GetNumFailDomainsPerFailRealm(), geom.GetNumVDisksPerFailDomain(), true)
                , RequiredSpace(requiredSpace)
                , RequireOperational(requireOperational)
                , ForbiddenDisks(std::move(forbiddenDisks))
                , BridgePileId(bridgePileId)
                , GroupLayout(Topology)
                , GroupSizeInUnits(groupSizeInUnits)
                , Group(Topology.GetTotalVDisksNum())
            {
                for (const auto& [vdiskId, pdiskId] : replacedDisks) {
                    OldGroupContent.insert(pdiskId);
                    ReplacedDisks.insert(pdiskId);
                }
            }

            bool InitializeGroup(const TGroupDefinition& group, TString& error) {
                struct TExError { TString error; };

                try {
                    Traverse(group, [&](TVDiskIdShort vdisk, TPDiskId pdiskId) {
                        if (pdiskId != TPDiskId()) {
                            const ui32 orderNumber = Topology.GetOrderNumber(vdisk);

                            const auto it = Self.PDisks.find(pdiskId);
                            if (it == Self.PDisks.end()) {
                                throw TExError{TStringBuilder() << "existing group contains missing PDiskId# " << pdiskId};
                            }
                            const TPDiskInfo& pdisk = it->second;

                            const auto [_, inserted] = OldGroupContent.insert(pdiskId);
                            if (!inserted) {
                                throw TExError{TStringBuilder() << "group contains duplicate PDiskId# " << pdiskId};
                            }

                            PlaceDisk(orderNumber, &pdisk);
                        }
                    });
                } catch (const TExError& e) {
                    error = e.error;
                    return false;
                }

                return !Group.empty();
            }

            TGroupConstraints ProcessGroupConstraints(const TGroupConstraintsDefinition& groupConstraints) {
                TGroupConstraints res(Topology.GetTotalVDisksNum());
                Traverse(groupConstraints, [&](TVDiskIdShort vdisk, TTargetDiskConstraints diskConstraints) {
                    const ui32 orderNumber = Topology.GetOrderNumber(vdisk);
                    res[orderNumber] = diskConstraints;
                });
                return res;
            }

            void Decompose(const TGroup& in, TGroupDefinition& out) {
                for (ui32 i = 0; i < in.size(); ++i) {
                    const TVDiskIdShort vdisk = Topology.GetVDiskId(i);
                    out[vdisk.FailRealm][vdisk.FailDomain][vdisk.VDisk] = in[i]->PDiskId;
                }
            }

            ui32 GetSlotsNeeded(const TPDiskInfo& pdisk) const {
                return pdisk.GetOwnerWeight(GroupSizeInUnits);
            }

            bool HasEnoughSpace(const TPDiskInfo& pdisk) const {
                if (Self.IgnoreVSlotQuotaCheck) {
                    return true;
                }
                if (pdisk.SpaceAvailable < RequiredSpace) {
                    return false;
                }
                if (pdisk.SlotSizeInBytes && RequiredSpace > 0) {
                    const ui64 slotsNeeded = GetSlotsNeeded(pdisk);
                    if (slotsNeeded > Max<ui64>() / pdisk.SlotSizeInBytes) {
                        return false;
                    }
                    if (pdisk.SlotSizeInBytes * slotsNeeded < static_cast<ui64>(RequiredSpace)) {
                        return false;
                    }
                }
                return true;
            }

            bool DiskIsUsable(const TPDiskInfo& pdisk) const {
                if (!pdisk.Usable) {
                    return false; // disk is not usable in this case
                }
                if (OldGroupContent.contains(pdisk.PDiskId) || ForbiddenDisks.contains(pdisk.PDiskId)) {
                    return false; // can't allow duplicate disks
                }
                if (RequireOperational && !pdisk.Operational) {
                    return false;
                }
                if (!HasEnoughSpace(pdisk)) {
                    return false;
                }
                if (pdisk.BridgePileId != BridgePileId) {
                    return false;
                }
                if (pdisk.FreeSlots() < i32(GetSlotsNeeded(pdisk))) {
                    return false;
                }
                return true;
            }

            TDiskCandidates FindCandidates(double maxScore) {
                TDiskCandidates candidates;
                candidates.reserve(Self.PDiskByPosition.size());
                NumDomainCandidates.assign(Self.DomainMapper.GetIdCount(), 0);

                ui32 realmGroupBegin = 0;
                ui32 realmBegin = 0;
                ui32 domainBegin = 0;
                TPDiskLayoutPosition prev;
                auto finishRange = [&](ui32& begin, auto skip) {
                    for (; begin < candidates.size(); ++begin) {
                        candidates[begin].*skip = candidates.size() - begin;
                    }
                };
                for (const auto& [position, pdisk] : Self.PDiskByPosition) {
                    if (pdisk->GetPickerScore(GroupSizeInUnits) > maxScore || !DiskIsUsable(*pdisk)) {
                        continue;
                    }
                    if (position.RealmGroup != prev.RealmGroup) {
                        finishRange(realmGroupBegin, &TDiskCandidate::SkipToNextRealmGroup);
                    }
                    if (position.Realm != prev.Realm) {
                        finishRange(realmBegin, &TDiskCandidate::SkipToNextRealm);
                    }
                    if (position.Domain != prev.Domain) {
                        finishRange(domainBegin, &TDiskCandidate::SkipToNextDomain);
                    }
                    prev = position;
                    candidates.push_back({.PDisk = pdisk});
                    ++NumDomainCandidates[position.Domain.Index()];
                }
                finishRange(realmGroupBegin, &TDiskCandidate::SkipToNextRealmGroup);
                finishRange(realmBegin, &TDiskCandidate::SkipToNextRealm);
                finishRange(domainBegin, &TDiskCandidate::SkipToNextDomain);
                return candidates;
            }

            void PlaceDisk(ui32 index, const TPDiskInfo* pdisk) {
                Y_DEBUG_ABORT_UNLESS(!Group[index]);
                Group[index] = pdisk;
                AddUsedDisk(*pdisk);
                GroupLayout.AddDisk(pdisk->Position, index, pdisk->Decommitted);
                WorstScore.reset();
            }

            void AssignDisk(ui32 index, const TPDiskInfo* pdisk) {
                AssignedSlots.push_back(index);
                PlaceDisk(index, pdisk);
            }

            void Rollback(size_t checkpoint) {
                while (AssignedSlots.size() > checkpoint) {
                    const ui32 index = AssignedSlots.back();
                    const auto* pdisk = std::exchange(Group[index], nullptr);
                    RemoveUsedDisk(*pdisk);
                    GroupLayout.RemoveDisk(pdisk->Position, index, pdisk->Decommitted);
                    AssignedSlots.pop_back();
                }
                WorstScore.reset();
            }

            template<typename TTryPlacement>
            auto FindPlacementByScore(TTryPlacement&& tryPlacement) {
                std::vector<double> scores;
                for (const auto& [pdiskId, pdisk] : Self.PDisks) {
                    if (DiskIsUsable(pdisk)) {
                        scores.push_back(pdisk.GetPickerScore(GroupSizeInUnits));
                    }
                }
                std::sort(scores.begin(), scores.end());
                scores.erase(std::unique(scores.begin(), scores.end()), scores.end());

                decltype(tryPlacement(0.0)) result;
                size_t begin = 0, end = scores.size();
                while (begin < end) {
                    const size_t mid = begin + (end - begin) / 2;
                    if (auto candidate = tryPlacement(scores[mid])) {
                        result = std::move(candidate);
                        end = mid;
                    } else {
                        begin = mid + 1;
                    }
                }
                return result;
            }

            bool DiskIsBetter(const TPDiskInfo& pretender, const TPDiskInfo& king) const {
                if (Self.PreferLessOccupiedRack) {
                    Y_ABORT_UNLESS(Self.PDiskSlotTracker.has_value());

                    auto& pdiskSlotTracker = *Self.PDiskSlotTracker;

                    // Compare by number of free slots in PDisk's rack.
                    i32 freeSlotsPretender = pdiskSlotTracker.GetFreeSlotsOnRack(pretender.Location.GetRackId());
                    i32 freeSlotsKing = pdiskSlotTracker.GetFreeSlotsOnRack(king.Location.GetRackId());

                    if (freeSlotsPretender != freeSlotsKing) {
                        return freeSlotsPretender > freeSlotsKing;
                    }
                }

                if (Self.WithAttentionToReplication) {
                    auto pretenderNode = pretender.PDiskId.NodeId;
                    auto kingNode = king.PDiskId.NodeId;

                    Y_ABORT_UNLESS(Self.PDiskSlotTracker.has_value());

                    auto& pdiskSlotTracker = *Self.PDiskSlotTracker;

                    // Compare by number of replicating VDisks on the PDisk's node.
                    auto pretenderNodeRepls = pdiskSlotTracker.GetReplicatingVDisksOnNode(pretenderNode);
                    auto kingNodeRepls = pdiskSlotTracker.GetReplicatingVDisksOnNode(kingNode);

                    if (pretenderNodeRepls != kingNodeRepls) {
                        return pretenderNodeRepls < kingNodeRepls;
                    }

                    // Compare by number of replicating VDisks on the PDisk.
                    auto pretenderPDiskRepls = pdiskSlotTracker.GetReplicatingVDisksOnPDisk(pretender.PDiskId);
                    auto kingPDiskRepls = pdiskSlotTracker.GetReplicatingVDisksOnPDisk(king.PDiskId);

                    if (pretenderPDiskRepls != kingPDiskRepls) {
                        return pretenderPDiskRepls < kingPDiskRepls;
                    }
                }

                if (pretender.FreeSlots() != king.FreeSlots()) {
                    return pretender.FreeSlots() > king.FreeSlots();
                }

                if (GivesLocalityBoost(pretender, king) || BetterQuotaMatch(pretender, king)) {
                    return true;
                } else {
                    const ui32 pretenderCandidates = NumDomainCandidates[pretender.Position.Domain.Index()];
                    const ui32 kingCandidates = NumDomainCandidates[king.Position.Domain.Index()];
                    if (pretenderCandidates != kingCandidates) {
                        return pretenderCandidates > kingCandidates;
                    }
                    return pretender.PDiskId < king.PDiskId;
                }
            }

            bool GivesLocalityBoost(const TPDiskInfo& pretender, const TPDiskInfo& king) const {
                const ui32 a = GetLocalityFactor(pretender);
                const ui32 b = GetLocalityFactor(king);
                return Self.Randomize ? a < b : a > b;
            }

            bool BetterQuotaMatch(const TPDiskInfo& pretender, const TPDiskInfo& king) const {
                return pretender.SpaceAvailable < king.SpaceAvailable;
            }

            void AddUsedDisk(const TPDiskInfo& pdisk) {
                for (ui32 groupId : pdisk.Groups) {
                    ++LocalityFactor[groupId];
                }
            }

            void RemoveUsedDisk(const TPDiskInfo& pdisk) {
                for (ui32 groupId : pdisk.Groups) {
                    if (!--LocalityFactor[groupId]) {
                        LocalityFactor.erase(groupId);
                    }
                }
            }

            unsigned GetLocalityFactor(const TPDiskInfo& pdisk) const {
                unsigned res = 0;
                for (ui32 groupId : pdisk.Groups) {
                    res += GetLocalityFactor(groupId);
                }
                return res;
            }

            unsigned GetLocalityFactor(ui32 groupId) const {
                const auto it = LocalityFactor.find(groupId);
                return it != LocalityFactor.end() ? it->second : 0;
            }
        };

        struct TAllocator : public TPlacementSearch {
            using TPlacementSearch::TPlacementSearch;

            std::optional<TGroup> TryPlacement(double maxScore, const TGroupConstraints& constraints, bool ignoreGroupLayoutChecks) {
                auto candidates = FindCandidates(maxScore);
                Y_DEFER { Rollback(0); };
                auto allocate = [&](auto what, ui32 index) {
                    TDynBitMap forbiddenEntities;
                    forbiddenEntities.Reserve(Self.DomainMapper.GetIdCount());
                    return AllocateWholeEntity(what, constraints, index,
                                               {candidates.begin(), candidates.end()}, forbiddenEntities) != nullptr;
                };

                const bool allocated = ignoreGroupLayoutChecks
                                       ? FillWithoutLayoutChecks(constraints, allocate)
                                       : FillByTopology(allocate);
                return allocated ? std::make_optional(Group) : std::nullopt;
            }

            bool FillWithoutLayoutChecks(const TGroupConstraints& constraints, const auto& allocate) {
                std::vector<ui32> unallocated;
                for (ui32 index = 0; index < Group.size(); ++index) {
                    if (!Group[index]) {
                        unallocated.push_back(index);
                    }
                }
                // Reserve explicit PDisks and node-constrained slots before unrestricted placements.
                auto priority = [&](ui32 index) {
                    const auto& constraint = constraints[index];
                    return std::make_tuple(!constraint.PDiskId.has_value(), !constraint.NodeId.has_value());
                };
                std::stable_sort(unallocated.begin(), unallocated.end(), [&](ui32 x, ui32 y) {
                    return priority(x) < priority(y);
                });
                for (ui32 index : unallocated) {
                    if (!allocate(TAllocateDisk{.IgnoreGroupLayoutChecks = true}, index)) {
                        return false;
                    }
                }
                return true;
            }

            bool FillByTopology(const auto& allocate) {
                auto isEmpty = [&](ui32 begin, ui32 size) {
                    return std::all_of(Group.begin() + begin, Group.begin() + begin + size, [](const auto *pdisk) {
                        return !pdisk;
                    });
                };
                if (isEmpty(0, Group.size())) {
                    return allocate(TAllocateWholeGroup(), 0);
                }

                const ui32 domainsPerRealm = Topology.GetNumFailDomainsPerFailRealm();
                const ui32 disksPerDomain = Topology.GetNumVDisksPerFailDomain();
                const ui32 disksPerRealm = domainsPerRealm * disksPerDomain;
                for (ui32 realmIdx = 0; realmIdx < Topology.GetTotalFailRealmsNum(); ++realmIdx) {
                    if (isEmpty(realmIdx * disksPerRealm, disksPerRealm)) {
                        if (!allocate(TAllocateWholeRealm(), realmIdx)) {
                            return false;
                        }
                        continue;
                    }
                    for (ui32 domainIdx = realmIdx * domainsPerRealm; domainIdx < (realmIdx + 1) * domainsPerRealm; ++domainIdx) {
                        const ui32 firstDisk = domainIdx * disksPerDomain;
                        if (isEmpty(firstDisk, disksPerDomain)) {
                            if (!allocate(TAllocateWholeDomain(), domainIdx)) {
                                return false;
                            }
                            continue;
                        }
                        for (ui32 diskIdx = firstDisk; diskIdx < firstDisk + disksPerDomain; ++diskIdx) {
                            if (!Group[diskIdx] && !allocate(TAllocateDisk(), diskIdx)) {
                                return false;
                            }
                        }
                    }
                }
                return true;
            }

            using TAllocateResult = const TPDiskLayoutPosition*;

            struct TAllocateDisk {
                bool IgnoreGroupLayoutChecks = false;
            };

            struct TAllocateWholeDomain {
                static constexpr auto GetEntityCount = &TBlobStorageGroupInfo::TTopology::GetNumVDisksPerFailDomain;
                using TNestedEntity = TAllocateDisk;

                static std::pair<TPDiskLayoutPosition, TPDiskLayoutPosition> MakeRange(const TPDiskLayoutPosition& x, TEntityId& scope) {
                    scope = x.Domain;
                    return {{x.RealmGroup, x.Realm, x.Domain, TEntityId::Min()}, {x.RealmGroup, x.Realm, x.Domain, TEntityId::Max()}};
                }
            };

            struct TAllocateWholeRealm {
                static constexpr auto GetEntityCount = &TBlobStorageGroupInfo::TTopology::GetNumFailDomainsPerFailRealm;
                using TNestedEntity = TAllocateWholeDomain;

                static std::pair<TPDiskLayoutPosition, TPDiskLayoutPosition> MakeRange(const TPDiskLayoutPosition& x, TEntityId& scope) {
                    scope = x.Realm;
                    return {{x.RealmGroup, x.Realm, TEntityId::Min(), TEntityId::Min()}, {x.RealmGroup, x.Realm, TEntityId::Max(), TEntityId::Max()}};
                }
            };

            struct TAllocateWholeGroup {
                static constexpr auto GetEntityCount = &TBlobStorageGroupInfo::TTopology::GetTotalFailRealmsNum;
                using TNestedEntity = TAllocateWholeRealm;

                static std::pair<TPDiskLayoutPosition, TPDiskLayoutPosition> MakeRange(const TPDiskLayoutPosition& x, TEntityId& scope) {
                    scope = x.RealmGroup;
                    return {{x.RealmGroup, TEntityId::Min(), TEntityId::Min(), TEntityId::Min()}, {x.RealmGroup, TEntityId::Max(), TEntityId::Max(), TEntityId::Max()}};
                }
            };

            using TDiskRange = std::pair<TDiskCandidates::iterator, TDiskCandidates::iterator>;

            template<typename T>
            TAllocateResult AllocateWholeEntity(T, const TGroupConstraints& constraints, ui32 parentEntityIndex, TDiskRange range,
                    TDynBitMap& forbiddenEntities) {
                const ui32 entityCount = (Topology.*T::GetEntityCount)();
                Y_ABORT_UNLESS(entityCount);
                const ui32 firstChild = parentEntityIndex * entityCount;
                const size_t checkpoint = AssignedSlots.size();

                for (;;) {
                    auto [from, to] = range;
                    const TPDiskLayoutPosition* prefix = nullptr;
                    TEntityId scope;
                    ui32 child = 0;
                    for (; child < entityCount; ++child) {
                        prefix = AllocateWholeEntity(typename T::TNestedEntity(), constraints, firstChild + child,
                                                     {from, to}, forbiddenEntities);
                        if (!prefix) {
                            break;
                        }
                        if (!child) {
                            auto [min, max] = T::MakeRange(*prefix, scope);
                            from = std::lower_bound(from, to, min, TCompareCandidatePosition());
                            to = std::upper_bound(from, to, max, TCompareCandidatePosition());
                        }
                    }
                    if (!child) {
                        return nullptr;
                    }
                    forbiddenEntities.Set(scope.Index());
                    if (child == entityCount) {
                        return prefix;
                    }
                    Rollback(checkpoint);
                }
            }

            bool MatchesConstraints(const TPDiskInfo& pdisk, const TTargetDiskConstraints& constraints) const {
                return (!constraints.NodeId || *constraints.NodeId == pdisk.PDiskId.NodeId)
                       && (!constraints.PDiskId || *constraints.PDiskId == pdisk.PDiskId);
            }

            TAllocateResult AllocateWholeEntity(TAllocateDisk options, const TGroupConstraints& constraints, ui32 index, TDiskRange range,
                    TDynBitMap& forbiddenEntities) {
                if (auto* candidate = FindBestDisk(constraints[index], index, range, forbiddenEntities, options.IgnoreGroupLayoutChecks)) {
                    AssignDisk(index, candidate->PDisk);
                    // Pruning lasts for this attempt; rolling back a placement does not retry the same candidate.
                    candidate->Tried = true;
                    return &candidate->PDisk->Position;
                }
                return nullptr;
            }

            TScore CalculateWorstScoreWithCache() {
                if (!WorstScore) {
                    // find the worst disk from a position of layout correctness and use it as a milestone for other
                    // disks -- they can't be misplaced worse
                    TScore worstScore;
                    for (ui32 i = 0; i < Topology.GetTotalVDisksNum(); ++i) {
                        if (const TPDiskInfo *pdisk = Group[i]) {
                            // calculate score for this pdisk, removing it from the set first -- to prevent counting itself
                            const TScore score = GroupLayout.GetExcludedDiskScore(pdisk->Position, i, pdisk->Decommitted);
                            if (worstScore.BetterThan(score)) {
                                worstScore = score;
                            }
                        }
                    }
                    WorstScore = worstScore;
                }
                return *WorstScore;
            }

            TDiskCandidate *FindBestDisk(const TTargetDiskConstraints& constraint, ui32 orderNumber,
                                         TDiskRange range, const TDynBitMap& forbiddenEntities, bool ignoreGroupLayoutChecks) {
                TScore bestScore = ignoreGroupLayoutChecks ? TScore::Max() : CalculateWorstScoreWithCache();
                TDiskCandidate *bestDisk = nullptr;

                while (range.first != range.second) {
                    auto& candidate = *range.first++;
                    const auto* pdisk = candidate.PDisk;
                    const auto& position = pdisk->Position;
                    if (candidate.Tried || !MatchesConstraints(*pdisk, constraint)) {
                        continue;
                    }
                    if (forbiddenEntities[position.RealmGroup.Index()]) {
                        range.first += Min<ui32>(std::distance(range.first, range.second), candidate.SkipToNextRealmGroup - 1);
                    } else if (forbiddenEntities[position.Realm.Index()]) {
                        range.first += Min<ui32>(std::distance(range.first, range.second), candidate.SkipToNextRealm - 1);
                    } else if (forbiddenEntities[position.Domain.Index()]) {
                        range.first += Min<ui32>(std::distance(range.first, range.second), candidate.SkipToNextDomain - 1);
                    } else {
                        const TScore score = GroupLayout.GetCandidateScore(position, orderNumber, pdisk->Decommitted);
                        if (score.BetterThan(bestScore)) {
                            bestScore = score;
                            bestDisk = &candidate;
                        } else if (score.SameAs(bestScore) && (!bestDisk || DiskIsBetter(*pdisk, *bestDisk->PDisk))) {
                            bestDisk = &candidate;
                        }
                    }
                }
                return bestDisk;
            }
        };

        struct TSanitizer : public TPlacementSearch {
            ui32 DesiredRealmGroup;
            std::vector<ui32> RealmNavigator;
            // failRealm -> pRealm
            std::unordered_map<ui32, std::vector<ui32>> DomainCandidates;
            // pRealm -> {pDomain1, pDomain2, ... }, sorted by number of slots in pDomains
            std::unordered_map<ui32, std::unordered_map<ui32, std::vector<const TPDiskInfo*>>> DiskCandidates;
            // {pRealm, pDomain} -> {pdisk1, pdisk2, ... }, sorted by DiskIsBetter() relation
            std::unordered_map<ui32, std::unordered_set<ui32>> BannedDomains;
            // pRealm -> {pDomain1, pDomain2, ... }
            // Cannot be a candidate, this domains are already placed correctly

            using TPlacementSearch::TPlacementSearch;

            bool SetupNavigation() {
                const auto matchingDisks = FindCandidates(::Max<double>());
                const ui32 totalFailRealmsNum = Topology.GetTotalFailRealmsNum();
                const ui32 numFailDomainsPerFailRealm = Topology.GetNumFailDomainsPerFailRealm();
                const ui32 numDisksPerFailRealm = numFailDomainsPerFailRealm * Topology.GetNumVDisksPerFailDomain();
                RealmNavigator.assign(totalFailRealmsNum, ::Max<ui32>());

                std::map<ui32, ui32> realmGroups;

                // {failRealm, pRealm} -> #number of pdisks from ${pRealm} in ${failRealm}
                std::vector<std::unordered_map<ui32, ui32>> disksInPRealmByFailRealm(totalFailRealmsNum);

                // pRealm -> #number of pdisks from ${pRealm} in ${group}
                std::unordered_map<ui32, ui32> disksInPRealm;
                std::set<ui32> realmCandidates;

                // the list of potentailly free pDomains in pRealm, which include free domains and
                // domains, currently occupied by group's pdisks
                std::unordered_map<ui32, std::unordered_set<ui32>> pDomainsInPRealm;

                for (ui32 orderNumber = 0; orderNumber < Group.size(); ++orderNumber) {
                    if (Group[orderNumber]) {
                        const TVDiskIdShort vdisk = Topology.GetVDiskId(orderNumber);
                        const ui32 pRealmGroup = Group[orderNumber]->Position.RealmGroup.Index();
                        const ui32 pRealm = Group[orderNumber]->Position.Realm.Index();
                        const ui32 pDomain = Group[orderNumber]->Position.Domain.Index();
                        realmGroups[pRealmGroup]++;
                        disksInPRealmByFailRealm[vdisk.FailRealm][pRealm]++;
                        disksInPRealm[pRealm]++;
                        pDomainsInPRealm[pRealm].insert(pDomain);
                    }
                }

                DesiredRealmGroup = 0;
                ui32 bestRealmGroupSize = 0;
                for (auto it = realmGroups.begin(); it != realmGroups.end(); ++it) {
                    if (it->second > bestRealmGroupSize) {
                        bestRealmGroupSize = it->second;
                        DesiredRealmGroup = it->first;
                    }
                }

                for (const auto& candidate : matchingDisks) {
                    const auto* pdisk = candidate.PDisk;
                    const auto& position = pdisk->Position;
                    if (position.RealmGroup.Index() == DesiredRealmGroup) {
                        pDomainsInPRealm[position.Realm.Index()].insert(position.Domain.Index());
                    }
                }

                for (auto& [pRealmIdx, pRealm] : pDomainsInPRealm) {
                    if (pRealm.size() >= numFailDomainsPerFailRealm) {
                        realmCandidates.insert(pRealmIdx);
                    }
                }


                std::vector<std::pair<ui32, ui32>> realmFilling(totalFailRealmsNum);
                for (ui32 failRealm = 0; failRealm < totalFailRealmsNum; ++failRealm) {
                    ui32 maxFilling = 0;
                    for (const auto& [pRealm, filling] : disksInPRealmByFailRealm[failRealm]) {
                        maxFilling = std::max(maxFilling, filling);
                    }
                    realmFilling[failRealm] = { numFailDomainsPerFailRealm - maxFilling, failRealm };
                }
                std::sort(realmFilling.begin(), realmFilling.end());

                for (const auto& [_, failRealm] : realmFilling) {
                    ui32 bestRealm = ::Max<ui32>();
                    std::pair<ui32, ui32> movesRequired = {::Max<ui32>(), ::Max<ui32>()};
                    // {toMoveIn, toMoveOut}. Latter parameter is less important
                    for (auto it = realmCandidates.begin(); it != realmCandidates.end(); ++it) {
                        ui32 pRealm = *it;
                        ui32 correctAlready = disksInPRealmByFailRealm[failRealm][pRealm];
                        ui32 toMoveIn = numDisksPerFailRealm - correctAlready;
                        ui32 toMoveOut = disksInPRealm[pRealm] - correctAlready;
                        ui32 freeDomains = pDomainsInPRealm[pRealm].size();
                        std::pair<ui32, ui32> newMovesRequired = {toMoveIn, toMoveOut};
                        if (toMoveOut + freeDomains < toMoveIn) {
                            continue; // not enough free domains to place all the disks
                        }
                        if (newMovesRequired < movesRequired || (newMovesRequired == movesRequired &&
                                freeDomains > pDomainsInPRealm[bestRealm].size())) {
                            bestRealm = pRealm;
                            movesRequired = newMovesRequired;
                        }
                    }
                    if (bestRealm == ::Max<ui32>()) {
                        return false;
                    }
                    RealmNavigator[failRealm] = bestRealm;
                    realmCandidates.erase(realmCandidates.find(bestRealm));
                }

                UpdateGroup();
                return true;
            }

            void UpdateGroup() {
                BannedDomains.clear();
                for (ui32 orderNumber = 0; orderNumber < Group.size(); ++orderNumber) {
                    if (Group[orderNumber]) {
                        const TVDiskIdShort vdisk = Topology.GetVDiskId(orderNumber);
                        const ui32 pRealm = Group[orderNumber]->Position.Realm.Index();
                        const ui32 pDomain = Group[orderNumber]->Position.Domain.Index();
                        if (pRealm == RealmNavigator[vdisk.FailRealm]) {
                            BannedDomains[pRealm].insert(pDomain);
                        }
                    }
                }
            }

            void SetupCandidates(double maxScore) {
                const auto matchingDisks = FindCandidates(maxScore);
                DomainCandidates.clear();
                DiskCandidates.clear();

                std::unordered_map<ui32, std::unordered_map<ui32, ui32>> slotsInPDomain;
                // {pRealm, pDomain} -> #summary number of slots in ${pDomain, pRealm}

                for (const auto& candidate : matchingDisks) {
                    const auto* pdisk = candidate.PDisk;
                    const auto& position = pdisk->Position;
                    if (position.RealmGroup.Index() == DesiredRealmGroup) {
                        ui32 pRealm = position.Realm.Index();
                        ui32 pDomain = position.Domain.Index();

                        if (BannedDomains[pRealm].count(pDomain) == 0) {
                            DomainCandidates[pRealm].push_back(pDomain);
                            DiskCandidates[pRealm][pDomain].push_back(pdisk);
                        }

                        slotsInPDomain[pRealm][pDomain] += pdisk->NumActiveSlots;
                    }
                }
                for (auto it = DomainCandidates.begin(); it != DomainCandidates.end(); ++it) {
                    const ui32 pRealmIdx = it->first;
                    // sort domains in realm by the number of free disks
                    const auto& pRealmInfo = slotsInPDomain[pRealmIdx];
                    auto realm = it->second;
                    std::sort(realm.begin(), realm.end(), [&pRealmInfo](const ui32& left, const ui32& right) {
                        return pRealmInfo.at(left) > pRealmInfo.at(right);
                    });
                    it->second = realm;

                    auto& diskCandidatesInRealm = DiskCandidates[pRealmIdx];
                    for (auto jt = diskCandidatesInRealm.begin(); jt != diskCandidatesInRealm.end(); ++jt) {
                        auto domain = jt->second;
                        // sort disks in domain by DiskIsBetter metric
                        // DiskIsBetter() is not suitable for std::sort, better ordering required
                        // std::sort(domain.begin(), domain.end(), [this](const TPDiskInfo* left, const TPDiskInfo* right) {
                        //     return this->DiskIsBetter(*left, *right);
                        // });

                        for (ui32 i = 0; i < domain.size(); ++i) {
                            if (DiskIsBetter(*domain[0], *domain[i])) {
                                std::swap(domain[0], domain[i]);
                            }
                        }
                        jt->second = domain;
                    }
                }
            }

            std::pair<TMisplacedVDisks::EFailLevel, std::vector<ui32>> FindMisplacedVDisks() {
                using EFailLevel = TMisplacedVDisks::EFailLevel;
                std::unordered_map<ui32, std::unordered_set<ui32>> usedPDomains; // pRealm -> { pDomain1, pDomain2, ... }
                std::set<TPDiskId> usedPDisks;
                // {pRealm, pDomain} -> { pdisk1, pdisk2, ... }

                EFailLevel failLevel = EFailLevel::ALL_OK;
                std::vector<ui32> misplacedVDisks;
                std::unordered_map<ui32, std::unordered_set<ui32>> realmOccupation;
                std::unordered_map<ui32, std::unordered_map<ui32, ui32>> domainInterlace;
                std::map<TPDiskId, ui32> diskInterlace;

                auto failDetected = [&](EFailLevel diskFailLevel, ui32 diskOrderNum) {
                    if ((ui32)failLevel == (ui32)diskFailLevel) {
                        misplacedVDisks.push_back(diskOrderNum);
                    } else if ((ui32)failLevel < (ui32)diskFailLevel) {
                        failLevel = diskFailLevel;
                        misplacedVDisks = { diskOrderNum };
                    }
                };

                for (ui32 orderNum = 0; orderNum < Group.size(); ++orderNum) {
                    if (Group[orderNum]) {
                        ui32 pRealm = Group[orderNum]->Position.Realm.Index();
                        ui32 pDomain = Group[orderNum]->Position.Domain.Index();
                        TPDiskId pdisk = Group[orderNum]->PDiskId;
                        domainInterlace[pRealm][pDomain]++;
                        diskInterlace[pdisk]++;
                    }
                }

                for (ui32 orderNum = 0; orderNum < Group.size(); ++orderNum) {
                    if (Group[orderNum]) {
                        const TVDiskIdShort vdisk = Topology.GetVDiskId(orderNum);
                        ui32 pRealm = Group[orderNum]->Position.Realm.Index();
                        ui32 pDomain = Group[orderNum]->Position.Domain.Index();
                        TPDiskId pdisk = Group[orderNum]->PDiskId;
                        realmOccupation[pRealm].insert(vdisk.FailRealm);

                        if (domainInterlace[pRealm][pDomain] > 1) {
                            failDetected(EFailLevel::DOMAIN_FAIL, orderNum);
                        } else if (diskInterlace[pdisk] > 1) {
                            failDetected(EFailLevel::PDISK_FAIL, orderNum);
                        }
                    } else {
                        if (failLevel == EFailLevel::EMPTY_SLOT) {
                            misplacedVDisks.clear();
                            failLevel = EFailLevel::INCORRECT_LAYOUT;
                        } else {
                            failDetected(EFailLevel::EMPTY_SLOT, orderNum);
                        }
                    }
                }

                for (ui32 orderNum = 0; orderNum < Group.size(); ++orderNum) {
                    if (Group[orderNum]) {
                        const TVDiskIdShort vdisk = Topology.GetVDiskId(orderNum);
                        ui32 pRealm = Group[orderNum]->Position.Realm.Index();
                        ui32 desiredPRealm = RealmNavigator[vdisk.FailRealm];
                        if (pRealm != desiredPRealm) {
                            if (realmOccupation[pRealm].size() > 1) {
                                // disks from different fail realms in one Realm present
                                failDetected(EFailLevel::REALM_FAIL, orderNum);
                            } else {
                                failDetected(EFailLevel::MULTIPLE_REALM_OCCUPATION, orderNum);
                            }
                        }
                    }
                }

                return {failLevel, misplacedVDisks};
            }

            std::optional<TPDiskId> TargetMisplacedVDisk(double maxScore, const TVDiskIdShort& vdisk) {
                for (ui32 orderNumber = 0; orderNumber < Group.size(); ++orderNumber) {
                    if (!Group[orderNumber] && orderNumber != Topology.GetOrderNumber(vdisk)) {
                        return std::nullopt;
                    }
                }

                UpdateGroup();
                SetupCandidates(maxScore);

                ui32 failRealm = vdisk.FailRealm;
                ui32 pRealm = RealmNavigator[failRealm];

                for (ui32 pDomain : DomainCandidates[pRealm]) {
                    const auto& diskCandidates = DiskCandidates[pRealm][pDomain];
                    if (!diskCandidates.empty()) {
                        return diskCandidates.front()->PDiskId;
                    }
                }

                return std::nullopt;
            }
        };

    private:
        const TGroupGeometryInfo Geom;
        const bool Randomize;
        TDomainMapper DomainMapper;
        TPDisks PDisks;
        TPDiskByPosition PDiskByPosition;
        bool Dirty = false;
        bool PreferLessOccupiedRack;
        bool WithAttentionToReplication;
        bool IgnoreVSlotQuotaCheck;
        std::optional<TPDiskSlotTracker> PDiskSlotTracker;

    public:
        TImpl(TGroupGeometryInfo geom, TGroupMapper::TOptions options)
            : Geom(std::move(geom))
            , Randomize(options.Randomize)
            , PreferLessOccupiedRack(options.PreferLessOccupiedRack)
            , WithAttentionToReplication(options.WithAttentionToReplication)
            , IgnoreVSlotQuotaCheck(options.IgnoreVSlotQuotaCheck)
        {
            static bool controlsRegistered = false;
            if (controlsRegistered) {
                return;
            }

            TActorSystem *actorSystem = TlsActivationContext ? TActivationContext::ActorSystem() : nullptr;
            if (actorSystem && actorSystem->AppData<TAppData>() && actorSystem->AppData<TAppData>()->Icb) {
                const TIntrusivePtr<NKikimr::TControlBoard>& icb = actorSystem->AppData<TAppData>()->Icb;

                TControlBoard::RegisterSharedControl(GroupSizeInUnitsLargerThanPDiskPenalty,
                    icb->GroupMapperControls.GroupSizeInUnitsLargerThanPDiskPenalty);

                TControlBoard::RegisterSharedControl(GroupSizeInUnitsSmallerThanPDiskPenalty,
                    icb->GroupMapperControls.GroupSizeInUnitsSmallerThanPDiskPenalty);
                controlsRegistered = true;
            }
        }

        void SetPDiskSlotTracker(TPDiskSlotTracker&& tracker) {
            PDiskSlotTracker = std::move(tracker);
        }

        TPDiskSlotTracker& GetPDiskSlotTracker() {
            return PDiskSlotTracker.value();
        }

        bool RegisterPDisk(const TPDiskRecord& pdisk) {
            // calculate disk position
            const TPDiskLayoutPosition p(DomainMapper, pdisk.Location, pdisk.DiskScope, pdisk.PDiskId, Geom);

            // insert PDisk into specific map
            TPDisks::iterator it;
            bool inserted;
            std::tie(it, inserted) = PDisks.try_emplace(pdisk.PDiskId, pdisk, p, pdisk.BridgePileId);
            if (inserted) {
                PDiskByPosition.emplace_back(it->second.Position, &it->second);
                Dirty = true;
            }

            return inserted;
        }

        TPDiskRecord UnregisterPDisk(TPDiskId pdiskId) {
            const auto it = PDisks.find(pdiskId);
            Y_ABORT_UNLESS(it != PDisks.end());
            auto x = std::remove(PDiskByPosition.begin(), PDiskByPosition.end(), std::make_pair(it->second.Position, &it->second));
            Y_ABORT_UNLESS(x + 1 == PDiskByPosition.end());
            PDiskByPosition.pop_back();
            TPDiskRecord ret = it->second;
            PDisks.erase(it);
            return ret;
        }

        void AdjustSpaceAvailable(TPDiskId pdiskId, i64 increment) {
            const auto it = PDisks.find(pdiskId);
            Y_ABORT_UNLESS(it != PDisks.end());
            it->second.SpaceAvailable += increment;
        }

        TGroupMapperError BuildGroupMappingError(const TPlacementSearch& diskManager) const {
            ui32 failRealmsNeeded = Geom.GetNumFailRealms();
            ui32 failDomainsPerRealmNeeded = Geom.GetNumFailDomainsPerFailRealm();
            ui32 disksPerDomainNeeded = Geom.GetNumVDisksPerFailDomain();

            auto keyName = [](TNodeLocation::TKeys::E k) -> TString {
                switch (k) {
                    case TNodeLocation::TKeys::BridgePileName: return "BridgePileName";
                    case TNodeLocation::TKeys::DataCenter:     return "DataCenter";
                    case TNodeLocation::TKeys::Module:         return "Module";
                    case TNodeLocation::TKeys::Rack:           return "Rack";
                    case TNodeLocation::TKeys::Unit:           return "Unit";
                }
                return "Unknown";
            };

            auto levelToKey = [](int v) {
                constexpr TNodeLocation::TKeys::E Keys[] = {
                    TNodeLocation::TKeys::BridgePileName,
                    TNodeLocation::TKeys::DataCenter,
                    TNodeLocation::TKeys::Module,
                    TNodeLocation::TKeys::Rack,
                    TNodeLocation::TKeys::Unit,
                };

                auto it = std::lower_bound(std::begin(Keys), std::end(Keys), v,
                                        [](TNodeLocation::TKeys::E e, int v) {
                                            return static_cast<int>(e) < v;
                                        });

                if (it == std::begin(Keys)) {
                    return Keys[0];
                }

                if (it == std::end(Keys) || *it >= v) {
                    --it;
                }

                return *it;
            };

            auto realmKey = levelToKey(Geom.GetRealmLevelEnd());
            auto domainKey = levelToKey(Geom.GetDomainLevelEnd());

            TGroupMapperError err;
            TStringStream s;
            s << "no group options PDisks# ";

            ui32 failRealmsSeen = 0;
            ui32 failDomainsInCurrentRealmSeen = 0;
            ui32 disksInCurrentDomainSeen = 0;

            ui32 missingFailRealmsCount = 0;
            ui32 failRealmsWithMissingDomainsCount = 0;
            ui32 domainsWithMissingDisksCount = 0;

            ui32 okDisksCount = 0;

            if (!PDiskByPosition.empty()) {
                failRealmsSeen = 1;
                failDomainsInCurrentRealmSeen = 1;

                TGroupMapperError::TStats& totalStats = err.TotalStats;
                std::vector<TGroupMapperError::TStats>& matchingDomainsStats = err.MatchingDomainsStats;
                TGroupMapperError::TStats domainStats;

                bool domainAlreadyOccupied = false;

                s << "{[(";
                TPDiskLayoutPosition prevPosition = PDiskByPosition.front().first;
                domainStats.Domain = PDiskByPosition.front().second->Location.ToStringUpTo(domainKey);
                const char *space = "";

                for (const auto& [position, pdisk] : PDiskByPosition) {
                    if (prevPosition != position) {
                        bool domainChanged = prevPosition.Domain != position.Domain;
                        bool realmChanged = prevPosition.Realm != position.Realm;

                        s << (domainChanged ? ")" : "")
                            << (realmChanged ? "]" : "")
                            << (prevPosition.RealmGroup != position.RealmGroup ? "} {" : "")
                            << (realmChanged ? "[" : "")
                            << (domainChanged ? "(" : "");
                        space = "";

                        if (realmChanged) {
                            failRealmsSeen++;
                            if (failDomainsInCurrentRealmSeen < failDomainsPerRealmNeeded) {
                                failRealmsWithMissingDomainsCount++;
                            }

                            failDomainsInCurrentRealmSeen = 0;
                        }

                        if (domainChanged) {
                            // If check is actually redundant, at least now, since any position change is a domain change
                            failDomainsInCurrentRealmSeen++;
                            if (disksInCurrentDomainSeen < disksPerDomainNeeded) {
                                domainsWithMissingDisksCount++;
                            }
                            disksInCurrentDomainSeen = 0;

                            if (!domainAlreadyOccupied) {
                                matchingDomainsStats.push_back(domainStats);
                                domainStats = TGroupMapperError::TStats();
                                domainStats.Domain = pdisk->Location.ToStringUpTo(domainKey);
                            }
                            domainAlreadyOccupied = false;
                        }
                    }

                    bool diskIsOk = true;

                    disksInCurrentDomainSeen++;

                    s << std::exchange(space, " ") << pdisk->PDiskId;

                    if (diskManager.OldGroupContent.contains(pdisk->PDiskId)) {
                        if (!diskManager.ReplacedDisks.contains(pdisk->PDiskId)) {
                            domainAlreadyOccupied = true;
                        }

                        s << "*";
                    }
                    const char *minus = "-";
                    if (diskManager.ForbiddenDisks.contains(pdisk->PDiskId)) {
                        s << std::exchange(minus, "") << "f";
                    }
                    if (!pdisk->Usable) {
                        if (pdisk->WhyUnusable.Contains('S')) {
                            totalStats.NotAcceptingNewSlots++;
                            domainStats.NotAcceptingNewSlots++;
                        }
                        if (pdisk->WhyUnusable.Contains('O')) {
                            totalStats.NotOperational++;
                            domainStats.NotOperational++;
                        }
                        if (pdisk->WhyUnusable.Contains('D')) {
                            totalStats.Decommission++;
                            domainStats.Decommission++;
                        }
                        diskIsOk = false;
                        s << std::exchange(minus, "") << pdisk->WhyUnusable;
                    }
                    if (pdisk->NumActiveSlots >= pdisk->ExpectedSlotCount) {
                        totalStats.AllSlotsAreOccupied++;
                        domainStats.AllSlotsAreOccupied++;
                        diskIsOk = false;

                        s << std::exchange(minus, "") << "s[" << pdisk->NumActiveSlots << "/" << pdisk->ExpectedSlotCount << "]";
                    }
                    if (!diskManager.HasEnoughSpace(*pdisk)) {
                        totalStats.NotEnoughSpace++;
                        domainStats.NotEnoughSpace++;
                        diskIsOk = false;
                        s << std::exchange(minus, "") << "v";
                    }
                    if (!pdisk->Operational) {
                        diskIsOk = false;
                        s << std::exchange(minus, "") << "o";
                    }
                    if (pdisk->BridgePileId != diskManager.BridgePileId) {
                        s << std::exchange(minus, "") << "p";
                    }
                    if (diskManager.DiskIsUsable(*pdisk)) {
                        s << "+";
                    }

                    prevPosition = position;

                    if (diskIsOk) {
                        okDisksCount++;
                    }
                }
                s << ")]}";

                // Handle last domain
                if (!domainAlreadyOccupied) {
                    matchingDomainsStats.push_back(domainStats);
                }

                if (failRealmsSeen < failRealmsNeeded) {
                    missingFailRealmsCount++;
                }

                if (failDomainsInCurrentRealmSeen < failDomainsPerRealmNeeded) {
                    failRealmsWithMissingDomainsCount++;
                }

                if (disksInCurrentDomainSeen < disksPerDomainNeeded) {
                    domainsWithMissingDisksCount++;
                }
            } else {
                s << "<empty>";
            }

            err.ErrorMessage = s.Str();

            err.MissingFailRealmsCount = missingFailRealmsCount;
            err.FailRealmsWithMissingDomainsCount = failRealmsWithMissingDomainsCount;
            err.DomainsWithMissingDisksCount = domainsWithMissingDisksCount;
            err.OkDisksCount = okDisksCount;

            err.RealmLocationKey = keyName(realmKey);
            err.DomainLocationKey = keyName(domainKey);

            return std::move(err);
        }

        void UpdateReservation(ui32 groupId, ui32 groupSizeInUnits, TPDiskId previous, TPDiskId next) {
            if (previous != TPDiskId()) {
                auto& pdisk = PDisks.at(previous);
                pdisk.NumActiveSlots -= pdisk.GetOwnerWeight(groupSizeInUnits);
                pdisk.EraseGroup(groupId);
            }
            if (next != TPDiskId()) {
                auto& pdisk = PDisks.at(next);
                pdisk.NumActiveSlots += pdisk.GetOwnerWeight(groupSizeInUnits);
                pdisk.InsertGroup(groupId);
            }
        }

        bool AllocateGroup(ui32 groupId, TGroupDefinition& groupDefinition, TGroupMapper::TGroupConstraintsDefinition& constraints,
                const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks, TForbiddenPDisks forbid,
                ui32 groupSizeInUnits, i64 requiredSpace, bool requireOperational,
                TBridgePileId bridgePileId, TGroupMapperError& error, bool ignoreGroupLayoutChecks = false) {
            if (Dirty) {
                std::sort(PDiskByPosition.begin(), PDiskByPosition.end());
                Dirty = false;
            }

            // create group of required size, if it is not created yet
            if (!Geom.ResizeGroup(groupDefinition)) {
                error.ErrorMessage = "incorrect existing group";
                return false;
            }

            // fill in the allocation context
            TAllocator allocator(*this, Geom, requiredSpace, requireOperational, std::move(forbid), replacedDisks, groupSizeInUnits,
                bridgePileId);
            if (!allocator.InitializeGroup(groupDefinition, error.ErrorMessage)) {
                return false;
            }
            if (std::ranges::all_of(allocator.Group, [](const auto* pdisk) { return pdisk; })) {
                return true;
            }
            const auto groupConstraints = allocator.ProcessGroupConstraints(constraints);

            auto result = allocator.FindPlacementByScore([&](double maxScore) {
                return allocator.TryPlacement(maxScore, groupConstraints, ignoreGroupLayoutChecks);
            });
            if (!result) {
                error = BuildGroupMappingError(allocator);
                return false;
            }
            for (const auto& [vdiskId, pdiskId] : replacedDisks) {
                UpdateReservation(groupId, groupSizeInUnits, pdiskId, {});
            }
            ui32 allocatedSlots = 0;
            for (ui32 index = 0; index < allocator.Group.size(); ++index) {
                if (!allocator.Group[index]) {
                    UpdateReservation(groupId, groupSizeInUnits, {}, result->at(index)->PDiskId);
                    ++allocatedSlots;
                }
            }
            Y_ABORT_UNLESS(allocatedSlots == allocator.Group.size() || allocatedSlots == replacedDisks.size());
            allocator.Decompose(*result, groupDefinition);
            return true;
        }

        struct TReassignmentAllocation {
            TGroupDefinition Group;
            TGroupConstraintsDefinition RequiredConstraints;
            TVector<std::pair<TVDiskIdShort, ui32>> PreferredNodes;
            THashMap<TVDiskIdShort, TPDiskId> ReplacedDisks;

            TGroupConstraintsDefinition MakeConstraintsWithPreferences() const {
                auto constraints = RequiredConstraints;
                for (const auto& [id, nodeId] : PreferredNodes) {
                    constraints[id.FailRealm][id.FailDomain][id.VDisk].NodeId = nodeId;
                }
                return constraints;
            }
        };

        bool PrepareReassignment(const TGroupMapper::TReassignmentRequest& request, TReassignmentAllocation& allocation,
                                 TGroupMapperError& error) {
            auto& group = allocation.Group;
            auto& requiredConstraints = allocation.RequiredConstraints;
            auto& replacedDisks = allocation.ReplacedDisks;
            Y_ABORT_UNLESS(Geom.ResizeGroup(group));
            Y_ABORT_UNLESS(Geom.ResizeGroup(requiredConstraints));

            const ui32 numFailDomains = Geom.GetNumFailDomainsPerFailRealm();
            const ui32 numVDisks = Geom.GetNumVDisksPerFailDomain();
            const ui32 totalVDisks = Geom.GetNumFailRealms() * numFailDomains * numVDisks;
            TVector<bool> seen(totalVDisks);
            ui32 numSeen = 0;

            for (const auto& disk : request.VDisks) {
                const auto& id = disk.VDiskId;
                if (id.FailRealm >= Geom.GetNumFailRealms() || id.FailDomain >= numFailDomains || id.VDisk >= numVDisks) {
                    error.ErrorMessage = "VDisk position is outside group geometry";
                    return false;
                }
                const ui32 orderNumber = (id.FailRealm * numFailDomains + id.FailDomain) * numVDisks + id.VDisk;
                if (seen[orderNumber]) {
                    error.ErrorMessage = "duplicate VDisk position";
                    return false;
                }
                seen[orderNumber] = true;
                ++numSeen;

                auto& pdiskId = group[id.FailRealm][id.FailDomain][id.VDisk];
                if (!std::holds_alternative<TKeepVDisk>(disk.Reassignment)) {
                    if (!PDisks.contains(disk.PDiskId)) {
                        error.ErrorMessage = TStringBuilder() << "missing replaced PDiskId# " << disk.PDiskId;
                        return false;
                    }
                    replacedDisks.emplace(id, disk.PDiskId);

                    if (const auto *force = std::get_if<TForceVDiskOnPDisk>(&disk.Reassignment)) {
                        if (force->PDiskId == TPDiskId()) {
                            error.ErrorMessage = "forced target PDiskId is empty";
                            return false;
                        }
                        pdiskId = force->PDiskId;
                    } else {
                        auto& required = requiredConstraints[id.FailRealm][id.FailDomain][id.VDisk];
                        if (const auto *target = std::get_if<TReplaceVDiskOnPDisk>(&disk.Reassignment)) {
                            if (target->PDiskId == TPDiskId()) {
                                error.ErrorMessage = "target PDiskId is empty";
                                return false;
                            }
                            required.PDiskId = target->PDiskId;
                        } else if (const auto *automatic = std::get_if<TReplaceVDisk>(&disk.Reassignment);
                                   automatic && automatic->RequireSameNode) {
                            required.NodeId = disk.PDiskId.NodeId;
                        }
                        if (request.TryToRelocateLocallyFirst && !required.NodeId) {
                            allocation.PreferredNodes.emplace_back(id, disk.PDiskId.NodeId);
                        }
                    }
                } else {
                    pdiskId = disk.PDiskId;
                }
            }

            if (request.ExistingGroup && numSeen != totalVDisks) {
                error.ErrorMessage = "incomplete existing group definition";
                return false;
            }

            return true;
        }

        bool TryAllocateReassignment(const TGroupMapper::TReassignmentRequest& request, TReassignmentAllocation& allocation,
                                     i64 requiredSpace, bool settleOnlyOnOperationalDisks, TGroupMapperError& error) {
            auto constraintsWithPreferences = allocation.MakeConstraintsWithPreferences();
            for (bool ignoreLayout : {false, true}) {
                if (ignoreLayout && !request.IgnoreGroupLayoutChecks) {
                    break;
                }
                for (auto *constraints : {&constraintsWithPreferences, &allocation.RequiredConstraints}) {
                    for (bool requireOperational : {true, false}) {
                        if (!requireOperational && settleOnlyOnOperationalDisks) {
                            break;
                        }
                        if (AllocateGroup(request.GroupId, allocation.Group, *constraints, allocation.ReplacedDisks,
                                          request.ForbiddenPDisks, request.GroupSizeInUnits, requiredSpace, requireOperational,
                                          request.BridgePileId, error, ignoreLayout)) {
                            return true;
                        }
                    }
                    if (!request.TryToRelocateLocallyFirst) {
                        break;
                    }
                }
            }
            return false;
        }

        bool ReassignGroup(const TGroupMapper::TReassignmentRequest& request, TGroupMapper::TReassignmentOutcome& outcome,
                           bool settleOnlyOnOperationalDisks) {
            outcome = {};
            TReassignmentAllocation allocation;
            if (!PrepareReassignment(request, allocation, outcome.Error)) {
                return false;
            }
            outcome.RequiredSpace = TGroupMapper::CalculateRequiredSpace(request.VDisks, request.MinimumRequiredSpace);
            outcome.Success = TryAllocateReassignment(request, allocation, outcome.RequiredSpace,
                                                      settleOnlyOnOperationalDisks, outcome.Error);
            if (outcome.Success) {
                outcome.Error = {};
                const TBlobStorageGroupInfo::TTopology topology(Geom.GetType(), Geom.GetNumFailRealms(),
                                                               Geom.GetNumFailDomainsPerFailRealm(), Geom.GetNumVDisksPerFailDomain(), true);
                TGroupLayout layout(topology);
                TGroupMapper::Traverse(allocation.Group, [&](TVDiskIdShort id, TPDiskId pdiskId) {
                    const auto& pdisk = PDisks.at(pdiskId);
                    layout.AddDisk(pdisk.Position, topology.GetOrderNumber(id), pdisk.Decommitted);
                });
                outcome.LayoutCorrect = layout.IsCorrect();
            }
            outcome.Group = std::move(allocation.Group);
            return outcome.Success;
        }

        TMisplacedVDisks FindMisplacedVDisks(const TGroupDefinition& groupDefinition, ui32 groupSizeInUnits) {
            using EFailLevel = TMisplacedVDisks::EFailLevel;
            // create group of required size, if it is not created yet
            if (!Geom.CheckGroupSize(groupDefinition)) {
                return TMisplacedVDisks(EFailLevel::INCORRECT_LAYOUT, {}, "Incorrect group");
            }

            TSanitizer sanitizer(*this, Geom, 0, false, {}, {}, groupSizeInUnits, {});
            TString error;
            if (!sanitizer.InitializeGroup(groupDefinition, error)) {
                return TMisplacedVDisks(EFailLevel::INCORRECT_LAYOUT, {}, error);
            }
            if (!sanitizer.SetupNavigation()) {
                return TMisplacedVDisks(EFailLevel::INCORRECT_LAYOUT, {}, "Cannot map failRealms to pRealms");
            }

            auto [failLevel, misplacedVDiskNums] = sanitizer.FindMisplacedVDisks();
            std::vector<TVDiskIdShort> misplacedVDisks;
            for (ui32 orderNum : misplacedVDiskNums) {
                misplacedVDisks.push_back(sanitizer.Topology.GetVDiskId(orderNum));
            }
            return TMisplacedVDisks(failLevel, misplacedVDisks);
        }

        std::optional<TPDiskId> TargetMisplacedVDisk(ui32 groupId, TGroupDefinition& groupDefinition, TVDiskIdShort vdisk,
                TForbiddenPDisks forbid, ui32 groupSizeInUnits, i64 requiredSpace, bool requireOperational, TBridgePileId bridgePileId,
                TString& error) {
            if (Dirty) {
                std::sort(PDiskByPosition.begin(), PDiskByPosition.end());
                Dirty = false;
            }

            // create group of required size, if it is not created yet
            if (!Geom.CheckGroupSize(groupDefinition)) {
                error = "Incorrect group";
                return std::nullopt;
            }

            TSanitizer sanitizer(*this, Geom, requiredSpace, requireOperational, std::move(forbid), {}, groupSizeInUnits, bridgePileId);
            if (!sanitizer.InitializeGroup(groupDefinition, error)) {
                error = "Empty group";
                return std::nullopt;
            }
            if (!sanitizer.SetupNavigation()) {
                error = "Cannot map failRealms to pRealms";
                return std::nullopt;
            }

            auto result = sanitizer.FindPlacementByScore([&](double maxScore) {
                return sanitizer.TargetMisplacedVDisk(maxScore, vdisk);
            });
            if (result) {
                auto& previous = groupDefinition[vdisk.FailRealm][vdisk.FailDomain][vdisk.VDisk];
                UpdateReservation(groupId, groupSizeInUnits, previous, *result);
                previous = *result;
                return result;
            }

            error = "Cannot replace vdisk";
            return std::nullopt;
        }
    };

    TGroupMapper::TGroupMapper(TGroupGeometryInfo geom, bool randomize, bool preferLessOccupiedRack, bool withAttentionToReplication)
        : TGroupMapper(std::move(geom), {
            .Randomize = randomize,
            .PreferLessOccupiedRack = preferLessOccupiedRack,
            .WithAttentionToReplication = withAttentionToReplication,
        })
    {}

    TGroupMapper::TGroupMapper(TGroupGeometryInfo geom, TOptions options)
        : Options(options)
        , Impl(new TImpl(std::move(geom), options))
    {}

    TGroupMapper::~TGroupMapper() = default;

    class TGroupMapper::TPlacementBuilder::TState {
    public:
        using TGroupKey = std::pair<ui32, ui32>;

        struct TAccumulatedPDisk {
            TPDiskState State;
            TStackVec<ui32, 16> Groups;
            i64 ReplicationSpaceAdjustment = 0;
        };

        TGroupMapper& Mapper;
        THashMap<TGroupKey, ui32> GroupSizes;
        THashMap<TGroupKey, i64> MaxGroupSlotSize;
        THashMap<TPDiskId, size_t> PDiskIndices;
        TVector<TAccumulatedPDisk> PDisks;
        TPDiskSlotTracker SlotTracker;
        bool HasPrecomputedReplicationTracker = false;

        explicit TState(TGroupMapper& mapper)
            : Mapper(mapper)
        {}
    };

    TGroupMapper::TPlacementBuilder::TPlacementBuilder(TGroupMapper& mapper)
        : State(MakeHolder<TState>(mapper))
    {}

    TGroupMapper::TPlacementBuilder::~TPlacementBuilder() = default;

    void TGroupMapper::TPlacementBuilder::AddGroup(const TGroupState& group) {
        const auto groupKey = std::make_pair(group.GroupId, group.GroupGeneration);
        State->GroupSizes[groupKey] = group.GroupSizeInUnits;
        if (group.MaxVDiskAllocatedSize) {
            State->MaxGroupSlotSize[groupKey] = *group.MaxVDiskAllocatedSize;
        }
    }

    void TGroupMapper::TPlacementBuilder::UpdateMaxGroupSlotSize(ui32 groupId, ui32 groupGeneration,
                                                                 i64 spaceUsed) {
        const auto groupKey = std::make_pair(groupId, groupGeneration);
        State->MaxGroupSlotSize[groupKey] = Max(State->MaxGroupSlotSize[groupKey], spaceUsed);
    }

    void TGroupMapper::TPlacementBuilder::AddPDisk(TPDiskState pdisk) {
        const TPDiskId pdiskId = pdisk.PDiskId;
        const auto [_, inserted] = State->PDiskIndices.try_emplace(pdiskId, State->PDisks.size());
        Y_ABORT_UNLESS(inserted);
        State->PDisks.push_back(TState::TAccumulatedPDisk{
            .State = std::move(pdisk),
        });
    }

    void TGroupMapper::TPlacementBuilder::AddVSlot(const TVSlotState& vslot) {
        if (vslot.OccupiedByGroup && vslot.GroupId && vslot.AllocatedSize) {
            State->Mapper.VDiskAllocatedSizes.emplace(
                TVDiskID(TGroupId::FromValue(*vslot.GroupId), vslot.GroupGeneration, vslot.VDiskId),
                *vslot.AllocatedSize);
        }
        if (State->Mapper.Options.WithAttentionToReplication
            && !State->HasPrecomputedReplicationTracker && vslot.Replicating) {
            State->SlotTracker.AddReplicatingVSlot(vslot.PDiskId);
        }

        const auto it = State->PDiskIndices.find(vslot.PDiskId);
        if (it == State->PDiskIndices.end()) {
            return;
        }

        auto& pdisk = State->PDisks[it->second];
        if (vslot.CountedInNumActiveSlots) {
            const auto groupKey = std::make_pair(vslot.GroupId.value_or(0), vslot.GroupGeneration);
            const auto groupIt = State->GroupSizes.find(groupKey);
            const ui32 groupSizeInUnits = groupIt != State->GroupSizes.end() ? groupIt->second : 1;
            pdisk.State.NumActiveSlots += TPDiskConfig::GetOwnerWeight(groupSizeInUnits, pdisk.State.SlotSizeInUnits,
                                                                 pdisk.State.SlotSizeInBytes);
        }
        if (vslot.OccupiedByGroup && vslot.GroupId) {
            pdisk.Groups.push_back(*vslot.GroupId);
        }
        if (!vslot.Ready && vslot.SpaceUsed && vslot.GroupId) {
            pdisk.ReplicationSpaceAdjustment += *vslot.SpaceUsed
                                                 - State->MaxGroupSlotSize[std::make_pair(*vslot.GroupId,
                                                                                         vslot.GroupGeneration)];
        }
    }

    void TGroupMapper::TPlacementBuilder::SetPrecomputedReplicationTracker(TPDiskSlotTracker tracker) {
        State->SlotTracker = std::move(tracker);
        State->HasPrecomputedReplicationTracker = true;
    }

    void TGroupMapper::TPlacementBuilder::Finish() {
        const bool populateSlotTracker = State->Mapper.Options.PreferLessOccupiedRack
                                         || State->Mapper.Options.WithAttentionToReplication;
        for (auto& pdisk : State->PDisks) {
            auto& disk = pdisk.State;
            if (!AcceptsNewSlots(disk.DriveStatus, disk.MaintenanceStatus)) {
                disk.Usable = false;
                disk.WhyUnusable += 'S';
            }
            if (State->Mapper.Options.SettleOnlyOnOperationalDisks && !disk.Operational) {
                disk.Usable = false;
                disk.WhyUnusable += 'O';
            }
            if (!UsableInTermsOfDecommission(disk.DecommitStatus, State->Mapper.Options.IsSelfHealReasonDecommit)) {
                disk.Usable = false;
                disk.WhyUnusable += 'D';
            }

            i64 availableSpace = Max<i64>();
            if (disk.Usable && !State->Mapper.Options.IgnoreVSlotQuotaCheck) {
                availableSpace = disk.Space
                                 ? CalculateSpaceAvailable(*disk.Space, State->Mapper.Options.SpaceColorBorder,
                                                           State->Mapper.Options.SpaceMarginPromille)
                                 : 0;
                if (!disk.Space || !SlotSpaceEnforced(*disk.Space, State->Mapper.Options.SpaceColorBorder)) {
                    availableSpace += pdisk.ReplicationSpaceAdjustment;
                }
            }

            const bool registered = State->Mapper.RegisterPDisk({
                .PDiskId = disk.PDiskId,
                .Location = disk.Location,
                .Usable = disk.Usable,
                .NumActiveSlots = disk.NumActiveSlots,
                .ExpectedSlotCount = disk.ExpectedSlotCount,
                .SlotSizeInUnits = disk.SlotSizeInUnits,
                .SlotSizeInBytes = disk.SlotSizeInBytes,
                .Groups = std::move(pdisk.Groups),
                .SpaceAvailable = availableSpace,
                .Operational = disk.Operational,
                .Decommitted = disk.DecommitStatus != NKikimrBlobStorage::DECOMMIT_UNSET
                               && IsDecommitted(disk.DecommitStatus),
                .WhyUnusable = std::move(disk.WhyUnusable),
                .BridgePileId = disk.BridgePileId,
                .DiskScope = std::move(disk.DiskScope),
            });
            Y_ABORT_UNLESS(registered);
            if (populateSlotTracker && disk.Usable) {
                State->SlotTracker.AddFreeSlotsForRack(disk.Location.GetRackId(),
                                                       i32(disk.ExpectedSlotCount) - disk.NumActiveSlots);
            }
        }
        State->Mapper.SetPDiskSlotTracker(std::move(State->SlotTracker));
    }

    TGroupMapper::TPDiskSpaceState TGroupMapper::CapturePDiskSpace(const NKikimrBlobStorage::TPDiskMetrics& metrics) {
        TPDiskSpaceState state{
            .AvailableSize = metrics.GetAvailableSize(),
            .TotalSize = metrics.GetTotalSize(),
        };
        if (metrics.HasEnforcedDynamicSlotSize()) {
            state.EnforcedDynamicSlotSize = metrics.GetEnforcedDynamicSlotSize();
        }
        return state;
    }

    bool TGroupMapper::SlotSpaceEnforced(const TPDiskSpaceState& space,
                                         NKikimrBlobStorage::TPDiskSpaceColor::E colorBorder) {
        return space.EnforcedDynamicSlotSize.has_value()
               && colorBorder >= NKikimrBlobStorage::TPDiskSpaceColor::YELLOW;
    }

    i64 TGroupMapper::CalculateSpaceAvailable(const TPDiskSpaceState& space,
                                              NKikimrBlobStorage::TPDiskSpaceColor::E colorBorder, ui32 marginPromille) {
        if (SlotSpaceEnforced(space, colorBorder)) {
            return *space.EnforcedDynamicSlotSize * (1000 - marginPromille) / 1000;
        }
        return space.AvailableSize - space.TotalSize * marginPromille / 1000;
    }

    i64 TGroupMapper::CalculateRequiredSpace(const TVector<TVDiskPlacement>& vdisks, i64 minimumRequiredSpace) {
        i64 requiredSpace = minimumRequiredSpace;
        for (const auto& disk : vdisks) {
            if (std::holds_alternative<TKeepVDisk>(disk.Reassignment) && disk.AllocatedSize) {
                requiredSpace = Max(requiredSpace, *disk.AllocatedSize);
            }
        }
        return requiredSpace;
    }

    TGroupMapper::TReassignmentOutcome TGroupMapper::PlanGroupReassignment(TGroupGeometryInfo geom, TOptions options,
                                                                           TPlacementSnapshot snapshot, TReassignmentRequest request) {
        TGroupMapper mapper(std::move(geom), std::move(options));
        mapper.Populate(std::move(snapshot));
        return mapper.AllocateGroupReassignment(std::move(request));
    }

    void TGroupMapper::Populate(TPlacementSnapshot snapshot) {
        TPlacementBuilder builder(*this);
        for (const auto& group : snapshot.Groups) {
            builder.AddGroup(group);
        }
        for (const auto& vslot : snapshot.VSlots) {
            if (vslot.OccupiedByGroup && vslot.GroupId && vslot.SpaceUsed) {
                builder.UpdateMaxGroupSlotSize(*vslot.GroupId, vslot.GroupGeneration, *vslot.SpaceUsed);
            }
        }
        for (auto& pdisk : snapshot.PDisks) {
            builder.AddPDisk(std::move(pdisk));
        }
        if (snapshot.PrecomputedReplicationTracker) {
            builder.SetPrecomputedReplicationTracker(std::move(*snapshot.PrecomputedReplicationTracker));
        }
        for (const auto& vslot : snapshot.VSlots) {
            builder.AddVSlot(vslot);
        }
        builder.Finish();
    }

    void TGroupMapper::SetPDiskSlotTracker(TPDiskSlotTracker&& tracker) {
        Impl->SetPDiskSlotTracker(std::move(tracker));
    }

    TPDiskSlotTracker& TGroupMapper::GetPDiskSlotTracker() {
        return Impl->GetPDiskSlotTracker();
    }

    bool TGroupMapper::RegisterPDisk(const TPDiskRecord& pdisk) {
        return Impl->RegisterPDisk(pdisk);
    }

    TGroupMapper::TReassignmentOutcome TGroupMapper::AllocateGroupReassignment(TReassignmentRequest request) {
        for (auto& disk : request.VDisks) {
            if (!disk.AllocatedSize) {
                const TVDiskID vdiskId(TGroupId::FromValue(request.GroupId), request.GroupGeneration, disk.VDiskId);
                if (const auto it = VDiskAllocatedSizes.find(vdiskId); it != VDiskAllocatedSizes.end()) {
                    disk.AllocatedSize = it->second;
                }
            }
        }
        TReassignmentOutcome outcome;
        Impl->ReassignGroup(request, outcome, Options.SettleOnlyOnOperationalDisks);
        return outcome;
    }

    TGroupMapper::TPDiskRecord TGroupMapper::UnregisterPDisk(TPDiskId pdiskId) {
        return Impl->UnregisterPDisk(pdiskId);
    }

    void TGroupMapper::AdjustSpaceAvailable(TPDiskId pdiskId, i64 increment) {
        return Impl->AdjustSpaceAvailable(pdiskId, increment);
    }

    bool TGroupMapper::AllocateGroup(ui32 groupId, TGroupDefinition& group, TGroupMapper::TGroupConstraintsDefinition& constraints,
            const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks, TForbiddenPDisks forbid,
            ui32 groupSizeInUnits, i64 requiredSpace, bool requireOperational,
            TBridgePileId bridgePileId, TGroupMapperError& error) {
        return Impl->AllocateGroup(groupId, group, constraints, replacedDisks, std::move(forbid),
            groupSizeInUnits, requiredSpace,
            requireOperational, bridgePileId, error);
    }

    bool TGroupMapper::AllocateGroup(ui32 groupId, TGroupDefinition& group, const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks,
            TForbiddenPDisks forbid, ui32 groupSizeInUnits, i64 requiredSpace, bool requireOperational, TBridgePileId bridgePileId,
            TGroupMapperError& error) {
        TGroupMapper::TGroupConstraintsDefinition emptyConstraints;
        return AllocateGroup(groupId, group, emptyConstraints, replacedDisks, std::move(forbid),
            groupSizeInUnits, requiredSpace,
            requireOperational, bridgePileId, error);
    }

    TGroupMapper::TMisplacedVDisks TGroupMapper::FindMisplacedVDisks(const TGroupDefinition& group, ui32 groupSizeInUnits) {
        return Impl->FindMisplacedVDisks(group, groupSizeInUnits);
    }

    std::optional<TPDiskId> TGroupMapper::TargetMisplacedVDisk(TGroupId groupId, TGroupMapper::TGroupDefinition& group,
            TVDiskIdShort vdisk, TForbiddenPDisks forbid, ui32 groupSizeInUnits, i64 requiredSpace, bool requireOperational,
            TBridgePileId bridgePileId, TString& error) {
        return Impl->TargetMisplacedVDisk(groupId.GetRawId(), group, vdisk, std::move(forbid), groupSizeInUnits, requiredSpace,
            requireOperational, bridgePileId, error);
    }
} // NKikimr::NBsController
