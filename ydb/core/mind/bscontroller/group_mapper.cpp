#include "group_mapper.h"
#include "group_geometry_info.h"
#include "group_layout_checker.h"

namespace NKikimr::NBsController {

    using namespace NLayoutChecker;

    struct TAllocator;

    class TGroupMapper::TImpl : TNonCopyable {
        struct TPDiskInfo : TPDiskRecord {
            TPDiskLayoutPosition Position;
            bool Matching;
            ui32 NumDomainMatchingDisks;
            ui32 SkipToNextRealmGroup;
            ui32 SkipToNextRealm;
            ui32 SkipToNextDomain;

            TPDiskInfo(const TPDiskRecord& pdisk, TPDiskLayoutPosition position)
                : TPDiskRecord(pdisk)
                , Position(std::move(position))
            {
                std::sort(Groups.begin(), Groups.end());
            }

            bool IsUsable() const {
                return Usable && NumSlots < MaxSlots;
            }

            void InsertGroup(ui32 groupId) {
                if (const auto it = std::lower_bound(Groups.begin(), Groups.end(), groupId); it == Groups.end() || *it < groupId) {
                    Groups.insert(it, groupId);
                }
            }

            void EraseGroup(ui32 groupId) {
                if (const auto it = std::lower_bound(Groups.begin(), Groups.end(), groupId); it != Groups.end() && !(*it < groupId)) {
                    Groups.erase(it);
                }
            }

            ui32 GetPickerScore() const {
                return NumSlots;
            }
        };

        using TPDisks = THashMap<TPDiskId, TPDiskInfo>;
        using TPDiskByPosition = std::vector<std::pair<TPDiskLayoutPosition, TPDiskInfo*>>;

        struct TComparePDiskByPosition {
            bool operator ()(const TPDiskByPosition::value_type& x, const TPDiskLayoutPosition& y) const {
                return x.first < y;
            }

            bool operator ()(const TPDiskLayoutPosition& x, const TPDiskByPosition::value_type& y) const {
                return x < y.first;
            }
        };

        using TGroup = std::vector<TPDiskInfo*>;
        using TGroupConstraints = std::vector<TTargetDiskConstraints>;

        // PDomain/PRealm - TPDiskLayoutPosition, Fail Domain/Fail Realm - VDiskId

        using TPDomainCandidatesRange = std::pair<std::vector<ui32>::const_iterator, std::vector<ui32>::const_iterator>;
        using TPDiskCandidatesRange = std::pair<std::vector<TPDiskInfo*>::const_iterator, std::vector<TPDiskInfo*>::const_iterator>;
       
        struct TDiskManager {
            TImpl& Self;
            const TBlobStorageGroupInfo::TTopology Topology;
            THashSet<TPDiskId> OldGroupContent; // set of all existing disks in the group, inclusing ones which are replaced
            const i64 RequiredSpace;
            const bool RequireOperational;
            TForbiddenPDisks ForbiddenDisks;
            THashMap<ui32, unsigned> LocalityFactor;
            TGroupLayout GroupLayout;
            std::optional<TScore> WorstScore;

            TDiskManager(TImpl& self, const TGroupGeometryInfo& geom, i64 requiredSpace, bool requireOperational,
                    TForbiddenPDisks forbiddenDisks, const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks)
                : Self(self)
                , Topology(geom.GetType(), geom.GetNumFailRealms(), geom.GetNumFailDomainsPerFailRealm(), geom.GetNumVDisksPerFailDomain(), true)
                , RequiredSpace(requiredSpace)
                , RequireOperational(requireOperational)
                , ForbiddenDisks(std::move(forbiddenDisks))
                , GroupLayout(Topology)
            {
                for (const auto& [vdiskId, pdiskId] : replacedDisks) {
                    OldGroupContent.insert(pdiskId);
                }
            }

            TGroup ProcessExistingGroup(const TGroupDefinition& group, TString& error) {
                TGroup res(Topology.GetTotalVDisksNum());

                struct TExError { TString error; };

                try {
                    Traverse(group, [&](TVDiskIdShort vdisk, TPDiskId pdiskId) {
                        if (pdiskId != TPDiskId()) {
                            const ui32 orderNumber = Topology.GetOrderNumber(vdisk);

                            const auto it = Self.PDisks.find(pdiskId);
                            if (it == Self.PDisks.end()) {
                                throw TExError{TStringBuilder() << "existing group contains missing PDiskId# " << pdiskId};
                            }
                            TPDiskInfo& pdisk = it->second;
                            res[orderNumber] = &pdisk;

                            const auto [_, inserted] = OldGroupContent.insert(pdiskId);
                            if (!inserted) {
                                throw TExError{TStringBuilder() << "group contains duplicate PDiskId# " << pdiskId};
                            }

                            if (!pdisk.Decommitted) {
                                AddUsedDisk(pdisk);
                                GroupLayout.AddDisk(pdisk.Position, orderNumber);
                            }
                        }
                    });
                } catch (const TExError& e) {
                    error = e.error;
                    return {};
                }

                return res;
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

            bool DiskIsUsable(const TPDiskInfo& pdisk) const {
                if (!pdisk.IsUsable()) {
                    return false; // disk is not usable in this case
                }
                if (OldGroupContent.contains(pdisk.PDiskId) || ForbiddenDisks.contains(pdisk.PDiskId)) {
                    return false; // can't allow duplicate disks
                }
                if (RequireOperational && !pdisk.Operational) {
                    return false;
                }
                if (pdisk.SpaceAvailable < RequiredSpace) {
                    return false;
                }
                return true;
            }

            TPDiskByPosition SetupMatchingDisks(ui32 maxScore) {
                TPDiskByPosition res;
                res.reserve(Self.PDiskByPosition.size());

                ui32 realmGroupBegin = 0;
                ui32 realmBegin = 0;
                ui32 domainBegin = 0;
                TPDiskLayoutPosition prev;

                std::vector<ui32> numMatchingDisksInDomain(Self.DomainMapper.GetIdCount(), 0);
                for (const auto& [position, pdisk] : Self.PDiskByPosition) {
                    pdisk->Matching = pdisk->GetPickerScore() <= maxScore && DiskIsUsable(*pdisk);
                    if (pdisk->Matching) {
                        if (position.RealmGroup != prev.RealmGroup) {
                            for (; realmGroupBegin < res.size(); ++realmGroupBegin) {
                                res[realmGroupBegin].second->SkipToNextRealmGroup = res.size() - realmGroupBegin;
                            }
                        }
                        if (position.Realm != prev.Realm) {
                            for (; realmBegin < res.size(); ++realmBegin) {
                                res[realmBegin].second->SkipToNextRealm = res.size() - realmBegin;
                            }
                        }
                        if (position.Domain != prev.Domain) {
                            for (; domainBegin < res.size(); ++domainBegin) {
                                res[domainBegin].second->SkipToNextDomain = res.size() - domainBegin;
                            }
                        }
                        prev = position;

                        res.emplace_back(position, pdisk);
                        ++numMatchingDisksInDomain[position.Domain.Index()];
                    }
                }
                for (; realmGroupBegin < res.size(); ++realmGroupBegin) {
                    res[realmGroupBegin].second->SkipToNextRealmGroup = res.size() - realmGroupBegin;
                }
                for (; realmBegin < res.size(); ++realmBegin) {
                    res[realmBegin].second->SkipToNextRealm = res.size() - realmBegin;
                }
                for (; domainBegin < res.size(); ++domainBegin) {
                    res[domainBegin].second->SkipToNextDomain = res.size() - domainBegin;
                }
                for (const auto& [position, pdisk] : res) {
                    pdisk->NumDomainMatchingDisks = numMatchingDisksInDomain[position.Domain.Index()];
                }

                return std::move(res);
            }

            struct TUndoLog {
                struct TItem {
                    ui32 Index;
                    TPDiskInfo *PDisk;
                };

                std::vector<TItem> Items;

                void Log(ui32 index, TPDiskInfo *pdisk) {
                    Items.push_back({index, pdisk});
                }

                size_t GetPosition() const {
                    return Items.size();
                }
            };

            void AddDiskViaUndoLog(TUndoLog& undo, TGroup& group, ui32 index, TPDiskInfo *pdisk) {
                undo.Log(index, pdisk);
                group[index] = pdisk;
                AddUsedDisk(*pdisk);
                GroupLayout.AddDisk(pdisk->Position, index);
                WorstScore.reset(); // invalidate score
            }

            void Revert(TUndoLog& undo, TGroup& group, size_t until) {
                for (; undo.Items.size() > until; undo.Items.pop_back()) {
                    const auto& item = undo.Items.back();
                    group[item.Index] = nullptr;
                    RemoveUsedDisk(*item.PDisk);
                    GroupLayout.RemoveDisk(item.PDisk->Position, item.Index);
                    WorstScore.reset(); // invalidate score
                }
            }

            bool DiskIsBetter(const TPDiskInfo& pretender, const TPDiskInfo& king) const {
                if (pretender.NumSlots != king.NumSlots) {
                    return pretender.NumSlots < king.NumSlots;
                } else if (GivesLocalityBoost(pretender, king) || BetterQuotaMatch(pretender, king)) {
                    return true;
                } else {
                    if (pretender.NumDomainMatchingDisks != king.NumDomainMatchingDisks) {
                        return pretender.NumDomainMatchingDisks > king.NumDomainMatchingDisks;
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

        struct TAllocator : public TDiskManager {

            TAllocator(TImpl& self, const TGroupGeometryInfo& geom, i64 requiredSpace, bool requireOperational,
                    TForbiddenPDisks forbiddenDisks, const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks)
                : TDiskManager(self, geom, requiredSpace, requireOperational, forbiddenDisks, replacedDisks)
            {
            }

            bool FillInGroup(ui32 maxScore, TUndoLog& undo, TGroup& group, const TGroupConstraints& constraints) {
                // determine PDisks that fit our requirements (including score)
                auto v = SetupMatchingDisks(maxScore);

                // find which entities we need to allocate -- whole group, some realms, maybe some domains within specific realms?
                bool isEmptyGroup = true;
                std::vector<bool> isEmptyRealm(Topology.GetTotalFailRealmsNum(), true);
                std::vector<bool> isEmptyDomain(Topology.GetTotalFailDomainsNum(), true);
                for (ui32 orderNumber = 0; orderNumber < group.size(); ++orderNumber) {
                    if (group[orderNumber]) {
                        const TVDiskIdShort vdisk = Topology.GetVDiskId(orderNumber);
                        isEmptyGroup = false;
                        isEmptyRealm[vdisk.FailRealm] = false;
                        const ui32 domainIdx = Topology.GetFailDomainOrderNumber(vdisk);
                        isEmptyDomain[domainIdx] = false;
                    }
                }

                auto allocate = [&](auto what, ui32 index) {
                    TDynBitMap forbiddenEntities;
                    forbiddenEntities.Reserve(Self.DomainMapper.GetIdCount());
                    if (!AllocateWholeEntity(what, group, constraints, undo, index, {v.begin(), v.end()}, forbiddenEntities)) {
                        Revert(undo, group, 0);
                        return false;
                    }
                    return true;
                };

                if (isEmptyGroup) {
                    return allocate(TAllocateWholeGroup(), 0);
                }

                const ui32 numFailDomainsPerFailRealm = Topology.GetNumFailDomainsPerFailRealm();
                const ui32 numVDisksPerFailDomain = Topology.GetNumVDisksPerFailDomain();
                ui32 domainOrderNumber = 0;
                ui32 orderNumber = 0;

                // scan all fail realms and allocate missing realms or their parts
                for (ui32 failRealmIdx = 0; failRealmIdx < isEmptyRealm.size(); ++failRealmIdx) {
                    if (isEmptyRealm[failRealmIdx]) {
                        // we have an empty realm -- we have to allocate it fully
                        if (!allocate(TAllocateWholeRealm(), failRealmIdx)) {
                            return false;
                        }
                        // skip to next realm
                        domainOrderNumber += numFailDomainsPerFailRealm;
                        orderNumber += numVDisksPerFailDomain * numFailDomainsPerFailRealm;
                        continue;
                    }

                    // scan through domains of this realm, find unallocated ones
                    for (ui32 failDomainIdx = 0; failDomainIdx < numFailDomainsPerFailRealm; ++failDomainIdx, ++domainOrderNumber) {
                        if (isEmptyDomain[domainOrderNumber]) {
                            // try to allocate full domain
                            if (!allocate(TAllocateWholeDomain(), domainOrderNumber)) {
                                return false;
                            }
                            // skip to next domain
                            orderNumber += numVDisksPerFailDomain;
                            continue;
                        }

                        // scan individual disks of the domain and fill gaps
                        for (ui32 vdiskIdx = 0; vdiskIdx < numVDisksPerFailDomain; ++vdiskIdx, ++orderNumber) {
                            if (!group[orderNumber] && !allocate(TAllocateDisk(), orderNumber)) {
                                return false;
                            }
                        }
                    }
                }

                Y_ABORT_UNLESS(domainOrderNumber == Topology.GetTotalFailDomainsNum());
                Y_ABORT_UNLESS(orderNumber == Topology.GetTotalVDisksNum());

                return true;
            }

            using TAllocateResult = TPDiskLayoutPosition*;

            struct TAllocateDisk {};

            struct TAllocateWholeDomain {
                static constexpr auto GetEntityCount = &TBlobStorageGroupInfo::TTopology::GetNumVDisksPerFailDomain;
                using TNestedEntity = TAllocateDisk;

                static std::pair<TPDiskLayoutPosition, TPDiskLayoutPosition> MakeRange(const TPDiskLayoutPosition& x, TEntityId& scope) {
                    scope = x.Domain;
                    return {x, x};
                }
            };

            struct TAllocateWholeRealm {
                static constexpr auto GetEntityCount = &TBlobStorageGroupInfo::TTopology::GetNumFailDomainsPerFailRealm;
                using TNestedEntity = TAllocateWholeDomain;

                static std::pair<TPDiskLayoutPosition, TPDiskLayoutPosition> MakeRange(const TPDiskLayoutPosition& x, TEntityId& scope) {
                    scope = x.Realm;
                    return {{x.RealmGroup, x.Realm, TEntityId::Min()}, {x.RealmGroup, x.Realm, TEntityId::Max()}};
                }
            };

            struct TAllocateWholeGroup {
                static constexpr auto GetEntityCount = &TBlobStorageGroupInfo::TTopology::GetTotalFailRealmsNum;
                using TNestedEntity = TAllocateWholeRealm;

                static std::pair<TPDiskLayoutPosition, TPDiskLayoutPosition> MakeRange(const TPDiskLayoutPosition& x, TEntityId& scope) {
                    scope = x.RealmGroup;
                    return {{x.RealmGroup, TEntityId::Min(), TEntityId::Min()}, {x.RealmGroup, TEntityId::Max(), TEntityId::Max()}};
                }
            };

            using TDiskRange = std::pair<TPDiskByPosition::const_iterator, TPDiskByPosition::const_iterator>;

            template<typename T>
            TAllocateResult AllocateWholeEntity(T, TGroup& group, const TGroupConstraints& constraints, TUndoLog& undo, ui32 parentEntityIndex, TDiskRange range,
                    TDynBitMap& forbiddenEntities) {
                // number of enclosed child entities within this one
                const ui32 entityCount = (Topology.*T::GetEntityCount)();
                Y_ABORT_UNLESS(entityCount);
                parentEntityIndex *= entityCount;
                // remember current undo stack size
                const size_t undoPosition = undo.GetPosition();

                for (;;) {
                    auto [from, to] = range;
                    TPDiskLayoutPosition *prefix;
                    TEntityId scope;

                    for (ui32 index = 0;; ++index) {
                        // allocate nested entity
                        prefix = AllocateWholeEntity(typename T::TNestedEntity(), group, constraints, undo, parentEntityIndex + index,
                            {from, to}, forbiddenEntities);

                        if (prefix) {
                            if (!index) {
                                // reduce range to specific realm/domain entity
                                auto [min, max] = T::MakeRange(*prefix, scope);
                                from = std::lower_bound(from, to, min, TComparePDiskByPosition());
                                to = std::upper_bound(from, to, max, TComparePDiskByPosition());
                            }
                            if (index + 1 == entityCount) {
                                // disable filled entity from further selection if it was really allocated
                                forbiddenEntities.Set(scope.Index());
                                return prefix;
                            }
                        } else if (index) {
                            // disable just checked entity (to prevent its selection again)
                            forbiddenEntities.Set(scope.Index());
                            // try another entity at this level
                            Revert(undo, group, undoPosition);
                            // break the loop and retry
                            break;
                        } else {
                            // no chance to allocate new entity, exit
                            return {};
                        }
                    }
                }
            }

            bool CheckConstraints(
                const TPDiskInfo& pdisk,
                const TTargetDiskConstraints& constraints
            ) {
                return !constraints.NodeId.has_value() || constraints.NodeId.value() == pdisk.PDiskId.NodeId;
            }

            TAllocateResult AllocateWholeEntity(TAllocateDisk, TGroup& group, const TGroupConstraints& constraints, TUndoLog& undo, ui32 index, TDiskRange range,
                    TDynBitMap& forbiddenEntities) {
                TPDiskInfo *pdisk = group[index];
                Y_ABORT_UNLESS(!pdisk);
                auto process = [this, &pdisk](TPDiskInfo *candidate) {
                    if (!pdisk || DiskIsBetter(*candidate, *pdisk)) {
                        pdisk = candidate;
                    }
                };
                FindMatchingDiskBasedOnScore(process, group, constraints, index, range, forbiddenEntities);
                if (pdisk) {
                    AddDiskViaUndoLog(undo, group, index, pdisk);
                    pdisk->Matching = false;
                    return &pdisk->Position;
                } else {
                    return {};
                }
            }

            TScore CalculateWorstScoreWithCache(const TGroup& group) {
                if (!WorstScore) {
                    // find the worst disk from a position of layout correctness and use it as a milestone for other
                    // disks -- they can't be misplaced worse
                    TScore worstScore;
                    for (ui32 i = 0; i < Topology.GetTotalVDisksNum(); ++i) {
                        if (TPDiskInfo *pdisk = group[i]; pdisk && !pdisk->Decommitted) {
                            // calculate score for this pdisk, removing it from the set first -- to prevent counting itself
                            const TScore score = GroupLayout.GetExcludedDiskScore(pdisk->Position, i);
                            if (worstScore.BetterThan(score)) {
                                worstScore = score;
                            }
                        }
                    }
                    WorstScore = worstScore;
                }
                return *WorstScore;
            }

            template<typename TCallback>
            void FindMatchingDiskBasedOnScore(
                    TCallback&&   cb,                     // callback to be invoked for every matching candidate
                    const TGroup& group,                  // group with peer disks
                    const TGroupConstraints& constraints, // disk constraints for group
                    ui32          orderNumber,            // order number of disk being allocated
                    TDiskRange    range,                  // range of PDisk candidates to scan
                    TDynBitMap&   forbiddenEntities) {    // a set of forbidden TEntityId's prevented from allocation
                // first, find the best score for current group layout -- we can't make failure model inconsistency
                // any worse than it already is
                TScore bestScore = CalculateWorstScoreWithCache(group);
                const TTargetDiskConstraints& constraint = constraints[orderNumber];

                std::vector<TPDiskInfo*> candidates;

                // scan the candidate range
                while (range.first != range.second) {
                    const auto& [position, pdisk] = *range.first++;

                    // skip inappropriate disks, whole realm groups, realms and domains
                    if (!pdisk->Matching) {
                        // just do nothing, skip this candidate disk
                    } else if (!CheckConstraints(*pdisk, constraint)) {
                        // just do nothing, skip this candidate disk
                    } else if (forbiddenEntities[position.RealmGroup.Index()]) {
                        range.first += Min<ui32>(std::distance(range.first, range.second), pdisk->SkipToNextRealmGroup - 1);
                    } else if (forbiddenEntities[position.Realm.Index()]) {
                        range.first += Min<ui32>(std::distance(range.first, range.second), pdisk->SkipToNextRealm - 1);
                    } else if (forbiddenEntities[position.Domain.Index()]) {
                        range.first += Min<ui32>(std::distance(range.first, range.second), pdisk->SkipToNextDomain - 1);
                    } else {
                        const TScore score = GroupLayout.GetCandidateScore(position, orderNumber);
                        if (score.BetterThan(bestScore)) {
                            candidates.clear();
                            bestScore = score;
                        }
                        if (score.SameAs(bestScore)) {
                            candidates.push_back(pdisk);
                        }
                    }
                }

                for (TPDiskInfo *pdisk : candidates) {
                    cb(pdisk);
                }
            }
        };

        struct TSanitizer : public TDiskManager {
            ui32 DesiredRealmGroup;
            std::vector<ui32> RealmNavigator;
            // failRealm -> pRealm
            std::unordered_map<ui32, std::vector<ui32>> DomainCandidates;
            // pRealm -> {pDomain1, pDomain2, ... }, sorted by number of slots in pDomains
            std::unordered_map<ui32, std::unordered_map<ui32, std::vector<TPDiskInfo*>>> DiskCandidates;
            // {pRealm, pDomain} -> {pdisk1, pdisk2, ... }, sorted by DiskIsBetter() relation
            std::unordered_map<ui32, std::unordered_set<ui32>> BannedDomains;
            // pRealm -> {pDomain1, pDomain2, ... }
            // Cannot be a candidate, this domains are already placed correctly

            TSanitizer(TImpl& self, const TGroupGeometryInfo& geom, i64 requiredSpace, bool requireOperational,
                    TForbiddenPDisks forbiddenDisks, const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks)
                : TDiskManager(self, geom, requiredSpace, requireOperational, forbiddenDisks, replacedDisks)
            {
            }

            bool SetupNavigation(const TGroup& group) {
                TPDiskByPosition matchingDisks = SetupMatchingDisks(::Max<ui32>());
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

                for (ui32 orderNumber = 0; orderNumber < group.size(); ++orderNumber) {
                    if (group[orderNumber]) {
                        const TVDiskIdShort vdisk = Topology.GetVDiskId(orderNumber);
                        const ui32 pRealmGroup = group[orderNumber]->Position.RealmGroup.Index();
                        const ui32 pRealm = group[orderNumber]->Position.Realm.Index();
                        const ui32 pDomain = group[orderNumber]->Position.Domain.Index();
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

                for (const auto& [position, pdisk] : matchingDisks) {
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

                UpdateGroup(group);
                return true;
            }

            void UpdateGroup(const TGroup& group) {
                BannedDomains.clear();
                for (ui32 orderNumber = 0; orderNumber < group.size(); ++orderNumber) {
                    if (group[orderNumber]) {
                        const TVDiskIdShort vdisk = Topology.GetVDiskId(orderNumber);
                        const ui32 pRealm = group[orderNumber]->Position.Realm.Index();
                        const ui32 pDomain = group[orderNumber]->Position.Domain.Index();
                        if (pRealm == RealmNavigator[vdisk.FailRealm]) {
                            BannedDomains[pRealm].insert(pDomain);
                        }
                    }
                }
            }

            void SetupCandidates(ui32 maxScore) {
                TPDiskByPosition matchingDisks = SetupMatchingDisks(maxScore);
                DomainCandidates.clear();
                DiskCandidates.clear();

                std::unordered_map<ui32, std::unordered_map<ui32, ui32>> slotsInPDomain;
                // {pRealm, pDomain} -> #summary number of slots in ${pDomain, pRealm}

                for (const auto& [position, pdisk] : matchingDisks) {
                    if (position.RealmGroup.Index() == DesiredRealmGroup) {
                        ui32 pRealm = position.Realm.Index();
                        ui32 pDomain = position.Domain.Index();

                        if (BannedDomains[pRealm].count(pDomain) == 0) {
                            DomainCandidates[pRealm].push_back(pDomain);
                            DiskCandidates[pRealm][pDomain].push_back(pdisk);
                        }

                        slotsInPDomain[pRealm][pDomain] += pdisk->NumSlots;
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

            // if optional is empty, then all disks in group are placed correctly
            std::pair<TMisplacedVDisks::EFailLevel, std::vector<ui32>> FindMisplacedVDisks(const TGroup& group) {
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

                for (ui32 orderNum = 0; orderNum < group.size(); ++orderNum) {
                    if (group[orderNum]) {
                        ui32 pRealm = group[orderNum]->Position.Realm.Index();
                        ui32 pDomain = group[orderNum]->Position.Domain.Index();
                        TPDiskId pdisk = group[orderNum]->PDiskId;
                        domainInterlace[pRealm][pDomain]++;
                        diskInterlace[pdisk]++;
                    }
                }

                for (ui32 orderNum = 0; orderNum < group.size(); ++orderNum) {
                    if (group[orderNum]) {
                        const TVDiskIdShort vdisk = Topology.GetVDiskId(orderNum);
                        ui32 pRealm = group[orderNum]->Position.Realm.Index();
                        ui32 pDomain = group[orderNum]->Position.Domain.Index();
                        TPDiskId pdisk = group[orderNum]->PDiskId;
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

                for (ui32 orderNum = 0; orderNum < group.size(); ++orderNum) {
                    const TVDiskIdShort vdisk = Topology.GetVDiskId(orderNum);
                    ui32 pRealm = group[orderNum]->Position.Realm.Index();
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

                return {failLevel, misplacedVDisks};
            }

            std::optional<TPDiskId> TargetMisplacedVDisk(ui32 maxScore, const TGroup& group, const TVDiskIdShort& vdisk) {
                for (ui32 orderNumber = 0; orderNumber < group.size(); ++orderNumber) {
                    if (!group[orderNumber] && orderNumber != Topology.GetOrderNumber(vdisk)) {
                        return std::nullopt;
                    }
                }

                UpdateGroup(group);
                SetupCandidates(maxScore);

                ui32 failRealm = vdisk.FailRealm;
                ui32 pRealm = RealmNavigator[failRealm];

                const auto& domainCandidates = DomainCandidates[pRealm];
                TPDomainCandidatesRange pDomainRange = { domainCandidates.begin(), domainCandidates.end() };
                
                for (; pDomainRange.first != pDomainRange.second;) {
                    ui32 pDomain = *pDomainRange.first++;
                    const auto& diskCandidates = DiskCandidates[pRealm][pDomain];
                    
                    if (!diskCandidates.empty()) {
                        return (*diskCandidates.begin())->PDiskId;
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

    public:
        TImpl(TGroupGeometryInfo geom, bool randomize)
            : Geom(std::move(geom))
            , Randomize(randomize)
        {}

        bool RegisterPDisk(const TPDiskRecord& pdisk) {
            // calculate disk position
            const TPDiskLayoutPosition p(DomainMapper, pdisk.Location, pdisk.PDiskId, Geom);

            // insert PDisk into specific map
            TPDisks::iterator it;
            bool inserted;
            std::tie(it, inserted) = PDisks.try_emplace(pdisk.PDiskId, pdisk, p);
            if (inserted) {
                PDiskByPosition.emplace_back(it->second.Position, &it->second);
                Dirty = true;
            }

            return inserted;
        }

        void UnregisterPDisk(TPDiskId pdiskId) {
            const auto it = PDisks.find(pdiskId);
            Y_ABORT_UNLESS(it != PDisks.end());
            auto x = std::remove(PDiskByPosition.begin(), PDiskByPosition.end(), std::make_pair(it->second.Position, &it->second));
            Y_ABORT_UNLESS(x + 1 == PDiskByPosition.end());
            PDiskByPosition.pop_back();
            PDisks.erase(it);
        }

        void AdjustSpaceAvailable(TPDiskId pdiskId, i64 increment) {
            const auto it = PDisks.find(pdiskId);
            Y_ABORT_UNLESS(it != PDisks.end());
            it->second.SpaceAvailable += increment;
        }

        TString FormatPDisks(const TDiskManager& diskManager) const {
            TStringStream s;
            s << "PDisks# ";

            if (!PDiskByPosition.empty()) {
                s << "{[(";
                TPDiskLayoutPosition prevPosition = PDiskByPosition.front().first;
                const char *space = "";
                for (const auto& [position, pdisk] : PDiskByPosition) {
                    if (prevPosition != position) {
                        s << (prevPosition.Domain != position.Domain ? ")" : "")
                            << (prevPosition.Realm != position.Realm ? "]" : "")
                            << (prevPosition.RealmGroup != position.RealmGroup ? "} {" : "")
                            << (prevPosition.Realm != position.Realm ? "[" : "")
                            << (prevPosition.Domain != position.Domain ? "(" : "");
                        space = "";
                    }

                    s << std::exchange(space, " ") << pdisk->PDiskId;

                    if (diskManager.OldGroupContent.contains(pdisk->PDiskId)) {
                        s << "*";
                    }
                    const char *minus = "-";
                    if (diskManager.ForbiddenDisks.contains(pdisk->PDiskId)) {
                        s << std::exchange(minus, "") << "f";
                    }
                    if (!pdisk->Usable) {
                        s << std::exchange(minus, "") << pdisk->WhyUnusable;
                    }
                    if (pdisk->NumSlots >= pdisk->MaxSlots) {
                        s << std::exchange(minus, "") << "s[" << pdisk->NumSlots << "/" << pdisk->MaxSlots << "]";
                    }
                    if (pdisk->SpaceAvailable < diskManager.RequiredSpace) {
                        s << std::exchange(minus, "") << "v";
                    }
                    if (!pdisk->Operational) {
                        s << std::exchange(minus, "") << "o";
                    }
                    if (diskManager.DiskIsUsable(*pdisk)) {
                        s << "+";
                    }

                    prevPosition = position;
                }
                s << ")]}";
            } else {
                s << "<empty>";
            }

            return s.Str();
        }

        bool AllocateGroup(ui32 groupId, TGroupDefinition& groupDefinition, TGroupMapper::TGroupConstraintsDefinition& constraints,
                const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks, TForbiddenPDisks forbid, i64 requiredSpace, bool requireOperational,
                TString& error) {
            if (Dirty) {
                std::sort(PDiskByPosition.begin(), PDiskByPosition.end());
                Dirty = false;
            }

            // create group of required size, if it is not created yet
            if (!Geom.ResizeGroup(groupDefinition)) {
                error = "incorrect existing group";
                return false;
            }

            // fill in the allocation context
            TAllocator allocator(*this, Geom, requiredSpace, requireOperational, std::move(forbid), replacedDisks);
            TGroup group = allocator.ProcessExistingGroup(groupDefinition, error);
            TGroupConstraints groupConstraints = allocator.ProcessGroupConstraints(constraints);
            if (group.empty()) {
                return false;
            }
            bool ok = true;
            for (TPDiskInfo *pdisk : group) {
                if (!pdisk) {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                return true;
            }

            // calculate score table
            std::vector<ui32> scores;
            for (const auto& [pdiskId, pdisk] : PDisks) {
                if (allocator.DiskIsUsable(pdisk)) {
                    scores.push_back(pdisk.GetPickerScore());
                }
            }
            std::sort(scores.begin(), scores.end());
            scores.erase(std::unique(scores.begin(), scores.end()), scores.end());

            // bisect scores to find optimal working one
            std::optional<TGroup> result;
            ui32 begin = 0, end = scores.size();
            while (begin < end) {
                const ui32 mid = begin + (end - begin) / 2;
                TAllocator::TUndoLog undo;
                if (allocator.FillInGroup(scores[mid], undo, group, groupConstraints)) {
                    result = group;
                    allocator.Revert(undo, group, 0);
                    end = mid;
                } else {
                    begin = mid + 1;
                }
            }

            if (result) {
                for (const auto& [vdiskId, pdiskId] : replacedDisks) {
                    const auto it = PDisks.find(pdiskId);
                    Y_ABORT_UNLESS(it != PDisks.end());
                    TPDiskInfo& pdisk = it->second;
                    --pdisk.NumSlots;
                    pdisk.EraseGroup(groupId);
                }
                ui32 numZero = 0;
                for (ui32 i = 0; i < allocator.Topology.GetTotalVDisksNum(); ++i) {
                    if (!group[i]) {
                        ++numZero;
                        TPDiskInfo *pdisk = result->at(i);
                        ++pdisk->NumSlots;
                        pdisk->InsertGroup(groupId);
                    }
                }
                Y_ABORT_UNLESS(numZero == allocator.Topology.GetTotalVDisksNum() || numZero == replacedDisks.size());
                allocator.Decompose(*result, groupDefinition);
                return true;
            } else {
                error = "no group options " + FormatPDisks(allocator);
                return false;
            }
        }

        TMisplacedVDisks FindMisplacedVDisks(const TGroupDefinition& groupDefinition) {
            using EFailLevel = TMisplacedVDisks::EFailLevel;
            // create group of required size, if it is not created yet
            if (!Geom.CheckGroupSize(groupDefinition)) {
                return TMisplacedVDisks(EFailLevel::INCORRECT_LAYOUT, {}, "Incorrect group");
            }

            TSanitizer sanitizer(*this, Geom, 0, false, {}, {});
            TString error;
            TGroup group = sanitizer.ProcessExistingGroup(groupDefinition, error);
            if (group.empty()) {
                return TMisplacedVDisks(EFailLevel::INCORRECT_LAYOUT, {}, error);
            }
            if (!sanitizer.SetupNavigation(group)) {
                return TMisplacedVDisks(EFailLevel::INCORRECT_LAYOUT, {}, "Cannot map failRealms to pRealms");
            }

            sanitizer.SetupCandidates(::Max<ui32>());
            auto [failLevel, misplacedVDiskNums] = sanitizer.FindMisplacedVDisks(group);
            std::vector<TVDiskIdShort> misplacedVDisks;
            for (ui32 orderNum : misplacedVDiskNums) {
                misplacedVDisks.push_back(sanitizer.Topology.GetVDiskId(orderNum));
            }
            return TMisplacedVDisks(failLevel, misplacedVDisks);
        }

        std::optional<TPDiskId> TargetMisplacedVDisk(ui32 groupId, TGroupDefinition& groupDefinition, TVDiskIdShort vdisk, 
                TForbiddenPDisks forbid, i64 requiredSpace, bool requireOperational, TString& error) {
            if (Dirty) {
                std::sort(PDiskByPosition.begin(), PDiskByPosition.end());
                Dirty = false;
            }

            // create group of required size, if it is not created yet
            if (!Geom.CheckGroupSize(groupDefinition)) {
                error = "Incorrect group";
                return std::nullopt;
            }

            TSanitizer sanitizer(*this, Geom, requiredSpace, requireOperational, std::move(forbid), {});
            TGroup group = sanitizer.ProcessExistingGroup(groupDefinition, error);
            if (group.empty()) {
                error = "Empty group";
                return std::nullopt;
            }
            if (!sanitizer.SetupNavigation(group)) {
                error = "Cannot map failRealms to pRealms";
                return std::nullopt;
            }

            // calculate score table
            std::vector<ui32> scores;
            for (const auto& [pdiskId, pdisk] : PDisks) {
                if (sanitizer.DiskIsUsable(pdisk)) {
                    scores.push_back(pdisk.GetPickerScore());
                }
            }
            std::sort(scores.begin(), scores.end());
            scores.erase(std::unique(scores.begin(), scores.end()), scores.end());

            // bisect scores to find optimal working one
            sanitizer.SetupCandidates(::Max<ui32>());

            std::optional<TPDiskId> result;

            ui32 begin = 0, end = scores.size();
            while (begin < end) {
                const ui32 mid = begin + (end - begin) / 2;
                std::optional<TPDiskId> target;
                if ((target = sanitizer.TargetMisplacedVDisk(scores[mid], group, vdisk))) {
                    result = target;
                    end = mid;
                } else {
                    begin = mid + 1;
                }
            }

            if (result) {
                ui32 orderNum = sanitizer.Topology.GetOrderNumber(vdisk);
                if (group[orderNum]) {
                    TPDiskId pdiskId = group[orderNum]->PDiskId;
                    const auto it = PDisks.find(pdiskId);
                    Y_ABORT_UNLESS(it != PDisks.end());
                    TPDiskInfo& pdisk = it->second;
                    --pdisk.NumSlots;
                    pdisk.EraseGroup(groupId);
                }
                {
                    const auto it = PDisks.find(*result);
                    Y_ABORT_UNLESS(it != PDisks.end());
                    TPDiskInfo& pdisk = it->second;
                    ++pdisk.NumSlots;
                    pdisk.InsertGroup(groupId);
                    groupDefinition[vdisk.FailRealm][vdisk.FailDomain][vdisk.VDisk] = *result;
                }
                return result;
            }

            error = "Cannot replace vdisk";
            return std::nullopt;
        }
    };

    TGroupMapper::TGroupMapper(TGroupGeometryInfo geom, bool randomize)
        : Impl(new TImpl(std::move(geom), randomize))
    {}

    TGroupMapper::~TGroupMapper() = default;

    bool TGroupMapper::RegisterPDisk(const TPDiskRecord& pdisk) {
        return Impl->RegisterPDisk(pdisk);
    }

    void TGroupMapper::UnregisterPDisk(TPDiskId pdiskId) {
        return Impl->UnregisterPDisk(pdiskId);
    }

    void TGroupMapper::AdjustSpaceAvailable(TPDiskId pdiskId, i64 increment) {
        return Impl->AdjustSpaceAvailable(pdiskId, increment);
    }

    bool TGroupMapper::AllocateGroup(ui32 groupId, TGroupDefinition& group, TGroupMapper::TGroupConstraintsDefinition& constraints,
            const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks, TForbiddenPDisks forbid, i64 requiredSpace, bool requireOperational, TString& error) {
        return Impl->AllocateGroup(groupId, group, constraints, replacedDisks, std::move(forbid), requiredSpace, requireOperational, error);
    }

    bool TGroupMapper::AllocateGroup(ui32 groupId, TGroupDefinition& group, const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks,
            TForbiddenPDisks forbid, i64 requiredSpace, bool requireOperational, TString& error) {
        TGroupMapper::TGroupConstraintsDefinition emptyConstraints;
        return AllocateGroup(groupId, group, emptyConstraints, replacedDisks, std::move(forbid), requiredSpace, requireOperational, error);
    }

    TGroupMapper::TMisplacedVDisks TGroupMapper::FindMisplacedVDisks(const TGroupDefinition& group) {
        return Impl->FindMisplacedVDisks(group);
    }

    std::optional<TPDiskId> TGroupMapper::TargetMisplacedVDisk(TGroupId groupId, TGroupMapper::TGroupDefinition& group, 
            TVDiskIdShort vdisk, TForbiddenPDisks forbid, i64 requiredSpace, bool requireOperational, TString& error) {
        return Impl->TargetMisplacedVDisk(groupId.GetRawId(), group, vdisk, std::move(forbid), requiredSpace, requireOperational, error);
    }
} // NKikimr::NBsController
