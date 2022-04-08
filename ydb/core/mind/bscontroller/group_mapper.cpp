#include "group_mapper.h"
#include "group_geometry_info.h"
#include "group_layout_checker.h"

namespace NKikimr::NBsController {

    class TGroupMapper::TImpl : TNonCopyable {
        using TPDiskLayoutPosition = NLayoutChecker::TPDiskLayoutPosition;

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
                return Usable && !Decommitted && NumSlots < MaxSlots;
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

        struct TAllocator {
            TImpl& Self;
            const ui32 NumFailRealms;
            const ui32 NumFailDomainsPerFailRealm;
            const ui32 NumFailDomainsTotal;
            const ui32 NumVDisksPerFailDomain;
            const ui32 GroupSize;
            TStackVec<ui8, 32> RealmIdx;
            TStackVec<ui8, 32> DomainIdx;
            TStackVec<ui8, 32> DomainThroughIdx;
            TStackVec<ui8, 32> VDiskIdx;
            THashSet<TPDiskId> OldGroupContent; // set of all existing disks in the group, inclusing ones which are replaced
            const i64 RequiredSpace;
            const bool RequireOperational;
            TForbiddenPDisks ForbiddenDisks;
            THashMap<ui32, unsigned> LocalityFactor;
            NLayoutChecker::TGroupLayout GroupLayout;
            std::optional<NLayoutChecker::TScore> BestScore;

            TAllocator(TImpl& self, const TGroupGeometryInfo& geom, i64 requiredSpace, bool requireOperational,
                    TForbiddenPDisks forbiddenDisks, const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks)
                : Self(self)
                , NumFailRealms(geom.GetNumFailRealms())
                , NumFailDomainsPerFailRealm(geom.GetNumFailDomainsPerFailRealm())
                , NumFailDomainsTotal(NumFailRealms * NumFailDomainsPerFailRealm)
                , NumVDisksPerFailDomain(geom.GetNumVDisksPerFailDomain())
                , GroupSize(NumFailDomainsTotal * NumVDisksPerFailDomain)
                , RealmIdx(GroupSize)
                , DomainIdx(GroupSize)
                , DomainThroughIdx(GroupSize)
                , VDiskIdx(GroupSize)
                , RequiredSpace(requiredSpace)
                , RequireOperational(requireOperational)
                , ForbiddenDisks(std::move(forbiddenDisks))
                , GroupLayout(NumFailRealms, NumFailDomainsPerFailRealm)
            {
                for (const auto& [vdiskId, pdiskId] : replacedDisks) {
                    OldGroupContent.insert(pdiskId);
                }
                for (ui32 index = 0, domainThroughIdx = 0, realmIdx = 0; realmIdx < NumFailRealms; ++realmIdx) {
                    for (ui32 domainIdx = 0; domainIdx < NumFailDomainsPerFailRealm; ++domainIdx, ++domainThroughIdx) {
                        for (ui32 vdiskIdx = 0; vdiskIdx < NumVDisksPerFailDomain; ++vdiskIdx, ++index) {
                            RealmIdx[index] = realmIdx;
                            DomainIdx[index] = domainIdx;
                            DomainThroughIdx[index] = domainThroughIdx;
                            VDiskIdx[index] = vdiskIdx;
                        }
                    }
                }
            }

            TGroup ProcessExistingGroup(const TGroupDefinition& group, TString& error) {
                TGroup res(GroupSize);

                ui32 index = 0;
                for (const auto& realm : group) {
                    for (const auto& domain : realm) {
                        for (const auto& pdiskId : domain) {
                            if (pdiskId != TPDiskId()) {
                                const auto it = Self.PDisks.find(pdiskId);
                                if (it == Self.PDisks.end()) {
                                    error = TStringBuilder() << "existing group contains missing PDiskId# " << pdiskId;
                                    return {};
                                }
                                TPDiskInfo& pdisk = it->second;
                                res[index] = &pdisk;

                                const auto [_, inserted] = OldGroupContent.insert(pdiskId);
                                if (!inserted) {
                                    error = TStringBuilder() << "group contains duplicate PDiskId# " << pdiskId;
                                    return {};
                                }

                                if (!pdisk.Decommitted) {
                                    AddUsedDisk(pdisk);
                                    GroupLayout.AddDisk(pdisk.Position, RealmIdx[index], DomainIdx[index]);
                                }
                            }

                            ++index;
                        }
                    }
                }

                return res;
            }

            void Decompose(const TGroup& in, TGroupDefinition& out) {
                for (ui32 i = 0; i < GroupSize; ++i) {
                    out[RealmIdx[i]][DomainIdx[i]][VDiskIdx[i]] = in[i]->PDiskId;
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
                        ++numMatchingDisksInDomain[position.Domain];
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
                    pdisk->NumDomainMatchingDisks = numMatchingDisksInDomain[position.Domain];
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
                GroupLayout.AddDisk(pdisk->Position, RealmIdx[index], DomainIdx[index]);
                BestScore.reset(); // invalidate score
            }

            void Revert(TUndoLog& undo, TGroup& group, size_t until) {
                for (; undo.Items.size() > until; undo.Items.pop_back()) {
                    const auto& item = undo.Items.back();
                    group[item.Index] = nullptr;
                    RemoveUsedDisk(*item.PDisk);
                    GroupLayout.RemoveDisk(item.PDisk->Position, RealmIdx[item.Index], DomainIdx[item.Index]);
                    BestScore.reset(); // invalidate score
                }
            }

            bool FillInGroup(ui32 maxScore, TUndoLog& undo, TGroup& group) {
                // Determine PDisks that fit our requirements (including score).
                auto set = SetupMatchingDisks(maxScore);

                // Determine what we have to fill in -- full group, some realms, domains, or just some cells.
                bool emptyGroup = true;

                TDynBitMap emptyRealms;
                emptyRealms.Set(0, NumFailRealms);

                TDynBitMap emptyDomains;
                emptyDomains.Set(0, NumFailDomainsTotal);

                TDynBitMap emptyDisks;
                emptyDisks.Set(0, GroupSize);

                for (ui32 i = 0; i < GroupSize; ++i) {
                    if (group[i]) {
                        emptyGroup = false;
                        emptyRealms[RealmIdx[i]] = false;
                        emptyDomains[DomainThroughIdx[i]] = false;
                        emptyDisks[i] = false;
                    }
                }

                // Allocate new full group and exit if it is absolutely empty.
                auto allocate = [&](auto what, ui32 index) {
                    TDiskRange fullRange(set.begin(), set.end());
                    TDynBitMap forbiddenEntities;
                    forbiddenEntities.Reserve(Self.DomainMapper.GetIdCount());
                    if (!AllocateWholeEntity(what, group, undo, index, fullRange, forbiddenEntities)) {
                        Revert(undo, group, 0);
                        return false;
                    }
                    return true;
                };

                if (emptyGroup) {
                    return allocate(TAllocateWholeGroup(), 0);
                }

                // Fill in missing fail realms.
                for (ui32 i = emptyRealms.FirstNonZeroBit(); i != emptyRealms.Size(); i = emptyRealms.NextNonZeroBit(i)) {
                    if (!allocate(TAllocateWholeRealm(), i)) {
                        return false;
                    }

                    // remove excessive domains and disk from the set
                    emptyDomains.Reset(i * NumFailDomainsPerFailRealm, (i + 1) * NumFailDomainsPerFailRealm);
                    emptyDisks.Reset(i * NumFailDomainsPerFailRealm * NumVDisksPerFailDomain,
                        (i + 1) * NumFailDomainsPerFailRealm * NumVDisksPerFailDomain);
                }

                // Fill in missing fail domains in some partially filled realms.
                for (ui32 i = emptyDomains.FirstNonZeroBit(); i != emptyDomains.Size(); i = emptyDomains.NextNonZeroBit(i)) {
                    if (!allocate(TAllocateWholeDomain(), i)) {
                        return false;
                    }

                    // remove excessive disks
                    emptyDisks.Reset(i * NumVDisksPerFailDomain, (i + 1) * NumVDisksPerFailDomain);
                }

                // Fill in missing disk cells.
                for (ui32 i = emptyDisks.FirstNonZeroBit(); i != emptyDisks.Size(); i = emptyDisks.NextNonZeroBit(i)) {
                    if (!allocate(TAllocateDisk(), i)) {
                        return false;
                    }
                }

                return true;
            }

            struct TAllocateDisk {};

            struct TAllocateWholeDomain {
                static constexpr auto EntityCount = &TAllocator::NumVDisksPerFailDomain;
                static constexpr auto PositionItem = &TPDiskLayoutPosition::Domain;
                using TNestedEntity = TAllocateDisk;

                static std::pair<TPDiskLayoutPosition, TPDiskLayoutPosition> MakeRange(const TPDiskLayoutPosition& x) {
                    return {x, x};
                }
            };

            struct TAllocateWholeRealm {
                static constexpr auto EntityCount = &TAllocator::NumFailDomainsPerFailRealm;
                static constexpr auto PositionItem = &TPDiskLayoutPosition::Realm;
                using TNestedEntity = TAllocateWholeDomain;

                static std::pair<TPDiskLayoutPosition, TPDiskLayoutPosition> MakeRange(const TPDiskLayoutPosition& x) {
                    return {{x.RealmGroup, x.Realm, 0}, {x.RealmGroup, x.Realm, Max<ui32>()}};
                }
            };

            struct TAllocateWholeGroup {
                static constexpr auto EntityCount = &TAllocator::NumFailRealms;
                static constexpr auto PositionItem = &TPDiskLayoutPosition::RealmGroup;
                using TNestedEntity = TAllocateWholeRealm;

                static std::pair<TPDiskLayoutPosition, TPDiskLayoutPosition> MakeRange(const TPDiskLayoutPosition& x) {
                    return {{x.RealmGroup, 0, 0}, {x.RealmGroup, Max<ui32>(), Max<ui32>()}};
                }
            };

            using TDiskRange = std::pair<TPDiskByPosition::const_iterator, TPDiskByPosition::const_iterator>;

            template<typename T>
            TPDiskLayoutPosition *AllocateWholeEntity(T, TGroup& group, TUndoLog& undo, ui32 parentEntityIndex,
                    TDiskRange range, TDynBitMap& forbiddenEntities) {
                const TDiskRange originalRange(range);
                const size_t undoPosition = undo.GetPosition();
                TPDiskLayoutPosition *prefix = nullptr;
                ui32 currentEntityId = Max<ui32>();
                for (ui32 index = 0, num = this->*T::EntityCount; index < num; ) {
                    // allocate nested entity
                    prefix = AllocateWholeEntity(typename T::TNestedEntity(), group, undo,
                        parentEntityIndex * num + index, range, forbiddenEntities);
                    if (prefix) {
                        if (!index) {
                            currentEntityId = prefix->*T::PositionItem;
                            auto [min, max] = T::MakeRange(*prefix);
                            range.first = std::lower_bound(range.first, range.second, min, TComparePDiskByPosition());
                            range.second = std::upper_bound(range.first, range.second, max, TComparePDiskByPosition());
                        }
                        ++index;
                    } else if (index) {
                        // disable just checked entity (to prevent its selection again)
                        Y_VERIFY(currentEntityId != Max<ui32>());
                        forbiddenEntities.Set(currentEntityId);
                        // try another entity at this level
                        Revert(undo, group, undoPosition);
                        // revert original wide range and start from the beginning
                        range = originalRange;
                        index = 0;
                        currentEntityId = Max<ui32>();
                    } else {
                        // no chance to allocate new entity, exit
                        return nullptr;
                    }
                }
                // disable filled entity from further selection
                Y_VERIFY(prefix && currentEntityId != Max<ui32>());
                forbiddenEntities.Set(currentEntityId);
                return prefix;
            }

            TPDiskLayoutPosition *AllocateWholeEntity(TAllocateDisk, TGroup& group, TUndoLog& undo, ui32 index,
                    TDiskRange range, TDynBitMap& forbiddenEntities) {
                TPDiskInfo *pdisk = nullptr;
                auto process = [this, &pdisk](TPDiskInfo *candidate) {
                    if (!pdisk || DiskIsBetter(*candidate, *pdisk)) {
                        pdisk = candidate;
                    }
                };
                FindMatchingDiskBasedOnScore(process, group, RealmIdx[index], DomainIdx[index],
                    range, forbiddenEntities);
                if (pdisk) {
                    AddDiskViaUndoLog(undo, group, index, pdisk);
                    pdisk->Matching = false;
                    return &pdisk->Position;
                } else {
                    return nullptr;
                }
            }

            NLayoutChecker::TScore CalculateBestScoreWithCache(const TGroup& group) {
                if (!BestScore) {
                    // find the worst disk from a position of layout correctness and use it as a milestone for other
                    // disks -- they can't be misplaced worse
                    NLayoutChecker::TScore bestScore;
                    for (ui32 i = 0; i < GroupSize; ++i) {
                        if (TPDiskInfo *pdisk = group[i]; pdisk && !pdisk->Decommitted) {
                            NLayoutChecker::TScore score = GroupLayout.GetCandidateScore(pdisk->Position, RealmIdx[i],
                                DomainIdx[i]);
                            if (bestScore.BetterThan(score)) {
                                bestScore = score;
                            }
                        }
                    }
                    BestScore = bestScore;
                }
                return *BestScore;
            }

            template<typename TCallback>
            void FindMatchingDiskBasedOnScore(TCallback&& cb, const TGroup& group, ui32 failRealmIdx, ui32 failDomainIdx,
                    TDiskRange range, TDynBitMap& forbiddenEntities) {
                NLayoutChecker::TScore bestScore = CalculateBestScoreWithCache(group);

                std::vector<TPDiskInfo*> candidates;

                while (range.first != range.second) {
                    const auto& [position, pdisk] = *range.first++;

                    if (!pdisk->Matching) {
                        continue;
                    } else if (forbiddenEntities[position.RealmGroup]) {
                        range.first += Min<ui32>(std::distance(range.first, range.second), pdisk->SkipToNextRealmGroup - 1);
                        continue;
                    } else if (forbiddenEntities[position.Realm]) {
                        range.first += Min<ui32>(std::distance(range.first, range.second), pdisk->SkipToNextRealm - 1);
                        continue;
                    } else if (forbiddenEntities[position.Domain]) {
                        range.first += Min<ui32>(std::distance(range.first, range.second), pdisk->SkipToNextDomain - 1);
                        continue;
                    }

                    NLayoutChecker::TScore score = GroupLayout.GetCandidateScore(position, failRealmIdx, failDomainIdx);
                    if (score.BetterThan(bestScore)) {
                        candidates.clear();
                        candidates.push_back(pdisk);
                        bestScore = score;
                    } else if (score.SameAs(bestScore)) {
                        candidates.push_back(pdisk);
                    }
                }

                for (TPDiskInfo *pdisk : candidates) {
                    cb(pdisk);
                }
            }

            bool DiskIsBetter(TPDiskInfo& pretender, TPDiskInfo& king) const {
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

            bool GivesLocalityBoost(TPDiskInfo& pretender, TPDiskInfo& king) const {
                const ui32 a = GetLocalityFactor(pretender);
                const ui32 b = GetLocalityFactor(king);
                return Self.Randomize ? a < b : a > b;
            }

            bool BetterQuotaMatch(TPDiskInfo& pretender, TPDiskInfo& king) const {
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

    private:
        const TGroupGeometryInfo Geom;
        const bool Randomize;
        NLayoutChecker::TDomainMapper DomainMapper;
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
            Y_VERIFY(it != PDisks.end());
            auto x = std::remove(PDiskByPosition.begin(), PDiskByPosition.end(), std::make_pair(it->second.Position, &it->second));
            Y_VERIFY(x + 1 == PDiskByPosition.end());
            PDiskByPosition.pop_back();
            PDisks.erase(it);
        }

        void AdjustSpaceAvailable(TPDiskId pdiskId, i64 increment) {
            const auto it = PDisks.find(pdiskId);
            Y_VERIFY(it != PDisks.end());
            it->second.SpaceAvailable += increment;
        }

        TString FormatPDisks(const TAllocator& allocator) const {
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

                    if (allocator.OldGroupContent.contains(pdisk->PDiskId)) {
                        s << "*";
                    }
                    const char *minus = "-";
                    if (allocator.ForbiddenDisks.contains(pdisk->PDiskId)) {
                        s << std::exchange(minus, "") << "f";
                    }
                    if (!pdisk->Usable) {
                        s << std::exchange(minus, "") << "u";
                    }
                    if (pdisk->Decommitted) {
                        s << std::exchange(minus, "") << "d";
                    }
                    if (pdisk->NumSlots >= pdisk->MaxSlots) {
                        s << std::exchange(minus, "") << "s[" << pdisk->NumSlots << "/" << pdisk->MaxSlots << "]";
                    }
                    if (pdisk->SpaceAvailable < allocator.RequiredSpace) {
                        s << std::exchange(minus, "") << "v";
                    }
                    if (!pdisk->Operational) {
                        s << std::exchange(minus, "") << "o";
                    }
                    if (allocator.DiskIsUsable(*pdisk)) {
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

        bool AllocateGroup(ui32 groupId, TGroupDefinition& groupDefinition, const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks,
                TForbiddenPDisks forbid, i64 requiredSpace, bool requireOperational,
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
                if (allocator.FillInGroup(scores[mid], undo, group)) {
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
                    Y_VERIFY(it != PDisks.end());
                    TPDiskInfo& pdisk = it->second;
                    --pdisk.NumSlots;
                    pdisk.EraseGroup(groupId);
                }
                ui32 numZero = 0;
                for (ui32 i = 0; i < allocator.GroupSize; ++i) {
                    if (!group[i]) {
                        ++numZero;
                        TPDiskInfo *pdisk = result->at(i);
                        ++pdisk->NumSlots;
                        pdisk->InsertGroup(groupId);
                    }
                }
                Y_VERIFY(numZero == allocator.GroupSize || numZero == replacedDisks.size());
                allocator.Decompose(*result, groupDefinition);
                return true;
            } else {
                error = "no group options " + FormatPDisks(allocator);
                return false;
            }
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

    bool TGroupMapper::AllocateGroup(ui32 groupId, TGroupDefinition& group, const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks,
            TForbiddenPDisks forbid, i64 requiredSpace, bool requireOperational, TString& error) {
        return Impl->AllocateGroup(groupId, group, replacedDisks, std::move(forbid), requiredSpace, requireOperational, error);
    }

} // NKikimr::NBsController
