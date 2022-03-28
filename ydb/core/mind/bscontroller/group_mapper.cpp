#include "group_mapper.h"
#include "group_geometry_info.h"

namespace NKikimr::NBsController {

    class TGroupMapper::TImpl : TNonCopyable {
        class TDomainMapper {
            std::unordered_map<TString, ui32> FailDomainId;

        public:
            ui32 operator ()(TString item) {
                return FailDomainId.emplace(std::move(item), FailDomainId.size() + 1).first->second;
            }
        };

        struct TPDiskLayoutPosition {
            ui32 RealmGroup = 0;
            ui32 RealmInGroup = 0;
            ui32 DomainGroup = 0;
            ui32 DomainInGroup = 0;

            TPDiskLayoutPosition() = default;

            TPDiskLayoutPosition(TDomainMapper& mapper, const TNodeLocation& location, TPDiskId pdiskId, const TGroupGeometryInfo& geom) {
                TStringStream realmGroup, realmInGroup, domainGroup, domainInGroup;
                const std::pair<int, TStringStream*> levels[] = {
                    {geom.GetRealmLevelBegin(), &realmGroup},
                    {geom.GetRealmLevelEnd(), &realmInGroup},
                    {geom.GetDomainLevelBegin(), &domainGroup},
                    {geom.GetDomainLevelEnd(), &domainInGroup}
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
                RealmInGroup = mapper(realmInGroup.Str());
                DomainGroup = mapper(domainGroup.Str());
                DomainInGroup = mapper(domainInGroup.Str());
            }

            auto AsTuple() const {
                return std::tie(RealmGroup, RealmInGroup, DomainGroup, DomainInGroup);
            }

            friend bool operator <(const TPDiskLayoutPosition& x, const TPDiskLayoutPosition& y) {
                return x.AsTuple() < y.AsTuple();
            }

            size_t GetDiffIndex(const TPDiskLayoutPosition& other) const {
                return RealmGroup != other.RealmGroup ? 0 :
                    RealmInGroup != other.RealmInGroup ? 1 :
                    DomainGroup != other.DomainGroup ? 2 :
                    DomainInGroup != other.DomainInGroup ? 3 : 4;
            }
        };

        struct TFailDomainInfo;

        struct TPDiskInfo : TPDiskRecord {
            TPDiskLayoutPosition Position;
            TFailDomainInfo *FailDomain;
            bool Matching = false;

            TPDiskInfo(const TPDiskRecord& pdisk, TPDiskLayoutPosition position, TFailDomainInfo *failDomain)
                : TPDiskRecord(pdisk)
                , Position(std::move(position))
                , FailDomain(failDomain)
            {
                std::sort(Groups.begin(), Groups.end());
            }

            TString ToString() const {
                return Location.ToString();
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

        struct TFailDomainInfo : std::vector<TPDisks::value_type*> {
            bool Matching;
            ui32 NumMatchingDisks;
        };

        struct TFailDomainGroup : std::unordered_map<ui32, TFailDomainInfo> {
            bool Matching;
        };

        struct TFailRealmInfo : std::unordered_map<ui32, TFailDomainGroup> {
            bool Matching;
        };

        struct TFailRealmGroup : std::unordered_map<ui32, TFailRealmInfo> {
            bool Matching;
        };

        struct TBox : std::unordered_map<ui32, TFailRealmGroup> {
            auto& operator ()(const TPDiskLayoutPosition& p) {
                return (*this)[p.RealmGroup][p.RealmInGroup][p.DomainGroup][p.DomainInGroup];
            }
        };

        struct TAllocateContext {
            const ui32 NumFailRealms;
            const ui32 NumFailDomainsPerFailRealm;
            ui32 RealmGroup = 0; // the realm group we are forced to use (if forced, of course)
            TStackVec<ui32, 8> RealmInGroup; // indexed by realm number
            TStackVec<ui32, 8> DomainGroup; // indexed by realm number
            TStackVec<ui32, 32> DomainInGroup; // indexed by realm/domain
            THashSet<TPDiskId> OldGroupContent; // set of all existing disks in the group, inclusing ones which are replaced
            THashSet<TPDiskId> NewGroupContent; // newly generated group content
            const i64 RequiredSpace;
            const bool RequireOperational;
            TForbiddenPDisks Forbid;

            TAllocateContext(const TGroupGeometryInfo& geom, i64 requiredSpace, bool requireOperational,
                    TForbiddenPDisks forbid)
                : NumFailRealms(geom.GetNumFailRealms())
                , NumFailDomainsPerFailRealm(geom.GetNumFailDomainsPerFailRealm())
                , RealmInGroup(NumFailRealms, 0)
                , DomainGroup(NumFailRealms, 0)
                , DomainInGroup(NumFailRealms * NumFailDomainsPerFailRealm, 0)
                , RequiredSpace(requiredSpace)
                , RequireOperational(requireOperational)
                , Forbid(std::move(forbid))
            {}

            bool ProcessExistingGroup(const TGroupDefinition& group, const TPDisks& pdisks, const TPDiskId replacedDiskIds[],
                   size_t numReplacedDisks, TString& error) {
                OldGroupContent = {replacedDiskIds, replacedDiskIds + numReplacedDisks};

                for (ui32 failRealmIdx = 0, domainThroughIdx = 0; failRealmIdx < group.size(); ++failRealmIdx) {
                    const auto& realm = group[failRealmIdx];
                    for (ui32 failDomainIdx = 0; failDomainIdx < realm.size(); ++failDomainIdx, ++domainThroughIdx) {
                        const auto& domain = realm[failDomainIdx];
                        for (const TPDiskId pdiskId : domain) {
                            if (pdiskId != TPDiskId()) {
                                // add to used pdisk set
                                const bool inserted = OldGroupContent.insert(pdiskId).second;
                                Y_VERIFY(inserted);

                                // find existing pdisk
                                auto it = pdisks.find(pdiskId);
                                if (it == pdisks.end()) {
                                    error = TStringBuilder() << "existing group contains missing PDisks";
                                    return false;
                                }
                                const TPDiskInfo& pdisk = it->second;

                                // register the disk in context
                                if (!pdisk.Decommitted && !AddDisk(pdisk, failRealmIdx, domainThroughIdx, error)) {
                                    return false;
                                }
                            }
                        }
                    }
                }
                return true;
            }

            bool AddDisk(const TPDiskInfo& pdisk, ui32 failRealmIdx, ui32 domainThroughIdx, TString& error) {
                auto update = [](ui32& place, ui32 value) {
                    const ui32 prev = std::exchange(place, value);
                    return !prev || prev == value;
                };
                if (!update(RealmGroup, pdisk.Position.RealmGroup)) {
                    error = "group contains PDisks from different realm groups";
                } else if (!update(RealmInGroup[failRealmIdx], pdisk.Position.RealmInGroup)) {
                    error = "group contains PDisks from different realms within same realm";
                } else if (!update(DomainGroup[failRealmIdx], pdisk.Position.DomainGroup)) {
                    error = "group contains PDisks from different domain groups within same realm";
                } else if (!update(DomainInGroup[domainThroughIdx], pdisk.Position.DomainInGroup)) {
                    error = "group contains PDisks from different domain groups within same realm";
                } else if (!NewGroupContent.insert(pdisk.PDiskId).second) {
                    error = "group contains duplicate PDisks";
                } else {
                    return true;
                }
                return false;
            }

            bool DiskIsUsable(const TPDiskInfo& pdisk) const {
                if (!pdisk.IsUsable()) {
                    return false; // disk is not usable in this case
                }
                if (OldGroupContent.count(pdisk.PDiskId) || NewGroupContent.count(pdisk.PDiskId) || Forbid.count(pdisk.PDiskId)) {
                    return false; // can't allow duplicate disks
                }
                if (RequireOperational && !pdisk.Operational) {
                    return false;
                }
                return pdisk.SpaceAvailable >= RequiredSpace;
            }
        };

        class THelper {
            TImpl& Self;
            TAllocateContext& Ctx;
            std::unordered_map<ui32, unsigned> LocalityFactor;

        public:
            THelper(TImpl& self, TAllocateContext& ctx)
                : Self(self)
                , Ctx(ctx)
            {
                for (const TPDiskId& pdiskId : Ctx.NewGroupContent) {
                    if (const auto it = Self.PDisks.find(pdiskId); it != Self.PDisks.end()) {
                        for (ui32 groupId : it->second.Groups) {
                            ++LocalityFactor[groupId];
                        }
                    } else {
                        Y_FAIL();
                    }
                }
                if (const ui32 id = Ctx.RealmGroup) {
                    auto& realmGroup = Self.Box.at(id);
                    Y_VERIFY_DEBUG(realmGroup.Matching);
                    realmGroup.Matching = false;
                    for (size_t i = 0; i < Ctx.NumFailRealms; ++i) {
                        if (const ui32 id = Ctx.RealmInGroup[i]) {
                            auto& realm = realmGroup.at(id);
                            Y_VERIFY_DEBUG(realm.Matching);
                            realm.Matching = false;
                            if (const ui32 id = Ctx.DomainGroup[i]) {
                                auto& domainGroup = realm.at(id);
                                Y_VERIFY_DEBUG(domainGroup.Matching);
                                domainGroup.Matching = false;
                                for (size_t j = 0; j < Ctx.NumFailDomainsPerFailRealm; ++j) {
                                    const size_t domainThroughIdx = i * Ctx.NumFailDomainsPerFailRealm + j;
                                    if (const ui32 id = Ctx.DomainInGroup[domainThroughIdx]) {
                                        auto& domain = domainGroup.at(id);
                                        Y_VERIFY_DEBUG(domain.Matching);
                                        domain.Matching = false;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            TPDiskId AddBestDisk(ui32 realmIdx, ui32 domainThroughIdx) {
                TPDiskInfo *pdisk = nullptr;
                auto descend = [&](auto& level, ui32& id, ui32 TPDiskLayoutPosition::*pos) -> auto& {
                    if (!id) {
                        pdisk = pdisk ? pdisk : FindBestDisk(level);
                        id = pdisk->Position.*pos;
                        level[id].Matching = false; // do not allow this level for further selection
                    } else if (pdisk) {
                        Y_VERIFY(id == pdisk->Position.*pos);
                    }
                    return level[id];
                };
                auto& realmGroup = descend(Self.Box, Ctx.RealmGroup, &TPDiskLayoutPosition::RealmGroup);
                auto& realm = descend(realmGroup, Ctx.RealmInGroup[realmIdx], &TPDiskLayoutPosition::RealmInGroup);
                auto& domainGroup = descend(realm, Ctx.DomainGroup[realmIdx], &TPDiskLayoutPosition::DomainGroup);
                auto& domain = descend(domainGroup, Ctx.DomainInGroup[domainThroughIdx], &TPDiskLayoutPosition::DomainInGroup);
                pdisk = pdisk ? pdisk : FindBestDisk(domain);

                TString error;
                const bool success = Ctx.AddDisk(*pdisk, realmIdx, domainThroughIdx, error);
                Y_VERIFY(success, "AddDisk: %s", error.data());
                pdisk->Matching = false; // disable this disk for further selection

                AddUsedDisk(*pdisk);

                return pdisk->PDiskId;
            }

        private:
            template<typename T>
            TPDiskInfo *FindBestDisk(T& level) {
                TPDiskInfo *res = nullptr;
                for (auto& [id, item] : level) {
                    if (item.Matching) {
                        TPDiskInfo *candidate = FindBestDisk(item);
                        Y_VERIFY(candidate);
                        res = !res || DiskIsBetter(*candidate, *res) ? candidate : res;
                    }
                }
                Y_VERIFY(res);
                return res;
            }

            TPDiskInfo *FindBestDisk(TFailDomainInfo& level) {
                TPDiskInfo *res = nullptr;
                for (TPDisks::value_type *it : level) {
                    auto& [pdiskId, pdisk] = *it;
                    if (pdisk.Matching) {
                        res = !res || DiskIsBetter(pdisk, *res) ? &pdisk : res;
                    }
                }
                Y_VERIFY(res);
                return res;
            }

            bool DiskIsBetter(TPDiskInfo& pretender, TPDiskInfo& king) const {
                if (pretender.NumSlots != king.NumSlots) {
                    return pretender.NumSlots < king.NumSlots;
                } else if (GivesLocalityBoost(pretender, king) || BetterQuotaMatch(pretender, king)) {
                    return true;
                } else {
                    const TFailDomainInfo& pretenderDomain = Self.Box(pretender.Position);
                    const TFailDomainInfo& kingDomain = Self.Box(king.Position);
                    if (pretenderDomain.NumMatchingDisks != kingDomain.NumMatchingDisks) {
                        return pretenderDomain.NumMatchingDisks > kingDomain.NumMatchingDisks;
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
        TDomainMapper DomainMapper;
        TPDisks PDisks;
        TBox Box;

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
            std::tie(it, inserted) = PDisks.try_emplace(pdisk.PDiskId, pdisk, p, &Box(p));
            if (inserted) {
                it->second.FailDomain->push_back(&*it);
            }

            return inserted;
        }

        void UnregisterPDisk(TPDiskId pdiskId) {
            const auto it = PDisks.find(pdiskId);
            Y_VERIFY(it != PDisks.end());
            TFailDomainInfo& fdom = *it->second.FailDomain;
            bool erased = false;
            for (auto x = fdom.begin(); x != fdom.end(); ++x) {
                if (*x == &*it) {
                    fdom.erase(x);
                    erased = true;
                    break;
                }
            }
            Y_VERIFY(erased);
            PDisks.erase(it);
        }

        void AdjustSpaceAvailable(TPDiskId pdiskId, i64 increment) {
            const auto it = PDisks.find(pdiskId);
            Y_VERIFY(it != PDisks.end());
            it->second.SpaceAvailable += increment;
        }

        TString FormatPDisks(const TAllocateContext& ctx) const {
            TStringStream s;
            s << "PDisks# ";

            bool first1 = true;
            for (const auto& [id, realmGroup] : Box) {
                s << (std::exchange(first1, false) ? "" : " | ") << "<";
                bool first2 = true;
                for (const auto& [id, realm] : realmGroup) {
                    s << (std::exchange(first2, false) ? "" : " ") << "{";
                    bool first3 = true;
                    for (const auto& [id, domainGroup] : realm) {
                        s << (std::exchange(first3, false) ? "" : " | ") << "[";
                        bool first4 = true;
                        for (const auto& [id, domain] : domainGroup) {
                            if (!domain.empty()) {
                                s << (std::exchange(first4, false) ? "" : " ") << "(";
                                std::vector<TPDisks::value_type*> v(domain);
                                auto comp = [](const auto *x, const auto *y) { return x->first < y->first; };
                                std::sort(v.begin(), v.end(), comp);
                                bool first5 = true;
                                for (const TPDisks::value_type *it : v) {
                                    s << (std::exchange(first5, false) ? "" : " ")
                                        << it->first.NodeId << ":" << it->first.PDiskId;
                                    if (ctx.OldGroupContent.count(it->first)) {
                                        s << "*";
                                    }
                                    const char *minus = "-";
                                    if (ctx.Forbid.count(it->second.PDiskId)) {
                                        s << std::exchange(minus, "") << "f";
                                    }
                                    if (!it->second.Usable) {
                                        s << std::exchange(minus, "") << "u";
                                    }
                                    if (it->second.Decommitted) {
                                        s << std::exchange(minus, "") << "d";
                                    }
                                    if (it->second.NumSlots >= it->second.MaxSlots) {
                                        s << std::exchange(minus, "") << "m";
                                    }
                                    if (it->second.NumSlots >= it->second.MaxSlots) {
                                        s << std::exchange(minus, "") << "s";
                                    }
                                    if (it->second.SpaceAvailable < ctx.RequiredSpace) {
                                        s << std::exchange(minus, "") << "v";
                                    }
                                    if (!it->second.Operational) {
                                        s << std::exchange(minus, "") << "o";
                                    }
                                    if (ctx.DiskIsUsable(it->second)) {
                                        s << "+";
                                    }
                                }
                                s << ")";
                            }
                        }
                        s << "]";
                    }
                    s << "}";
                }
                s << ">";
            }

            return s.Str();
        }

        bool AllocateGroup(ui32 groupId, TGroupDefinition& group, const TPDiskId replacedDiskIds[],
                size_t numReplacedDisks, TForbiddenPDisks forbid, i64 requiredSpace, bool requireOperational,
                TString& error) {
            // fill in the allocation context
            TAllocateContext ctx(Geom, requiredSpace, requireOperational, std::move(forbid));
            if (!ctx.ProcessExistingGroup(group, PDisks, replacedDiskIds, numReplacedDisks, error)) {
                return false;
            }

            // create group of required size, if it is not created yet
            if (!Geom.ResizeGroup(group)) {
                error = "incorrect existing group";
                return false;
            }

            // if the group is already created, check for missing entities
            bool hasMissingEntities = false;
            for (const auto& realm : group) {
                for (const auto& domain : realm) {
                    for (const TPDiskId& pdiskId : domain) {
                        if (pdiskId == TPDiskId()) {
                            hasMissingEntities = true;
                            break;
                        }
                    }
                    if (hasMissingEntities) {
                        break;
                    }
                }
                if (hasMissingEntities) {
                    break;
                }
            }
            if (!hasMissingEntities) {
                return true; // group is okay
            }

            // adjust number of slots
            for (TPDiskId pdiskId : ctx.OldGroupContent) {
                --PDisks.at(pdiskId).NumSlots;
            }
            for (size_t i = 0; i < numReplacedDisks; ++i) {
                PDisks.at(replacedDiskIds[i]).EraseGroup(groupId);
            }

            // check if we can map a group only with PDisks that have NumSlots <= nums
            if (!FindMatchingDisks(ctx)) {
                // undo changes to the mapper content
                for (TPDiskId pdiskId : ctx.OldGroupContent) {
                    ++PDisks.at(pdiskId).NumSlots;
                }
                for (size_t i = 0; i < numReplacedDisks; ++i) {
                    PDisks.at(replacedDiskIds[i]).InsertGroup(groupId);
                }
                error = "no group options " + FormatPDisks(ctx);
                return false;
            }

            // fill in missing PDisk entities
            THelper helper(*this, ctx);
            for (ui32 realmIdx = 0, domainThroughIdx = 0; realmIdx < ctx.NumFailRealms; ++realmIdx) {
                for (ui32 domainIdx = 0; domainIdx < ctx.NumFailDomainsPerFailRealm; ++domainIdx, ++domainThroughIdx) {
                    for (TPDiskId& pdiskId : group[realmIdx][domainIdx]) {
                        if (pdiskId == TPDiskId()) {
                            pdiskId = helper.AddBestDisk(realmIdx, domainThroughIdx);
                        }
                    }
                }
            }

            // adjust number of slots
            for (TPDiskId pdiskId : ctx.NewGroupContent) {
                if (const auto it = PDisks.find(pdiskId); it != PDisks.end()) {
                    ++it->second.NumSlots;
                    it->second.InsertGroup(groupId);
                } else {
                    Y_FAIL();
                }
            }

            return true;
        }

        class TScorePicker {
            static constexpr size_t MaxStaticItems = 16;
            ui32 NumFake = 0;
            TStackVec<ui32, MaxStaticItems> StaticHist;
            std::map<ui32, ui32> ScoreHist;

        public:
            static constexpr ui32 FakeScore = Max<ui32>();

        public:
            void AddFake() {
                ++NumFake;
            }

            void Add(ui32 score) {
                if (score < MaxStaticItems) {
                    if (StaticHist.size() <= score) {
                        StaticHist.resize(score + 1);
                    }
                    ++StaticHist[score];
                } else {
                    ++ScoreHist[score];
                }
            }

            std::optional<ui32> CalculateMaxScore(ui32 threshold) const {
                if (threshold <= NumFake) {
                    return FakeScore;
                } else {
                    threshold -= NumFake;
                }
                for (size_t score = 0; score < StaticHist.size(); ++score) {
                    if (threshold <= StaticHist[score]) {
                        return score;
                    } else {
                        threshold -= StaticHist[score];
                    }
                }
                for (const auto& [score, num] : ScoreHist) {
                    if (threshold <= num) {
                        return score;
                    } else {
                        threshold -= num;
                    }
                }
                Y_VERIFY_DEBUG(threshold);
                return std::nullopt;
            }

            void Cascade(TScorePicker& parent, ui32 threshold) {
                if (std::optional<ui32> maxScore = CalculateMaxScore(threshold)) {
                    if (*maxScore != FakeScore) {
                        parent.Add(*maxScore);
                    } else {
                        parent.AddFake();
                    }
                }
            }
        };

        bool FindMatchingDisks(const TAllocateContext& ctx) {
            struct TMatchingDisk {
                TPDiskInfo *PDisk;
                bool IsInGroup;
                ui32 Score = 0;

                TPDiskInfo *operator ->() const { return PDisk; }
                bool operator <(const TMatchingDisk& other) const { return PDisk->Position < other->Position; }
            };

            TScorePicker realmGroupPicker;
            std::vector<TMatchingDisk> matchingDisks;
            for (auto& [id, realmGroup] : Box) {
                TScorePicker realmPicker;
                realmGroup.Matching = false;
                for (auto& [id, realm] : realmGroup) {
                    TScorePicker domainGroupPicker;
                    realm.Matching = false;
                    for (auto& [id, domainGroup] : realm) {
                        TScorePicker domainPicker;
                        domainGroup.Matching = false;
                        for (auto& [id, domain] : domainGroup) {
                            TScorePicker diskPicker;
                            domain.Matching = false;
                            domain.NumMatchingDisks = 0;
                            for (TPDisks::value_type *it : domain) {
                                auto& [pdiskId, pdisk] = *it;
                                pdisk.Matching = ctx.DiskIsUsable(pdisk);
                                if (pdisk.Matching) {
                                    const ui32 score = pdisk.GetPickerScore();
                                    matchingDisks.push_back(TMatchingDisk{&pdisk, false, score});
                                    diskPicker.Add(score);
                                } else if (ctx.NewGroupContent.count(pdiskId)) {
                                    // we create a fake record to keep correct count of fail realms and domains, but
                                    // this particular one never gets selected
                                    matchingDisks.push_back(TMatchingDisk{&pdisk, true});
                                    diskPicker.AddFake();
                                }
                            }
                            diskPicker.Cascade(domainPicker, Geom.GetNumVDisksPerFailDomain());
                        }
                        domainPicker.Cascade(domainGroupPicker, Geom.GetNumFailDomainsPerFailRealm());
                    }
                    domainGroupPicker.Cascade(realmPicker, 1);
                }
                realmPicker.Cascade(realmGroupPicker, Geom.GetNumFailRealms());
            }

            bool boxMatching = false;

            if (const std::optional<ui32> maxScore = realmGroupPicker.CalculateMaxScore(1)) {
                Y_VERIFY_DEBUG(*maxScore != TScorePicker::FakeScore);

                // remove all mismatched candidate disks and sort them according to their position
                auto remove = [maxScore = *maxScore](const TMatchingDisk& p) {
                    p->Matching = p->Matching && p.Score <= maxScore;
                    return !p->Matching && !p.IsInGroup;
                };
                const auto begin = matchingDisks.begin();
                const auto end = matchingDisks.end();
                matchingDisks.erase(std::remove_if(begin, end, remove), end);
                std::sort(matchingDisks.begin(), matchingDisks.end());

                std::optional<TPDiskLayoutPosition> prev;

                ui32 numMatchingRealmGroupsInBox = 0;
                ui32 numMatchingRealmsInRealmGroup = 0;
                ui32 numMatchingDomainGroupsInRealm = 0;
                ui32 numMatchingDomainsInDomainGroup = 0;
                ui32 numMatchingDisksInDomain = 0;

                TFailRealmGroup *realmGroup = nullptr;
                TFailRealmInfo *realm = nullptr;
                TFailDomainGroup *domainGroup = nullptr;
                TFailDomainInfo *domain = nullptr;

                for (const auto& disk : matchingDisks) {
                    const auto& cur = disk->Position;
                    switch (prev ? prev->GetDiffIndex(cur) : 0) {
                        case 0:
                            numMatchingRealmsInRealmGroup = 0;
                            realmGroup = &Box[cur.RealmGroup];
                            [[fallthrough]];
                        case 1:
                            numMatchingDomainGroupsInRealm = 0;
                            realm = &(*realmGroup)[cur.RealmInGroup];
                            [[fallthrough]];
                        case 2:
                            numMatchingDomainsInDomainGroup = 0;
                            domainGroup = &(*realm)[cur.DomainGroup];
                            [[fallthrough]];
                        case 3:
                            numMatchingDisksInDomain = 0;
                            domain = &(*domainGroup)[cur.DomainInGroup];
                            prev = cur;
                            [[fallthrough]];
                        case 4:
                            break;
                    }

                    ++domain->NumMatchingDisks;
                    const std::tuple<ui32&, ui32, bool&> items[] = {
                        {numMatchingDisksInDomain,        Geom.GetNumVDisksPerFailDomain(),     domain->Matching     },
                        {numMatchingDomainsInDomainGroup, Geom.GetNumFailDomainsPerFailRealm(), domainGroup->Matching},
                        {numMatchingDomainGroupsInRealm,  1,                                    realm->Matching      },
                        {numMatchingRealmsInRealmGroup,   Geom.GetNumFailRealms(),              realmGroup->Matching },
                        {numMatchingRealmGroupsInBox,     1,                                    boxMatching          },
                    };
                    for (const auto& item : items) {
                        if (++std::get<0>(item) == std::get<1>(item)) {
                            std::get<2>(item) = true;
                        } else {
                            break;
                        }
                    }
                }
                Y_VERIFY(boxMatching);
            }

            return boxMatching;
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

    bool TGroupMapper::AllocateGroup(ui32 groupId, TGroupDefinition& group, const TPDiskId replacedDiskIds[],
            size_t numReplacedDisks, TForbiddenPDisks forbid, i64 requiredSpace, bool requireOperational, TString& error) {
        return Impl->AllocateGroup(groupId, group, replacedDiskIds, numReplacedDisks, std::move(forbid),
            requiredSpace, requireOperational, error);
    }

} // NKikimr::NBsController
