#include "group_mapper.h"
#include "group_geometry_info.h"

namespace NKikimr::NBsController {

    class TGroupMapper::TImpl : TNonCopyable {
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

        enum class EPositionItem {
            RealmGroup,
            Realm,
            Domain,
            None,
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

            auto AsTuple() const {
                return std::tie(RealmGroup, Realm, Domain);
            }

            friend bool operator ==(const TPDiskLayoutPosition& x, const TPDiskLayoutPosition& y) {
                return x.AsTuple() == y.AsTuple();
            }

            friend bool operator !=(const TPDiskLayoutPosition& x, const TPDiskLayoutPosition& y) {
                return x.AsTuple() != y.AsTuple();
            }

            friend bool operator <(const TPDiskLayoutPosition& x, const TPDiskLayoutPosition& y) {
                return x.AsTuple() < y.AsTuple();
            }
        };

        struct TPDiskInfo : TPDiskRecord {
            TPDiskLayoutPosition Position;
            bool Matching = false;
            ui32 NumDomainMatchingDisks = 0;

            TPDiskInfo(const TPDiskRecord& pdisk, TPDiskLayoutPosition position)
                : TPDiskRecord(pdisk)
                , Position(std::move(position))
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
        using TPDiskByPosition = std::vector<std::pair<TPDiskLayoutPosition, TPDiskInfo*>>;

        struct TComparePDiskByPosition {
            bool operator ()(const TPDiskByPosition::value_type& x, const TPDiskLayoutPosition& y) const {
                return x.first < y;
            }

            bool operator ()(const TPDiskLayoutPosition& x, const TPDiskByPosition::value_type& y) const {
                return x < y.first;
            }
        };

        struct TAllocateContext {
            struct TDomainBound {
                ui32 NumChildren = 0;
            };

            struct TRealmBound {
                ui32 NumChildren = 0;
                TStackVec<THashMap<ui32, TDomainBound>, 8> Items;

                TRealmBound(size_t numFailDomains)
                    : Items(numFailDomains)
                {}
            };

            struct TRealmGroupBound {
                ui32 NumChildren = 0;
                TStackVec<THashMap<ui32, TRealmBound>, 4> Items;

                TRealmGroupBound(size_t numFailRealms)
                    : Items(numFailRealms)
                {}
            };

            const ui32 NumFailRealms;
            const ui32 NumFailDomainsPerFailRealm;
            THashMap<ui32, TRealmGroupBound> RealmGroup;
            THashSet<TPDiskId> OldGroupContent; // set of all existing disks in the group, inclusing ones which are replaced
            THashSet<TPDiskId> NewGroupContent; // newly generated group content
            const i64 RequiredSpace;
            const bool RequireOperational;
            TForbiddenPDisks Forbid;

            TAllocateContext(const TGroupGeometryInfo& geom, i64 requiredSpace, bool requireOperational,
                    TForbiddenPDisks forbid)
                : NumFailRealms(geom.GetNumFailRealms())
                , NumFailDomainsPerFailRealm(geom.GetNumFailDomainsPerFailRealm())
                , RequiredSpace(requiredSpace)
                , RequireOperational(requireOperational)
                , Forbid(std::move(forbid))
            {}

            bool ProcessExistingGroup(const TGroupDefinition& group, const TPDisks& pdisks, const TPDiskId replacedDiskIds[],
                   size_t numReplacedDisks, TString& error) {
                OldGroupContent = {replacedDiskIds, replacedDiskIds + numReplacedDisks};

                for (ui32 failRealmIdx = 0; failRealmIdx < group.size(); ++failRealmIdx) {
                    const auto& realm = group[failRealmIdx];
                    for (ui32 failDomainIdx = 0; failDomainIdx < realm.size(); ++failDomainIdx) {
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

                                if (pdisk.Decommitted) {
                                    continue;
                                }

                                if (!AddDisk(pdisk, failRealmIdx, failDomainIdx)) {
                                    error = "group contains duplicate PDisks";
                                    return false;
                                }
                            }
                        }
                    }
                }

                return true;
            }

            void UndoAddDisk(const TPDiskInfo& pdisk, ui32 failRealmIdx, ui32 failDomainIdx) {
                const size_t num = NewGroupContent.erase(pdisk.PDiskId);
                Y_VERIFY(num);
                auto realmGroupIt = RealmGroup.find(pdisk.Position.RealmGroup);
                Y_VERIFY(realmGroupIt != RealmGroup.end());
                auto& realms = realmGroupIt->second.Items[failRealmIdx];
                auto realmIt = realms.find(pdisk.Position.Realm);
                Y_VERIFY(realmIt != realms.end());
                auto& domains = realmIt->second.Items[failDomainIdx];
                auto domainIt = domains.find(pdisk.Position.Domain);
                Y_VERIFY(domainIt != domains.end());
                if (!--domainIt->second.NumChildren) {
                    domains.erase(domainIt);
                }
                if (!--realmIt->second.NumChildren) {
                    realms.erase(realmIt);
                }
                if (!--realmGroupIt->second.NumChildren) {
                    RealmGroup.erase(realmGroupIt);
                }
            }

            bool AddDisk(const TPDiskInfo& pdisk, ui32 failRealmIdx, ui32 failDomainIdx) {
                auto& realmGroup = RealmGroup.try_emplace(pdisk.Position.RealmGroup, NumFailRealms).first->second;
                auto& realm = realmGroup.Items[failRealmIdx].try_emplace(pdisk.Position.Realm, NumFailDomainsPerFailRealm).first->second;
                auto& domain = realm.Items[failDomainIdx].try_emplace(pdisk.Position.Domain).first->second;
                ++realmGroup.NumChildren;
                ++realm.NumChildren;
                ++domain.NumChildren;
                const auto& [_, inserted] = NewGroupContent.insert(pdisk.PDiskId);
                return inserted;
            }

            bool DiskIsUsable(const TPDiskInfo& pdisk) const {
                if (!pdisk.IsUsable()) {
                    return false; // disk is not usable in this case
                }
                if (OldGroupContent.contains(pdisk.PDiskId) || NewGroupContent.contains(pdisk.PDiskId) || Forbid.contains(pdisk.PDiskId)) {
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
        };

        class THelper {
            TImpl& Self;
            TAllocateContext& Ctx;
            std::unordered_map<ui32, unsigned> LocalityFactor;
            TDynBitMap ForbiddenEntities;

        public:
            THelper(TImpl& self, TAllocateContext& ctx)
                : Self(self)
                , Ctx(ctx)
            {
                ForbiddenEntities.Reserve(Self.DomainMapper.GetIdCount());
                for (const TPDiskId& pdiskId : Ctx.NewGroupContent) {
                    try {
                        const TPDiskInfo& pdisk = Self.PDisks.at(pdiskId);
                        AddUsedDisk(pdisk);
                        Forbid(pdisk);
                    } catch (const std::out_of_range&) {
                        Y_FAIL();
                    }
                }

                ui32 numMatchingDisksInDomain = 0;
                ui32 numMatchingDomainsInRealm = 0;
                ui32 numMatchingRealmsInRealmGroup = 0;

                const ui32 numFailRealms = Self.Geom.GetNumFailRealms();
                const ui32 numFailDomainsPerFailRealm = Self.Geom.GetNumFailDomainsPerFailRealm();
                const ui32 numVDisksPerFailDomain = Self.Geom.GetNumVDisksPerFailDomain();

                auto advance = [&](bool domainExhausted, bool realmExhausted, bool realmGroupExhausted, const TPDiskLayoutPosition& prev) {
                    if (domainExhausted) {
                        if (numMatchingDisksInDomain < numVDisksPerFailDomain) {
                            ForbiddenEntities.Set(prev.Domain);
                        } else {
                            ++numMatchingDomainsInRealm;
                        }
                        numMatchingDisksInDomain = 0;
                    }
                    if (realmExhausted) {
                        if (numMatchingDomainsInRealm < numFailDomainsPerFailRealm) {
                            ForbiddenEntities.Set(prev.Realm);
                        } else {
                            ++numMatchingRealmsInRealmGroup;
                        }
                        numMatchingDomainsInRealm = 0;
                    }
                    if (realmGroupExhausted) {
                        if (numMatchingRealmsInRealmGroup < numFailRealms) {
                            ForbiddenEntities.Set(prev.RealmGroup);
                        }
                        numMatchingRealmsInRealmGroup = 0;
                    }
                };

                if (const auto *begin = Self.PDiskByPosition.data(), *end = begin + Self.PDiskByPosition.size(); begin != end) {
                    --end;
                    while (begin != end) {
                        numMatchingDisksInDomain += begin->second->Matching || Ctx.NewGroupContent.contains(begin->second->PDiskId);
                        const auto& prev = begin++->first;
                        const auto& cur = begin->first;
                        advance(prev.Domain != cur.Domain, prev.Realm != cur.Realm, prev.RealmGroup != cur.RealmGroup, prev);
                    }
                    numMatchingDisksInDomain += begin->second->Matching || Ctx.NewGroupContent.contains(begin->second->PDiskId);
                    advance(true, true, true, begin->first);
                }
            }

            TPDiskId AddBestDisk(ui32 realmIdx, ui32 domainIdx) {
                TPDiskInfo *pdisk = nullptr;
                auto process = [this, &pdisk](TPDiskInfo *candidate) {
                    if (!pdisk || DiskIsBetter(*candidate, *pdisk)) {
                        pdisk = candidate;
                    }
                };
                FindMatchingDisksBounded(process, Self.PDiskByPosition.begin(), Self.PDiskByPosition.end(),
                    Ctx.RealmGroup, {realmIdx, domainIdx});
                if (!pdisk) {
                    return TPDiskId();
                }
                const bool success = Ctx.AddDisk(*pdisk, realmIdx, domainIdx);
                Y_VERIFY(success);
                Forbid(*pdisk);
                pdisk->Matching = false; // disable this disk for further selection
                AddUsedDisk(*pdisk);
                return pdisk->PDiskId;
            }

        private:
            void Forbid(const TPDiskInfo& pdisk) {
                for (const ui32 id : {pdisk.Position.RealmGroup, pdisk.Position.Realm, pdisk.Position.Domain}) {
                    ForbiddenEntities.Set(id);
                }
            }

            template<typename T>
            struct TBoundTraits {};

            template<>
            struct TBoundTraits<TAllocateContext::TRealmGroupBound> {
                static constexpr ui32 TPDiskLayoutPosition::*Ptr = &TPDiskLayoutPosition::RealmGroup;
                static constexpr EPositionItem ForbiddenCheckLevel = EPositionItem::RealmGroup;
                static constexpr size_t CoordIndex = 0;
                static constexpr bool Descend = true;

                static TPDiskLayoutPosition LowerBound(TPDiskLayoutPosition, ui32 id) {
                    return {id, 0, 0};
                }

                static TPDiskLayoutPosition UpperBoundFromLowerBound(TPDiskLayoutPosition lower) {
                    return {lower.RealmGroup, Max<ui32>(), Max<ui32>()};
                }

                static bool PrefixEquals(const TPDiskLayoutPosition& a, const TPDiskLayoutPosition& b) {
                    return a.RealmGroup == b.RealmGroup;
                }
            };

            template<>
            struct TBoundTraits<TAllocateContext::TRealmBound> {
                static constexpr ui32 TPDiskLayoutPosition::*Ptr = &TPDiskLayoutPosition::Realm;
                static constexpr EPositionItem ForbiddenCheckLevel = EPositionItem::Realm;
                static constexpr size_t CoordIndex = 1;
                static constexpr bool Descend = true;

                static TPDiskLayoutPosition LowerBound(TPDiskLayoutPosition prefix, ui32 id) {
                    return {prefix.RealmGroup, id, 0};
                }

                static TPDiskLayoutPosition UpperBoundFromLowerBound(TPDiskLayoutPosition lower) {
                    return {lower.RealmGroup, lower.Realm, Max<ui32>()};
                }

                static bool PrefixEquals(const TPDiskLayoutPosition& a, const TPDiskLayoutPosition& b) {
                    return a.RealmGroup == b.RealmGroup && a.Realm == b.Realm;
                }
            };

            template<>
            struct TBoundTraits<TAllocateContext::TDomainBound> {
                static constexpr ui32 TPDiskLayoutPosition::*Ptr = &TPDiskLayoutPosition::Domain;
                static constexpr EPositionItem ForbiddenCheckLevel = EPositionItem::Domain;
                static constexpr bool Descend = false;

                static TPDiskLayoutPosition LowerBound(TPDiskLayoutPosition prefix, ui32 id) {
                    return {prefix.RealmGroup, prefix.Realm, id};
                }

                static TPDiskLayoutPosition UpperBoundFromLowerBound(TPDiskLayoutPosition lower) {
                    return lower;
                }

                static bool PrefixEquals(const TPDiskLayoutPosition& a, const TPDiskLayoutPosition& b) {
                    return a == b;
                }
            };

            template<typename TCallback, typename TBound, typename... TRest>
            void FindMatchingDisksBounded(TCallback&& cb, TPDiskByPosition::const_iterator begin,
                    TPDiskByPosition::const_iterator end, const TBound& bound,
                    std::tuple<ui32, ui32> posInGroup) {
                using Traits = TBoundTraits<typename TBound::mapped_type>;
                if (bound && begin != end) {
                    ui32 max = 0;
                    for (const auto& [_, item] : bound) {
                        max = Max(item.NumChildren, max);
                    }
                    for (const auto& [id, item] : bound) {
                        if (item.NumChildren != max) {
                            continue;
                        }
                        const TPDiskLayoutPosition lower = Traits::LowerBound(begin->first, id);
                        const auto childBegin = std::lower_bound(begin, end, lower, TComparePDiskByPosition());
                        const auto childEnd = std::upper_bound(childBegin, end, Traits::UpperBoundFromLowerBound(lower),
                            TComparePDiskByPosition());
                        if constexpr (Traits::Descend) {
                            const ui32 index = std::get<Traits::CoordIndex>(posInGroup);
                            FindMatchingDisksBounded(cb, childBegin, childEnd, item.Items[index], posInGroup);
                        } else {
                            FindMatchingDisks<EPositionItem::None>(cb, childBegin, childEnd);
                        }
                    }
                } else {
                    FindMatchingDisks<Traits::ForbiddenCheckLevel>(cb, begin, end);
                }
            }

            template<EPositionItem ForbiddenCheckLevel, typename TCallback>
            void FindMatchingDisks(TCallback&& cb, TPDiskByPosition::const_iterator begin, TPDiskByPosition::const_iterator end) {
                while (begin != end) {
                    const auto& [position, pdisk] = *begin++;
                    if constexpr (ForbiddenCheckLevel <= EPositionItem::RealmGroup) {
                        if (ForbiddenEntities[position.RealmGroup]) {
                            continue;
                        }
                    }
                    if constexpr (ForbiddenCheckLevel <= EPositionItem::Realm) {
                        if (ForbiddenEntities[position.Realm]) {
                            continue;
                        }
                    }
                    if constexpr (ForbiddenCheckLevel <= EPositionItem::Domain) {
                        if (ForbiddenEntities[position.Domain]) {
                            continue;
                        }
                    }
                    if (pdisk->Matching) {
                        cb(pdisk);
                    }
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

        TString FormatPDisks(const TAllocateContext& ctx) const {
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

                    if (ctx.OldGroupContent.contains(pdisk->PDiskId)) {
                        s << "*";
                    }
                    const char *minus = "-";
                    if (ctx.Forbid.contains(pdisk->PDiskId)) {
                        s << std::exchange(minus, "") << "f";
                    }
                    if (!pdisk->Usable) {
                        s << std::exchange(minus, "") << "u";
                    }
                    if (pdisk->Decommitted) {
                        s << std::exchange(minus, "") << "d";
                    }
                    if (pdisk->NumSlots >= pdisk->MaxSlots) {
                        s << std::exchange(minus, "") << "s";
                    }
                    if (pdisk->SpaceAvailable < ctx.RequiredSpace) {
                        s << std::exchange(minus, "") << "v";
                    }
                    if (!pdisk->Operational) {
                        s << std::exchange(minus, "") << "o";
                    }
                    if (ctx.DiskIsUsable(*pdisk)) {
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

        bool AllocateGroup(ui32 groupId, TGroupDefinition& group, const TPDiskId replacedDiskIds[],
                size_t numReplacedDisks, TForbiddenPDisks forbid, i64 requiredSpace, bool requireOperational,
                TString& error) {
            if (Dirty) {
                std::sort(PDiskByPosition.begin(), PDiskByPosition.end());
                Dirty = false;
            }

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
                            goto out;
                        }
                    }
                }
            }
out:        if (!hasMissingEntities) {
                return true; // group is okay
            }

            // adjust number of slots
            for (TPDiskId pdiskId : ctx.OldGroupContent) {
                Y_VERIFY_DEBUG(PDisks.contains(pdiskId));
                --PDisks.at(pdiskId).NumSlots;
            }
            for (size_t i = 0; i < numReplacedDisks; ++i) {
                Y_VERIFY_DEBUG(PDisks.contains(replacedDiskIds[i]));
                PDisks.at(replacedDiskIds[i]).EraseGroup(groupId);
            }

            ui32 minScore = Max<ui32>();
            ui32 maxScore = Min<ui32>();
            for (const auto& [pdiskId, pdisk] : PDisks) {
                const ui32 score = pdisk.GetPickerScore();
                minScore = Min(minScore, score);
                maxScore = Max(maxScore, score + 1);
            }

            std::optional<TGroupDefinition> outGroup;

            auto tryIteration = [&](ui32 score) {
                ui32 numDomainMatchingDisks = 0;
                auto domainBegin = PDiskByPosition.begin();
                for (auto it = domainBegin; it != PDiskByPosition.end(); ++it) {
                    auto& [position, pdisk] = *it;
                    if (position != domainBegin->first) {
                        for (; domainBegin != it; ++domainBegin) {
                            domainBegin->second->NumDomainMatchingDisks = numDomainMatchingDisks;
                        }
                        numDomainMatchingDisks = 0;
                    }
                    pdisk->Matching = ctx.DiskIsUsable(*pdisk) && pdisk->GetPickerScore() <= score;
                    numDomainMatchingDisks += pdisk->Matching;
                }
                for (; domainBegin != PDiskByPosition.end(); ++domainBegin) {
                    domainBegin->second->NumDomainMatchingDisks = numDomainMatchingDisks;
                }

                TStackVec<std::tuple<ui32, ui32, ui32>, 32> undoLog;
                THelper helper(*this, ctx);

                auto revert = [&]() {
                    for (const auto& item : undoLog) {
                        ui32 realmIdx, domainIdx, vdiskIdx;
                        std::tie(realmIdx, domainIdx, vdiskIdx) = item; // thanks to Microsoft
                        auto& pdiskId = group[realmIdx][domainIdx][vdiskIdx];
                        const auto it = PDisks.find(pdiskId);
                        Y_VERIFY(it != PDisks.end());
                        ctx.UndoAddDisk(it->second, realmIdx, domainIdx);
                        pdiskId = TPDiskId();
                    }
                };

                for (ui32 realmIdx = 0; realmIdx < ctx.NumFailRealms; ++realmIdx) {
                    for (ui32 domainIdx = 0; domainIdx < ctx.NumFailDomainsPerFailRealm; ++domainIdx) {
                        auto& domain = group[realmIdx][domainIdx];
                        for (ui32 vdiskIdx = 0; vdiskIdx < domain.size(); ++vdiskIdx) {
                            if (auto& pdiskId = domain[vdiskIdx]; pdiskId == TPDiskId()) {
                                pdiskId = helper.AddBestDisk(realmIdx, domainIdx);
                                if (pdiskId == TPDiskId()) {
                                    revert();
                                    return false;
                                } else {
                                    undoLog.emplace_back(realmIdx, domainIdx, vdiskIdx);
                                }
                            }
                        }
                    }
                }

                outGroup = group;
                revert();
                return true;
            };

            while (minScore < maxScore) {
                const ui32 score = minScore + (maxScore - minScore) / 2;
                if (tryIteration(score)) {
                    maxScore = score;
                } else {
                    minScore = score + 1;
                }
            }

            if (outGroup) {
                group = *outGroup;
                for (const auto& realm : group) {
                    for (const auto& domain : realm) {
                        for (const auto& pdiskId : domain) {
                            if (const auto it = PDisks.find(pdiskId); it != PDisks.end()) {
                                ++it->second.NumSlots;
                                it->second.InsertGroup(groupId);
                            } else {
                                Y_FAIL();
                            }
                        }
                    }
                }
                return true;
            }

            // undo changes to the mapper content
            for (TPDiskId pdiskId : ctx.OldGroupContent) {
                Y_VERIFY_DEBUG(PDisks.contains(pdiskId));
                ++PDisks.at(pdiskId).NumSlots;
            }
            for (size_t i = 0; i < numReplacedDisks; ++i) {
                Y_VERIFY_DEBUG(PDisks.contains(replacedDiskIds[i]));
                PDisks.at(replacedDiskIds[i]).InsertGroup(groupId);
            }
            error = "no group options " + FormatPDisks(ctx);
            return false;
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
