#include "grouper.h"

namespace NKikimr {
namespace NBsController {

TCandidate::TCandidate(TFailDomain failDomain, ui8 beginLevel, ui8 lastLevel, ui32 badness, ui32 nodeId, ui32 pDiskId,
        ui32 vDiskSlotId)
    : FailDomain(failDomain)
    , Badness(badness)
    , NodeId(nodeId)
    , PDiskId(pDiskId)
    , VDiskSlotId(vDiskSlotId)
{
    TFailDomain::TLevels::iterator a = failDomain.Levels.begin();
    while (a != failDomain.Levels.end()) {
        if (a->first < beginLevel) {
            PrefixFailDomain.Levels[a->first] = a->second;
            ++a;
        } else if (a->first <= lastLevel) {
            InfixFailDomain.Levels[a->first] = a->second;
            ++a;
        } else {
            PostfixFailDomain.Levels[a->first] = a->second;
            ++a;
        }
    }
}

struct TGrouper {
    struct TCandidateSet {
        typedef TVector<const TCandidate*> TGroupFailDomain;
        struct TGroup {
            TVector<TGroupFailDomain> Domains;
            ui32 FullDomainCount;

            TGroup()
                : FullDomainCount(0)
            {}
        };
        typedef TMap<TFailDomain, TGroup> TGroupMap;

        TGroupMap CandidatesMap;
        TGroup *LastGroup;

        TCandidateSet()
            : LastGroup(nullptr)
        {}

        bool IsEmpty() const {
            return (LastGroup == nullptr);
        }

        bool Insert(const TFailDomain::TLevelIds &id, const TCandidate *candidate, ui32 domainCount,
                ui32 candidatePerDomainCount) {
            // Candidates may belong to more than 1 equiv. class!
            // Store several sets of candidates, attempt adding to each one.
            TGroup &group = CandidatesMap[candidate->PrefixFailDomain];
            i32 matchingGroupDomainIdx = -1;
            for (size_t domainIdx = 0; domainIdx < group.Domains.size(); ++domainIdx) {
                TGroupFailDomain &domain = group.Domains[domainIdx];
                bool isColliding = false;
                for (size_t i = 0; i < domain.size(); ++i) {
                    if (!domain[i]->InfixFailDomain.IsDifferentAt(id, candidate->InfixFailDomain)) {
                        isColliding = true;
                        break;
                    }
                }
                if (isColliding) {
                    if (matchingGroupDomainIdx == -1 && group.Domains[domainIdx].size() < candidatePerDomainCount) {
                        matchingGroupDomainIdx = (i32)domainIdx;
                    } else {
                        return false;
                    }
                }
            }

            if (matchingGroupDomainIdx < 0) {
                group.Domains.emplace_back();
                matchingGroupDomainIdx = (i32)group.Domains.size() - 1;
                //Cout << "push group " << matchingGroupDomainIdx << " back" << Endl;
            }

            //Cout << "push candidate " << group.Domains[matchingGroupDomainIdx].size();
            //Cout << " to " << matchingGroupDomainIdx << " back" << Endl;
            group.Domains[matchingGroupDomainIdx].push_back(candidate);
            if (group.Domains[matchingGroupDomainIdx].size() == candidatePerDomainCount) {
                ++group.FullDomainCount;
            };
            LastGroup = &group;
            return (group.FullDomainCount == domainCount);
        }

        void GetLastGroup(ui32 domainCount, ui32 candidatePerDomainCount,
                TVector<TVector<const TCandidate*>> &outGroup) {
            outGroup.resize(domainCount);
            size_t writeDomainIdx = 0;
            for (size_t domainIdx = 0; domainIdx < LastGroup->Domains.size(); ++domainIdx) {
                if (LastGroup->Domains[domainIdx].size() == candidatePerDomainCount) {
                    outGroup[writeDomainIdx] = LastGroup->Domains[domainIdx];
                    ++writeDomainIdx;
                }
            }
            return;
        }
    };

    TMap<TFailDomain::TLevelIds, TCandidateSet> Maps;
    TVector<TMap<TFailDomain::TLevelIds, TCandidateSet>::iterator> SetsToPopulate;
    ui32 DomainCount;
    ui32 CandidatePerDomainCount;
    bool IsBingo;

    TVector<TVector<const TCandidate*>> *BestGroup;

    TGrouper(ui32 domainCount, ui32 candidatesPerDomainCount)
        : DomainCount(domainCount)
        , CandidatePerDomainCount(candidatesPerDomainCount)
        , IsBingo(false)
        , BestGroup(nullptr)
    {}

    bool Populate(TMap<TFailDomain::TLevelIds, TCandidateSet>::iterator it, const TCandidate *candidate) {
        // TODO: Calculate isEmpty and isKeyEqual without creating the intersection.
        TFailDomain::TLevelIds intersection = candidate->InfixFailDomain.Intersect(it->first);
        if (intersection.IsEmpty()) {
            return false;
        }
        bool isKeyEqual = (intersection == it->first);

        // Set IsBingo flag and fill on success.
        if (it->second.Insert(intersection, candidate, DomainCount, CandidatePerDomainCount)) {
            IsBingo = true;
            it->second.GetLastGroup(DomainCount, CandidatePerDomainCount, *BestGroup);
        }

        return isKeyEqual;
    }

    void AddToMaps(const TCandidate *candidate) {
        // Add candidate to all existing sets, while checking for exact match of the keys.
        bool isEqualKeyPresent = false;
        for (TMap<TFailDomain::TLevelIds, TCandidateSet>::iterator it = Maps.begin(); it != Maps.end(); ++it) {
            bool isKeyEqual = Populate(it, candidate);
            if (IsBingo) {
                return;
            }
            isEqualKeyPresent |= isKeyEqual;
        }
        // If there is no such key in the map, generate all intersections.
        if (!isEqualKeyPresent) {
            TVector<TFailDomain::TLevelIds> newIds;
            newIds.reserve(Maps.size());
            for (TMap<TFailDomain::TLevelIds, TCandidateSet>::iterator it = Maps.begin(); it != Maps.end(); ++it) {
                newIds.push_back(candidate->InfixFailDomain.Intersect(it->first));
            }
            TFailDomain::TLevelIds idToInsert = candidate->InfixFailDomain.MakeIds();
            Maps[idToInsert];
            for (size_t i = 0; i < newIds.size(); ++i) {
                Maps[newIds[i]];
            }
            // List all empty sets in SetsToPopulate.
            for (TMap<TFailDomain::TLevelIds, TCandidateSet>::iterator it = Maps.begin(); it != Maps.end(); ++it) {
                if (it->second.IsEmpty()) {
                    SetsToPopulate.push_back(it);
                }
            }
        }
    }

    void PopulateSets(const TCandidate* candidate) {
        // Populate all sets listed in SetsToPopulate.
        for (size_t i = 0; i < SetsToPopulate.size(); ++i) {
            Populate(SetsToPopulate[i], candidate);
            if (IsBingo) {
                return;
            }
        }
    }

    bool SelectCandidates(const TVector<TCandidate> &candidates, TVector<TVector<const TCandidate*>> &outBestGroup) {
        IsBingo = false;
        BestGroup = &outBestGroup;

        Maps.clear();
        SetsToPopulate.clear();

        // TODO: Candidates may be required to have a specific prefix.
        TVector<const TCandidate*> sortedCandidates(candidates.size());
        for (size_t i = 0; i < candidates.size(); ++i) {
            sortedCandidates[i] = &candidates[i];
        }
        StableSort(sortedCandidates.begin(), sortedCandidates.end(),
            [&](const TCandidate *a, const TCandidate *b) {
                 return a->Badness < b->Badness;
            });

        for (size_t candidateIdx = 0; candidateIdx < sortedCandidates.size(); ++candidateIdx) {
            AddToMaps(sortedCandidates[candidateIdx]);
            if (IsBingo) {
                return true;
            }
            if (!SetsToPopulate.empty()) {
                for (size_t i = 0; i <= candidateIdx; ++i) {
                    PopulateSets(sortedCandidates[i]);
                    if (IsBingo) {
                        return true;
                    }
                }
                SetsToPopulate.clear();
            }
        }
        return false;
    }

    bool VerifyGroup(const TVector<TVector<const TCandidate*>> &group) const {
        if (group.size() != DomainCount) {
            Cout << "a" << Endl;
            return false;
        }
        for (ui32 aDomainIdx = 0; aDomainIdx < DomainCount; ++aDomainIdx) {
            if (group[aDomainIdx].size() != CandidatePerDomainCount) {
                Cout << "aDomainIdx = " << aDomainIdx << " size = " << group[aDomainIdx].size() << Endl;
                return false;
            }
        }
        for (ui32 aDomainIdx = 0; aDomainIdx < DomainCount; ++aDomainIdx) {
            for (ui32 aIdx = 0; aIdx < CandidatePerDomainCount; ++aIdx) {
                const TCandidate *a = group[aDomainIdx][aIdx];
                for (ui32 bIdx = aIdx + 1; bIdx < CandidatePerDomainCount; ++bIdx) {
                    const TCandidate *b = group[aDomainIdx][bIdx];
                    if (a == b) {
                        Cout << "c" << Endl;
                        return false;
                    }
                }
                for (ui32 bDomainIdx = aDomainIdx + 1; bDomainIdx < DomainCount; ++bDomainIdx) {
                    for (ui32 bIdx = 0; bIdx < CandidatePerDomainCount; ++bIdx) {
                        const TCandidate *b = group[bDomainIdx][bIdx];
                        if (!a->PrefixFailDomain.IsEqual(b->PrefixFailDomain)) {
                            Cout << "d" << Endl;
                            return false;
                        }
                        if (a->InfixFailDomain.IsColliding(b->InfixFailDomain)) {
                            Cout << "e" << Endl;
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }
};

bool GroupFromCandidates(TVector<TCandidate> &candidates, ui32 domainCount, ui32 candidatesPerDomainCount,
        TVector<TVector<const TCandidate*>> &outBestGroup) {
    TGrouper grouper(domainCount, candidatesPerDomainCount);
    return grouper.SelectCandidates(candidates, outBestGroup);
}

bool VerifyGroup(const TVector<TVector<const TCandidate*>> &group, ui32 domainCount, ui32 candidatesPerDomainCount) {
    TGrouper grouper(domainCount, candidatesPerDomainCount);
    return grouper.VerifyGroup(group);
}

bool CreateGroupWithRings(const TVector<TCandidate>& candidates, ui32 numRings, ui32 numFailDomainsPerRing,
        ui32 numDisksPerFailDomain, ui32 firstRingDxLevel, ui32 lastRingDxLevel,
        TVector<TVector<TVector<const TCandidate*>>>& bestGroup) {
    // calculate per-ring map of candidates
    TVector<TString> keys;
    TMultiMap<TString, const TCandidate*> perRingMap;
    for (const TCandidate& candidate : candidates) {
        // calculate infix according to ring distinction rules
        TFailDomain infix;
        for (const auto& level : candidate.FailDomain.Levels) {
            if (level.first >= firstRingDxLevel && level.first <= lastRingDxLevel) {
                infix.Levels.insert(level);
            }
        }

        // insert key into vector to keep ordering of candidates according to initial sequence
        TString key = infix.SerializeFailDomain();
        if (!perRingMap.count(key)) {
            keys.push_back(key);
        }

        perRingMap.emplace(key, &candidate);
    }

    // traverse through all rings and create groups for these rings
    TVector<const TCandidate*> candptr;
    TVector<TCandidate> ringCandidates;
    bestGroup.reserve(bestGroup.size() + keys.size());
    for (const TString& key : keys) {
        // get candidates for this ring
        auto range = perRingMap.equal_range(key);
        for (auto it = range.first; it != range.second; ++it) {
            candptr.push_back(it->second);
        }

        // create vector of candidates to generate fail domain
        for (const TCandidate *ptr : candptr) {
            ringCandidates.push_back(*ptr);
        }

        // generate group
        TVector<TVector<const TCandidate*>> failDomain;
        if (!GroupFromCandidates(ringCandidates, numFailDomainsPerRing, numDisksPerFailDomain, failDomain)) {
            return false;
        }

        // convert pointers
        for (auto& domain : failDomain) {
            for (auto& item : domain) {
                ui32 index = item - ringCandidates.data();
                item = candptr[index];
            }
        }

        bestGroup.push_back(std::move(failDomain));

        // clear vectors for next cycle
        candptr.clear();
        ringCandidates.clear();
    }

    if (bestGroup.size() < numRings) {
        // there is not enough rings to build up requested group
        return false;
    } else if (bestGroup.size() == numRings) {
        // there is a group of exactly requested rings count
        return true;
    }

    // calculate overall badness of all fail domains and delete the worst ones
    TVector<ui32> badness;
    badness.reserve(bestGroup.size());
    for (const auto& ring : bestGroup) {
        ui32 sum = 0;
        for (const auto& failDomain : ring) {
            for (const auto& disk : failDomain) {
                sum += disk->Badness;
            }
        }
        badness.push_back(sum);
    }
    while (bestGroup.size() > numRings) {
        ui32 worstIdx = 0;
        for (ui32 i = 1; i < badness.size(); ++i) {
            if (badness[i] > badness[worstIdx]) {
                worstIdx = i;
            }
        }
        badness.erase(badness.begin() + worstIdx);
        bestGroup.erase(bestGroup.begin() + worstIdx);
    }

    return true;
}

} //NBsController

} //NKikimr
