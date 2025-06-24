#include "statestorage.h"

#include <util/generic/xrange.h>
#include <util/generic/mem_copy.h>
#include <util/generic/algorithm.h>
#include <library/cpp/testing/unittest/registar.h>
#include "tabletid.h"
#include <ydb/core/blobstorage/base/utility.h>
#include "statestorage_ringwalker.h"

namespace NKikimr {

namespace NStateStorageOld {
struct TStateStorageInfo : public TThrRefBase {
        struct TSelection {
            ui32 Sz;
            TArrayHolder<TActorId> SelectedReplicas;
    
            TSelection()
                : Sz(0)
            {}
    
            const TActorId* begin() const { return SelectedReplicas.Get(); }
            const TActorId* end() const { return SelectedReplicas.Get() + Sz; }
        };
    
        struct TRing {
            bool IsDisabled;
            bool UseRingSpecificNodeSelection;
            TVector<TActorId> Replicas;
    
            TActorId SelectReplica(ui32 hash) const;
            ui32 ContentHash() const;
        };
    
        ui32 NToSelect;
        TVector<TRing> Rings;
    
        ui32 StateStorageVersion;
        TVector<ui32> CompatibleVersions;
    
        void SelectReplicas(ui64 tabletId, TSelection *selection) const;
        TList<TActorId> SelectAllReplicas() const;
        ui32 ContentHash() const;
    
        TStateStorageInfo()
            : NToSelect(0)
            , Hash(Max<ui64>())
        {}
    
    private:
        mutable ui64 Hash;
    };

void TStateStorageInfo::SelectReplicas(ui64 tabletId, TSelection *selection) const {
    const ui32 hash = StateStorageHashFromTabletID(tabletId);
    const ui32 total = Rings.size();

    Y_ABORT_UNLESS(NToSelect <= total);

    if (selection->Sz < NToSelect) {
        selection->SelectedReplicas.Reset(new TActorId[NToSelect]);
    }

    selection->Sz = NToSelect;

    if (NToSelect == total) {
        for (ui32 idx : xrange(total)) {
            selection->SelectedReplicas[idx] = Rings[idx].SelectReplica(hash);
        }
    } else { // NToSelect < total, first - select rings with walker, then select concrete node
        TStateStorageRingWalker walker(hash, total);
        for (ui32 idx : xrange(NToSelect))
            selection->SelectedReplicas[idx] = Rings[walker.Next()].SelectReplica(hash);
    }
}

TActorId TStateStorageInfo::TRing::SelectReplica(ui32 hash) const {
    if (Replicas.size() == 1)
        return Replicas[0];

    Y_ABORT_UNLESS(!Replicas.empty());
    if (UseRingSpecificNodeSelection) {
        return Replicas[CombineHashes(hash, ContentHash()) % Replicas.size()];
    } else {
        return Replicas[hash % Replicas.size()];
    }
}

TList<TActorId> TStateStorageInfo::SelectAllReplicas() const {
// TODO: we really need this method in such way?
    TList<TActorId> replicas;

    for (auto &ring : Rings) {
        for (TActorId replica : ring.Replicas)
            replicas.push_back(replica);
    }

    return replicas;
}

ui32 TStateStorageInfo::TRing::ContentHash() const {
    ui64 hash = 17;
    for (TActorId replica : Replicas) {
        hash = Hash64to32((hash << 32) | replica.Hash32());
    }
    return static_cast<ui32>(hash);
}

ui32 TStateStorageInfo::ContentHash() const {
    ui64 hash = RelaxedLoad<ui64>(&Hash);
    if (Y_UNLIKELY(hash == Max<ui64>())) {
        hash = 37;
        for (const TRing &ring : Rings) {
            hash = Hash64to32((hash << 32) | ring.ContentHash());
        }
        RelaxedStore<ui64>(&Hash, static_cast<ui32>(hash));
    }
    return static_cast<ui32>(hash);
}

static void CopyStateStorageRingInfo(
    const NKikimrConfig::TDomainsConfig::TStateStorage::TRing &source,
    TStateStorageInfo *info,
    char *serviceId,
    ui32 depth
) {
    info->NToSelect = source.GetNToSelect();

    const bool hasRings = source.RingSize() > 0;
    const bool hasNodes = source.NodeSize() > 0;

    if (hasRings) { // has explicitely defined rings, use them as info rings
        Y_ABORT_UNLESS(!hasNodes);
        info->Rings.resize(source.RingSize());

        for (ui32 iring = 0, ering = source.RingSize(); iring != ering; ++iring) {
            serviceId[depth] = (iring + 1);

            const NKikimrConfig::TDomainsConfig::TStateStorage::TRing &ring = source.GetRing(iring);
            info->Rings[iring].UseRingSpecificNodeSelection = ring.GetUseRingSpecificNodeSelection();
            info->Rings[iring].IsDisabled = ring.GetIsDisabled();

            if (ring.GetUseSingleNodeActorId()) {
                Y_ABORT_UNLESS(ring.NodeSize() == 1);

                const TActorId replicaActorID = TActorId(ring.GetNode(0), TStringBuf(serviceId, serviceId + 12));
                info->Rings[iring].Replicas.push_back(replicaActorID);
            }
            else {
               Y_ABORT_UNLESS(ring.NodeSize() > 0);

               for (ui32 inode = 0, enode = ring.NodeSize(); inode != enode; ++inode) {
                    serviceId[depth + 1] = (inode + 1);
                    const TActorId replicaActorID = TActorId(ring.GetNode(inode), TStringBuf(serviceId, serviceId + 12));
                    info->Rings[iring].Replicas.push_back(replicaActorID);
               }
            }
            // reset for next ring
            serviceId[depth + 1] = char();
        }

        return;
    }

    if (hasNodes) { // has explicitely defined replicas, use nodes as 1-node rings
        Y_ABORT_UNLESS(!hasRings);

        info->Rings.resize(source.NodeSize());
        for (ui32 inode = 0, enode = source.NodeSize(); inode != enode; ++inode) {
            serviceId[depth] = (inode + 1);

            const TActorId replicaActorID = TActorId(source.GetNode(inode), TStringBuf(serviceId, serviceId + 12));
            info->Rings[inode].Replicas.push_back(replicaActorID);
        }

        return;
    }

    Y_ABORT("must have rings or legacy node config");
}

TIntrusivePtr<TStateStorageInfo> BuildStateStorageInfo(char (&namePrefix)[TActorId::MaxServiceIDLength], const NKikimrConfig::TDomainsConfig::TStateStorage& config) {
    TIntrusivePtr<TStateStorageInfo> info = new TStateStorageInfo();
    Y_ABORT_UNLESS(config.GetSSId() == 1);
    info->StateStorageVersion = config.GetStateStorageVersion();
    
    info->CompatibleVersions.reserve(config.CompatibleVersionsSize());
    for (ui32 version : config.GetCompatibleVersions()) {
        info->CompatibleVersions.push_back(version);
    }
    
    const size_t offset = FindIndex(namePrefix, char());
    Y_ABORT_UNLESS(offset != NPOS && (offset + sizeof(ui32)) < TActorId::MaxServiceIDLength);

    const ui32 stateStorageGroup = 1;
    memcpy(namePrefix + offset, reinterpret_cast<const char *>(&stateStorageGroup), sizeof(ui32));
    CopyStateStorageRingInfo(config.GetRing(), info.Get(), namePrefix, offset + sizeof(ui32));

    return info;
}

void BuildStateStorageInfos(const NKikimrConfig::TDomainsConfig::TStateStorage& config,
    TIntrusivePtr<TStateStorageInfo> &stateStorageInfo,
    TIntrusivePtr<TStateStorageInfo> &boardInfo,
    TIntrusivePtr<TStateStorageInfo> &schemeBoardInfo)
{
    char ssr[TActorId::MaxServiceIDLength] = { 's', 's', 'r' }; // state storage replica
    char ssb[TActorId::MaxServiceIDLength] = { 's', 's', 'b' }; // state storage board
    char sbr[TActorId::MaxServiceIDLength] = { 's', 'b', 'r' }; // scheme board replica

    stateStorageInfo = BuildStateStorageInfo(ssr, config);
    boardInfo = BuildStateStorageInfo(ssb, config);
    schemeBoardInfo = BuildStateStorageInfo(sbr, config);
}
}

Y_UNIT_TEST_SUITE(TStateStorageConfigCompareWithOld) {
    void Compare(TIntrusivePtr<NStateStorageOld::TStateStorageInfo> oldSS, TIntrusivePtr<NKikimr::TStateStorageInfo> newSS, ui32 rgIndex) {
        auto &newRings = newSS->RingGroups[rgIndex];
        Y_ABORT_UNLESS(oldSS->NToSelect == newRings.NToSelect);
        Y_ABORT_UNLESS(oldSS->NToSelect > 0);
        Y_ABORT_UNLESS(oldSS->Rings.size() == newRings.Rings.size());
        Y_ABORT_UNLESS(oldSS->Rings.size() > 0);
        for (auto i : xrange(oldSS->Rings.size())) {
            Y_ABORT_UNLESS(oldSS->Rings[i].Replicas.size() > 0);
            Y_ABORT_UNLESS(oldSS->Rings[i].Replicas.size() == newRings.Rings[i].Replicas.size());
            Y_ABORT_UNLESS(oldSS->Rings[i].IsDisabled == newRings.Rings[i].IsDisabled);
            Y_ABORT_UNLESS(oldSS->Rings[i].ContentHash() == newRings.Rings[i].ContentHash());
            Y_ABORT_UNLESS(oldSS->Rings[i].UseRingSpecificNodeSelection == newRings.Rings[i].UseRingSpecificNodeSelection);
            for (auto j : xrange(oldSS->Rings[i].Replicas.size())) {
                Y_ABORT_UNLESS(oldSS->Rings[i].Replicas[j] == newRings.Rings[i].Replicas[j]);
            }
        }
        if (newSS->RingGroups.size() == 1) {
            Y_ABORT_UNLESS(oldSS->SelectAllReplicas() == newSS->SelectAllReplicas());
            Y_ABORT_UNLESS(oldSS->ContentHash() == newSS->ContentHash());
        
            NStateStorageOld::TStateStorageInfo::TSelection oldSelection;
            NKikimr::TStateStorageInfo::TSelection newSelection;
            ui64 oldRetHash = 0;
            ui64 newRetHash = 0;
            for (ui64 tabletId = 8000000; tabletId < 9000000; ++tabletId) {
                oldSS->SelectReplicas(tabletId, &oldSelection);
                newSS->SelectReplicas(tabletId, &newSelection, 0);
                Y_ABORT_UNLESS(oldSS->NToSelect == oldSelection.Sz);
                Y_ABORT_UNLESS(oldSS->NToSelect == newSelection.Sz);
                for (ui32 idx : xrange(oldSS->NToSelect)) {
                    oldRetHash = CombineHashes<ui64>(oldRetHash, oldSelection.SelectedReplicas[idx].Hash());
                    newRetHash = CombineHashes<ui64>(newRetHash, newSelection.SelectedReplicas[idx].Hash());
                }
            }
            Y_ABORT_UNLESS(oldRetHash > 0);
            Y_ABORT_UNLESS(oldRetHash == newRetHash);
        }
    }
    void DoTest(NKikimrConfig::TDomainsConfig::TStateStorage oldConfig, NKikimrConfig::TDomainsConfig::TStateStorage newConfig, ui32 rgIndex = 0) {
        TIntrusivePtr<NStateStorageOld::TStateStorageInfo> oldstateStorageInfo;
        TIntrusivePtr<NStateStorageOld::TStateStorageInfo> oldboardInfo;
        TIntrusivePtr<NStateStorageOld::TStateStorageInfo> oldschemeBoardInfo;
        TIntrusivePtr<NKikimr::TStateStorageInfo> stateStorageInfo;
        TIntrusivePtr<NKikimr::TStateStorageInfo> boardInfo;
        TIntrusivePtr<NKikimr::TStateStorageInfo> schemeBoardInfo;
        
        NStateStorageOld::BuildStateStorageInfos(oldConfig, oldstateStorageInfo, oldboardInfo, oldschemeBoardInfo);
        NKikimr::BuildStateStorageInfos(newConfig, stateStorageInfo, boardInfo, schemeBoardInfo);
        Compare(oldstateStorageInfo, stateStorageInfo, rgIndex);
        Compare(oldboardInfo, boardInfo, rgIndex);
        Compare(oldschemeBoardInfo, schemeBoardInfo, rgIndex);
    }
    
    void FillRing1(NKikimrConfig::TDomainsConfig::TStateStorage::TRing* ring) {
        ring->SetNToSelect(5);
        for (ui32 nodeId : xrange(5, 13)) {
            ring->AddNode(nodeId);
        }
    }

    Y_UNIT_TEST(TestReplicaActorIdAndSelectionIsSame1) {
        NKikimrConfig::TDomainsConfig::TStateStorage config;
        auto* ring = config.MutableRing();
        FillRing1(ring);
        DoTest(config, config);
    }

    void FillRing2(NKikimrConfig::TDomainsConfig::TStateStorage::TRing* ring) {
        ring->SetNToSelect(9);
        ui32 node = 10;
        for (ui32 j : xrange(0, 9)) {
            node += j;
            auto* r = ring->AddRing();
            for (ui32 i : xrange(10)) {
                node += i;
                r->AddNode(node++);
            }
        }
    }
    Y_UNIT_TEST(TestReplicaActorIdAndSelectionIsSame2) {
        NKikimrConfig::TDomainsConfig::TStateStorage config;
        auto* ring = config.MutableRing();
        FillRing2(ring);
        DoTest(config, config);
    }

    Y_UNIT_TEST(TestReplicaActorIdAndSelectionIsSame3) {
        NKikimrConfig::TDomainsConfig::TStateStorage oldConfig;
        NKikimrConfig::TDomainsConfig::TStateStorage newConfig;
        auto* oldRing = oldConfig.MutableRing();
        auto* newRing = newConfig.AddRingGroups();
        FillRing2(oldRing);
        FillRing2(newRing);
        DoTest(oldConfig, newConfig);
    }

    Y_UNIT_TEST(TestReplicaActorIdAndSelectionIsSame4) {
        NKikimrConfig::TDomainsConfig::TStateStorage oldConfig;
        NKikimrConfig::TDomainsConfig::TStateStorage newConfig;
        auto* oldRing = oldConfig.MutableRing();
        auto* newRing = newConfig.AddRingGroups();
        FillRing1(newRing);
        newRing = newConfig.AddRingGroups();
        FillRing2(oldRing);
        FillRing2(newRing);
        DoTest(oldConfig, newConfig, 1);
    }
}

Y_UNIT_TEST_SUITE(TStateStorageConfig) {

    void FillStateStorageInfo(TStateStorageInfo *info, ui32 replicas, ui32 nToSelect, ui32 replicasInRing, bool useRingSpecificNodeSelection) {
        info->RingGroups.resize(info->RingGroups.size() + 1);
        auto& group = info->RingGroups.back();
        group.WriteOnly = false;
        group.NToSelect = nToSelect;
        group.Rings.resize(replicas);
        for (ui32 i : xrange(replicas)) {
            for (ui32 j : xrange(replicasInRing)) {
                group.Rings[i].Replicas.push_back(TActorId(i, i, i + j, i));
                group.Rings[i].UseRingSpecificNodeSelection = useRingSpecificNodeSelection;
            }
        }
    }

    ui64 StabilityRun(ui32 replicas, ui32 nToSelect, ui32 replicasInRing, bool useRingSpecificNodeSelection) {
        ui64 retHash = 0;

        TStateStorageInfo info;
        FillStateStorageInfo(&info, replicas, nToSelect, replicasInRing, useRingSpecificNodeSelection);

        TStateStorageInfo::TSelection selection;
        for (ui64 tabletId = 8000000; tabletId < 9000000; ++tabletId) {
            info.SelectReplicas(tabletId, &selection, 0);
            Y_ABORT_UNLESS(nToSelect == selection.Sz);
            for (ui32 idx : xrange(nToSelect))
                retHash = CombineHashes<ui64>(retHash, selection.SelectedReplicas[idx].Hash());
        }
        return retHash;
    }

    double UniqueCombinationsRun(ui32 replicas, ui32 nToSelect, ui32 replicasInRing, bool useRingSpecificNodeSelection) {
        const ui64 tabletStartId = 8000000;
        const ui64 tabletCount = 1000000;
        TStateStorageInfo info;
        FillStateStorageInfo(&info, replicas, nToSelect, replicasInRing, useRingSpecificNodeSelection);

        THashSet<ui64> hashes;

        TStateStorageInfo::TSelection selection;
        for (ui64 tabletId = tabletStartId; tabletId < tabletStartId + tabletCount; ++tabletId) {
            ui64 selectionHash = 0;
            info.SelectReplicas(tabletId, &selection, 0);
            Y_ABORT_UNLESS(nToSelect == selection.Sz);
            for (ui32 idx : xrange(nToSelect))
                selectionHash = CombineHashes<ui64>(selectionHash, selection.SelectedReplicas[idx].Hash());
            hashes.insert(selectionHash);
        }
        return static_cast<double>(hashes.size()) / static_cast<double>(tabletCount);
    }

    Y_UNIT_TEST(TestReplicaSelection) {
        UNIT_ASSERT(StabilityRun(3, 3, 1, false) == 17606246762804570019ULL);
        UNIT_ASSERT(StabilityRun(13, 3, 1, false) == 421354124534079828ULL);
        UNIT_ASSERT(StabilityRun(13, 9, 1, false) == 10581416019959162949ULL);
        UNIT_ASSERT(StabilityRun(3, 3, 1, true) == 17606246762804570019ULL);
        UNIT_ASSERT(StabilityRun(13, 3, 1, true) == 421354124534079828ULL);
        UNIT_ASSERT(StabilityRun(13, 9, 1, true) == 10581416019959162949ULL);
    }

    Y_UNIT_TEST(TestMultiReplicaFailDomains) {
        UNIT_ASSERT(StabilityRun(3, 3, 3, false) == 12043409773822600429ULL);
        UNIT_ASSERT(StabilityRun(13, 3, 5, false) == 3265154396592024904ULL);
        UNIT_ASSERT(StabilityRun(13, 9, 8, false) == 12079940289459527060ULL);
        UNIT_ASSERT(StabilityRun(3, 3, 3, true) == 7845257406715748850ULL);
        UNIT_ASSERT(StabilityRun(13, 3, 5, true) == 1986618578793030392ULL);
        UNIT_ASSERT(StabilityRun(13, 9, 8, true) == 6173011524598124144ULL);
    }

    Y_UNIT_TEST(TestReplicaSelectionUniqueCombinations) {
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(13, 3, 1, false), 0.000206, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(13, 3, 3, false), 0.000519, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 3, 1, false), 0.009091, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 3, 5, false), 0.045251, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 9, 1, false), 0.009237, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 9, 8, false), 0.01387, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(13, 3, 1, true), 0.000206, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(13, 3, 3, true), 0.004263, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 3, 1, true), 0.009091, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 3, 5, true), 0.63673, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 9, 1, true), 0.009237, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 9, 8, true), 0.072514, 1e-7);
    }

    double UniformityRun(ui32 replicas, ui32 nToSelect, ui32 replicasInRing, bool useRingSpecificNodeSelection) {
        THashMap<TActorId, ui32> history;

        TStateStorageInfo info;
        FillStateStorageInfo(&info, replicas, nToSelect, replicasInRing, useRingSpecificNodeSelection);

        TStateStorageInfo::TSelection selection;
        for (ui64 tabletId = 8000000; tabletId < 9000000; ++tabletId) {
            info.SelectReplicas(tabletId, &selection, 0);
            Y_ABORT_UNLESS(nToSelect == selection.Sz);
            for (ui32 idx : xrange(nToSelect))
                history[selection.SelectedReplicas[idx]] += 1;
        }

        ui32 mn = history.begin()->second;
        ui32 mx = history.begin()->second;

        for (auto &x : history) {
            const ui32 cur = x.second;
            if (cur < mn)
                mn = cur;
            if (cur > mx)
                mx = cur;
        }

        return static_cast<double>(mx - mn) / static_cast<double>(mx);
    }

    Y_UNIT_TEST(UniformityTest) {
        UNIT_ASSERT(UniformityRun(13, 3, 1, false) < 0.10);
        UNIT_ASSERT(UniformityRun(13, 3, 3, false) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 3, 1, false) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 3, 5, false) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 9, 1, false) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 9, 8, false) < 0.10);
        UNIT_ASSERT(UniformityRun(13, 3, 1, true) < 0.10);
        UNIT_ASSERT(UniformityRun(13, 3, 3, true) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 3, 1, true) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 3, 5, true) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 9, 1, true) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 9, 8, true) < 0.10);
    }
}

}
