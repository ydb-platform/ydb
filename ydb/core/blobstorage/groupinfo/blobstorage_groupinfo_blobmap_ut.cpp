#include <library/cpp/testing/unittest/registar.h>
#include <util/random/fast.h>
#include "blobstorage_groupinfo_blobmap.h"
#include "blobstorage_groupinfo_iter.h"

using namespace NKikimr;

namespace {

    struct TOriginalBlobStorageGroupInfo {
        ui32 BlobSubgroupSize;
        TVector<TBlobStorageGroupInfo::TFailRealm> Realms;
        const ui32 GroupID;
        const ui32 GroupGeneration;
        TBlobStorageGroupInfo::TDynamicInfo DynamicInfo;

        TOriginalBlobStorageGroupInfo(ui32 blobSubgroupSize, const TBlobStorageGroupInfo& groupInfo)
            : BlobSubgroupSize(blobSubgroupSize)
            , GroupID(groupInfo.GroupID.GetRawId())
            , GroupGeneration(groupInfo.GroupGeneration)
            , DynamicInfo(groupInfo.GetDynamicInfo())
        {
            for (auto it = groupInfo.FailRealmsBegin(); it != groupInfo.FailRealmsEnd(); ++it) {
                Realms.push_back(*it);
            }
        }

        bool PickSubgroup(ui32 hash, ui32 vdiskSz, TVDiskID *outVDisk, TActorId *outServiceIds) const {
            const ui32 subgroupSz = BlobSubgroupSize;
            Y_ABORT_UNLESS(vdiskSz == subgroupSz);

            const TBlobStorageGroupInfo::TFailRealm& realm = Realms[0];
            Y_DEBUG_ABORT_UNLESS(realm.FailDomains.size() >= subgroupSz);

            ui32 domainIdx = hash % realm.FailDomains.size();
            TReallyFastRng32 rng(hash);

            for (ui32 i = 0; i < subgroupSz; ++i) {
                const ui32 dx = domainIdx % realm.FailDomains.size();
                const TBlobStorageGroupInfo::TFailDomain &domain = realm.FailDomains[dx];
                const ui32 vx = rng() % domain.VDisks.size();

                outVDisk[i] = TVDiskID(GroupID, GroupGeneration, 0, dx, vx);
                auto orderNum = domain.VDisks[vx].OrderNumber;
                outServiceIds[i] = DynamicInfo.ServiceIdForOrderNumber[orderNum];

                ++domainIdx;
            }

            return true;
        }

        bool BelongsToSubgroup(const TVDiskID &vdisk, ui32 hash) const {
            bool isReplica = GetIdxInSubgroup(vdisk, hash) < BlobSubgroupSize;
            return isReplica;
        }

        // Returns either vdisk idx in the blob subgroup, or BlobSubgroupSize if the vdisk is not in the blob subgroup
        ui32 GetIdxInSubgroup(const TVDiskID &vdisk, ui32 hash) const {
            Y_ABORT_UNLESS(vdisk.GroupID.GetRawId() == GroupID && vdisk.GroupGeneration == GroupGeneration);
            Y_ABORT_UNLESS(vdisk.FailRealm < Realms.size());

            const TBlobStorageGroupInfo::TFailRealm &realm = Realms[vdisk.FailRealm];
            ui32 idx = (vdisk.FailDomain + realm.FailDomains.size() - hash % realm.FailDomains.size()) % realm.FailDomains.size();

            if (idx < BlobSubgroupSize) {
                ui32 vdiskIdx;
                TReallyFastRng32 rng(hash);
                for (ui32 i = 0; i < idx; ++i)
                    rng();
                vdiskIdx = rng() % realm.FailDomains[vdisk.FailDomain].VDisks.size();
                if (vdiskIdx == vdisk.VDisk) {
                    return idx;
                }
            }
            return BlobSubgroupSize;
        }

        TVDiskID GetVDiskInSubgroup(ui32 idxInSubgroup, ui32 hash) const {
            Y_ABORT_UNLESS(idxInSubgroup < BlobSubgroupSize);

            const TBlobStorageGroupInfo::TFailRealm &xrealm = Realms[0];
            ui32 domainIdx = hash % xrealm.FailDomains.size() + idxInSubgroup;
            const ui32 dx = domainIdx % xrealm.FailDomains.size();
            const TBlobStorageGroupInfo::TFailDomain &domain = xrealm.FailDomains[dx];
            TReallyFastRng32 rng(hash);
            for (ui32 i = 0; i < idxInSubgroup; ++i) {
                rng();
            }
            const ui32 vx = rng() % domain.VDisks.size();

            return TVDiskID(GroupID, GroupGeneration, 0, dx, vx);
        }
    };

} // anon

Y_UNIT_TEST_SUITE(TBlobStorageGroupInfoBlobMapTest) {

    void MakeBelongsToSubgroupBenchmark(TBlobStorageGroupType::EErasureSpecies erasure, ui32 numFailDomains,
            NUnitTest::TTestContext& ut_context) {
        auto groupInfo = std::make_unique<TBlobStorageGroupInfo>(erasure, 1, numFailDomains, 1);
        const ui32 blobSubgroupSize = groupInfo->Type.BlobSubgroupSize();
        TOriginalBlobStorageGroupInfo orig(blobSubgroupSize, *groupInfo);

        TVector<TLogoBlobID> ids;
        for (ui32 i = 0; i < 10000; ++i) {
            for (ui32 j = 0; j < 1000; ++j) {
                ids.emplace_back(i, 1, j, 1, 1000, 1);
            }
        }

        constexpr ui64 blobCount = 10'000'000;
        UNIT_ASSERT(blobCount == ids.size());
        ui64 iterationCount = numFailDomains * blobCount;

        THPTimer timer;
        ui32 num = 0;
        for (const auto& vdisk : groupInfo->GetVDisks()) {
            for (const TLogoBlobID& id : ids) {
                auto vd = groupInfo->GetVDiskId(vdisk.OrderNumber);
                num += groupInfo->BelongsToSubgroup(vd, id.Hash()) ? 1 : 0;
            }
        }
        double newMetric = 1'000'000'000 * timer.PassedReset() / iterationCount;
        TString newMetricsName = TStringBuilder() << TErasureType::ErasureSpeciesToStr(erasure)
                << " domains " << numFailDomains
                << " new (ns)";
        ut_context.Metrics[newMetricsName] = newMetric;
        Cerr << newMetricsName << ": " << newMetric<< Endl;

        timer.Reset();
        ui32 num2 = 0;
        for (const auto& vdisk : groupInfo->GetVDisks()) {
            for (const TLogoBlobID& id : ids) {
                auto vd = groupInfo->GetVDiskId(vdisk.OrderNumber);
                num2 += orig.BelongsToSubgroup(vd, id.Hash()) ? 1 : 0;
            }
        }
        double oldMetric = 1'000'000'000 * timer.PassedReset() / iterationCount;
        TString oldMetricsName = TStringBuilder() << TErasureType::ErasureSpeciesToStr(erasure)
                << " domains " << numFailDomains
                << " old (ns)";
        ut_context.Metrics[oldMetricsName] = oldMetric;
        Cerr << oldMetricsName << ": " << oldMetric << Endl;

        UNIT_ASSERT_VALUES_EQUAL(num, num2);
    }

    Y_UNIT_TEST(BelongsToSubgroupBenchmark) {
        auto erasures = {TBlobStorageGroupType::ErasureNone,
                TBlobStorageGroupType::ErasureMirror3,
                TBlobStorageGroupType::Erasure4Plus2Block,
                TBlobStorageGroupType::ErasureMirror3of4};
        for (auto erasure : erasures) {
            TBlobStorageGroupType type(erasure);
            for (ui32 domains : {type.BlobSubgroupSize(), 9u}) {
                MakeBelongsToSubgroupBenchmark(erasure, domains, ut_context);
            }
        }
    }

    void BasicCheck(const std::unique_ptr<TBlobStorageGroupInfo> &groupInfo, TOriginalBlobStorageGroupInfo &orig,
            TLogoBlobID id, ui32 blobSubgroupSize) {
        std::array<TVDiskID, 8> vdisks;
        std::array<TActorId, 8> services;
        orig.PickSubgroup(id.Hash(), blobSubgroupSize, vdisks.data(), services.data());

        TBlobStorageGroupInfo::TVDiskIds vdisks2;
        TBlobStorageGroupInfo::TServiceIds services2;
        groupInfo->PickSubgroup(id.Hash(), &vdisks2, &services2);

        UNIT_ASSERT_EQUAL(vdisks2.size(), blobSubgroupSize);
        UNIT_ASSERT_EQUAL(services2.size(), blobSubgroupSize);
        UNIT_ASSERT(std::equal(vdisks2.begin(), vdisks2.end(), vdisks.begin()));
        UNIT_ASSERT(std::equal(services2.begin(), services2.end(), services.begin()));

        for (ui32 i = 0; i < blobSubgroupSize; ++i) {
            const TVDiskID& vdisk = vdisks[i];

            UNIT_ASSERT_EQUAL(groupInfo->GetVDiskInSubgroup(i, id.Hash()),
                    orig.GetVDiskInSubgroup(i, id.Hash()));
            UNIT_ASSERT_EQUAL(groupInfo->GetVDiskInSubgroup(i, id.Hash()), vdisk);

            UNIT_ASSERT_EQUAL(groupInfo->GetIdxInSubgroup(vdisk, id.Hash()),
                    orig.GetIdxInSubgroup(vdisk, id.Hash()));
            UNIT_ASSERT_EQUAL(groupInfo->GetIdxInSubgroup(vdisk, id.Hash()), i);
        }

        THashMap<TVDiskID, ui32> disk2index;
        for (ui32 i = 0; i < blobSubgroupSize; ++i) {
            disk2index[vdisks2[i]] = i;
        }

        for (const auto& vdisk : groupInfo->GetVDisks()) {
            auto vd = groupInfo->GetVDiskId(vdisk.OrderNumber);
            auto it = disk2index.find(vd);
            bool isReplicaFor = it != disk2index.end();

            UNIT_ASSERT_VALUES_EQUAL(orig.BelongsToSubgroup(vd, id.Hash()), isReplicaFor);
            UNIT_ASSERT_VALUES_EQUAL(groupInfo->BelongsToSubgroup(vd, id.Hash()), isReplicaFor);

            const ui32 index = isReplicaFor ? it->second : blobSubgroupSize;
            UNIT_ASSERT_VALUES_EQUAL(orig.GetIdxInSubgroup(vd, id.Hash()), index);
            UNIT_ASSERT_VALUES_EQUAL(groupInfo->GetIdxInSubgroup(vd, id.Hash()), index);
        }
    }

    Y_UNIT_TEST(BasicChecks) {
        for (auto erasure : {TBlobStorageGroupType::ErasureNone, TBlobStorageGroupType::ErasureMirror3,
                TBlobStorageGroupType::Erasure3Plus1Block, TBlobStorageGroupType::Erasure3Plus1Stripe,
                TBlobStorageGroupType::Erasure4Plus2Block, TBlobStorageGroupType::Erasure3Plus2Block,
                TBlobStorageGroupType::Erasure4Plus2Stripe, TBlobStorageGroupType::Erasure3Plus2Stripe,
                TBlobStorageGroupType::ErasureMirror3Plus2}) {
            auto groupInfo = std::make_unique<TBlobStorageGroupInfo>(erasure, 3U, 8U, 1U);
            const ui32 blobSubgroupSize = groupInfo->Type.BlobSubgroupSize();
            TOriginalBlobStorageGroupInfo orig(blobSubgroupSize, *groupInfo);

            for (ui32 i = 0; i < 100; ++i) {
                for (ui32 j = 0; j < 10; ++j) {
                    TLogoBlobID id(i, 1, j, 1, 1000, 1);
                    BasicCheck(groupInfo, orig, id, blobSubgroupSize);
                }
            }
        }
    }

    Y_UNIT_TEST(CheckCorrectBehaviourWithHashOverlow) {
        auto groupInfo = std::make_unique<TBlobStorageGroupInfo>(TErasureType::ErasureMirror3, 1U, 5U, 1U);
        const ui32 blobSubgroupSize = groupInfo->Type.BlobSubgroupSize();
        TOriginalBlobStorageGroupInfo orig(blobSubgroupSize, *groupInfo);
        TLogoBlobID id(4550843067551373890, 2564314201, 2840555155, 59, 0, 2230444);
        BasicCheck(groupInfo, orig, id, blobSubgroupSize);
    }

    Y_UNIT_TEST(Mirror3dcMapper) {
        auto groupInfo = std::make_unique<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureMirror3, 3U, 5U, 4U);

        std::unique_ptr<IBlobToDiskMapper> mapper{IBlobToDiskMapper::CreateMirror3dcMapper(&groupInfo->GetTopology())};

        THashMap<TVDiskID, TVector<ui32>> usageMap;
        for (const auto& vdisk : groupInfo->GetVDisks()) {
            auto vd = groupInfo->GetVDiskId(vdisk.OrderNumber);
            usageMap.emplace(vd, TVector<ui32>(9));
        }

        TReallyFastRng32 rng(1);
        for (ui32 i = 0; i < 10000; ++i) {
            const ui32 hash = rng();

            TBlobStorageGroupInfo::TOrderNums orderNums;
            TBlobStorageGroupInfo::TVDiskIds vdisks;
            mapper->PickSubgroup(hash, orderNums);
            for (const auto &x : orderNums) {
                vdisks.push_back(groupInfo->GetVDiskId(x));
            }

            for (ui32 i = 0; i < vdisks.size(); ++i) {
                ++usageMap[vdisks[i]][i];
            }

            for (ui32 i = 0; i < 3; ++i) {
                // ensure that cells within a column are in the same ring
                UNIT_ASSERT_VALUES_EQUAL(vdisks[i].FailRealm, vdisks[i + 3].FailRealm);
                UNIT_ASSERT_VALUES_EQUAL(vdisks[i].FailRealm, vdisks[i + 6].FailRealm);
                // and from different fail domains
                UNIT_ASSERT(vdisks[i].FailDomain != vdisks[i + 3].FailDomain);
                UNIT_ASSERT(vdisks[i].FailDomain != vdisks[i + 6].FailDomain);
                UNIT_ASSERT(vdisks[i + 3].FailDomain != vdisks[i + 6].FailDomain);
            }
            UNIT_ASSERT(vdisks[0].FailRealm != vdisks[1].FailRealm);
            UNIT_ASSERT(vdisks[0].FailRealm != vdisks[2].FailRealm);
            UNIT_ASSERT(vdisks[1].FailRealm != vdisks[2].FailRealm);

            THashMap<TVDiskID, ui32> disk2index;
            for (ui32 i = 0; i < vdisks.size(); ++i) {
                disk2index[vdisks[i]] = i;
            }

            for (const auto& vdisk : groupInfo->GetVDisks()) {
                auto vd = groupInfo->GetVDiskId(vdisk.OrderNumber);
                auto it = disk2index.find(vd);
                bool isReplicaFor = it != disk2index.end();
                UNIT_ASSERT_VALUES_EQUAL(mapper->GetIdxInSubgroup(vd, hash), isReplicaFor ? it->second : 9);
                UNIT_ASSERT_VALUES_EQUAL(mapper->BelongsToSubgroup(vd, hash), isReplicaFor);
            }

            for (ui32 i = 0; i < vdisks.size(); ++i) {
                UNIT_ASSERT_EQUAL(mapper->GetVDiskInSubgroup(i, hash), vdisks[i]);
            }
        }

        TVector<double> xv;
        for (const auto& kv : usageMap) {
            Cerr << kv.first.ToString() << "#";
            for (ui32 i : kv.second) {
                xv.push_back(i);
                Cerr << " " << i;
            }
            Cerr << Endl;
        }

        double mean = 0;
        for (double x : xv) {
            mean += x;
        }
        mean /= xv.size();

        double dev = 0;
        for (double x : xv) {
            double d = x - mean;
            dev += d * d;
        }
        dev = sqrt(dev / xv.size());

        Cerr << "mean# " << mean << " dev# " << dev << Endl;
    }

}
