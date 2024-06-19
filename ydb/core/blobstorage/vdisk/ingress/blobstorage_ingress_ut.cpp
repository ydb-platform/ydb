#include "blobstorage_ingress.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

#define STR Cnull

namespace NKikimr {

    using namespace NMatrix;

    Y_UNIT_TEST_SUITE(TBlobStorageIngress) {

        Y_UNIT_TEST(Ingress) {
            TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::ErasureMirror3, 2, 4);
            using TGroupId = TGroupId;
            TVDiskID vdisk01 = TVDiskID(TGroupId::Zero(), 1, 0, 0, 1);
            TVDiskID vdisk10 = TVDiskID(TGroupId::Zero(), 1, 0, 1, 0);
            TVDiskID vdisk21 = TVDiskID(TGroupId::Zero(), 1, 0, 2, 1);
            TVDiskID vdisk30 = TVDiskID(TGroupId::Zero(), 1, 0, 3, 0);

            TLogoBlobID lb1(0, 0, 0, 0, 0, 0);

            // correspondings parts
            TIngress i1 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk10, TLogoBlobID(lb1, 1));
            TIngress i2 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk21, TLogoBlobID(lb1, 2));
            TIngress i3 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk30, TLogoBlobID(lb1, 3));

            // merge
            TIngress res;
            TIngress::Merge(res, i1);
            TIngress::Merge(res, i2.CopyWithoutLocal(groupInfo.Type));
            TIngress::Merge(res, i3);

            TVectorType vec = res.PartsWeKnowAbout(groupInfo.Type);
            UNIT_ASSERT(vec == TVectorType(0xE0, 3));

            TVectorType parts(0, 3);
            ////
            parts = res.PartsWeMustHaveLocally(&groupInfo.GetTopology(), vdisk10, lb1) - res.LocalParts(groupInfo.Type);
            UNIT_ASSERT(parts == TVectorType(0, 3));
            ////
            parts = res.PartsWeMustHaveLocally(&groupInfo.GetTopology(), vdisk21, lb1) - res.LocalParts(groupInfo.Type);
            UNIT_ASSERT(parts == TVectorType(0x40, 3));

            // hand off with data
            TIngress i4 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk01, TLogoBlobID(lb1, 2));
            parts = i4.PartsWeMustHaveLocally(&groupInfo.GetTopology(), vdisk01, lb1) - i4.LocalParts(groupInfo.Type);
            UNIT_ASSERT(parts == TVectorType(0x0, 3));

            // hand off without data
            TIngress i5(i4.CopyWithoutLocal(groupInfo.Type));
            parts = i5.PartsWeMustHaveLocally(&groupInfo.GetTopology(), vdisk01, lb1) - i5.LocalParts(groupInfo.Type);
            UNIT_ASSERT(parts == TVectorType(0x40, 3));
        }

        Y_UNIT_TEST(IngressPartsWeMustHaveLocally) {
            TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::ErasureMirror3, 2, 4);
            TLogoBlobID lb1(78364, 2, 763, 0, 0, 0);
            STR << "INFO: " << TIngress::PrintVDisksForLogoBlob(&groupInfo, lb1) << "\n";
            using TGroupId = TGroupId;
            // subgroup
            TVDiskID vdisk01 = TVDiskID(TGroupId::Zero(), 1, 0, 0, 1);
            TVDiskID vdisk10 = TVDiskID(TGroupId::Zero(), 1, 0, 1, 0);
            TVDiskID vdisk21 = TVDiskID(TGroupId::Zero(), 1, 0, 2, 1);
            TVDiskID vdisk30 = TVDiskID(TGroupId::Zero(), 1, 0, 3, 0);

            // correspondings parts
            // main
            TIngress i1 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk01, TLogoBlobID(lb1, 1));
            TIngress i2 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk10, TLogoBlobID(lb1, 2));
            TIngress i3 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk21, TLogoBlobID(lb1, 3));
            // handoff
            TIngress i4 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk30, TLogoBlobID(lb1, 3));
            TIngress i5 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk30, TLogoBlobID(lb1, 1));
            TIngress i6 = i4;
            i6.Merge(i5);

            auto printLocalParts = [&] (const TIngress &i, const TVDiskID vd) {
                auto v = i.PartsWeMustHaveLocally(&groupInfo.GetTopology(), vd, lb1);
                STR << "vd=" << vd.ToString() << " v=" << v.ToString() << "\n";
            };

            printLocalParts(i1, vdisk01);
            printLocalParts(i2, vdisk10);
            printLocalParts(i3, vdisk21);
            printLocalParts(i4, vdisk30);
            printLocalParts(i5, vdisk30);
            printLocalParts(i6, vdisk30);

            // main
            UNIT_ASSERT(i1.PartsWeMustHaveLocally(&groupInfo.GetTopology(), vdisk01, lb1) == TVectorType(0b10000000, 3));
            UNIT_ASSERT(i2.PartsWeMustHaveLocally(&groupInfo.GetTopology(), vdisk10, lb1) == TVectorType(0b01000000, 3));
            UNIT_ASSERT(i3.PartsWeMustHaveLocally(&groupInfo.GetTopology(), vdisk21, lb1) == TVectorType(0b00100000, 3));
            // handoff
            UNIT_ASSERT(i4.PartsWeMustHaveLocally(&groupInfo.GetTopology(), vdisk30, lb1) == TVectorType(0b00100000, 3));
            UNIT_ASSERT(i5.PartsWeMustHaveLocally(&groupInfo.GetTopology(), vdisk30, lb1) == TVectorType(0b10000000, 3));
            UNIT_ASSERT(i6.PartsWeMustHaveLocally(&groupInfo.GetTopology(), vdisk30, lb1) == TVectorType(0b10100000, 3));
        }

        Y_UNIT_TEST(IngressLocalParts) {
            TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::ErasureMirror3, 2, 4);
            TLogoBlobID lb1(0, 1, 0, 0, 0, 0);
            TBlobStorageGroupInfo::TVDiskIds vDisks;
            TBlobStorageGroupInfo::TServiceIds serviceIds;
            groupInfo.PickSubgroup(lb1.Hash(), &vDisks, &serviceIds);
            TVDiskID vdiskM1 = vDisks[0];
            TVDiskID vdiskM2 = vDisks[1];
            //TVDiskID vdiskM3 = vDisks[2];
            TVDiskID vdisk00 = vDisks[3];
            // replicas for this disk:
            // main1
            // main2
            // main3
            // handoff

            // for main disk 0
            TIngress i1 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdiskM1, TLogoBlobID(lb1, 1));
            {
                TVectorType vec = i1.LocalParts(groupInfo.Type);
                TVectorType canon(0, 3);
                canon.Set(0);
                UNIT_ASSERT(vec == canon);
            }

            // for main disk 1
            TIngress i2 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdiskM2, TLogoBlobID(lb1, 2));
            {
                TVectorType vec = i2.LocalParts(groupInfo.Type);
                TVectorType canon(0, 3);
                canon.Set(1);
                UNIT_ASSERT(vec == canon);
            }

            // for handoff disk
            TIngress i3 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk00, TLogoBlobID(lb1, 1));
            TIngress i4 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk00, TLogoBlobID(lb1, 3));
            {
                TIngress res;
                TIngress::Merge(res, i3);
                TIngress::Merge(res, i4);
                TVectorType vec = res.LocalParts(groupInfo.Type);
                TVectorType canon(0, 3);
                canon.Set(0);
                canon.Set(2);
                UNIT_ASSERT(vec == canon);
            }
        }

        Y_UNIT_TEST(IngressCreateFromRepl) {
            TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::Erasure4Plus2Block, 1, 8);
            TLogoBlobID id(0, 1, 0, 0, 10, 0);
            const ui8 subgroup = groupInfo.Type.BlobSubgroupSize();
            TBlobStorageGroupInfo::TVDiskIds vdisks;
            TBlobStorageGroupInfo::TServiceIds services;
            groupInfo.PickSubgroup(id.Hash(), &vdisks, &services);
            const ui8 totalParts = groupInfo.Type.TotalPartCount();

            for (ui32 vdisk = 0; vdisk < subgroup; ++vdisk) {
                for (ui32 mask = 1; mask < (1U << totalParts); ++mask) {
                    NMatrix::TVectorType parts(0, totalParts);
                    for (ui8 i = 0; i < totalParts; ++i) {
                        if (mask >> i & 1) {
                            parts.Set(i);
                        }
                    }

                    if (vdisk < totalParts) {
                        if (parts.CountBits() > 1 || !parts.Get(vdisk)) {
                            continue;
                        }
                    }

                    TIngress ingress = TIngress::CreateFromRepl(&groupInfo.GetTopology(), vdisks[vdisk], id, parts);

                    UNIT_ASSERT_EQUAL(ingress.LocalParts(groupInfo.Type), parts);

                    if (vdisk < totalParts) {
                        UNIT_ASSERT_EQUAL(ingress.KnownParts(groupInfo.Type, vdisk), parts & NMatrix::TVectorType(0x80 >> vdisk, totalParts));
                        for (ui32 i = 0; i < groupInfo.Type.Handoff(); ++i) {
                            UNIT_ASSERT(ingress.KnownParts(groupInfo.Type, totalParts + i).Empty());
                        }
                    } else {
                        for (ui32 i = 0; i < totalParts; ++i) {
                            UNIT_ASSERT(ingress.KnownParts(groupInfo.Type, i).Empty());
                        }
                        for (ui32 i = 0; i < groupInfo.Type.Handoff(); ++i) {
                            UNIT_ASSERT_EQUAL(ingress.KnownParts(groupInfo.Type, totalParts + i), totalParts + i == vdisk ? parts : NMatrix::TVectorType(0, totalParts));
                        }
                    }
                }
            }
        }

        Y_UNIT_TEST(IngressGetMainReplica) {
            TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::ErasureMirror3, 2, 4);
            TLogoBlobID lb1(0, 1, 0, 0, 0, 0);
            TBlobStorageGroupInfo::TVDiskIds vDisks;
            TBlobStorageGroupInfo::TServiceIds serviceIds;
            groupInfo.PickSubgroup(lb1.Hash(), &vDisks, &serviceIds);
            TVDiskID vdiskM1 = vDisks[0];
            TVDiskID vdiskM2 = vDisks[1];
            TVDiskID vdiskM3 = vDisks[2];
            // replicas for this blob
            // main1
            // main2
            // main3
            // handoff


            UNIT_ASSERT(TVDiskIdShort(vdiskM1) == TIngress::GetMainReplica(&groupInfo.GetTopology(), TLogoBlobID(lb1, 1)));
            UNIT_ASSERT(TVDiskIdShort(vdiskM2) == TIngress::GetMainReplica(&groupInfo.GetTopology(), TLogoBlobID(lb1, 2)));
            UNIT_ASSERT(TVDiskIdShort(vdiskM3) == TIngress::GetMainReplica(&groupInfo.GetTopology(), TLogoBlobID(lb1, 3)));
        }

        Y_UNIT_TEST(IngressHandoffPartsDelete) {
            TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::ErasureMirror3, 2, 4);
            TLogoBlobID lb1(0, 1, 0, 0, 0, 0);
            TBlobStorageGroupInfo::TVDiskIds vDisks;
            TBlobStorageGroupInfo::TServiceIds serviceIds;
            groupInfo.PickSubgroup(lb1.Hash(), &vDisks, &serviceIds);
            TVDiskID vdiskM1 = vDisks[0];
            TVDiskID vdiskM2 = vDisks[1];
            TVDiskID vdiskM3 = vDisks[2];
            TVDiskID vdisk00 = vDisks[3];
            // replicas for this logoblob:
            // main1
            // main2
            // main3
            // handoff

            TVectorType emptyVec(0, 3);

            // for main disk 0
            TIngress i1 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdiskM1, TLogoBlobID(lb1, 1));
            TIngress::TPairOfVectors res1 = i1.HandoffParts(&groupInfo.GetTopology(), vdiskM1, lb1);
            UNIT_ASSERT(res1 == TIngress::TPairOfVectors(emptyVec, emptyVec));
            TIngress i1woLocal = i1.CopyWithoutLocal(groupInfo.Type);

            // for main disk 1
            TIngress i2 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdiskM2, TLogoBlobID(lb1, 2));
            TIngress::TPairOfVectors res2 = i2.HandoffParts(&groupInfo.GetTopology(), vdiskM2, lb1);
            UNIT_ASSERT(res2 == TIngress::TPairOfVectors(emptyVec, emptyVec));
            TIngress i2woLocal = i2.CopyWithoutLocal(groupInfo.Type);

            // for handoff
            TIngress i3 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk00, TLogoBlobID(lb1, 2));
            TIngress i4 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk00, TLogoBlobID(lb1, 3));

            TIngress i5;
            i5.Merge(i1woLocal);
            i5.Merge(i2woLocal);
            i5.Merge(i3);
            i5.Merge(i4);
            TIngress::TPairOfVectors res3 = i5.HandoffParts(&groupInfo.GetTopology(), vdisk00, lb1);
            {
                TVectorType moveVec(0, 3);
                moveVec.Set(2);
                TVectorType delVec(0, 3);
                delVec.Set(1);
                UNIT_ASSERT(res3 == TIngress::TPairOfVectors(moveVec, delVec));
            }

            i5.DeleteHandoff(&groupInfo.GetTopology(), vdisk00, TLogoBlobID(lb1, 2));
            TIngress::TPairOfVectors res4 = i5.HandoffParts(&groupInfo.GetTopology(), vdisk00, lb1);
            {
                TVectorType moveVec(0, 3);
                moveVec.Set(2);
                TVectorType delVec(0, 3);
                UNIT_ASSERT(res4 == TIngress::TPairOfVectors(moveVec, delVec));
            }

            // for main disk 2
            TIngress i6 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdiskM3, TLogoBlobID(lb1, 3));
            TIngress i6woLocal = i6.CopyWithoutLocal(groupInfo.Type);

            i5.Merge(i6woLocal);
            TIngress::TPairOfVectors res5 = i5.HandoffParts(&groupInfo.GetTopology(), vdisk00, lb1);
            {
                TVectorType moveVec(0, 3);
                TVectorType delVec(0, 3);
                delVec.Set(2);
                UNIT_ASSERT(res5 == TIngress::TPairOfVectors(moveVec, delVec));
            }
        }


        Y_UNIT_TEST(IngressCacheMirror3) {
            TBlobStorageGroupInfo info(TBlobStorageGroupType::ErasureMirror3, 2, 4);
            TVDiskID vdisk(TGroupId::Zero(), 1, 0, 1 /*domain*/, 0 /*vdisk*/);
            TIngressCachePtr cache = TIngressCache::Create(info.PickTopology(), vdisk);

            UNIT_ASSERT(cache->VDiskOrderNum == 2);
            UNIT_ASSERT(cache->TotalVDisks == 8);
            UNIT_ASSERT(cache->DomainsNum == 4);
            UNIT_ASSERT(cache->DisksInDomain == 2);
            UNIT_ASSERT(cache->Handoff == 1);
            UNIT_ASSERT(cache->BarrierIngressValueMask == 0xFF);
            UNIT_ASSERT(cache->BarrierIngressDomainMask == 0x3);
        }


        Y_UNIT_TEST(IngressCache4Plus2) {
            TBlobStorageGroupInfo info(TBlobStorageGroupType::Erasure4Plus2Block, 2, 8);
            TVDiskID vdisk(TGroupId::Zero(), 1, 0, 3 /*domain*/, 1 /*vdisk*/);
            TIngressCachePtr cache = TIngressCache::Create(info.PickTopology(), vdisk);

            UNIT_ASSERT(cache->VDiskOrderNum == 7);
            UNIT_ASSERT(cache->TotalVDisks == 16);
            UNIT_ASSERT(cache->DomainsNum == 8);
            UNIT_ASSERT(cache->DisksInDomain == 2);
            UNIT_ASSERT(cache->Handoff == 2);
            UNIT_ASSERT(cache->BarrierIngressValueMask == 0xFFFF);
            UNIT_ASSERT(cache->BarrierIngressDomainMask == 0x3);
        }

        Y_UNIT_TEST(BarrierIngressQuorumBasicMirror3_4_2) {
            TBlobStorageGroupInfo info(TBlobStorageGroupType::ErasureMirror3, 2, 4);
            TVDiskID vdisk(TGroupId::Zero(), 1, 0, 1 /*domain*/, 0 /*vdisk*/);
            TIngressCachePtr cache = TIngressCache::Create(info.PickTopology(), vdisk);

            TBarrierIngress i1(2);
            TBarrierIngress i2(5);
            TBarrierIngress i3(6);

            TBarrierIngress merged;
            TBarrierIngress::Merge(merged, i1);
            TBarrierIngress::Merge(merged, i2);
            TBarrierIngress::Merge(merged, i3);
            UNIT_ASSERT(!merged.IsQuorum(cache.Get()));

            TBarrierIngress i4(ui8(0));
            TBarrierIngress::Merge(merged, i4);
            UNIT_ASSERT(!merged.IsQuorum(cache.Get()));

            TBarrierIngress::Merge(merged, i4);
            TBarrierIngress::Merge(merged, i1);
            TBarrierIngress::Merge(merged, i2);
            TBarrierIngress::Merge(merged, i3);
            UNIT_ASSERT(!merged.IsQuorum(cache.Get()));

            TBarrierIngress i5(1);
            TBarrierIngress::Merge(merged, i5);
            UNIT_ASSERT(!merged.IsQuorum(cache.Get()));

            TBarrierIngress i6(3);
            TBarrierIngress::Merge(merged, i6);
            UNIT_ASSERT(!merged.IsQuorum(cache.Get()));

            TBarrierIngress i7(7);
            TBarrierIngress::Merge(merged, i7);
            UNIT_ASSERT(merged.IsQuorum(cache.Get()));
        }


        Y_UNIT_TEST(BarrierIngressQuorumBasic4Plus2_8_1) {
            TBlobStorageGroupInfo info(TBlobStorageGroupType::Erasure4Plus2Block, 1, 8);
            TVDiskID vdisk(TGroupId::Zero(), 1, 0, 4 /*domain*/, 0 /*vdisk*/);
            TIngressCachePtr cache = TIngressCache::Create(info.PickTopology(), vdisk);

            TBarrierIngress i1(2);
            TBarrierIngress i2(5);
            TBarrierIngress i3(6);

            TBarrierIngress merged;
            TBarrierIngress::Merge(merged, i1);
            TBarrierIngress::Merge(merged, i2);
            TBarrierIngress::Merge(merged, i3);
            UNIT_ASSERT(!merged.IsQuorum(cache.Get()));

            TBarrierIngress i4(ui8(0));
            TBarrierIngress::Merge(merged, i4);
            UNIT_ASSERT(!merged.IsQuorum(cache.Get()));

            TBarrierIngress::Merge(merged, i4);
            TBarrierIngress::Merge(merged, i1);
            TBarrierIngress::Merge(merged, i2);
            TBarrierIngress::Merge(merged, i3);
            UNIT_ASSERT(!merged.IsQuorum(cache.Get()));

            TBarrierIngress i5(1);
            TBarrierIngress::Merge(merged, i5);
            UNIT_ASSERT(!merged.IsQuorum(cache.Get()));

            TBarrierIngress i6(3);
            TBarrierIngress::Merge(merged, i6);
            UNIT_ASSERT(merged.IsQuorum(cache.Get()));

            TBarrierIngress i7(7);
            TBarrierIngress::Merge(merged, i7);
            UNIT_ASSERT(merged.IsQuorum(cache.Get()));

            TBarrierIngress i8(4);
            TBarrierIngress::Merge(merged, i8);
            UNIT_ASSERT(merged.IsQuorum(cache.Get()));
        }


        Y_UNIT_TEST(BarrierIngressQuorumMirror3) {
            TBlobStorageGroupInfo info(TBlobStorageGroupType::ErasureMirror3, 2, 4);
            using TGroupId = TGroupId;
            TVDiskID vdisk(TGroupId::Zero(), 1, 0, 1 /*domain*/, 0 /*vdisk*/);
            TIngressCachePtr cache = TIngressCache::Create(info.PickTopology(), vdisk);

            TVDiskID vdisk00 = TVDiskID(TGroupId::Zero(), 1, 0, 0, 0);
            TVDiskID vdisk01 = TVDiskID(TGroupId::Zero(), 1, 0, 0, 1);
            TVDiskID vdisk10 = TVDiskID(TGroupId::Zero(), 1, 0, 1, 0);
            TVDiskID vdisk11 = TVDiskID(TGroupId::Zero(), 1, 0, 1, 1);
            TVDiskID vdisk20 = TVDiskID(TGroupId::Zero(), 1, 0, 2, 0);
            TVDiskID vdisk21 = TVDiskID(TGroupId::Zero(), 1, 0, 2, 1);
            TVDiskID vdisk30 = TVDiskID(TGroupId::Zero(), 1, 0, 3, 0);
            TVDiskID vdisk31 = TVDiskID(TGroupId::Zero(), 1, 0, 3, 1);

            UNIT_ASSERT(info.GetOrderNumber(vdisk00) == 0);
            UNIT_ASSERT(info.GetOrderNumber(vdisk01) == 1);
            UNIT_ASSERT(info.GetOrderNumber(vdisk10) == 2);
            UNIT_ASSERT(info.GetOrderNumber(vdisk11) == 3);
            UNIT_ASSERT(info.GetOrderNumber(vdisk20) == 4);
            UNIT_ASSERT(info.GetOrderNumber(vdisk21) == 5);
            UNIT_ASSERT(info.GetOrderNumber(vdisk30) == 6);
            UNIT_ASSERT(info.GetOrderNumber(vdisk31) == 7);

            TBarrierIngress i0(info.GetOrderNumber(vdisk00));
            TBarrierIngress i1(info.GetOrderNumber(vdisk01));
            TBarrierIngress i2(info.GetOrderNumber(vdisk10));
            TBarrierIngress i3(info.GetOrderNumber(vdisk11));
            TBarrierIngress i4(info.GetOrderNumber(vdisk20));
            TBarrierIngress i5(info.GetOrderNumber(vdisk21));
            TBarrierIngress i6(info.GetOrderNumber(vdisk30));
            TBarrierIngress i7(info.GetOrderNumber(vdisk31));


            TBarrierIngress merged;
            TBarrierIngress::Merge(merged, i1);
            UNIT_ASSERT(!merged.IsQuorum(cache.Get()));
            TBarrierIngress::Merge(merged, i2);
            UNIT_ASSERT(!merged.IsQuorum(cache.Get()));
            TBarrierIngress::Merge(merged, i5);
            UNIT_ASSERT(!merged.IsQuorum(cache.Get()));
            TBarrierIngress::Merge(merged, i6);
            UNIT_ASSERT(!merged.IsQuorum(cache.Get()));

            TBarrierIngress::Merge(merged, i7);
            UNIT_ASSERT(!merged.IsQuorum(cache.Get()));
            TBarrierIngress::Merge(merged, i3);
            UNIT_ASSERT(!merged.IsQuorum(cache.Get()));
            TBarrierIngress::Merge(merged, i0);
            UNIT_ASSERT(merged.IsQuorum(cache.Get()));
            TBarrierIngress::Merge(merged, i4);
            UNIT_ASSERT(merged.IsQuorum(cache.Get()));
        }


        Y_UNIT_TEST(IngressPrintDistribution) {
            TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::ErasureMirror3, 2, 4);

            for (ui32 i = 0; i < 3000; i++) {
                TLogoBlobID lb(0, 1, i, 0, 0, 0);
                TBlobStorageGroupInfo::TVDiskIds vDisks;
                TBlobStorageGroupInfo::TServiceIds serviceIds;
                groupInfo.PickSubgroup(lb.Hash(), &vDisks, &serviceIds);
                if (vDisks[0].FailDomain == 0 && vDisks[0].VDisk == 0 &&
                    vDisks[1].FailDomain == 1 && vDisks[1].VDisk == 1 &&
                    vDisks[2].FailDomain == 2 && vDisks[2].VDisk == 0 &&
                    vDisks[3].FailDomain == 3 && vDisks[3].VDisk == 1) {
                    for (unsigned j = 0; j < 4; j++) {
                        auto f = [] (unsigned p) {
                            if (p < 3)
                                return "main";
                            else
                                return "handoff";
                        };
                        STR << "Step=" << i << " LogoBlobID=" << lb.ToString() << " VDiskID=" << vDisks[j].ToString()
                            << " " << f(j) << "\n";
                    }
                }
            }
        }
    }

} // NKikimr
