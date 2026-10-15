#include "blobstorage_ingress.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>

#include <array>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

#define STR Cnull

namespace NKikimr {

    using namespace NMatrix;

    namespace {

        constexpr auto Species = TBlobStorageGroupType::Erasure8Plus2Block;

        TVectorType Parts(ui32 mask) {
            TVectorType result(0, 10);
            for (ui32 i = 0; i != 10; ++i) {
                if (mask & (1u << i)) {
                    result.Set(i);
                }
            }
            return result;
        }

        // Persistent ingress numbers bits from the most significant bit of each byte.
        // Construct its expected bytes independently of all shifted-vector accessors.
        ui64 RawBit(ui32 bit) {
            std::array<ui8, 8> bytes{};
            bytes[bit / 8] = 0x80u >> (bit % 8);
            ui64 raw;
            memcpy(&raw, bytes.data(), sizeof(raw));
            return raw;
        }

        ui64 ExpectedRaw(ui32 main, ui32 h0, ui32 h1, ui32 local) {
            ui64 raw = 0;
            for (ui32 part = 0; part != 10; ++part) {
                if (main & (1u << part)) {
                    raw |= RawBit(2 + part);
                }
                if (local & (1u << part)) {
                    raw |= RawBit(12 + part);
                }
                if (h0 & (1u << part)) {
                    raw |= RawBit(23 + 2 * part);
                }
                if (h1 & (1u << part)) {
                    raw |= RawBit(43 + 2 * part);
                }
            }
            return raw;
        }

        struct TBlock82Fixture {
            TBlobStorageGroupInfo Info{Species, 1, 12};
            const TLogoBlobID Id{0x123456, 5, 9, 0, 1000, 7};
            TBlobStorageGroupInfo::TVDiskIds Disks;

            TBlock82Fixture() {
                Info.PickSubgroup(Id.Hash(), &Disks, nullptr);
                UNIT_ASSERT_VALUES_EQUAL(Disks.size(), 12);
            }

            TIngress Make(ui32 main, ui32 h0, ui32 h1, bool local) const {
                TIngress result;
                for (ui32 part = 0; part != 10; ++part) {
                    for (ui32 family = 0; family != 3; ++family) {
                        const ui32 mask = family == 0 ? main : family == 1 ? h0 : h1;
                        if (mask & (1u << part)) {
                            const ui32 disk = family == 0 ? part : 9 + family;
                            const auto ingress = local
                                ? TIngress::CreateIngressWithLocal(&Info.GetTopology(), Disks[disk], TLogoBlobID(Id, part + 1))
                                : TIngress::CreateIngressWOLocal(&Info.GetTopology(), Disks[disk], TLogoBlobID(Id, part + 1));
                            UNIT_ASSERT(ingress);
                            result.Merge(*ingress);
                        }
                    }
                }
                return result;
            }

            void Check(const TIngress& ingress, ui32 main, ui32 h0, ui32 h1, ui32 local) const {
                UNIT_ASSERT_VALUES_EQUAL(ingress.Raw(), ExpectedRaw(main, h0, h1, local));
                const TIngress fromRaw(ingress.Raw());
                UNIT_ASSERT_EQUAL(fromRaw.LocalParts(Info.Type), Parts(local));
                UNIT_ASSERT_EQUAL(fromRaw.PartsWeKnowAbout(Info.Type), Parts(main | h0 | h1));
                TSubgroupPartLayout expected;
                for (ui32 disk = 0; disk != 12; ++disk) {
                    const ui32 mask = disk < 10 ? main & (1u << disk) : disk == 10 ? h0 : h1;
                    UNIT_ASSERT_EQUAL(fromRaw.KnownParts(Info.Type, disk), Parts(mask));
                    UNIT_ASSERT_EQUAL(fromRaw.PartsWeMustHaveLocally(&Info.GetTopology(), Disks[disk], Id), Parts(mask));
                    for (ui32 part = 0; part != 10; ++part) {
                        if (mask & (1u << part)) {
                            expected.AddItem(disk, part, Info.Type);
                        }
                    }
                }
                UNIT_ASSERT(expected == TSubgroupPartLayout::CreateFromIngress(fromRaw, Info.Type));
                const auto withoutLocal = fromRaw.CopyWithoutLocal(Info.Type);
                UNIT_ASSERT_VALUES_EQUAL(withoutLocal.Raw(), ExpectedRaw(main, h0, h1, 0));
                UNIT_ASSERT(withoutLocal.LocalParts(Info.Type).Empty());
                UNIT_ASSERT_EQUAL(fromRaw.ReplaceLocal(Info.Type, Parts(0x300)).LocalParts(Info.Type), Parts(0x300));
                UNIT_ASSERT_VALUES_EQUAL(fromRaw.ReplaceLocal(Info.Type, Parts(0x300)).Raw(), ExpectedRaw(main, h0, h1, 0x300));
            }
        };

    } // anonymous namespace

    Y_UNIT_TEST_SUITE(TBlobStorageIngress) {
        Y_UNIT_TEST(Block82AllThirtyValidPlacementsAndInvalidMainPlacements) {
            const TBlock82Fixture f;
            ui32 valid = 0, invalid = 0;
            for (ui32 disk = 0; disk != 12; ++disk) {
                for (ui32 part = 0; part != 10; ++part) {
                    const auto local = TIngress::CreateIngressWithLocal(&f.Info.GetTopology(), f.Disks[disk],
                        TLogoBlobID(f.Id, part + 1));
                    const auto remote = TIngress::CreateIngressWOLocal(&f.Info.GetTopology(), f.Disks[disk],
                        TLogoBlobID(f.Id, part + 1));
                    if (disk < 10 && disk != part) {
                        UNIT_ASSERT(!local && !remote);
                        ++invalid;
                    } else {
                        UNIT_ASSERT(local && remote);
                        const ui32 main = disk < 10 ? 1u << part : 0;
                        const ui32 h0 = disk == 10 ? 1u << part : 0;
                        const ui32 h1 = disk == 11 ? 1u << part : 0;
                        f.Check(*local, main, h0, h1, 1u << part);
                        f.Check(*remote, main, h0, h1, 0);
                        ++valid;
                    }
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(valid, 30);
            UNIT_ASSERT_VALUES_EQUAL(invalid, 90);
        }

        Y_UNIT_TEST(Block82All3072SingleFamilyMasksAndFixedMixedMasks) {
            const TBlock82Fixture f;
            ui32 cases = 0;
            for (ui32 family = 0; family != 3; ++family) {
                for (ui32 mask = 0; mask != 1024; ++mask) {
                    const ui32 main = family == 0 ? mask : 0;
                    const ui32 h0 = family == 1 ? mask : 0;
                    const ui32 h1 = family == 2 ? mask : 0;
                    const auto ingress = f.Make(main, h0, h1, true);
                    f.Check(ingress, main, h0, h1, mask);
                    if (family) {
                        const auto fromRepl = TIngress::CreateFromRepl(&f.Info.GetTopology(), f.Disks[9 + family], f.Id, Parts(mask));
                        UNIT_ASSERT_VALUES_EQUAL(fromRepl.Raw(), ingress.Raw());
                    }
                    ++cases;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(cases, 3072);
            for (const auto& masks : std::array<std::array<ui32, 3>, 7>{{
                    {0x3ff, 0x3ff, 0x3ff}, {0x155, 0x2aa, 0x300}, {0x200, 0x100, 0x80},
                    {0x80, 0x200, 0x100}, {0x1ff, 0x200, 0x200}, {0, 0x300, 0x300}, {0x3fe, 1, 0}}}) {
                for (bool local : {false, true}) {
                    const auto ingress = f.Make(masks[0], masks[1], masks[2], local);
                    f.Check(ingress, masks[0], masks[1], masks[2], local ? masks[0] | masks[1] | masks[2] : 0);
                }
            }
        }

        Y_UNIT_TEST(Block82HandoffPresentDeletedAndMergedStates) {
            const TBlock82Fixture f;
            for (ui32 disk : {10, 11}) {
                for (ui32 part : {0, 7, 8, 9}) {
                    const auto partId = TLogoBlobID(f.Id, part + 1);
                    const auto one = Parts(1u << part);
                    const TVectorType empty(0, 10);
                    const TIngress present = *TIngress::CreateIngressWithLocal(&f.Info.GetTopology(), f.Disks[disk], partId);
                    UNIT_ASSERT_EQUAL(present.GetVDiskHandoffVec(&f.Info.GetTopology(), f.Disks[disk], f.Id), one);
                    UNIT_ASSERT_EQUAL(present.GetVDiskHandoffDeletedVec(&f.Info.GetTopology(), f.Disks[disk], f.Id), empty);
                    const auto move = present.HandoffParts(&f.Info.GetTopology(), f.Disks[disk], f.Id);
                    UNIT_ASSERT_EQUAL(move.first, one);
                    UNIT_ASSERT_EQUAL(move.second, empty);

                    TIngress withMain = present;
                    withMain.Merge(*TIngress::CreateIngressWOLocal(&f.Info.GetTopology(), f.Disks[part], partId));
                    const auto remove = withMain.HandoffParts(&f.Info.GetTopology(), f.Disks[disk], f.Id);
                    UNIT_ASSERT_EQUAL(remove.first, empty);
                    UNIT_ASSERT_EQUAL(remove.second, one);

                    TIngress deletedOnly;
                    deletedOnly.DeleteHandoff(&f.Info.GetTopology(), f.Disks[disk], partId);
                    const ui64 deletedBit = RawBit(22 + (disk - 10) * 20 + 2 * part);
                    UNIT_ASSERT_VALUES_EQUAL(deletedOnly.Raw(), deletedBit); // state 10
                    UNIT_ASSERT_EQUAL(deletedOnly.KnownParts(f.Info.Type, disk), empty);
                    UNIT_ASSERT_EQUAL(deletedOnly.GetVDiskHandoffDeletedVec(&f.Info.GetTopology(), f.Disks[disk], f.Id), one);

                    TIngress deletedPresent = present;
                    deletedPresent.DeleteHandoff(&f.Info.GetTopology(), f.Disks[disk], partId, true);
                    UNIT_ASSERT_VALUES_EQUAL(deletedPresent.Raw(), present.CopyWithoutLocal(f.Info.Type).Raw() | deletedBit); // 11
                    UNIT_ASSERT_EQUAL(deletedPresent.KnownParts(f.Info.Type, disk), empty);
                    UNIT_ASSERT_EQUAL(deletedPresent.LocalParts(f.Info.Type), empty);
                    UNIT_ASSERT_EQUAL(deletedPresent.GetVDiskHandoffDeletedVec(&f.Info.GetTopology(), f.Disks[disk], f.Id), one);
                    const auto layout = TSubgroupPartLayout::CreateFromIngress(TIngress(deletedPresent.Raw()), f.Info.Type);
                    UNIT_ASSERT_VALUES_EQUAL(layout.CountDistinctParts(f.Info.Type), 0);

                    TIngress merged = deletedOnly;
                    merged.Merge(present.CopyWithoutLocal(f.Info.Type));
                    UNIT_ASSERT_VALUES_EQUAL(merged.Raw(), deletedPresent.Raw());
                }
            }
        }

        Y_UNIT_TEST(Block82LastIngressBitAndTextOutput) {
            const TBlock82Fixture f;
            const auto ingress = f.Make(0x300, 0x180, 0x200, false);
            UNIT_ASSERT(ingress.Raw() & RawBit(61));
            UNIT_ASSERT(!(ingress.Raw() & (RawBit(62) | RawBit(63))));
            const TString text = ingress.ToString(&f.Info.GetTopology(), f.Disks[11], f.Id);
            UNIT_ASSERT_C(text.Contains("main: 0 0 0 0 0 0 0 0 1 1"), text);
            UNIT_ASSERT_C(text.Contains("handoff1: 00 00 00 00 00 00 00 00 00 01"), text);
            UNIT_ASSERT_VALUES_EQUAL(TIngress(ingress.Raw()).Raw(), ingress.Raw());
        }


        Y_UNIT_TEST(Ingress) {
            TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::ErasureMirror3of4, 2, 8);
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

        // TODO: https://github.com/ydb-platform/ydb/issues/32548
        // Y_UNIT_TEST(IngressPartsWeMustHaveLocally) {
        //     TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::ErasureMirror3of4, 2, 8);
        //     TLogoBlobID lb1(78364, 2, 763, 0, 0, 0);
        //     STR << "INFO: " << TIngress::PrintVDisksForLogoBlob(&groupInfo, lb1) << "\n";
        //     using TGroupId = TGroupId;
        //     // subgroup
        //     TVDiskID vdisk01 = TVDiskID(TGroupId::Zero(), 1, 0, 0, 1);
        //     TVDiskID vdisk10 = TVDiskID(TGroupId::Zero(), 1, 0, 1, 0);
        //     TVDiskID vdisk21 = TVDiskID(TGroupId::Zero(), 1, 0, 2, 1);
        //     TVDiskID vdisk30 = TVDiskID(TGroupId::Zero(), 1, 0, 3, 0);

        //     // correspondings parts
        //     // main
        //     TIngress i1 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk01, TLogoBlobID(lb1, 1));
        //     TIngress i2 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk10, TLogoBlobID(lb1, 2));
        //     TIngress i3 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk21, TLogoBlobID(lb1, 3));
        //     // handoff
        //     TIngress i4 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk30, TLogoBlobID(lb1, 3));
        //     TIngress i5 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk30, TLogoBlobID(lb1, 1));
        //     TIngress i6 = i4;
        //     i6.Merge(i5);

        //     auto printLocalParts = [&] (const TIngress &i, const TVDiskID vd) {
        //         auto v = i.PartsWeMustHaveLocally(&groupInfo.GetTopology(), vd, lb1);
        //         STR << "vd=" << vd.ToString() << " v=" << v.ToString() << "\n";
        //     };

        //     printLocalParts(i1, vdisk01);
        //     printLocalParts(i2, vdisk10);
        //     printLocalParts(i3, vdisk21);
        //     printLocalParts(i4, vdisk30);
        //     printLocalParts(i5, vdisk30);
        //     printLocalParts(i6, vdisk30);

        //     // main
        //     UNIT_ASSERT(i1.PartsWeMustHaveLocally(&groupInfo.GetTopology(), vdisk01, lb1) == TVectorType(0b10000000, 3));
        //     UNIT_ASSERT(i2.PartsWeMustHaveLocally(&groupInfo.GetTopology(), vdisk10, lb1) == TVectorType(0b01000000, 3));
        //     UNIT_ASSERT(i3.PartsWeMustHaveLocally(&groupInfo.GetTopology(), vdisk21, lb1) == TVectorType(0b00100000, 3));
        //     // handoff
        //     UNIT_ASSERT(i4.PartsWeMustHaveLocally(&groupInfo.GetTopology(), vdisk30, lb1) == TVectorType(0b00100000, 3));
        //     UNIT_ASSERT(i5.PartsWeMustHaveLocally(&groupInfo.GetTopology(), vdisk30, lb1) == TVectorType(0b10000000, 3));
        //     UNIT_ASSERT(i6.PartsWeMustHaveLocally(&groupInfo.GetTopology(), vdisk30, lb1) == TVectorType(0b10100000, 3));
        // }

        Y_UNIT_TEST(IngressLocalParts) {
            TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::ErasureMirror3of4, 2, 8);
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
            TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::Erasure4Plus2Block, 2, 4);
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

        // TODO: https://github.com/ydb-platform/ydb/issues/32548
        // Y_UNIT_TEST(IngressHandoffPartsDelete) {
        //     TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::ErasureMirror3of4, 2, 8);
        //     TLogoBlobID lb1(0, 1, 0, 0, 0, 0);
        //     TBlobStorageGroupInfo::TVDiskIds vDisks;
        //     TBlobStorageGroupInfo::TServiceIds serviceIds;
        //     groupInfo.PickSubgroup(lb1.Hash(), &vDisks, &serviceIds);
        //     TVDiskID vdiskM1 = vDisks[0];
        //     TVDiskID vdiskM2 = vDisks[1];
        //     TVDiskID vdiskM3 = vDisks[2];
        //     TVDiskID vdisk00 = vDisks[3];
        //     // replicas for this logoblob:
        //     // main1
        //     // main2
        //     // main3
        //     // handoff

        //     TVectorType emptyVec(0, 3);

        //     // for main disk 0
        //     TIngress i1 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdiskM1, TLogoBlobID(lb1, 1));
        //     TIngress::TPairOfVectors res1 = i1.HandoffParts(&groupInfo.GetTopology(), vdiskM1, lb1);
        //     UNIT_ASSERT(res1 == TIngress::TPairOfVectors(emptyVec, emptyVec));
        //     TIngress i1woLocal = i1.CopyWithoutLocal(groupInfo.Type);

        //     // for main disk 1
        //     TIngress i2 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdiskM2, TLogoBlobID(lb1, 2));
        //     TIngress::TPairOfVectors res2 = i2.HandoffParts(&groupInfo.GetTopology(), vdiskM2, lb1);
        //     UNIT_ASSERT(res2 == TIngress::TPairOfVectors(emptyVec, emptyVec));
        //     TIngress i2woLocal = i2.CopyWithoutLocal(groupInfo.Type);

        //     // for handoff
        //     TIngress i3 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk00, TLogoBlobID(lb1, 2));
        //     TIngress i4 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdisk00, TLogoBlobID(lb1, 3));

        //     TIngress i5;
        //     i5.Merge(i1woLocal);
        //     i5.Merge(i2woLocal);
        //     i5.Merge(i3);
        //     i5.Merge(i4);
        //     TIngress::TPairOfVectors res3 = i5.HandoffParts(&groupInfo.GetTopology(), vdisk00, lb1);
        //     {
        //         TVectorType moveVec(0, 3);
        //         moveVec.Set(2);
        //         TVectorType delVec(0, 3);
        //         delVec.Set(1);
        //         UNIT_ASSERT(res3 == TIngress::TPairOfVectors(moveVec, delVec));
        //     }

        //     i5.DeleteHandoff(&groupInfo.GetTopology(), vdisk00, TLogoBlobID(lb1, 2));
        //     TIngress::TPairOfVectors res4 = i5.HandoffParts(&groupInfo.GetTopology(), vdisk00, lb1);
        //     {
        //         TVectorType moveVec(0, 3);
        //         moveVec.Set(2);
        //         TVectorType delVec(0, 3);
        //         UNIT_ASSERT(res4 == TIngress::TPairOfVectors(moveVec, delVec));
        //     }

        //     // for main disk 2
        //     TIngress i6 = *TIngress::CreateIngressWithLocal(&groupInfo.GetTopology(), vdiskM3, TLogoBlobID(lb1, 3));
        //     TIngress i6woLocal = i6.CopyWithoutLocal(groupInfo.Type);

        //     i5.Merge(i6woLocal);
        //     TIngress::TPairOfVectors res5 = i5.HandoffParts(&groupInfo.GetTopology(), vdisk00, lb1);
        //     {
        //         TVectorType moveVec(0, 3);
        //         TVectorType delVec(0, 3);
        //         delVec.Set(2);
        //         UNIT_ASSERT(res5 == TIngress::TPairOfVectors(moveVec, delVec));
        //     }
        // }

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

        Y_UNIT_TEST(IngressPrintDistribution) {
            TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::Erasure4Plus2Block, 2, 4);

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
