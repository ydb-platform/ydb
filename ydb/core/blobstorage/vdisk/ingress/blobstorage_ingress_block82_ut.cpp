#include "blobstorage_ingress.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>
#include <library/cpp/testing/unittest/registar.h>

#include <array>

using namespace NKikimr;
using namespace NKikimr::NMatrix;

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

struct TFixture {
    TBlobStorageGroupInfo Info{Species, 2, 12};
    const TLogoBlobID Id{0x123456, 5, 9, 0, 1000, 7};
    TBlobStorageGroupInfo::TVDiskIds Disks;

    TFixture() {
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

Y_UNIT_TEST_SUITE(IngressBlock82) {
    Y_UNIT_TEST(AllThirtyValidPlacementsAndInvalidMainPlacements) {
        const TFixture f;
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

    Y_UNIT_TEST(All3072SingleFamilyMasksAndFixedMixedMasks) {
        const TFixture f;
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

    Y_UNIT_TEST(HandoffPresentDeletedAndMergedStates) {
        const TFixture f;
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

    Y_UNIT_TEST(LastIngressBitAndTextOutput) {
        const TFixture f;
        const auto ingress = f.Make(0x300, 0x180, 0x200, false);
        UNIT_ASSERT(ingress.Raw() & RawBit(61));
        UNIT_ASSERT(!(ingress.Raw() & (RawBit(62) | RawBit(63))));
        const TString text = ingress.ToString(&f.Info.GetTopology(), f.Disks[11], f.Id);
        UNIT_ASSERT_C(text.Contains("main: 0 0 0 0 0 0 0 0 1 1"), text);
        UNIT_ASSERT_C(text.Contains("handoff1: 00 00 00 00 00 00 00 00 00 01"), text);
        UNIT_ASSERT_VALUES_EQUAL(TIngress(ingress.Raw()).Raw(), ingress.Raw());
    }
}
