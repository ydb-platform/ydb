#pragma once
#include "defs.h"

namespace NKikimr {
    // extract 8 bits of state-storage group
    inline ui32 StateStorageHashFromTabletID(ui64 tabletId) {
        return (ui32)Hash64to32(tabletId);
    }

    // extract 12 bits of hive group id (zero is 'human assigned')
    inline ui64 HiveUidFromTabletID(ui64 tabletId) {
        return (tabletId >> 44) & 0xFFFull;
    }

    inline ui64 UniqPartFromTabletID(ui64 tabletId) {
        return (tabletId & 0x00000FFFFFFFFFFFull);
    }

    inline ui64 AvoidReservedUniqPart(ui64 candidate, ui64 brokenBegin, ui64 brokenEnd) {
        if (candidate >= brokenBegin && candidate < brokenEnd) {
            return brokenEnd;
        }
        return candidate;
    }

    static const ui64 TABLET_ID_BLACKHOLE_BEGIN = 0x800000;
    static const ui64 TABLET_ID_BLACKHOLE_END = 0x900000;

    inline ui64 AvoidReservedUniqPartsBySystemTablets(ui64 candidate) {
        // candidate = AvoidReservedUniqPart(candidate, 0x800000, 0x800100); // coordinators
        // candidate = AvoidReservedUniqPart(candidate, 0x810000, 0x810100); // mediators
        // candidate = AvoidReservedUniqPart(candidate, 0x820000, 0x821000); // allocators
        // candidate = AvoidReservedUniqPart(candidate, 0x840000, 0x860000); // schemeshard
        return AvoidReservedUniqPart(candidate, TABLET_ID_BLACKHOLE_BEGIN, TABLET_ID_BLACKHOLE_END); // for sure
    }

    inline bool IsReservedTabletId(ui64 tabletId) {
        const ui64 uniqPart = UniqPartFromTabletID(tabletId);
        return uniqPart != AvoidReservedUniqPartsBySystemTablets(uniqPart);
    }

    // 8 + 12 + 44
    inline constexpr ui64 MakeTabletID(bool fromHive, ui64 uniqPart) {
        Y_ABORT_UNLESS(uniqPart < (static_cast<ui64>(1) << 44));
        return (static_cast<ui64>(1) << 56)
            | (static_cast<ui64>(fromHive ? 1 : 0) << 44)
            | uniqPart;
    }

    // blob storage controller (exactly one per domain in default state storage group)
    inline ui64 MakeBSControllerID() {
        return MakeTabletID(false, 0x1001);
    }

    // one default hive per domain (in default state storage group!)
    inline ui64 MakeDefaultHiveID() {
        return MakeTabletID(false, 0x0001);
    }

    // cluster management system tablet (exactly one per domain in default state storage group)
    inline ui64 MakeCmsID() {
        return MakeTabletID(false, 0x2000);
    }

    // node broker tablet (exactly one per domain in default state storage group)
    inline ui64 MakeNodeBrokerID() {
        return MakeTabletID(false, 0x2001);
    }

    // tenant slot broker tablet (exactly one per domain in default state storage group)
    inline ui64 MakeTenantSlotBrokerID() {
        return MakeTabletID(false, 0x2002);
    }

    // console tablet (exactly one per domain in default state storage group)
    inline ui64 MakeConsoleID() {
        return MakeTabletID(false, 0x2003);
    }

    // TODO: think about encoding scheme for sibling group hive

    inline TActorId MakeStateStorageProxyID() {
        char x[12] = { 's', 't', 's', 'p', 'r', 'o', 'x', 'y', 0, 0, 0, 0};
        return TActorId(0, TStringBuf(x, 12));
    }

    // NEVER USE THIS
    /*[[deprecated]]*/ inline ui64 MakeBSControllerID(ui32) { return MakeBSControllerID(); }
    /*[[deprecated]]*/ inline ui64 MakeDefaultHiveID(ui32) { return MakeDefaultHiveID(); }
    /*[[deprecated]]*/ inline ui64 MakeTabletID(ui32, ui32 hiveId, ui64 uniqId) { return MakeTabletID(hiveId != 0, uniqId); }
}
