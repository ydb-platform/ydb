#include "vdisk_outofspace.h"

#include <ydb/core/blobstorage/base/html.h>
#include <ydb/core/protos/blobstorage.pb.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TOutOfSpaceState
    ////////////////////////////////////////////////////////////////////////////
    TOutOfSpaceState::TOutOfSpaceState(ui32 totalVDisks, ui32 selfOrderNum)
        : TotalVDisks(totalVDisks)
        , SelfOrderNum(selfOrderNum)
    {
        Y_ABORT_UNLESS(totalVDisks <= MaxVDisksInGroup);
        for (ui32 i = 0; i < TotalVDisks; ++i) {
            AtomicSet(AllVDiskFlags[i], TAtomicBase(0));
        }
    }

    NKikimrWhiteboard::EFlag TOutOfSpaceState::ToWhiteboardFlag(const ESpaceColor color) {
        switch (color) {
            case TSpaceColor::GREEN:
            case TSpaceColor::CYAN:
                return NKikimrWhiteboard::EFlag::Green;
            case TSpaceColor::LIGHT_YELLOW:
            case TSpaceColor::YELLOW:
            case TSpaceColor::LIGHT_ORANGE:
                return NKikimrWhiteboard::EFlag::Yellow;
            case TSpaceColor::PRE_ORANGE:
            case TSpaceColor::ORANGE:
                return NKikimrWhiteboard::EFlag::Orange;
            case TSpaceColor::RED:
            case TSpaceColor::BLACK:
            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MIN_SENTINEL_DO_NOT_USE_:
            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MAX_SENTINEL_DO_NOT_USE_:
                return NKikimrWhiteboard::EFlag::Red;
        }
    }

    void TOutOfSpaceState::Update(ui32 vdiskOrderNum, NPDisk::TStatusFlags flags) {
        NPDisk::TStatusFlags curFlags = static_cast<NPDisk::TStatusFlags>(AtomicGet(AllVDiskFlags[vdiskOrderNum]));
        if (curFlags == flags) {
            // nothing changed since last time
            return;
        }

        // setup new value
        AtomicSet(AllVDiskFlags[vdiskOrderNum], static_cast<TAtomicBase>(flags));

        // recalculate merge
        TAtomicBase merged = 0;
        for (ui32 i = 0; i < TotalVDisks; ++i) {
            TAtomicBase value = AtomicGet(AllVDiskFlags[i]);
            merged |= value;
        }

        AtomicSet(GlobalFlags, merged);
    }
} // NKikimr
