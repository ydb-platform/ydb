#include "vdisk_outofspace.h"

#include <ydb/core/blobstorage/base/html.h>
#include <ydb/core/protos/blobstorage.pb.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TOutOfSpaceState
    ////////////////////////////////////////////////////////////////////////////
    TOutOfSpaceState::TOutOfSpaceState(ui32 totalVDisks, ui32 selfOrderNum,
        const TString& vDiskLogPrefix, std::shared_ptr<NMonGroup::TOutOfSpaceGroup> monGroup)
        : TotalVDisks(totalVDisks)
        , SelfOrderNum(selfOrderNum)
        , VDiskLogPrefix(vDiskLogPrefix)
        , MonGroup(monGroup)
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

    void UpdateMonGroup(NPDisk::TStatusFlags flags, std::shared_ptr<NMonGroup::TOutOfSpaceGroup> monGroup) {
        if (!monGroup) {
            return;
        }

        if (flags & NKikimrBlobStorage::StatusDiskSpaceRed) {
            monGroup->StatusDiskSpaceRed().Inc();
        } else if (flags & NKikimrBlobStorage::StatusDiskSpaceOrange) {
            monGroup->StatusDiskSpaceOrange().Inc();
        } else if (flags & NKikimrBlobStorage::StatusDiskSpaceLightOrange) {
            monGroup->StatusDiskSpaceLightOrange().Inc();
        } else if (flags & NKikimrBlobStorage::StatusDiskSpacePreOrange) {
            monGroup->StatusDiskSpacePreOrange().Inc();
        } else if (flags & NKikimrBlobStorage::StatusDiskSpaceYellowStop) {
            monGroup->StatusDiskSpaceYellowStop().Inc();
        } else if (flags & NKikimrBlobStorage::StatusDiskSpaceLightYellowMove) {
            monGroup->StatusDiskSpaceLightYellowMove().Inc();
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

        UpdateMonGroup(flags, MonGroup);

        if (TlsActivationContext) {
            LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BS_VDISK_CHUNKS,
                VDiskLogPrefix << "Disk space status changed to " <<
                TPDiskSpaceColor_Name(StatusFlagToSpaceColor(flags)));
        }
    }
} // NKikimr
