#include "blobstorage_grouptype.h"

#include <library/cpp/digest/old_crc/crc.h>
#include <util/generic/yexception.h>

#define MAX_TOTAL_PARTS 8

#define IS_VERBOSE 0

#if IS_VERBOSE
#   include <util/stream/str.h>
#   define VERBOSE_COUT(a) \
       Cerr << a

static TString DebugFormatBits(ui64 value) {
    TStringStream s;
    for (size_t i = 7; i >=4; --i) {
        s << ((value >> i) & 1);
    }
    s << "_";
    for (size_t i = 3; i <= 3; --i) {
        s << ((value >> i) & 1);
    }
    return s.Str();
}
#else
#   define VERBOSE_COUT(a) \
       while (false) { \
       }
#endif

namespace NKikimr {

struct TBlobStorageErasureParameters {
    ui32 Handoff; // number of selected hinted handoff (1 | 2)
};

static const std::array<TBlobStorageErasureParameters, TErasureType::ErasureSpeciesCount>
        BlobStorageGroupErasureSpeciesParameters{{
    {0} // 0 = ErasureSpicies::ErasureNone
    ,{1} // 1 = ErasureSpicies::ErasureMirror3
    ,{1} // 2 = ErasureSpicies::Erasure3Plus1Block
    ,{1} // 3 = ErasureSpicies::Erasure3Plus1Stipe
    ,{2} // 4 = ErasureSpicies::Erasure4Plus2Block
    ,{2} // 5 = ErasureSpicies::Erasure3Plus2Block
    ,{2} // 6 = ErasureSpicies::Erasure4Plus2Stipe
    ,{2} // 7 = ErasureSpicies::Erasure3Plus2Stipe
    ,{2} // 8 = ErasureSpicies::ErasureMirror3Plus2
    ,{6} // 9 = ErasureSpicies::ErasireMirror3dc
    ,{3} // 10 = ErasureSpicies::Erasure4Plus3Block
    ,{3} // 11 = ErasureSpicies::Erasure4Plus3Stripe
    ,{3} // 12 = ErasureSpicies::Erasure3Plus3Block
    ,{3} // 13 = ErasureSpicies::Erasure3Plus3Stripe
    ,{3} // 14 = ErasureSpicies::Erasure2Plus3Block
    ,{3} // 15 = ErasureSpicies::Erasure2Plus3Stripe
    ,{2} // 16 = ErasureSpicies::Erasure2Plus2Block
    ,{2} // 17 = ErasureSpicies::Erasure2Plus2Stripe
    ,{5} // 18 = ErasureSpicies::ErasureMirror3of4
}};


ui32 TBlobStorageGroupType::BlobSubgroupSize() const {
    const TBlobStorageErasureParameters& erasure = BlobStorageGroupErasureSpeciesParameters[ErasureSpecies];
    return DataParts() + ParityParts() + erasure.Handoff;
}

ui32 TBlobStorageGroupType::Handoff() const {
    const TBlobStorageErasureParameters& erasure = BlobStorageGroupErasureSpeciesParameters[ErasureSpecies];
    return erasure.Handoff;
}

bool TBlobStorageGroupType::IsHandoffInSubgroup(ui32 idxInSubgroup) const {
    return idxInSubgroup >= DataParts() + ParityParts();
}

struct TReorderablePartLayout {
    struct TVDiskParts {
        ui32 PartMaskInv;
        ui8 VDiskIdx;

        TVDiskParts(ui8 vDiskIdx, ui32 partMaskInv)
            : PartMaskInv(partMaskInv)
            , VDiskIdx(vDiskIdx)
        {}
    };
    TStackVec<TVDiskParts, 8> Records;
};

ui32 ReverseMask(ui32 mask) {
    mask = ((mask & 0x0000FFFF) << 16) | ((mask & 0xFFFF0000) >> 16);
    mask = ((mask & 0x00FF00FF) << 8) | ((mask & 0xFF00FF00) >> 8);
    mask = ((mask & 0x0F0F0F0F) << 4) | ((mask & 0xF0F0F0F0) >> 4);
    mask = ((mask & 0x33333333) << 2) | ((mask & 0xCCCCCCCC) >> 2);
    mask = ((mask & 0x55555555) << 1) | ((mask & 0xAAAAAAAA) >> 1);
    return mask;
}

bool TBlobStorageGroupType::CorrectLayout(const TPartLayout &layout, TPartPlacement &outCorrection) const {
    VERBOSE_COUT("Start CorrectLayout" << Endl);
    // TODO: produce 'properly hosted part idx' for each VDisk available
    TReorderablePartLayout remaining;
    TStackVec<ui8, 8> missingParts;

    ui32 totalPartCount = TotalPartCount();
    ui32 blobSubgroupSize = BlobSubgroupSize();

    const ui32 lastBit = 0x80000000;

    ui32 vDiskMask = layout.VDiskMask;
    ui32 slowVDiskMask = layout.SlowVDiskMask;

    VERBOSE_COUT("reverseVDiskMask# " << DebugFormatBits(vDiskMask) << Endl);

    ui32 handoffDestinedPartMaskInv = 0;
    for (ui32 i = 0; i < totalPartCount; ++i) {
        ui32 bit = (1 << i);
        if (vDiskMask & bit) {
            VERBOSE_COUT("layout.VDiskPartMask[" << i << "]# " << DebugFormatBits(layout.VDiskPartMask[i]) << Endl);
            if (!(layout.VDiskPartMask[i] & (1 << i))) {
                if (slowVDiskMask & bit) {
                    missingParts.push_back(i);
                    handoffDestinedPartMaskInv |= (lastBit >> i);
                } else {
                    outCorrection.Records.push_back(TPartPlacement::TVDiskPart(i, i));
                }
            }
        } else {
            missingParts.push_back(i);
            handoffDestinedPartMaskInv |= (lastBit >> i);
        }
    }
    VERBOSE_COUT("handoffDestinedPartMaskInv# " << DebugFormatBits(ReverseMask(handoffDestinedPartMaskInv)) << Endl);

    for (ui32 i = totalPartCount; i < blobSubgroupSize; ++i) {
        if (vDiskMask & ~slowVDiskMask & (1 << i)) {
            VERBOSE_COUT("layout.VDiskPartMask[" << i << "]# " << DebugFormatBits(layout.VDiskPartMask[i]) << Endl);
            remaining.Records.push_back(TReorderablePartLayout::TVDiskParts(i, handoffDestinedPartMaskInv &
                ReverseMask(layout.VDiskPartMask[i]) ));
        }
    }

    while (remaining.Records.size()) {
        // select one from remaining
        ui8 selectedIdx = 0;
        for (ui32 i = 1; i < remaining.Records.size(); ++i) {
            if (remaining.Records[i].PartMaskInv > remaining.Records[selectedIdx].PartMaskInv) {
                selectedIdx = i;
            } else if (remaining.Records[i].PartMaskInv == remaining.Records[selectedIdx].PartMaskInv &&
                remaining.Records[i].VDiskIdx < remaining.Records[selectedIdx].VDiskIdx) {
                selectedIdx = i;
            }
        }
        TReorderablePartLayout::TVDiskParts disk = remaining.Records[selectedIdx];
        remaining.Records.erase(remaining.Records.begin() + selectedIdx);
        ui32 diskPartMaskInv = 0;
        ui32 partIdx = 0;
        for (ui32 i = totalPartCount-1; i != (ui32)-1; --i) {
            if (disk.PartMaskInv & (lastBit >> i)) {
                diskPartMaskInv = (lastBit >> i);
                partIdx = i;
                break;
            }
        }
        disk.PartMaskInv = diskPartMaskInv;
        for (ui32 i = 0; i < remaining.Records.size(); ++i) {
            remaining.Records[i].PartMaskInv &= ~diskPartMaskInv;
        }
        if (diskPartMaskInv != 0) {
            for (ui32 i = 0; i < missingParts.size(); ++i) {
                if (missingParts[i] == partIdx) {
                    missingParts.erase(missingParts.begin() + i);
                    break;
                }
            }
        } else {
            if (missingParts.size()) {
                VERBOSE_COUT(missingParts.size() << " missing part " << (ui64)missingParts[0] << Endl);
                outCorrection.Records.push_back(
                    TPartPlacement::TVDiskPart(disk.VDiskIdx, missingParts[0]));
                missingParts.erase(missingParts.begin());
            }
        }
    }
    VERBOSE_COUT("End CorrectLayout" << Endl);
    return (missingParts.size() == 0);
}

ui64 TBlobStorageGroupType::PartSize(const TLogoBlobID &id) const {
    // Y_ABORT_UNLESS(id.PartId()); // TODO(alexvru): uncomment when dsproxy is ready for KIKIMR-9881
    if (GetErasure() == TBlobStorageGroupType::ErasureMirror3of4 && id.PartId() == 3) {
        return 0;
    } else {
        return TErasureType::PartSize((TErasureType::ECrcMode)id.CrcMode(), id.BlobSize());
    }
}

ui64 TBlobStorageGroupType::MaxPartSize(const TLogoBlobID &id) const {
    Y_ABORT_UNLESS(!id.PartId());
    return MaxPartSize((TErasureType::ECrcMode)id.CrcMode(), id.BlobSize());
}

ui64 TBlobStorageGroupType::MaxPartSize(TErasureType::ECrcMode crcMode, ui32 blobSize) const {
    return TErasureType::PartSize(crcMode, blobSize);
}

bool TBlobStorageGroupType::PartFits(ui32 partId, ui32 idxInSubgroup) const {
    switch (GetErasure()) {
        case TBlobStorageGroupType::ErasureMirror3dc:
            return idxInSubgroup % 3 == partId - 1;

        case TBlobStorageGroupType::ErasureMirror3of4:
            return idxInSubgroup >= 4 || partId == 3 || idxInSubgroup % 2 == partId - 1;

        default:
            return idxInSubgroup >= TotalPartCount() || idxInSubgroup == partId - 1;
    }
}

}
