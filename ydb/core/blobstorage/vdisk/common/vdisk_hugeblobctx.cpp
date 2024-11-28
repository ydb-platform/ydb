#include "vdisk_hugeblobctx.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>

namespace NKikimr {

    THugeSlotsMap::THugeSlotsMap(ui32 appendBlockSize, ui32 minHugeBlobInBlocks, TAllSlotsInfo &&slotsInfo,
            TSearchTable &&searchTable)
        : AppendBlockSize(appendBlockSize)
        , MinHugeBlobInBlocks(minHugeBlobInBlocks)
        , AllSlotsInfo(std::move(slotsInfo))
        , SearchTable(std::move(searchTable))
    {}

    const THugeSlotsMap::TSlotInfo *THugeSlotsMap::GetSlotInfo(ui32 size) const {
        const ui32 sizeInBlocks = (size + AppendBlockSize - 1) / AppendBlockSize;
        Y_ABORT_UNLESS(MinHugeBlobInBlocks <= sizeInBlocks);
        const ui64 idx = SearchTable.at(sizeInBlocks - MinHugeBlobInBlocks);
        return &AllSlotsInfo.at(idx);
    }

    ui32 THugeSlotsMap::AlignByBlockSize(ui32 size) const {
        return Max(MinHugeBlobInBlocks * AppendBlockSize, size - size % AppendBlockSize);
    }

    void THugeSlotsMap::Output(IOutputStream &str) const {
        str << "{AllSlotsInfo# [\n";
        for (const auto &x : AllSlotsInfo) {
            x.Output(str);
            str << "\n";
        }
        str << "]}\n";
        str << "{SearchTable# [";
        for (const auto &idx : SearchTable) {
            AllSlotsInfo.at(idx).Output(str);
            str << "\n";
        }
        str << "]}";
    }

    TString THugeSlotsMap::ToString() const {
        TStringStream str;
        Output(str);
        return str.Str();
    }

    // check whether this blob is huge one; userPartSize doesn't include any metadata stored along with blob
    bool THugeBlobCtx::IsHugeBlob(TBlobStorageGroupType gtype, const TLogoBlobID& fullId, ui32 minHugeBlobInBytes) const {
        return gtype.MaxPartSize(fullId) + (AddHeader ? TDiskBlob::HeaderSize : 0) >= minHugeBlobInBytes;
    }

} // NKikimr

