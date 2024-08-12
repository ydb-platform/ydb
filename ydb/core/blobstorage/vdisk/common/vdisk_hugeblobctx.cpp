#include "vdisk_hugeblobctx.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>

namespace NKikimr {

    THugeSlotsMap::THugeSlotsMap(ui32 appendBlockSize, TAllSlotsInfo &&slotsInfo, TSearchTable &&searchTable)
        : AppendBlockSize(appendBlockSize)
        , AllSlotsInfo(std::move(slotsInfo))
        , SearchTable(std::move(searchTable))
    {}

    const THugeSlotsMap::TSlotInfo *THugeSlotsMap::GetSlotInfo(ui32 size) const {
        ui32 sizeInBlocks = size / AppendBlockSize;
        sizeInBlocks += !(sizeInBlocks * AppendBlockSize == size);
        const ui64 idx = SearchTable.at(sizeInBlocks);
        return &AllSlotsInfo.at(idx);
    }

    ui32 THugeSlotsMap::AlignByBlockSize(ui32 size) const {
        ui32 sizeInBlocks = size / AppendBlockSize;
        Y_ABORT_UNLESS(sizeInBlocks,  "Blob size to align is smaller than a single block. BlobSize# %" PRIu32, size);
        return sizeInBlocks * AppendBlockSize;
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
            if (idx != NoOpIdx) {
                AllSlotsInfo.at(idx).Output(str);
            } else {
                str << "null";
            }
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
    bool THugeBlobCtx::IsHugeBlob(TBlobStorageGroupType gtype, const TLogoBlobID& fullId, ui32 minREALHugeBlobInBytes) const {
        return gtype.MaxPartSize(fullId) + (AddHeader ? TDiskBlob::HeaderSize : 0) >= minREALHugeBlobInBytes;
    }

} // NKikimr

