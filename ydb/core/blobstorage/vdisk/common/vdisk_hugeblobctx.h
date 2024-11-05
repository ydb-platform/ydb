#pragma once
#include "defs.h"

namespace NKikimr {

    namespace NHuge {
        class THeap;
    };

    ////////////////////////////////////////////////////////////////////////////
    // THugeSlotsMap
    // Info about huge slots; can get huge chunk info for a blob based on its size.
    ////////////////////////////////////////////////////////////////////////////
    class THugeSlotsMap {
    public:
        struct TSlotInfo {
            ui32 SlotSize;
            ui32 NumberOfSlotsInChunk;

            TSlotInfo(ui32 slotSize, ui32 slotsInChunk)
                : SlotSize(slotSize)
                , NumberOfSlotsInChunk(slotsInChunk)
            {}

            bool operator ==(const TSlotInfo &s) const {
                return SlotSize == s.SlotSize && NumberOfSlotsInChunk == s.NumberOfSlotsInChunk;
            }

            void Output(IOutputStream &str) const {
                str << "{SlotSize# " << SlotSize << " NumberOfSlotsInChunk# " << NumberOfSlotsInChunk << "}";
            }

            TString ToString() const {
                TStringStream str;
                Output(str);
                return str.Str();
            }
        };

        // All slot types
        using TAllSlotsInfo = TVector<TSlotInfo>;
        // Type to address TAllSlotsInfo
        using TIndex = ui16;
        // Size in AppendBlockSize -> index in TAllSlotsInfo
        using TSearchTable = TVector<TIndex>;
        // Idx that indicates there is no record for it in TAllSlotsInfo
        static constexpr TIndex NoOpIdx = Max<TIndex>();


        THugeSlotsMap(ui32 appendBlockSize, TAllSlotsInfo &&slotsInfo, TSearchTable &&searchTable);
        const TSlotInfo *GetSlotInfo(ui32 size) const;
        ui32 AlignByBlockSize(ui32 size) const;
        void Output(IOutputStream &str) const;
        TString ToString() const;

    private:
        const ui32 AppendBlockSize;
        TAllSlotsInfo AllSlotsInfo;
        TSearchTable SearchTable;
    };

    ////////////////////////////////////////////////////////////////////////////
    // A place for metadata about huge blobs
    ////////////////////////////////////////////////////////////////////////////
    class THugeBlobCtx {
    public:
        // this value is multiply of AppendBlockSize and is calculated from Config->MinHugeBlobSize
        const std::shared_ptr<const THugeSlotsMap> HugeSlotsMap;
        const bool AddHeader;

        // check whether this NEW blob is huge one; userPartSize doesn't include any metadata stored along with blob
        bool IsHugeBlob(TBlobStorageGroupType gtype, const TLogoBlobID& fullId, ui32 minREALHugeBlobInBytes) const;

        THugeBlobCtx(const std::shared_ptr<const THugeSlotsMap> &hugeSlotsMap, bool addHeader)
            : HugeSlotsMap(hugeSlotsMap)
            , AddHeader(addHeader)
        {
        }
    };

    using THugeBlobCtxPtr = std::shared_ptr<THugeBlobCtx>;

} // NKikimr
