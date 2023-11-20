#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hulldefs.h>

namespace NKikimr {

    /////////////////////////////////////////////////////////////////////////
    // TIdxDiskPlaceHolder -- placeholder for SST index
    /////////////////////////////////////////////////////////////////////////
    struct TIdxDiskPlaceHolder {
        static const ui32 Signature = 0x12345679;

        enum class EFlags : ui32 {
            CREATED_BY_REPL = 0x00000001
        };

        struct TInfo {
            ui64 FirstLsn = 0;
            ui64 LastLsn = 0;
            ui64 InplaceDataTotalSize = 0;
            ui64 HugeDataTotalSize = 0;
            ui32 IdxTotalSize = 0;
            ui32 Chunks = 0;
            ui32 IndexParts = 0;
            ui32 Items = 0;
            ui32 ItemsWithInplacedData = 0;
            ui32 ItemsWithHugeData = 0;
            ui32 OutboundItems = 0;
            ui32 Flags = 0;
            TInstant CTime;

            void SetCreatedByRepl() {
                Flags |= static_cast<ui32>(EFlags::CREATED_BY_REPL);
            }

            bool IsCreatedByRepl() const {
                return (Flags & static_cast<ui32>(EFlags::CREATED_BY_REPL)) != 0;
            }

            TInfo &operator +=(const TInfo &info) {
                FirstLsn = Min(FirstLsn, info.FirstLsn);
                LastLsn = Max(LastLsn, info.LastLsn);
                InplaceDataTotalSize += info.InplaceDataTotalSize;
                HugeDataTotalSize += info.HugeDataTotalSize;
                IdxTotalSize += info.IdxTotalSize;
                Chunks += info.Chunks;
                IndexParts += info.IndexParts;
                Items += info.Items;
                ItemsWithInplacedData += info.ItemsWithInplacedData;
                ItemsWithHugeData += info.ItemsWithHugeData;
                OutboundItems += info.OutboundItems;
                // don't know what to do with CTime
                return *this;
            }

            void Output(IOutputStream &str) const {
                str << "[" << (IsCreatedByRepl() ? "REPL" : "COMP") << " CTime# "<< CTime << "]";
            }

            friend bool operator ==(const TInfo& x, const TInfo& y) {
                return x.FirstLsn == y.FirstLsn &&
                    x.LastLsn == y.LastLsn &&
                    x.InplaceDataTotalSize == y.InplaceDataTotalSize &&
                    x.HugeDataTotalSize == y.HugeDataTotalSize &&
                    x.IdxTotalSize == y.IdxTotalSize &&
                    x.Chunks == y.Chunks &&
                    x.IndexParts == y.IndexParts &&
                    x.Items == y.Items &&
                    x.ItemsWithInplacedData == y.ItemsWithInplacedData &&
                    x.ItemsWithHugeData == y.ItemsWithHugeData &&
                    x.OutboundItems == y.OutboundItems &&
                    x.Flags == y.Flags &&
                    x.CTime == y.CTime;
            }
        };

        ui32 MagicNumber;
        TDiskPart PrevPart;
        ui64 SstId;
        TInfo Info;

        TIdxDiskPlaceHolder(ui64 sstId)
            : MagicNumber(Signature)
            , PrevPart()
            , SstId(sstId)
            , Info()
        {
            Y_DEBUG_ABORT_UNLESS(PrevPart.Empty());
        }

        friend bool operator ==(const TIdxDiskPlaceHolder& x, const TIdxDiskPlaceHolder& y) {
            return x.MagicNumber == y.MagicNumber &&
                x.PrevPart == y.PrevPart &&
                x.SstId == y.SstId &&
                x.Info == y.Info;
        }
    };

    /////////////////////////////////////////////////////////////////////////
    // TIdxDiskLinker -- linker record for SST index parts
    /////////////////////////////////////////////////////////////////////////
    struct TIdxDiskLinker {
        TDiskPart PrevPart;

        TIdxDiskLinker()
            : PrevPart()
        {}

        TIdxDiskLinker(const TDiskPart &part)
            : PrevPart(part)
        {}

        friend bool operator ==(const TIdxDiskLinker& x, const TIdxDiskLinker& y) {
            return x.PrevPart == y.PrevPart;
        }
    };

} // NKikimr

