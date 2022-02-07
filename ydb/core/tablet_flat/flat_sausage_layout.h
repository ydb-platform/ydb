#pragma once

#include <ydb/core/base/logoblob.h>

namespace NKikimr {
namespace NPageCollection {

    /* Page collection binary layout (integers are little endian):

      RAW := THeader
            , { Blobs * TBlobId }
            , { Pages * TEntry }
            , { Pages * TExtra }
            , Inbound
            , ui32_crc;
     */

    using TBlobId = TLogoBlobID;

    enum EMagic: ui64 {
        Magic = 0x44320999F0037888ull,
    };

    struct THeader {
        ui64 Magic = Max<ui64>();
        ui32 Blobs = 0;
        ui32 Pages = 0;
    };

    struct TEntry {
        TEntry(ui64 data, ui64 inPlace)
            : Page(data)
            , Inplace(inPlace)
        {

        }

        ui64 Page = 0;      /* Page end offset      */
        ui64 Inplace = 0;   /* Inplace end offset   */
    };

    struct TExtra {
        TExtra(ui32 type, ui32 crc)
            : Type(type)
            , Crc32(crc)
        {

        }

        ui32 Type = Max<ui32>();
        ui32 Crc32 = Max<ui32>();
    };
}
}
