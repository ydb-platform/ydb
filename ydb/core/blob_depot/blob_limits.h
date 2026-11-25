#pragma once

#include <ydb/core/base/logoblob.h>

namespace NKikimr::NBlobDepot {

#pragma pack(push, 1)
    struct TVirtualGroupBlobFooter {
        TLogoBlobID StoredBlobId;
    };
#pragma pack(pop)

    static constexpr ui32 MaxBlobSize = 10 << 20; // 10 MB BlobStorage hard limit

} // NKikimr::NBlobDepot
