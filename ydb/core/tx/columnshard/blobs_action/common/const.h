#pragma once
#include <util/generic/string.h>
#include <util/system/types.h>

namespace NKikimr::NOlap::NBlobOperations {

class TGlobal {
public:
    // Channels 0 and 1 belong to the executor (log, local database); portions start here.
    static constexpr ui32 FirstDataChannel = 2;

    static const inline TString DefaultStorageId = "__DEFAULT";
    static const inline TString MemoryStorageId = "__MEMORY";
    static const inline TString LocalMetadataStorageId = "__LOCAL_METADATA";
};

}   // namespace NKikimr::NOlap::NBlobOperations
