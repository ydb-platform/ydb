#pragma once

#include <ydb/core/base/logoblob.h>

namespace NKikimr {

    class TBlobStorageGroupInfo;

    void CryptGroupBlob(char *destination, const char *source, size_t shift, size_t sizeBytes,
        const TLogoBlobID& id, const TBlobStorageGroupInfo& info);

} // NKikimr
