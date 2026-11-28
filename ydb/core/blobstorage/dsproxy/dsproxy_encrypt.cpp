#include "dsproxy_impl.h"
#include <ydb/core/blobstorage/groupinfo/group_blob_cipher.h>

namespace NKikimr {

    void Encrypt(char *destination, const char *source, size_t shift, size_t sizeBytes, const TLogoBlobID &id,
            const TBlobStorageGroupInfo &info) {
        CryptGroupBlob(destination, source, shift, sizeBytes, id, info);
    }

    void EncryptInplace(TRope& rope, ui32 offset, ui32 size, const TLogoBlobID& id, const TBlobStorageGroupInfo& info) {
        if (info.GetEncryptionMode() == TBlobStorageGroupInfo::EEM_NONE) {
            return;
        }
        auto span = rope.GetContiguousSpanMut();
        Y_ABORT_UNLESS(offset < span.size() && size <= span.size() - offset);
        Encrypt(span.data() + offset, span.data() + offset, offset, size, id, info);
    }

    void Decrypt(char *destination, const char *source, size_t shift, size_t sizeBytes, const TLogoBlobID &id,
            const TBlobStorageGroupInfo &info) {
        Encrypt(destination, source, shift, sizeBytes, id, info);
    }

    void DecryptInplace(TRope& rope, ui32 offset, ui32 shift, ui32 size, const TLogoBlobID& id, const TBlobStorageGroupInfo& info) {
        if (info.GetEncryptionMode() == TBlobStorageGroupInfo::EEM_NONE) {
            return;
        }
        auto span = rope.GetContiguousSpanMut();
        Y_ABORT_UNLESS(offset < span.size() && size <= span.size() - offset);
        Decrypt(span.data() + offset, span.data() + offset, shift, size, id, info);
    }

} // NKikimr
