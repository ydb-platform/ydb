#include "dsproxy_impl.h"

namespace NKikimr {

    void Encrypt(char *destination, const char *source, size_t shift, size_t sizeBytes, const TLogoBlobID &id,
            const TBlobStorageGroupInfo &info) {
        switch (info.GetEncryptionMode()) {
            case TBlobStorageGroupInfo::EEM_NONE:
                {
                    if (source != destination) {
                        memcpy(destination, source, sizeBytes);
                    }
                }
                return;
            case TBlobStorageGroupInfo::EEM_ENC_V1:
                {
                    // Get the 'Tenant group key'
                    const TCypherKey &tenantGroupKey = *info.GetCypherKey();
                    Y_ABORT_UNLESS(tenantGroupKey.GetIsKeySet());
                    Y_ABORT_UNLESS(tenantGroupKey.GetKeySizeBytes() == 32);

                    // Obtain Hash_key(Tablet,Generation)
                    THashCalculator keyHash;
                    keyHash.SetKey(tenantGroupKey);
                    ui64 tabletId = id.TabletID();
                    keyHash.Hash(&tabletId, sizeof(ui64));
                    ui32 generation = id.Generation();
                    keyHash.Hash(&generation, sizeof(ui32));
                    ui64 hash2 = 0;
                    ui64 hash1 = keyHash.GetHashResult(&hash2);

                    // Create the 'Blob key' in 2 steps:
                    //   1) Copy the 'Teneat group key' to the 'Blob key'
                    TCypherKey blobKey(tenantGroupKey);
                    //   2) Mix-in the Hash_key(Tablet,Generation)
                    ui8 *blobKeyData = nullptr;
                    ui32 blobKeySizeBytes = 0;
                    blobKey.MutableKeyBytes(&blobKeyData, &blobKeySizeBytes);
                    Y_ABORT_UNLESS(blobKeySizeBytes == 32);
                    ui64 *p = (ui64*)blobKeyData;
                    *p ^= hash1;
                    p++;
                    *p ^= hash2;

                    // Preapre nonce = {Step(32), Cookie(24), Channel(8)}
                    ui64 nonce = (ui64(id.Step()) << 32) | (ui64(id.Cookie()) << 8) | (ui64(id.Channel()));

                    // Encrypt the data
                    TStreamCypher cypher;
                    cypher.SetKey(blobKey);
                    cypher.StartMessage(nonce, shift);
                    cypher.Encrypt(destination, source, sizeBytes);
                }
                return;
        }
        Y_ABORT_UNLESS(false, "Unexpected Encryption Mode# %" PRIu64, (ui64)info.GetEncryptionMode());
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
