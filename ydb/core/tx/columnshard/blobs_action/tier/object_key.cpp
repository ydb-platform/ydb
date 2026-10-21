#include "object_key.h"

#include <util/string/builder.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

namespace {

ui64 MixObjectKeyHash(ui64 value) {
    value += ~(value << 32);
    value ^= value >> 22;
    value += ~(value << 13);
    value ^= value >> 8;
    value += value << 3;
    value ^= value >> 15;
    value += ~(value << 27);
    value ^= value >> 31;
    return value;
}

ui64 GetObjectKeyHash(const TLogoBlobID& blobId) {
    ui64 hash = blobId.Cookie();
    hash = MixObjectKeyHash(hash) ^ blobId.Step();
    hash = MixObjectKeyHash(hash) ^ blobId.Generation();
    return MixObjectKeyHash(hash) ^ blobId.TabletID();
}

}   // namespace

ui32 TObjectKey::GetChannelForWriting() const {
    return StorageId.GetObjectKeyPrefix() ? TreeLayoutChannel : FlatLayoutChannel;
}

TString TObjectKey::Make(const TLogoBlobID& blobId) const {
    if (blobId.Channel() != TreeLayoutChannel) {
        return blobId.ToString();
    }

    const ui64 hash = GetObjectKeyHash(blobId);
    static constexpr char digits[] = "0123456789abcdefghijklmnopqrstuvwxyz";
    TStringBuilder key;
    if (StorageId.GetObjectKeyPrefix() && !StorageId.GetObjectKeyPrefix()->empty()) {
        key << *StorageId.GetObjectKeyPrefix() << '/';
    }

    return key << blobId.TabletID() << '/' << blobId.Generation() << '/' << digits[hash % 36] << '/' << digits[hash / 36 % 36] << '/'
               << blobId.ToString();
}

bool TObjectKey::Parse(const TString& key, TLogoBlobID& blobId, TString& error) const {
    TStringBuf blobIdStr(key);
    if (const size_t pos = blobIdStr.rfind('/'); pos != TStringBuf::npos) {
        blobIdStr = blobIdStr.SubStr(pos + 1);
    }

    TLogoBlobID parsed;
    if (!TLogoBlobID::Parse(parsed, TString(blobIdStr), error)) {
        return false;
    }

    if (Make(parsed) != key) {
        error = "object key does not match the blob id it contains";
        return false;
    }

    blobId = parsed;
    return true;
}

}   // namespace NKikimr::NOlap::NBlobOperations::NTier
