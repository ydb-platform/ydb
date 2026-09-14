#pragma once

#include <ydb/core/base/logoblob.h>
#include <ydb/core/tx/tiering/tier/identifier.h>

#include <util/generic/string.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TObjectKey {
private:
    const NColumnShard::NTiers::TExternalStorageId StorageId;

public:
    explicit TObjectKey(const TString& storageId)
        : StorageId(NColumnShard::NTiers::TExternalStorageId::FromString(storageId))
    {
    }

    // The channel persists the layout in the blob ID. Reads and GC must never
    // depend on the current value of the feature flag.
    static constexpr ui32 FlatLayoutChannel = TLogoBlobID::MaxChannel;
    static constexpr ui32 TreeLayoutChannel = TLogoBlobID::MaxChannel - 1;

    ui32 GetChannelForWriting() const;

    TString Make(const TLogoBlobID& blobId) const;

    bool Parse(const TString& key, TLogoBlobID& blobId, TString& error) const;
};

}   // namespace NKikimr::NOlap::NBlobOperations::NTier
