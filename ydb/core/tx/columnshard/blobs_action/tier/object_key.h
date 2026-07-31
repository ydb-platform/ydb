#pragma once

#include <ydb/core/base/logoblob.h>

#include <util/generic/string.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

// Layout of S3 object keys for tier blobs.
//
// Blobs used to be stored in the bucket root under their blob id string, so a bucket was a flat pile of objects
// sharing a single key prefix. New blobs are placed into a tree instead:
//
//     <tabletId>/<generation>/<x>/<y>/[<blob id>]
//
// where <x>/<y> is the same fan-out BlobDepot builds for its own objects (see MakeS3KeyFanout).
//
// The layout of a blob is derived from the blob id itself: a tier blob does not belong to any blobstorage channel,
// so the channel field of its id marks the layout it was written with. That way blobs of both layouts live in one
// bucket and are read and deleted by any tablet, with no migration and no extra state to keep.
class TObjectKey {
public:
    static constexpr ui32 FlatLayoutChannel = TLogoBlobID::MaxChannel;
    static constexpr ui32 TreeLayoutChannel = TLogoBlobID::MaxChannel - 1;

    // Channel to stamp into ids of blobs being written now, i.e. the layout new blobs get.
    static ui32 GetChannelForWriting();

    static TString Make(const TLogoBlobID& blobId);

    // Restores the blob id from an object key of either layout. The key is rebuilt from the parsed id and compared
    // with the original one, so a key that does not belong to us is rejected instead of being misread.
    static bool Parse(const TString& key, TLogoBlobID& blobId, TString& error);
};

}   // namespace NKikimr::NOlap::NBlobOperations::NTier
