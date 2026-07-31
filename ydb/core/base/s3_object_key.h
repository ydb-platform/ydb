#pragma once

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NKikimr {

// Object storages shard their key space by prefix, so writing every object under a single prefix turns the whole
// storage into one hot spot. Both BlobDepot and ColumnShard tiering spread their objects over the same 36-ary
// two-level fan-out, built from a hash of the object identity.
inline constexpr size_t S3KeyFanoutBase = 36;

inline TString MakeS3KeyFanout(const size_t hash) {
    static constexpr char vec[] = "0123456789abcdefghijklmnopqrstuvwxyz";
    return TStringBuilder() << vec[hash % S3KeyFanoutBase] << '/' << vec[hash / S3KeyFanoutBase % S3KeyFanoutBase];
}

} // NKikimr
