#pragma once

#include <util/genetic/string.h>
#include <util/string/builder.h>

namespace NKikimr {

inline constexpr size_t S3KeyFanoutBase = 36;

inline TString MakeS3KeyFanout(const size_t hash) {
    static constexpr char vec[] = "0123456789abcdefghijklmnopqrstuvwxyz";
    return TStringBuilder() << vec[hash % S3KeyFanoutBase] << '/' << vec[hash / S3KeyFanoutBase % S3KeyFanoutBase];
}

}  // namespace NKikimr