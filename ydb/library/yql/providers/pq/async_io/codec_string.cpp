#include "codec_string.h"

#include <util/generic/yexception.h>
#include <util/string/ascii.h>
#include <util/string/cast.h>

namespace NYql::NDq {

std::pair<NYdb::NTopic::ECodec, std::optional<int>> ParseCodecString(TStringBuf str) {
    // Split on the last '_'. If the suffix is a valid positive integer, it is the level.
    std::optional<int> level;

    TStringBuf name = str;
    int parsedLevel = 0;
    if (TryFromString(name.RNextTok('_'), parsedLevel) && parsedLevel > 0) {
        level = parsedLevel;
    } else {
        name = str;
    }

    const TString nameLower = to_lower(TString(name));
    if (nameLower == "raw") {
        return {NYdb::NTopic::ECodec::RAW, level};
    }
    if (nameLower == "gzip") {
        return {NYdb::NTopic::ECodec::GZIP, level};
    }
    if (nameLower == "lzop") {
        return {NYdb::NTopic::ECodec::LZOP, level};
    }
    if (nameLower == "zstd") {
        return {NYdb::NTopic::ECodec::ZSTD, level};
    }
    throw yexception() << "Unknown codec '" << name << "' тАФ should have been caught by optimizer validation";
}

} // namespace NYql::NDq
