#include "source_id_encoding.h"

#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/yexception.h>
#include <util/string/strip.h>

namespace NKikimr {
namespace NPQ {
namespace NSourceIdEncoding {

struct TTags {
    static constexpr char Simple = 0;
    static constexpr char Base64 = 1;
};

static constexpr TStringBuf Base64Prefix = "base64:";

TString EncodeSimple(const TString& sourceId) {
    return TString(1, TTags::Simple) + sourceId;
}

TString DecodeSimple(const TString& sourceId) {
    Y_VERIFY(!sourceId.empty() && sourceId[0] == TTags::Simple);
    return sourceId.substr(1);
}

TString EncodeBase64(const TString& sourceId) {
    const auto size = sourceId.size();
    const auto prefixSize = Base64Prefix.size();

    if (size == prefixSize || sourceId.find('=') != TString::npos) {
        ythrow yexception() << "started from \"" << Base64Prefix << "\" must be valid base64 encoded string, without padding";
    }

    const auto remainder = (size - prefixSize) % 4;
    const auto padding = remainder ? TString(4 - remainder, '=') : "";
    const auto effective = sourceId.substr(prefixSize) + padding;

    try {
        return TString(1, TTags::Base64) + Base64StrictDecode(effective);
    } catch (yexception& e) {
        ythrow yexception() << "started from \"" << Base64Prefix << "\" must be valid base64 encoded string: " << e.what();
    }
}

TString DecodeBase64(const TString& sourceId) {
    Y_VERIFY(!sourceId.empty() && sourceId[0] == TTags::Base64);
    return Base64Prefix + StripStringRight(Base64EncodeUrl(sourceId.substr(1)), EqualsStripAdapter(','));
}

TString Encode(const TString& sourceId) {
    if (sourceId.StartsWith(Base64Prefix)) {
        return EncodeBase64(sourceId);
    } else {
        return EncodeSimple(sourceId);
    }
}

TString Decode(const TString& sourceId) {
    Y_VERIFY(!sourceId.empty());

    switch (sourceId[0]) {
    case TTags::Simple:
        return DecodeSimple(sourceId);
    case TTags::Base64:
        return DecodeBase64(sourceId);
    default:
        return sourceId;
    }
}

bool IsValidEncoded(const TString& sourceId) {
    if (sourceId.empty()) {
        return false;
    }

    switch (sourceId[0]) {
    case TTags::Simple:
    case TTags::Base64:
        return true;
    default:
        return false;
    }
}

} // NSourceIdEncoding
} // NPQ
} // NKikimr
