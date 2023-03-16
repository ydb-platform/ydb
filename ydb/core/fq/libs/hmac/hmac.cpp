#include "hmac.h"

#include <openssl/hmac.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <util/generic/yexception.h>

namespace NFq {
TString HmacSha1(const TStringBuf data, const TStringBuf secret) {
    unsigned char md_value[EVP_MAX_MD_SIZE];
    unsigned md_len = 0;

    Y_ENSURE(HMAC(EVP_sha1(), secret.data(), secret.size(), (unsigned char*)data.data(), data.size(), md_value, &md_len),
        "could not encrypt data using HMAC SHA1 method");

    return TString((char*)md_value, md_len);
}

TString HmacSha1Base64(const TStringBuf data, const TStringBuf secret) {
    return Base64Encode(HmacSha1(data, secret));
}
}
