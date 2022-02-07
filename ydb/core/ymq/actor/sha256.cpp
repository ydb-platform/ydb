#include "sha256.h"

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/string/hex.h>

#include <openssl/sha.h>

#include <stdexcept>

namespace NKikimr::NSQS {

TString CalcSHA256(TStringBuf data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_CTX sha256;
    if (!SHA256_Init(&sha256)) {
        throw std::runtime_error("Failed to init SHA-256");
    }
    if (!SHA256_Update(&sha256, data.data(), data.size())) {
        throw std::runtime_error("Failed to update SHA-256");
    }
    if (!SHA256_Final(hash, &sha256)) {
        throw std::runtime_error("Failed to finalize SHA-256");
    }
    return HexEncode(TStringBuf(reinterpret_cast<const char*>(hash), SHA256_DIGEST_LENGTH));
}

} // namespace NKikimr::NSQS
