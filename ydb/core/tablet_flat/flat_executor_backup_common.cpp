#include "flat_executor_backup_common.h"

#include <array>

#include <util/string/hex.h>

namespace NKikimr::NTabletFlatExecutor::NBackup {

namespace {

using TDigest = std::array<unsigned char, SHA256_DIGEST_LENGTH>;

TString FormatDigest(const TDigest& digest) {
    return to_lower(HexEncode(digest.data(), digest.size()));
}

} // anonymous namespace

TSha256Hasher::TSha256Hasher() {
    SHA256_Init(&Ctx);
}

void TSha256Hasher::Update(const void* data, size_t size) {
    SHA256_Update(&Ctx, data, size);
}

void TSha256Hasher::Update(TStringBuf s) {
    Update(s.data(), s.size());
}

TString TSha256Hasher::Intermediate() const {
    SHA256_CTX copy = Ctx;
    TDigest digest;
    SHA256_Final(digest.data(), &copy);
    return FormatDigest(digest);
}

TString TSha256Hasher::Final() {
    TDigest digest;
    SHA256_Final(digest.data(), &Ctx);
    return FormatDigest(digest);
}

TString TSha256Hasher::Hash(TStringBuf data) {
    TSha256Hasher hasher;
    hasher.Update(data);
    return hasher.Final();
}

} // namespace NKikimr::NTabletFlatExecutor::NBackup
