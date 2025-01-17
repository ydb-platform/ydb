#include "yql_hash_builder.h"

namespace NYql {

THashBuilder::THashBuilder() {
    SHA256_Init(&Sha256);
}

void THashBuilder::Update(const void* data, size_t len) {
    SHA256_Update(&Sha256, data, len);
}

void THashBuilder::Update(TStringBuf data) {
    SHA256_Update(&Sha256, data.data(), data.size());
}

void THashBuilder::Update(const TString& data) {
    SHA256_Update(&Sha256, data.data(), data.size());
}

TString THashBuilder::Finish() {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_Final(hash, &Sha256);
    return TString((const char*)hash, (const char*)hash + SHA256_DIGEST_LENGTH);
}

} // NYql
