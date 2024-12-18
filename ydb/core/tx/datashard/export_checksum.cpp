#include "export_checksum.h"

#include <util/string/hex.h>

namespace NKikimr::NDataShard {

TExportChecksum::TExportChecksum() {
    SHA256_Init(&Context);
}

void TExportChecksum::AddData(const char* data, size_t dataSize) {
    SHA256_Update(&Context, data, dataSize);
}

TString TExportChecksum::Serialize() {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_Final(hash, &Context);
    return to_lower(HexEncode(hash, SHA256_DIGEST_LENGTH));
}

TString TExportChecksum::Compute(const char* data, size_t dataSize) {
    TExportChecksum checksum;
    checksum.AddData(data, dataSize);
    return checksum.Serialize();
}

} // NKikimr::NDataShard
