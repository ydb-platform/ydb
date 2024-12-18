#pragma once

#include <openssl/sha.h>

#include <util/generic/string.h>

namespace NKikimr::NDataShard {

class TExportChecksum {
public:
    TExportChecksum();

    void AddData(const char* data, size_t dataSize);
    TString Serialize();

    static TString Compute(const char* data, size_t dataSize);
private:
    SHA256_CTX Context;
};

} // NKikimr::NDataShard
