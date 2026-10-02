#pragma once

#include <openssl/sha.h>

#include <util/generic/string.h>
#include <util/generic/strbuf.h>

namespace NKikimr::NTabletFlatExecutor::NBackup {

// Wrapper around SHA256_CTX that supports Intermediate()
class TSha256Hasher {
public:
    static TString Hash(TStringBuf data);

    TSha256Hasher();

    void Update(const void* data, size_t size);
    void Update(TStringBuf s);

    TString Intermediate() const;
    TString Final();

private:
    SHA256_CTX Ctx;
};

} // namespace NKikimr::NTabletFlatExecutor::NBackup
