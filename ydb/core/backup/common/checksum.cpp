#include "checksum.h"

#include <openssl/sha.h>

#include <util/string/hex.h>

namespace NKikimr::NBackup {

class TSHA256 : public IChecksum {
public:
    TSHA256() {
        SHA256_Init(&Context);
    }

    void AddData(TStringBuf data) override {
        SHA256_Update(&Context, data.data(), data.size());
    }

    TString Serialize() override {
        unsigned char hash[SHA256_DIGEST_LENGTH];
        SHA256_Final(hash, &Context);
        return to_lower(HexEncode(hash, SHA256_DIGEST_LENGTH));
    }

private:
    SHA256_CTX Context;
};

TString ComputeChecksum(TStringBuf data) {
    IChecksum::TPtr checksum(CreateChecksum());
    checksum->AddData(data);
    return checksum->Serialize();
}

IChecksum* CreateChecksum() {
    return new TSHA256();
}

TString ChecksumKey(const TString& objKey) {
    return objKey + ".sha256";
}

} // NKikimr::NDataShard
