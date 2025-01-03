#include "export_checksum.h"

#include <openssl/sha.h>

#include <util/string/hex.h>

namespace NKikimr::NDataShard {

class TSHA256 : public IExportChecksum {
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

TString ComputeExportChecksum(TStringBuf data) {
    IExportChecksum::TPtr checksum(CreateExportChecksum());
    checksum->AddData(data);
    return checksum->Serialize();
}

IExportChecksum* CreateExportChecksum() {
    return new TSHA256();
}

} // NKikimr::NDataShard
