#include "keys.h"

#include <ydb/core/blobstorage/crypto/crypto.h>
#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/blobstorage/crypto/secured_block.h>

#include <util/system/file.h>

namespace NKikimr::NPDiskTool {

static bool HashKeyFile(const TString& path, const TString& pinIn, ui64& outKey, TIssueLog& issues) {
    TFileHandle file(path, OpenExisting | RdOnly);
    if (!file.IsOpen()) {
        issues.Error("key-file", TStringBuilder() << "Cannot open " << path.Quote());
        return false;
    }
    const ui64 length = file.GetLength();
    if (length == 0) {
        issues.Error("key-file", TStringBuilder() << "Empty key container " << path.Quote());
        return false;
    }
    TString data = TString::Uninitialized(length);
    const size_t bytesRead = file.Read(data.Detach(), length);
    if (bytesRead != length) {
        issues.Error("key-file", TStringBuilder() << "Short read of " << path.Quote());
        return false;
    }

    TString pin = pinIn;
    if (pin.empty()) {
        pin = "EmptyPin";
    }

    NKikimr::THashCalculator hasher;
    hasher.SetKey(reinterpret_cast<const ui8*>(pin.data()), pin.size());
    hasher.Hash(data.Detach(), data.size());
    ui64 hash2 = 0;
    outKey = hasher.GetHashResult(&hash2);
    SecureWipeBuffer(reinterpret_cast<ui8*>(data.Detach()), data.size());
    return true;
}

TMainKey MakeMainKey(
    const TVector<ui64>& numericKeys,
    const TString& keyFile,
    const TString& pin,
    bool keySpecified,
    TIssueLog& issues)
{
    TMainKey mainKey;
    for (ui64 k : numericKeys) {
        mainKey.Keys.push_back(k);
    }
    if (keyFile) {
        ui64 hashed = 0;
        if (HashKeyFile(keyFile, pin, hashed, issues)) {
            mainKey.Keys.push_back(hashed);
        }
    }
    if (mainKey.Keys.empty()) {
        mainKey.Keys.push_back(NPDisk::YdbDefaultPDiskSequence);
        if (!keySpecified) {
            issues.Warning("main-key",
                "No --main-key / --key-file given; using the built-in default PDisk sequence");
        }
    }
    mainKey.IsInitialized = true;
    return mainKey;
}

} // namespace NKikimr::NPDiskTool
