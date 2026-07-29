#include "test_key.h"

#include <yt/yt/core/crypto/config.h>

#include <library/cpp/resource/resource.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

std::string GetTestKeyContent(TStringBuf name)
{
    return NResource::Find(std::string("/testdata/") + std::string(name));
}

NCrypto::TPemBlobConfigPtr CreateTestKeyBlob(TStringBuf name)
{
    auto config = New<NCrypto::TPemBlobConfig>();
    config->Value = GetTestKeyContent(name);
    return config;
}

NCrypto::TPemBlobConfigPtr CreateTestKeyFile(TStringBuf name)
{
    auto fileName = std::string("testdata_") + std::string(name);
    auto output = TFileOutput(fileName);
    output.Write(GetTestKeyContent(name));
    output.Finish();
    return NCrypto::TPemBlobConfig::CreateFileReference(fileName);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
