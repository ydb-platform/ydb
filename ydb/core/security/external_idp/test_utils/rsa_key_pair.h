#pragma once

#include <util/generic/string.h>

namespace NKikimr::NExternalIdpTestUtils {

struct TRsaKeyPair {
    TString PrivateKeyPem;
    TString PublicKeyPem;
    TString X5cBase64;
};

TRsaKeyPair GenerateRsaKeyPair();

} // namespace NKikimr::NExternalIdpTestUtils
