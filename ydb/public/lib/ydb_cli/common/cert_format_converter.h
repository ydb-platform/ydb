#pragma once
#include "interactive.h"

#include <util/generic/string.h>

#include <utility>

namespace NYdb::NConsoleClient {

// Checks current certificate/private key format.
// Supported formats are: PEM and PKCS#12.
// If certificate/private key are not encoded in PEM format,
// or if private key is encrypted with password phrase,
// tries to convert them to PEM without password protection
// (grpc lib requires client certificate to be encoded in this way).
// In interactive mode asks user to enter password if it is required and not set.
// In non-interactive mode fails in this case.
// Returns <certificate, private key>.
//
// So, supported combinations are:
// 1. Cert and key in PEM format, key is not encrypted.
//    Just return cert and key as is.
// 2. Cert and key in PEM format, key is encrypted.
//    Reencode private key to be suitable for grpc lib (not encrypted). Maybe ask password from user interactively.
// 3. Cert is in PKCS#12 encoding, unencrypted key is embedded into it.
//    Reencode cert and key to PEM
// 4. Cert is in PKCS#12 encoding, encrypted key is embedded into it.
//    Reencode cert and key to PEM without encryption. Maybe ask password from user interactively.
std::pair<TString, TString> ConvertCertToPEM(
    const TString& certificate,
    const TString& privateKey,
    const TString& privateKeyPassword,
    bool isInteractiveMode = IsStdinInteractive()
);

} // namespace NYdb::NConsoleClient
