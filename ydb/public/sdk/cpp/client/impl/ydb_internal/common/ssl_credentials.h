#pragma once

#include "type_switcher.h"

namespace NYdb {

struct TSslCredentials {
    bool IsEnabled = false;
    TStringType CaCert;
    TStringType Cert;
    TStringType PrivateKey;

    TSslCredentials() = default;

    TSslCredentials(const bool isEnabled, const TStringType& caCert = "", const TStringType& cert = "", const TStringType& privateKey = "")
    : IsEnabled(isEnabled), CaCert(caCert), Cert(cert), PrivateKey(privateKey) {}

    bool operator==(const TSslCredentials& other) const {
        return IsEnabled == other.IsEnabled &&
               CaCert == other.CaCert &&
               Cert == other.Cert &&
               PrivateKey == other.PrivateKey;
    }
};

} // namespace NYdb
