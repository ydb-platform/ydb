#pragma once

#include <string>

namespace NYdb::inline Dev {

struct TSslCredentials {
    bool IsEnabled = false;
    std::string CaCert;
    std::string Cert;
    std::string PrivateKey;

    TSslCredentials() = default;

    TSslCredentials(const bool isEnabled, const std::string& caCert = "", const std::string& cert = "", const std::string& privateKey = "")
    : IsEnabled(isEnabled), CaCert(caCert), Cert(cert), PrivateKey(privateKey) {}

    bool operator==(const TSslCredentials& other) const {
        return IsEnabled == other.IsEnabled &&
               CaCert == other.CaCert &&
               Cert == other.Cert &&
               PrivateKey == other.PrivateKey;
    }
};

} // namespace NYdb
