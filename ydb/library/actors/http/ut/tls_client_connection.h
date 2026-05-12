#pragma once

#include <util/network/ip.h>
#include <util/generic/string.h>

namespace NHttp::NTest {

// Sends an HTTP request via TLS connection with client certificate (mTLS)
void SendTlsRequest(
    const TIpPort port,
    const TString& clientCertFile,
    const TString& clientKeyFile,
    const TString& caCertFile,
    const TString& httpRequest);

} // namespace NHttp::NTest
