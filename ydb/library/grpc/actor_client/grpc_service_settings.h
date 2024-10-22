#pragma once
#include <util/generic/string.h>
#include <util/system/types.h>

namespace NGrpcActorClient {

struct TGrpcClientSettings {
    TString Endpoint;
    TString CertificateRootCA; // root CA certificate PEM/x509
    ui32 GrpcKeepAliveTimeMs = 10000;
    ui32 GrpcKeepAliveTimeoutMs = 1000;
    ui32 GrpcKeepAlivePingInterval = 5000;
    bool EnableSsl = false;
    ui64 RequestTimeoutMs = 0; // zero means 10 mins
};

} // namespace NGrpcActorClient
