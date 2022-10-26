#pragma once

namespace NCloud {

struct TGrpcClientSettings {
    TString Endpoint;
    TString CertificateRootCA; // root CA certificate PEM/x509
    ui32 GrpcKeepAliveTimeMs = 10000;
    ui32 GrpcKeepAliveTimeoutMs = 1000;
    ui32 GrpcKeepAlivePingInterval = 5000;
    bool EnableSsl = false;
};

}
