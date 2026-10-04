#pragma once

#include "public.h"

#include "grpcpp/security/credentials.h"
#include "grpcpp/security/server_credentials.h"
#include "tls_certificate_provider.h"

#include <library/cpp/logger/log.h>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/system/yassert.h>

#include <memory>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

inline TString ReadFile(const TString& fileName)
{
    TFileInput in(fileName);
    return in.ReadAll();
}

////////////////////////////////////////////////////////////////////////////////

template <typename TConfig>
std::shared_ptr<grpc::ChannelCredentials> CreateTcpClientChannelCredentials(
    bool secureEndpoint,
    const TConfig& config,
    ICertificateProviderPtr certificateProvider)
{
    std::shared_ptr<grpc::ChannelCredentials> credentials;
    if (!secureEndpoint) {
        credentials = grpc::InsecureChannelCredentials();
    } else if (config.GetSkipCertVerification()) {
        grpc::experimental::TlsChannelCredentialsOptions tlsOptions;
        tlsOptions.set_verify_server_certs(false);
        credentials = grpc::experimental::TlsCredentials(tlsOptions);
    } else {
        credentials = certificateProvider->CreateSecureClientCredentials();
    }
    return credentials;
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<grpc::ServerCredentials> CreateInsecureServerCredentials();

}   // namespace NYdb::NBS
