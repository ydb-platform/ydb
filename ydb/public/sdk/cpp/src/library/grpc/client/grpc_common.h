#pragma once

#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/type_switcher.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/grpc_common/constants.h>

#include <library/cpp/openssl/holders/holder.h>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <util/datetime/base.h>
#include <unordered_map>
#include <string>
#include <memory>
#include <new>

namespace NYdbGrpc {
inline namespace Dev {

struct TGRpcClientConfig {
    std::string Locator; // format host:port
    TDuration Timeout = TDuration::Max(); // request timeout
    ui64 MaxMessageSize = NYdb::NGrpc::DEFAULT_GRPC_MESSAGE_SIZE_LIMIT; // Max request and response size
    ui64 MaxInboundMessageSize = 0; // overrides MaxMessageSize for incoming requests
    ui64 MaxOutboundMessageSize = 0; // overrides MaxMessageSize for outgoing requests
    ui32 MaxInFlight = 0;
    bool EnableSsl = false;
    grpc::SslCredentialsOptions SslCredentials;
    grpc_compression_algorithm CompressionAlgorithm = GRPC_COMPRESS_NONE;
    ui64 MemQuota = 0;
    std::unordered_map<std::string, std::string> StringChannelParams;
    std::unordered_map<std::string, int> IntChannelParams;
    std::string LoadBalancingPolicy = { };
    std::string SslTargetNameOverride = { };
    bool UseXds = false;

    TGRpcClientConfig() = default;
    TGRpcClientConfig(const TGRpcClientConfig&) = default;
    TGRpcClientConfig(TGRpcClientConfig&&) = default;
    TGRpcClientConfig& operator=(const TGRpcClientConfig&) = default;
    TGRpcClientConfig& operator=(TGRpcClientConfig&&) = default;

    TGRpcClientConfig(const std::string& locator, TDuration timeout = TDuration::Max(),
            ui64 maxMessageSize = NYdb::NGrpc::DEFAULT_GRPC_MESSAGE_SIZE_LIMIT, ui32 maxInFlight = 0, const std::string& caCert = "", const std::string& clientCert = "",
            const std::string& clientPrivateKey = "", grpc_compression_algorithm compressionAlgorithm = GRPC_COMPRESS_NONE, bool enableSsl = false)
        : Locator(locator)
        , Timeout(timeout)
        , MaxMessageSize(maxMessageSize)
        , MaxInFlight(maxInFlight)
        , EnableSsl(enableSsl)
        , SslCredentials{.pem_root_certs = NYdb::TStringType{caCert},
                         .pem_private_key = NYdb::TStringType{clientPrivateKey},
                         .pem_cert_chain = NYdb::TStringType{clientCert}}
        , CompressionAlgorithm(compressionAlgorithm)
        , UseXds((Locator.starts_with("xds:///")))
    {}
};

inline X509* ReadX509FromBio(BIO* bio) {
    return PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
}

inline EVP_PKEY* ReadPrivateKeyFromBio(BIO* bio) {
    return PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr);
}

inline bool ValidateRootCertificates(const std::string& pemRootCerts) {
    if (pemRootCerts.empty()) {
        return true;
    }

    using TBioHolder = NOpenSSL::THolder<BIO, BIO_new_mem_buf, BIO_free, const void*, int>;
    TBioHolder rootCertsBio(pemRootCerts.data(), static_cast<int>(pemRootCerts.size()));

    ERR_clear_error();
    size_t certsParsed = 0;
    while (true) {
        std::unique_ptr<X509, decltype(&X509_free)> cert(
            PEM_read_bio_X509(rootCertsBio, nullptr, nullptr, nullptr),
            &X509_free);
        if (!cert) {
            const unsigned long errorCode = ERR_peek_last_error();
            if (errorCode == 0) {
                break;
            }
            ERR_clear_error();
            return false;
        }

        std::unique_ptr<BASIC_CONSTRAINTS, decltype(&BASIC_CONSTRAINTS_free)> basicConstraints(
            static_cast<BASIC_CONSTRAINTS*>(X509_get_ext_d2i(cert.get(), NID_basic_constraints, nullptr, nullptr)),
            &BASIC_CONSTRAINTS_free);
        const auto isCaCert = basicConstraints && basicConstraints->ca;

        if (!isCaCert) {
            return false;
        }
        ++certsParsed;
    }

    return certsParsed > 0;
}

inline bool ValidateTlsCredentials(
        const grpc::SslCredentialsOptions& sslCredentials)
{
    if (!ValidateRootCertificates(sslCredentials.pem_root_certs)) {
        return false;
    }

    const bool hasClientCert = !sslCredentials.pem_cert_chain.empty();
    const bool hasPrivateKey = !sslCredentials.pem_private_key.empty();
    if (!hasClientCert && !hasPrivateKey) {
        return true;
    }
    if (!hasClientCert || !hasPrivateKey) {
        return false;
    }

    using TSslCtxHolder = NOpenSSL::THolder<SSL_CTX, SSL_CTX_new, SSL_CTX_free, const SSL_METHOD*>;
    using TBioHolder = NOpenSSL::THolder<BIO, BIO_new_mem_buf, BIO_free, const void*, int>;
    using TX509Holder = NOpenSSL::THolder<X509, ReadX509FromBio, X509_free, BIO*>;
    using TPkeyHolder = NOpenSSL::THolder<EVP_PKEY, ReadPrivateKeyFromBio, EVP_PKEY_free, BIO*>;
    try {
        TSslCtxHolder sslCtx(TLS_method());
        TBioHolder certBio(sslCredentials.pem_cert_chain.data(), static_cast<int>(sslCredentials.pem_cert_chain.size()));
        TX509Holder cert(certBio);
        if (SSL_CTX_use_certificate(sslCtx, cert) != 1) {
            return false;
        }
        TBioHolder keyBio(sslCredentials.pem_private_key.data(), static_cast<int>(sslCredentials.pem_private_key.size()));
        TPkeyHolder privateKey(keyBio);
        if (SSL_CTX_use_PrivateKey(sslCtx, privateKey) != 1) {
            return false;
        }
        if (SSL_CTX_check_private_key(sslCtx) != 1) {
            return false;
        }
    } catch (const std::exception& e) {
        return false;
    }
    return true;
}

inline std::shared_ptr<grpc::ChannelInterface> CreateChannelInterface(const TGRpcClientConfig& config, grpc_socket_mutator* mutator = nullptr){
    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(config.MaxInboundMessageSize ? config.MaxInboundMessageSize : config.MaxMessageSize);
    args.SetMaxSendMessageSize(config.MaxOutboundMessageSize ? config.MaxOutboundMessageSize : config.MaxMessageSize);
    args.SetCompressionAlgorithm(config.CompressionAlgorithm);

    for (const auto& kvp: config.StringChannelParams) {
        args.SetString(NYdb::TStringType{kvp.first}, NYdb::TStringType{kvp.second});
    }

    for (const auto& kvp: config.IntChannelParams) {
        args.SetInt(NYdb::TStringType{kvp.first}, kvp.second);
    }

    if (config.MemQuota) {
        grpc::ResourceQuota quota;
        quota.Resize(config.MemQuota);
        args.SetResourceQuota(quota);
    }
    if (mutator) {
        args.SetSocketMutator(mutator);
    }
    if (!config.LoadBalancingPolicy.empty()) {
        args.SetLoadBalancingPolicyName(NYdb::TStringType{config.LoadBalancingPolicy});
    }
    if (!config.SslTargetNameOverride.empty()) {
        args.SetSslTargetNameOverride(NYdb::TStringType{config.SslTargetNameOverride});
    }
    std::shared_ptr<grpc::ChannelCredentials> channelCredentials = nullptr;
    if (config.EnableSsl || !config.SslCredentials.pem_root_certs.empty()) {
        channelCredentials = grpc::SslCredentials(config.SslCredentials);
    } else {
        channelCredentials = grpc::InsecureChannelCredentials();
    }
    if (config.UseXds) {
        channelCredentials = grpc::XdsCredentials(channelCredentials);
    }
    return grpc::CreateCustomChannel(grpc::string(config.Locator), channelCredentials, args);
}

}
}
