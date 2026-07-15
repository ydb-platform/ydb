#include "grpc_common.h"

#include <library/cpp/openssl/holders/holder.h>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <memory>
#include <string>

namespace NYdbGrpc {
inline namespace Dev {

namespace {

std::string DrainOpenSslErrors() {
    std::string out;
    unsigned long e = 0;
    while ((e = ERR_get_error()) != 0) {
        char buf[256];
        ERR_error_string_n(e, buf, sizeof(buf));
        if (!out.empty()) {
            out += "; ";
        }
        out += buf;
    }
    if (out.empty()) {
        return "OpenSSL error queue empty";
    }
    return out;
}

X509* ReadX509FromBio(BIO* bio) {
    return PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
}

EVP_PKEY* ReadPrivateKeyFromBio(BIO* bio) {
    return PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr);
}

bool ValidateRootCertificates(const std::string& pemRootCerts, std::string& errorMessage) {
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
            if (errorCode == 0 || ERR_GET_REASON(errorCode) == PEM_R_NO_START_LINE) {
                ERR_clear_error();
                break;
            }
            errorMessage = "root CA PEM: " + DrainOpenSslErrors();
            return false;
        }

        std::unique_ptr<BASIC_CONSTRAINTS, decltype(&BASIC_CONSTRAINTS_free)> basicConstraints(
            static_cast<BASIC_CONSTRAINTS*>(X509_get_ext_d2i(cert.get(), NID_basic_constraints, nullptr, nullptr)),
            &BASIC_CONSTRAINTS_free);
        const auto isCaCert = basicConstraints && basicConstraints->ca;

        if (!isCaCert) {
            ERR_clear_error();
            errorMessage = "root CA PEM: certificate is not a CA (BasicConstraints)";
            return false;
        }
        ERR_clear_error();
        ++certsParsed;
    }

    if (certsParsed == 0) {
        errorMessage = "root CA PEM: no certificates parsed";
        return false;
    }
    return true;
}

} // namespace

bool ValidateTlsCredentials(const grpc::SslCredentialsOptions& sslCredentials, std::string& errorMessage) {
    errorMessage.clear();

    if (!ValidateRootCertificates(sslCredentials.pem_root_certs, errorMessage)) {
        return false;
    }

    const bool hasClientCert = !sslCredentials.pem_cert_chain.empty();
    const bool hasPrivateKey = !sslCredentials.pem_private_key.empty();
    if (!hasClientCert && !hasPrivateKey) {
        return true;
    }
    if (!hasClientCert || !hasPrivateKey) {
        errorMessage = "client TLS: certificate and private key must both be set";
        return false;
    }

    using TSslCtxHolder = NOpenSSL::THolder<SSL_CTX, SSL_CTX_new, SSL_CTX_free, const SSL_METHOD*>;
    using TBioHolder = NOpenSSL::THolder<BIO, BIO_new_mem_buf, BIO_free, const void*, int>;
    using TX509Holder = NOpenSSL::THolder<X509, ReadX509FromBio, X509_free, BIO*>;
    using TPkeyHolder = NOpenSSL::THolder<EVP_PKEY, ReadPrivateKeyFromBio, EVP_PKEY_free, BIO*>;
    try {
        ERR_clear_error();
        TSslCtxHolder sslCtx(TLS_method());
        TBioHolder certBio(sslCredentials.pem_cert_chain.data(), static_cast<int>(sslCredentials.pem_cert_chain.size()));
        TX509Holder cert(certBio);
        if (SSL_CTX_use_certificate(sslCtx, cert) != 1) {
            errorMessage = "client certificate: " + DrainOpenSslErrors();
            return false;
        }
        ERR_clear_error();
        TBioHolder keyBio(sslCredentials.pem_private_key.data(), static_cast<int>(sslCredentials.pem_private_key.size()));
        TPkeyHolder privateKey(keyBio);
        if (SSL_CTX_use_PrivateKey(sslCtx, privateKey) != 1) {
            errorMessage = "client private key: " + DrainOpenSslErrors();
            return false;
        }
        ERR_clear_error();
        if (SSL_CTX_check_private_key(sslCtx) != 1) {
            errorMessage = "client cert/key: " + DrainOpenSslErrors();
            return false;
        }
    } catch (const std::exception& e) {
        ERR_clear_error();
        errorMessage = std::string("client TLS: ") + e.what();
        return false;
    }
    ERR_clear_error();
    return true;
}

}
}
