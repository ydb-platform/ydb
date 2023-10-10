#include "ssl_helpers.h"

#include <yt/yt/core/misc/error.h>

#include <openssl/ssl.h>
#include <openssl/err.h>

#include <util/generic/string.h>

#include <memory>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

void TDeleter::operator()(SSL_CTX* ctx) const
{
    SSL_CTX_free(ctx);
}

void TDeleter::operator()(SSL* ctx) const
{
    SSL_free(ctx);
}

void TDeleter::operator()(BIO* bio) const
{
    BIO_free(bio);
}

void TDeleter::operator()(RSA* rsa) const
{
    RSA_free(rsa);
}

void TDeleter::operator()(X509* x509) const
{
    X509_free(x509);
}

////////////////////////////////////////////////////////////////////////////////

TString GetLastSslErrorString()
{
    char errorStr[256];
    ERR_error_string_n(ERR_get_error(), errorStr, sizeof(errorStr));
    return errorStr;
}

////////////////////////////////////////////////////////////////////////////////

bool UseCertificateChain(const TString& certificate, SSL* ssl)
{
    std::unique_ptr<BIO, TDeleter> bio(BIO_new_mem_buf(certificate.data(), certificate.size()));
    if (!bio) {
        return false;
    }

    // The first certificate in the chain is expected to be a leaf.
    std::unique_ptr<X509, TDeleter> cert(PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr));
    if (!cert) {
        return false;
    }

    if (SSL_use_certificate(ssl, cert.get()) != 1) {
        return false;
    }

    // Load additional certificates in the chain.
    while (auto cert = PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr)) {
        if (SSL_add0_chain_cert(ssl, cert) != 1) {
            return false;
        }
        // Do not X509_free() if certificate was added by SSL_CTX_add0_chain_cert().
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

bool UsePrivateKey(const TString& privateKey, SSL* ssl)
{
    std::unique_ptr<BIO, TDeleter> bio(BIO_new_mem_buf(privateKey.data(), privateKey.size()));
    if (!bio) {
        return false;
    }

    std::unique_ptr<RSA, TDeleter> pkey(PEM_read_bio_RSAPrivateKey(bio.get(), nullptr, nullptr, nullptr));
    if (!pkey) {
        return false;
    }

    if (SSL_use_RSAPrivateKey(ssl, pkey.get()) != 1) {
        return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
