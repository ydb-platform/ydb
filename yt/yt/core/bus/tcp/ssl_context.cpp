#include "ssl_context.h"
#include "ssl_helpers.h"

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/singleton.h>

#include <library/cpp/openssl/init/init.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/system/mutex.h>

#include <openssl/err.h>
#include <openssl/ssl.h>

#include <memory>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

class TSslContext::TImpl
{
public:
    TImpl()
    {
        InitOpenSSL();

        SslCtx_.reset(SSL_CTX_new(TLS_method()));
        if (!SslCtx_) {
            THROW_ERROR_EXCEPTION("Failed to create TLS/SSL context: %v", GetLastSslErrorString());
        }

        if (SSL_CTX_set_min_proto_version(SslCtx_.get(), TLS1_2_VERSION) != 1) {
            THROW_ERROR_EXCEPTION("Failed to set min protocol version: %v", GetLastSslErrorString());
        }

        if (SSL_CTX_set_max_proto_version(SslCtx_.get(), TLS1_2_VERSION) != 1) {
            THROW_ERROR_EXCEPTION("Failed to set max protocol version: %v", GetLastSslErrorString());
        }

        SSL_CTX_set_mode(SslCtx_.get(), SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
    }

    SSL_CTX* GetCtx()
    {
        return SslCtx_.get();
    }

    //! This function is for testing purposes.
    void LoadCAFile(const TString& filePath)
    {
        auto guard = Guard(SpinLock_);

        LoadCAFileUnlocked(filePath);

        CAIsLoaded_ = true;
    }

    void LoadCAFileIfNotLoaded(const TString& filePath)
    {
        if (CAIsLoaded_) {
            return;
        }

        auto guard = Guard(SpinLock_);

        if (CAIsLoaded_) {
            return;
        }

        LoadCAFileUnlocked(filePath);

        CAIsLoaded_ = true;
    }

    void LoadCertificateChain(const TString& filePath)
    {
        auto ret = SSL_CTX_use_certificate_chain_file(SslCtx_.get(), filePath.data());
        if (ret != 1) {
            THROW_ERROR_EXCEPTION("Failed to load certificate chain: %v", GetLastSslErrorString());
        }
    }

    void LoadPrivateKey(const TString& filePath)
    {
        auto ret = SSL_CTX_use_RSAPrivateKey_file(SslCtx_.get(), filePath.data(), SSL_FILETYPE_PEM);
        if (ret != 1) {
            THROW_ERROR_EXCEPTION("Failed to load private key: %v", GetLastSslErrorString());
        }
    }

    //! This function is for testing purposes.
    void UseCA(const TString& ca)
    {
        auto guard = Guard(SpinLock_);

        UseCAUnlocked(ca);

        CAIsLoaded_ = true;
    }

    void UseCAIfNotUsed(const TString& ca)
    {
        if (CAIsLoaded_) {
            return;
        }

        auto guard = Guard(SpinLock_);

        if (CAIsLoaded_) {
            return;
        }

        UseCAUnlocked(ca);

        CAIsLoaded_ = true;
    }

    void UseCertificateChain(const TString& certificate)
    {
        std::unique_ptr<BIO, TDeleter> bio(BIO_new_mem_buf(certificate.data(), certificate.size()));
        if (!bio) {
            THROW_ERROR_EXCEPTION("Failed to allocate memory buffer for certificate: %v", GetLastSslErrorString());
        }

        // The first certificate in the chain is expected to be a leaf.
        std::unique_ptr<X509, TDeleter> cert(PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr));
        if (!cert) {
            THROW_ERROR_EXCEPTION("Failed to read certificate from memory buffer: %v", GetLastSslErrorString());
        }

        if (SSL_CTX_use_certificate(SslCtx_.get(), cert.get()) != 1) {
            THROW_ERROR_EXCEPTION("Failed to use cert in ssl: %v", GetLastSslErrorString());
        }

        // Load additional certificates in the chain.
        while (auto cert = PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr)) {
            if (SSL_CTX_add0_chain_cert(SslCtx_.get(), cert) != 1) {
                THROW_ERROR_EXCEPTION("Failed to add cert to ssl: %v", GetLastSslErrorString());
            }
            // Do not X509_free() if certificate was added by SSL_CTX_add0_chain_cert().
        }
    }

    void UsePrivateKey(const TString& privateKey)
    {
        std::unique_ptr<BIO, TDeleter> bio(BIO_new_mem_buf(privateKey.data(), privateKey.size()));
        if (!bio) {
            THROW_ERROR_EXCEPTION("Failed to allocate memory buffer for private key: %v", GetLastSslErrorString());
        }

        std::unique_ptr<RSA, TDeleter> pkey(PEM_read_bio_RSAPrivateKey(bio.get(), nullptr, nullptr, nullptr));
        if (!pkey) {
            THROW_ERROR_EXCEPTION("Failed to read private key from memory buffer: %v", GetLastSslErrorString());
        }

        if (SSL_CTX_use_RSAPrivateKey(SslCtx_.get(), pkey.get()) != 1) {
            THROW_ERROR_EXCEPTION("Failed to add the private RSA key to ctx: %v", GetLastSslErrorString());
        }
    }

    void SetCipherListIfUnset(const TString& cipherList)
    {
        if (CipherListIsSet_) {
            return;
        }

        auto guard = Guard(SpinLock_);

        if (CipherListIsSet_) {
            return;
        }

        SetCipherListUnlocked(cipherList);

        CipherListIsSet_ = true;
    }

    //! This function is for testing purposes.
    void SetCipherList(const TString& cipherList)
    {
        auto guard = Guard(SpinLock_);

        SetCipherListUnlocked(cipherList);

        CipherListIsSet_ = true;
    }

    //! Check the consistency of a private key with the corresponding certificate.
    //! A private key and the corresponding certificate have to be loaded into ctx.
    void CheckPrivateKeyWithCertificate()
    {
        if (SSL_CTX_check_private_key(SslCtx_.get()) != 1) {
            THROW_ERROR_EXCEPTION("Failed to check the consistency of a private key with the corresponding certificate: %v", GetLastSslErrorString());
        }
    }

private:
    void LoadCAFileUnlocked(const TString& filePath)
    {
        auto ret = SSL_CTX_load_verify_locations(SslCtx_.get(), filePath.data(), nullptr);
        if (ret != 1) {
            THROW_ERROR_EXCEPTION("Failed to load CA file: %v", GetLastSslErrorString());
        }
    }

    void SetCipherListUnlocked(const TString& cipherList)
    {
        if (SSL_CTX_set_cipher_list(SslCtx_.get(), cipherList.data()) != 1) {
            THROW_ERROR_EXCEPTION("Failed to set cipher list: %v", GetLastSslErrorString());
        }
    }

    void UseCAUnlocked(const TString& ca)
    {
        std::unique_ptr<BIO, TDeleter> bio(BIO_new_mem_buf(ca.data(), ca.size()));
        if (!bio) {
            THROW_ERROR_EXCEPTION("Failed to allocate memory buffer for CA certificate: %v", GetLastSslErrorString());
        }

        auto store = SSL_CTX_get_cert_store(SslCtx_.get());

        // Load certificate chain.
        while (auto cert = PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr)) {
            if (X509_STORE_add_cert(store, cert) != 1) {
                THROW_ERROR_EXCEPTION("Failed to add cert to store: %v", GetLastSslErrorString());
            }
            X509_free(cert);
        }
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    std::atomic<bool> CAIsLoaded_ = false;
    std::atomic<bool> CipherListIsSet_ = false;
    std::unique_ptr<SSL_CTX, TDeleter> SslCtx_;
};

////////////////////////////////////////////////////////////////////////////////

TSslContext::TSslContext()
{
    Reset();
}

TSslContext::~TSslContext() = default;

SSL_CTX* TSslContext::GetSslCtx()
{
    return Impl_->GetCtx();
}

TSslContext* TSslContext::Get()
{
    return LeakySingleton<TSslContext>();
}

void TSslContext::LoadCAFile(const TString& filePath)
{
    return Impl_->LoadCAFile(filePath);
}

void TSslContext::LoadCAFileIfNotLoaded(const TString& filePath)
{
    return Impl_->LoadCAFileIfNotLoaded(filePath);
}

void TSslContext::LoadCertificateChain(const TString& filePath)
{
    return Impl_->LoadCertificateChain(filePath);
}

void TSslContext::LoadPrivateKey(const TString& filePath)
{
    return Impl_->LoadPrivateKey(filePath);
}

void TSslContext::UseCA(const TString& ca)
{
    return Impl_->UseCA(ca);
}

void TSslContext::UseCAIfNotUsed(const TString& ca)
{
    return Impl_->UseCAIfNotUsed(ca);
}

void TSslContext::UseCertificateChain(const TString& certificate)
{
    return Impl_->UseCertificateChain(certificate);
}

void TSslContext::UsePrivateKey(const TString& privateKey)
{
    return Impl_->UsePrivateKey(privateKey);
}

void TSslContext::SetCipherList(const TString& cipherList)
{
    return Impl_->SetCipherList(cipherList);
}

void TSslContext::SetCipherListIfUnset(const TString& cipherList)
{
    return Impl_->SetCipherListIfUnset(cipherList);
}

void TSslContext::CheckPrivateKeyWithCertificate()
{
    return Impl_->CheckPrivateKeyWithCertificate();
}

void TSslContext::Reset()
{
    Impl_ = std::make_unique<TImpl>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
