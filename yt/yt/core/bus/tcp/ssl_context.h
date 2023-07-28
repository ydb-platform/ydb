#pragma once

#include "public.h"

#include <openssl/ssl.h>

#include <memory>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

TString GetLastSslErrorString();

////////////////////////////////////////////////////////////////////////////////

class TSslContext
{
public:
    TSslContext();
    ~TSslContext();

    SSL_CTX* GetSslCtx();
    static TSslContext* Get();

    void LoadCAFile(const TString& filePath);
    void LoadCAFileIfNotLoaded(const TString& filePath);
    void LoadCertificateChain(const TString& filePath);
    void LoadPrivateKey(const TString& filePath);
    void UseCA(const TString& ca);
    void UseCertificateChain(const TString& certificate);
    void UsePrivateKey(const TString& privateKey);
    void SetCipherList(const TString& cipherList);
    void CheckPrivateKeyWithCertificate();
    // For testing purposes.
    void Reset();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
