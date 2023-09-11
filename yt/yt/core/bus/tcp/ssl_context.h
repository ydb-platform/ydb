#pragma once

#include "public.h"

#include <openssl/ssl.h>

#include <memory>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

class TSslContext
{
public:
    TSslContext();
    ~TSslContext();

    SSL_CTX* GetSslCtx();
    static TSslContext* Get();

    void LoadCAFileIfNotLoaded(const TString& filePath);
    void LoadCertificateChain(const TString& filePath);
    void LoadPrivateKey(const TString& filePath);
    void UseCAIfNotUsed(const TString& ca);
    void UseCertificateChain(const TString& certificate);
    void UsePrivateKey(const TString& privateKey);
    void SetCipherListIfUnset(const TString& cipherList);
    void CheckPrivateKeyWithCertificate();

    // For testing purposes.
    void LoadCAFile(const TString& filePath);
    void Reset();
    void SetCipherList(const TString& cipherList);
    void UseCA(const TString& ca);

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
