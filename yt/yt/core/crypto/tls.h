#pragma once

#include "public.h"

#include <yt/yt/core/net/public.h>

#include <yt/yt/core/logging/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <openssl/ossl_typ.h>

namespace NYT::NCrypto {

////////////////////////////////////////////////////////////////////////////////

TError GetLastSslError(TString message);

////////////////////////////////////////////////////////////////////////////////

struct TSslDeleter
{
    void operator() (BIO*) const noexcept;
    void operator() (X509*) const noexcept;
    void operator() (EVP_PKEY*) const noexcept;
    void operator() (SSL_CTX*) const noexcept;
    void operator() (SSL*) const noexcept;
};

using TBioPtr = std::unique_ptr<BIO, TSslDeleter>;
using TX509Ptr = std::unique_ptr<X509, TSslDeleter>;
using TEvpPKeyPtr = std::unique_ptr<EVP_PKEY, TSslDeleter>;
using TSslCtxPtr = std::unique_ptr<SSL_CTX, TSslDeleter>;
using TSslPtr = std::unique_ptr<SSL, TSslDeleter>;

////////////////////////////////////////////////////////////////////////////////

TString GetFingerprintSHA256(const TX509Ptr& certificate);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSslContextImpl)

class TSslContext
    : public TRefCounted
{
public:
    TSslContext();

    void Reset();
    void Commit(TInstant time = TInstant::Zero());
    TInstant GetCommitTime() const;

    void ApplyConfig(const TSslContextConfigPtr& config, TCertificatePathResolver pathResolver = nullptr);

    void UseBuiltinOpenSslX509Store();

    void SetCipherList(const TString& list);

    void AddCertificateAuthorityFromFile(const TString& path);
    void AddCertificateFromFile(const TString& path);
    void AddCertificateChainFromFile(const TString& path);
    void AddPrivateKeyFromFile(const TString& path);

    void AddCertificateAuthority(const TString& ca);
    void AddCertificate(const TString& certificate);
    void AddCertificateChain(const TString& certificateChain);
    void AddPrivateKey(const TString& privateKey);

    void AddCertificateAuthority(const TPemBlobConfigPtr& pem, TCertificatePathResolver resolver = nullptr);
    void AddCertificate(const TPemBlobConfigPtr& pem, TCertificatePathResolver resolver = nullptr);
    void AddCertificateChain(const TPemBlobConfigPtr& pem, TCertificatePathResolver resolver = nullptr);
    void AddPrivateKey(const TPemBlobConfigPtr& pem, TCertificatePathResolver resolver = nullptr);

    TSslPtr NewSsl();

    NNet::IDialerPtr CreateDialer(
        const NNet::TDialerConfigPtr& config,
        const NConcurrency::IPollerPtr& poller,
        const NLogging::TLogger& logger);

    NNet::IListenerPtr CreateListener(
        const NNet::TNetworkAddress& at,
        const NConcurrency::IPollerPtr& poller,
        const NConcurrency::IPollerPtr& acceptor);

    NNet::IListenerPtr CreateListener(
        const NNet::IListenerPtr& underlying,
        const NConcurrency::IPollerPtr& poller);

private:
    const TIntrusivePtr<TSslContextImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TSslContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrypto
