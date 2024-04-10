#pragma once

#include "public.h"

#include <yt/yt/core/net/public.h>

#include <yt/yt/core/logging/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NCrypto {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSslContextImpl)

class TSslContext
    : public TRefCounted
{
public:
    TSslContext();

    void Reset();
    void Commit(TInstant time = TInstant::Zero());
    TInstant GetCommitTime();

    void UseBuiltinOpenSslX509Store();

    void SetCipherList(const TString& list);

    void AddCertificateFromFile(const TString& path);
    void AddCertificateChainFromFile(const TString& path);
    void AddPrivateKeyFromFile(const TString& path);

    void AddCertificate(const TString& certificate);
    void AddCertificateChain(const TString& certificateChain);
    void AddPrivateKey(const TString& privateKey);

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
    TInstant CommitTime_;
};

DEFINE_REFCOUNTED_TYPE(TSslContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrypto
