#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/misc/hash.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct ISecretVaultService
    : public virtual TRefCounted
{
    struct TSecretSubrequest
    {
        TString SecretId;
        TString SecretVersion;
        TString DelegationToken;
        TString Signature;

        bool operator == (const TSecretSubrequest& other) const
        {
            return
                std::tie(SecretId, SecretVersion, DelegationToken, Signature) ==
                std::tie(other.SecretId, other.SecretVersion, other.DelegationToken, other.Signature);
        }

        operator size_t() const
        {
            size_t hash = 0;
            HashCombine(hash, SecretId);
            HashCombine(hash, SecretVersion);
            HashCombine(hash, DelegationToken);
            HashCombine(hash, Signature);
            return hash;
        }
    };

    struct TSecretValue
    {
        TString Key;
        TString Value;
        TString Encoding;
    };

    struct TSecretSubresponse
    {
        std::vector<TSecretValue> Values;
    };

    using TErrorOrSecretSubresponse = TErrorOr<TSecretSubresponse>;

    virtual TFuture<std::vector<TErrorOrSecretSubresponse>> GetSecrets(
        const std::vector<TSecretSubrequest>& subrequests) = 0;

    struct TDelegationTokenRequest
    {
        TString UserTicket;
        TString SecretId;
        TString Signature;
        TString Comment;
    };

    virtual TFuture<TString> GetDelegationToken(TDelegationTokenRequest request) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISecretVaultService)

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilder* builder,
    const ISecretVaultService::TSecretSubrequest& subrequest,
    TStringBuf spec);
TString ToString(const ISecretVaultService::TSecretSubrequest& subrequest);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
