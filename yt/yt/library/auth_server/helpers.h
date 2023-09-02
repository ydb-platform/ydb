#pragma once

#include "public.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/core/net/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TErrorOr<T> GetByYPath(const NYTree::INodePtr& node, const NYPath::TYPath& path)
{
    try {
        auto child = NYTree::FindNodeByYPath(node, path);
        if (!child) {
            return TError("Missing %v", path);
        }
        return NYTree::ConvertTo<T>(std::move(child));
    } catch (const std::exception& ex) {
        return TError("Unable to extract %v", path) << ex;
    }
}

TString GetCryptoHash(TStringBuf secret);
TString FormatUserIP(const NNet::TNetworkAddress& address);

////////////////////////////////////////////////////////////////////////////////

class TSafeUrlBuilder
{
public:
    void AppendString(TStringBuf str);
    void AppendChar(char ch);
    void AppendParam(TStringBuf key, TStringBuf value);

    TString FlushRealUrl();
    TString FlushSafeUrl();

private:
    TStringBuilder RealUrl_;
    TStringBuilder SafeUrl_;
};

////////////////////////////////////////////////////////////////////////////////

struct THashedCredentials
{
    std::optional<TString> TokenHash;
    // TODO(max42): add remaining fields from TCredentialsExt when needed.
};

THashedCredentials HashCredentials(const NRpc::NProto::TCredentialsExt& credentialsExt);

void Serialize(const THashedCredentials& hashedCredentials, NYson::IYsonConsumer* consumer);

TString GetLoginForTvmId(TTvmId tvmId);

////////////////////////////////////////////////////////////////////////////////

TString SignCsrfToken(
    const TString& userId,
    const TString& key,
    TInstant now);
TError CheckCsrfToken(
    const TString& csrfToken,
    const TString& userId,
    const TString& key,
    TInstant expirationTime);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
