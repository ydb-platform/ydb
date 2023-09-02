#include "helpers.h"

#include <yt/yt/core/crypto/crypto.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/string_utils/url/url.h>

#include <util/string/split.h>

namespace NYT::NAuth {

using namespace NCrypto;
using namespace NYson;
using namespace NYTree;
using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

TString GetCryptoHash(TStringBuf secret)
{
    return NCrypto::TSha1Hasher()
        .Append(secret)
        .GetHexDigestLowerCase();
}

TString FormatUserIP(const NNet::TNetworkAddress& address)
{
    if (!address.IsIP()) {
        // Sometimes userIP is missing (e.g. user is connecting
        // from job using unix socket), but it is required by
        // blackbox. Put placeholder in place of a real IP.
        static const TString LocalUserIP = "127.0.0.1";
        return LocalUserIP;
    }
    return ToString(
        address,
        NNet::TNetworkAddressFormatOptions{
            .IncludePort = false,
            .IncludeTcpProtocol = false
        });
}

TString GetLoginForTvmId(TTvmId tvmId)
{
    return "tvm:" + ToString(tvmId);
}

////////////////////////////////////////////////////////////////////////////////

static const THashSet<TString> PrivateUrlParams{
    "userip",
    "oauth_token",
    "sessionid",
    "sslsessionid",
    "user_ticket",
};

void TSafeUrlBuilder::AppendString(TStringBuf str)
{
    RealUrl_.AppendString(str);
    SafeUrl_.AppendString(str);
}

void TSafeUrlBuilder::AppendChar(char ch)
{
    RealUrl_.AppendChar(ch);
    SafeUrl_.AppendChar(ch);
}

void TSafeUrlBuilder::AppendParam(TStringBuf key, TStringBuf value)
{
    auto size = key.length() + 4 + CgiEscapeBufLen(value.length());

    char* realBegin = RealUrl_.Preallocate(size);
    char* realIt = realBegin;
    memcpy(realIt, key.data(), key.length());
    realIt += key.length();
    *realIt = '=';
    realIt += 1;
    auto realEnd = CGIEscape(realIt, value.data(), value.length());
    RealUrl_.Advance(realEnd - realBegin);

    char* safeBegin = SafeUrl_.Preallocate(size);
    char* safeEnd = safeBegin;
    if (PrivateUrlParams.contains(key)) {
        memcpy(safeEnd, realBegin, realIt - realBegin);
        safeEnd += realIt - realBegin;
        memcpy(safeEnd, "***", 3);
        safeEnd += 3;
    } else {
        memcpy(safeEnd, realBegin, realEnd - realBegin);
        safeEnd += realEnd - realBegin;
    }
    SafeUrl_.Advance(safeEnd - safeBegin);
}

TString TSafeUrlBuilder::FlushRealUrl()
{
    return RealUrl_.Flush();
}

TString TSafeUrlBuilder::FlushSafeUrl()
{
    return SafeUrl_.Flush();
}

////////////////////////////////////////////////////////////////////////////////

THashedCredentials HashCredentials(const NRpc::NProto::TCredentialsExt& credentialsExt)
{
    THashedCredentials result;
    if (credentialsExt.has_token()) {
        result.TokenHash = GetCryptoHash(credentialsExt.token());
    }
    return result;
}

void Serialize(const THashedCredentials& hashedCredentials, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .OptionalItem("token_hash", hashedCredentials.TokenHash)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TString SignCsrfToken(const TString& userId, const TString& key, TInstant now)
{
    auto msg = userId + ":" + ToString(now.TimeT());
    return CreateSha256Hmac(key, msg) + ":" + ToString(now.TimeT());
}

TError CheckCsrfToken(
    const TString& csrfToken,
    const TString& userId,
    const TString& key,
    TInstant expirationTime)
{
    std::vector<TString> parts;
    StringSplitter(csrfToken).Split(':').AddTo(&parts);
    if (parts.size() != 2) {
        return TError("Malformed CSRF token");
    }

    auto signTime = TInstant::Seconds(FromString<time_t>(parts[1]));
    if (signTime < expirationTime) {
        return TError(NRpc::EErrorCode::InvalidCsrfToken, "CSRF token expired")
            << TErrorAttribute("sign_time", signTime);
    }

    auto msg = userId + ":" + ToString(signTime.TimeT());
    auto expectedToken = CreateSha256Hmac(key, msg);
    if (!ConstantTimeCompare(expectedToken, parts[0])) {
        return TError(NRpc::EErrorCode::InvalidCsrfToken, "Invalid CSFR token signature")
            << TErrorAttribute("provided_signature", parts[0])
            << TErrorAttribute("user_fingerprint", msg);
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

