#include "yql_aws_signature.h"

#include <ydb/library/actors/http/http.h>

#include <openssl/hmac.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/stream/mem.h>
#include <util/string/builder.h>
#include <util/string/hex.h>
#include <util/string/split.h>

#include <contrib/libs/openssl/include/openssl/evp.h>
#include <contrib/libs/openssl/include/openssl/sha.h>

namespace NYql {

TAwsSignature::TAwsSignature(const TString& method, const TString& url, const TString& contentType, const TString& payload, const TString& awsSigV4, const TString& userPwd, const TInstant& currentTime)
    : Method(method)
    , Url(url)
    , ContentType(contentType)
    , Payload(payload)
    , CurrentTime(currentTime)
{
    const TVector<TString> secrets = StringSplitter(userPwd).Split(':');
    if (secrets.size() == 2) {
        AccessKey = secrets[0];
        AccessSecret = secrets[1];
    }

    const TVector<TString> providers = StringSplitter(awsSigV4).Split(':');
    if (providers.size() == 4) {
        Region = providers[2];
        Service = providers[3];
    }

    TStringBuf scheme;
    TStringBuf host;
    TStringBuf uri;
    NHttp::CrackURL(Url, scheme, host, uri);

    Host = host;
    Uri = uri;

    TStringBuf path;
    TStringBuf cgi;
    TStringBuf{Uri}.Split('?', path, cgi);

    Uri = path;
    Cgi = cgi;

    PrepareCgiParameters();
}

TString TAwsSignature::GetAuthorization() const {
    return TStringBuilder{} 
        << "AWS4-HMAC-SHA256 Credential=" << AccessKey << "/" << GetDate() << "/" << Region << "/" << Service << "/aws4_request, "
        << "SignedHeaders=" << GetListSignedHeaders() 
        << ", Signature=" << CalcSignature();
}

TString TAwsSignature::GetXAmzContentSha256() const {
    return HashSHA256(TStringBuilder{} << "\"" << Payload << "\"");
}

TString TAwsSignature::GetAmzDate() const {
    return CurrentTime.FormatLocalTime("%Y%m%dT%H%M%SZ");
}

TString TAwsSignature::GetContentType() const {
    return ContentType;
}

TString TAwsSignature::GetListSignedHeaders() const {
    return ContentType ? "content-type;host;x-amz-content-sha256;x-amz-date" : "host;x-amz-content-sha256;x-amz-date";
}

TString TAwsSignature::SignHeaders() const {
    return TStringBuilder{} << (ContentType ? (TStringBuilder{} << "content-type:" << GetContentType() << Endl) : TString{})
            << "host:" << Host << Endl
            << "x-amz-content-sha256:" << GetXAmzContentSha256() << Endl
            << "x-amz-date:" << GetAmzDate();
}

TString TAwsSignature::CreateHttpCanonicalRequest() const {
    return TStringBuilder{} << Method << Endl
            << UriEncode(Uri) << Endl
            << Cgi << Endl
            << SignHeaders() << Endl << Endl
            << GetListSignedHeaders() << Endl
            << GetXAmzContentSha256();
}

TString TAwsSignature::CreateStringToSign() const {
    auto canonical = CreateHttpCanonicalRequest();
    return TStringBuilder{} << "AWS4-HMAC-SHA256" << Endl
            << GetAmzDate() << Endl
            << GetDate() << "/" << Region << "/" << Service << "/" << "aws4_request" << Endl
            << HashSHA256(canonical);
}

TString TAwsSignature::CalcSignature() const {
    const auto dateKey = HmacSHA256(TString::Join("AWS4", AccessSecret), GetDate());
    const auto dateRegionKey = HmacSHA256(dateKey, Region);
    const auto dateRegionServiceKey = HmacSHA256(dateRegionKey, Service);
    const auto signingKey = HmacSHA256(dateRegionServiceKey, "aws4_request");
    const auto signatureHmac = HmacSHA256(signingKey, CreateStringToSign());
    return to_lower(HexEncode(signatureHmac.data(), signatureHmac.size()));
}

TString TAwsSignature::GetDate() const {
    return CurrentTime.FormatLocalTime("%Y%m%d");
}

TString TAwsSignature::HmacSHA256(TStringBuf key, TStringBuf data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    ui32 hl = SHA256_DIGEST_LENGTH;
    const auto* res = HMAC(EVP_sha256(), key.data(), key.size(), reinterpret_cast<const unsigned char*>(data.data()), data.size(), hash, &hl);
    Y_ENSURE(res);
    Y_ENSURE(hl == SHA256_DIGEST_LENGTH);
    return TString{reinterpret_cast<const char*>(res), hl};
}

TString TAwsSignature::HashSHA256(TStringBuf data) {
    TMemoryInput stream{data};
    SHA256_CTX hasher;
    SHA256_Init(&hasher);
    char buf[4096];
    size_t read = 0;
    while ((read = stream.Read(buf, sizeof(buf))) != 0) {
        SHA256_Update(&hasher, buf, read);
    }
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_Final(hash, &hasher);
    return to_lower(HexEncode(hash, SHA256_DIGEST_LENGTH));
}

TString TAwsSignature::UriEncode(const TStringBuf input, bool encodeSlash) {
    TStringStream result;
    for (const char ch : input) {
        if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' ||
            ch == '-' || ch == '~' || ch == '.') {
            result << ch;
        } else if (ch == '/') {
            if (encodeSlash) {
                result << "%2F";
            } else {
                result << ch;
            }
        } else {
            result << "%" << HexEncode(&ch, 1);
        }
    }
    return result.Str();
}

void TAwsSignature::PrepareCgiParameters() {
    TCgiParameters cgi(Cgi);
    TMap<TString, TVector<TString>> sortedCgi;

    for (const auto& [key, value] : cgi) {
        sortedCgi[key].push_back(value);
    }

    for (auto& pair : sortedCgi) {
        ::Sort(pair.second.begin(), pair.second.end());
    }

    if (!sortedCgi.empty()) {
        TStringStream canonicalCgi;

        auto printSingleParam = [&canonicalCgi](const TString& key, const TVector<TString>& values) {
            auto it = values.begin();
            canonicalCgi << UriEncode(key, true) << "=" << UriEncode(*it, true);
            while (++it != values.end()) {
                canonicalCgi << "&" << UriEncode(key, true) << "=" << UriEncode(*it, true);
            }

        };

        auto it = sortedCgi.begin();
        printSingleParam(it->first, it->second);
        while (++it != sortedCgi.end()) {
            canonicalCgi << "&";
            printSingleParam(it->first, it->second);
        }

        Cgi = canonicalCgi.Str();
    }
}

}
