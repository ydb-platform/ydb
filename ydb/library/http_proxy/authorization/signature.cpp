#include "auth_helpers.h"
#include "signature.h"

#include <library/cpp/http/io/stream.h>
#include <library/cpp/http/misc/parsed_request.h>

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <util/generic/algorithm.h>
#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <library/cpp/cgiparam/cgiparam.h>
#include <util/string/hex.h>
#include <util/string/join.h>
#include <util/string/strip.h>

namespace NKikimr::NSQS {

static TString HmacSHA256(TStringBuf key, TStringBuf data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    ui32 hl = SHA256_DIGEST_LENGTH;
    const auto* res = HMAC(EVP_sha256(), key.data(), key.size(), reinterpret_cast<const unsigned char*>(data.data()), data.size(), hash, &hl);
    Y_ENSURE(res);
    Y_ENSURE(hl == SHA256_DIGEST_LENGTH);
    return TString{reinterpret_cast<const char*>(res), hl};
}

static TString HashSHA256(IInputStream& stream) {
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

static TString HashSHA256(TStringBuf data) {
    TMemoryInput in{data};
    return HashSHA256(in);
}

static const char newline = '\n';
static const char pathDelim = '/';

constexpr TStringBuf AUTHORIZATION_HEADER = "authorization";

static const TString SIGNED_HEADERS_PARAM = "signedheaders";
static const TString SIGNATURE_PARAM = "signature";
static const TString CREDENTIAL_PARAM = "credential";

TAwsRequestSignV4::TAwsRequestSignV4(const TString& request) {
    // special standalone ctor for tests
    TStringInput si(request);
    THttpInput input(&si);
    TParsedHttpFull parsed(input.FirstLine());

    TMaybe<TBuffer> inputData;
    ui64 contentLength = 0;
    if (parsed.Method == "POST") {
        if (input.GetContentLength(contentLength)) {
            inputData.ConstructInPlace();
            inputData->Resize(contentLength);
            if (input.Load(inputData->Data(), (size_t)contentLength) != contentLength) {
                Y_ABORT_UNLESS(false);
            }
        }
    }

    Process(input, parsed, inputData);
}

TAwsRequestSignV4::TAwsRequestSignV4(const THttpInput& input, const TParsedHttpFull& parsed, const TMaybe<TBuffer>& inputData) {
    Process(input, parsed, inputData);
}

const TString& TAwsRequestSignV4::GetCanonicalRequest() const {
    return CanonicalRequestStr_;
}

const TString& TAwsRequestSignV4::GetStringToSign() const {
    return FinalStringToSignStr_;
}

const TString& TAwsRequestSignV4::GetParsedSignature() const {
    return ParsedSignature_;
}

const TString& TAwsRequestSignV4::GetSigningTimestamp() const {
    return AwsTimestamp_;
}

const TString& TAwsRequestSignV4::GetRegion() const {
    return AwsRegion_;
}

const TString& TAwsRequestSignV4::GetAccessKeyId() const {
    return AccessKeyId_;
}

void TAwsRequestSignV4::Process(const THttpInput& input, const TParsedHttpFull& parsed, const TMaybe<TBuffer>& inputData) {
    ParseAuthorization(input);
    MakeCanonicalRequest(input, parsed, inputData);
    MakeFinalStringToSign();
}

TString TAwsRequestSignV4::CalcSignature(const TString& secretKey) const {
    const auto dateKey = HmacSHA256(TString::Join("AWS4", secretKey), AwsDate_);
    const auto dateRegionKey = HmacSHA256(dateKey, AwsRegion_);
    const auto dateRegionServiceKey = HmacSHA256(dateRegionKey, AwsService_);
    const auto signingKey = HmacSHA256(dateRegionServiceKey, AwsRequest_);
    const auto signatureHmac = HmacSHA256(signingKey, FinalStringToSignStr_);

    return to_lower(HexEncode(signatureHmac.data(), signatureHmac.size()));
}

void TAwsRequestSignV4::ParseAuthorization(const THttpInput& input) {
    for (const auto& header : input.Headers()) {
        if (AsciiEqualsIgnoreCase(header.Name(), AUTHORIZATION_HEADER)) {
            auto params = ParseAuthorizationParams(header.Value());

            SignedHeadersList_ = TString(params[SIGNED_HEADERS_PARAM]);
            ParsedSignature_ = TString(params[SIGNATURE_PARAM]);

            TStringBuf credential = params[CREDENTIAL_PARAM];
            AccessKeyId_ = TString(credential.NextTok(pathDelim));
            AwsDate_ = TString(credential.NextTok(pathDelim));
            AwsRegion_ = TString(credential.NextTok(pathDelim));
            AwsService_ = TString(credential.NextTok(pathDelim));
            break;
        }
    }
}

static TString UriEncode(const TStringBuf input, bool encodeSlash = false) {
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

void TAwsRequestSignV4::MakeCanonicalRequest(const THttpInput& input, const TParsedHttpFull& parsed, const TMaybe<TBuffer>& inputData) {
    TStringStream canonicalRequest;
    // METHOD
    canonicalRequest << parsed.Method << newline;
    // PATH
    canonicalRequest << UriEncode(parsed.Path) << newline;
    // CGI
    TCgiParameters cgi(parsed.Cgi);
    TMap<TString, TVector<TString>> sortedCgi;

    for (const auto& [key, value] : cgi) {
        sortedCgi[key].push_back(value);
    }

    for (auto& pair : sortedCgi) {
        ::Sort(pair.second.begin(), pair.second.end());
    }

    if (sortedCgi.size()) {
        TStringStream canonicalCgi;

        auto printSingleParam = [&canonicalCgi](const TString& key, const TVector<TString>& values) {
            auto it = values.begin();
            canonicalCgi << UriEncode(key) << "=" << UriEncode(*it);
            while (++it != values.end()) {
                canonicalCgi << "&" << UriEncode(key) << "=" << UriEncode(*it);
            }

        };

        auto it = sortedCgi.begin();
        printSingleParam(it->first, it->second);
        while (++it != sortedCgi.end()) {
            canonicalCgi << "&";
            printSingleParam(it->first, it->second);
        }

        canonicalRequest << canonicalCgi.Str() << newline;
    } else {
        canonicalRequest << newline;
    }
    // CANONICAL HEADERS
    TMap<TStringBuf, TVector<TString>> canonicalHeaders;

    TStringBuf headersList = SignedHeadersList_;
    while (TStringBuf headerName = headersList.NextTok(';')) {
        canonicalHeaders[headerName] = {};
    }

    for (const auto& header : input.Headers()) {
        const auto lowercaseHeaderName = to_lower(header.Name());
        auto it = canonicalHeaders.find(lowercaseHeaderName);
        if (it != canonicalHeaders.end()) {
            it->second.push_back(Strip(header.Value()));
        }
    }

    for (const auto& [key, value] : canonicalHeaders) {
        canonicalRequest << key << ":"sv << JoinRange(",", value.begin(), value.end()) << newline;
    }

    canonicalRequest << newline; // skip additional line after headers
    // SIGNED HEADERS
    canonicalRequest << SignedHeadersList_ << newline;

    static const TStringBuf hashedContentHeader = "x-amz-content-sha256";

    // PAYLOAD
    auto contentIt = canonicalHeaders.find(hashedContentHeader);
    static const TStringBuf unsignedPayloadLiteral = "UNSIGNED-PAYLOAD";
    if (contentIt != canonicalHeaders.end() && !contentIt->second.empty() && contentIt->second[0] == unsignedPayloadLiteral) {
        canonicalRequest << unsignedPayloadLiteral;
    } else {
        if (inputData) {
            canonicalRequest << HashSHA256(TStringBuf(inputData->Data(), inputData->Size()));
        } else {
            static const TStringBuf emptyStringSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
            canonicalRequest << emptyStringSHA256;
        }
    }

    auto amzDateIt = canonicalHeaders.find("x-amz-date");
    if (amzDateIt != canonicalHeaders.end() && !amzDateIt->second.empty()) {
        AwsTimestamp_ = amzDateIt->second[0];
    }

    CanonicalRequestStr_ = canonicalRequest.Str();
}

void TAwsRequestSignV4::MakeFinalStringToSign() {
    TStringStream finalStringToSign;
    finalStringToSign << "AWS4-HMAC-SHA256"sv << newline;
    finalStringToSign << AwsTimestamp_ << newline;

    finalStringToSign << AwsDate_ << pathDelim;
    finalStringToSign << AwsRegion_ << pathDelim;
    finalStringToSign << AwsService_ << pathDelim;
    finalStringToSign << AwsRequest_ << newline;

    finalStringToSign << HashSHA256(CanonicalRequestStr_);
    FinalStringToSignStr_ = finalStringToSign.Str();
}

} // namespace NKikimr::NSQS
