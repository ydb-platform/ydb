#include <ydb/library/http_proxy/authorization/signature.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace {

TString ConsumeToken(FuzzedDataProvider& fdp, size_t maxLen, const TStringBuf alphabet) {
    TString token = fdp.ConsumeRandomLengthString(maxLen);
    if (token.empty()) {
        return "x";
    }
    for (char& ch : token) {
        ch = alphabet[static_cast<unsigned char>(ch) % alphabet.size()];
    }
    return token;
}

TString ConsumeHex(FuzzedDataProvider& fdp, size_t len) {
    TString token = ConsumeToken(fdp, len, "0123456789abcdef");
    while (token.size() < len) {
        token.append("0");
    }
    token.resize(len);
    return token;
}

TString ConsumeDate(FuzzedDataProvider& fdp) {
    return ConsumeHex(fdp, 8);
}

TString ConsumeTimestamp(FuzzedDataProvider& fdp, const TString& date) {
    return TStringBuilder() << date << 'T' << ConsumeHex(fdp, 6) << 'Z';
}

TString BuildStructuredRequest(FuzzedDataProvider& fdp) {
    static const TStringBuf methods[] = {"GET", "POST", "PUT", "DELETE"};

    const TStringBuf methodBuf = methods[fdp.ConsumeIntegralInRange<size_t>(0, sizeof(methods) / sizeof(methods[0]) - 1)];
    const TString method(methodBuf);
    const TString date = ConsumeDate(fdp);
    const TString timestamp = ConsumeTimestamp(fdp, date);
    const TString region = ConsumeToken(fdp, 12, "abcdefghijklmnopqrstuvwxyz0123456789-");
    const TString service = ConsumeToken(fdp, 12, "abcdefghijklmnopqrstuvwxyz0123456789-");
    const TString accessKey = ConsumeToken(fdp, 24, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
    const TString signature = ConsumeHex(fdp, 64);
    const TString host = ConsumeToken(fdp, 32, "abcdefghijklmnopqrstuvwxyz0123456789.-");
    const TString secret = ConsumeToken(fdp, 32, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/+=");
    const bool unsignedPayload = fdp.ConsumeBool();
    TString body;
    if (method == "POST") {
        body = TString(fdp.ConsumeRandomLengthString(1024));
    }

    TStringBuilder pathBuilder;
    pathBuilder << '/';
    const size_t segments = fdp.ConsumeIntegralInRange<size_t>(0, 4);
    for (size_t i = 0; i < segments; ++i) {
        if (i != 0) {
            pathBuilder << '/';
        }
        pathBuilder << ConsumeToken(fdp, 12, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~");
    }

    TStringBuilder queryBuilder;
    const size_t queryCount = fdp.ConsumeIntegralInRange<size_t>(0, 4);
    for (size_t i = 0; i < queryCount; ++i) {
        if (i != 0) {
            queryBuilder << '&';
        }
        queryBuilder << ConsumeToken(fdp, 8, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~")
                     << '='
                     << ConsumeToken(fdp, 16, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~");
    }

    const TString query = queryBuilder;

    TStringBuilder request;
    request << method << ' ' << pathBuilder;
    if (!query.empty()) {
        request << '?' << query;
    }
    request << " HTTP/1.1\r\n";
    request << "Host: " << host << "\r\n";
    request << "X-Amz-Date: " << timestamp << "\r\n";
    request << "X-Amz-Content-Sha256: " << (unsignedPayload ? "UNSIGNED-PAYLOAD" : ConsumeHex(fdp, 64)) << "\r\n";
    request << "Authorization: AWS4-HMAC-SHA256 "
            << "Credential=" << accessKey << '/' << date << '/' << region << '/' << service << "/aws4_request, "
            << "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
            << "Signature=" << signature << "\r\n";
    if (method == "POST") {
        request << "Content-Length: " << body.size() << "\r\n";
    }
    request << "\r\n";
    request << body;

    try {
        const TString requestString = request;
        NKikimr::NSQS::TAwsRequestSignV4 sign(requestString);
        (void)sign.GetCanonicalRequest();
        (void)sign.GetStringToSign();
        (void)sign.GetParsedSignature();
        (void)sign.GetAccessKeyId();
        (void)sign.GetRegion();
        (void)sign.GetSigningTimestamp();
        (void)sign.CalcSignature(secret);
    } catch (...) {
    }

    return request;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size > 64 * 1024) return 0;

    TString input(reinterpret_cast<const char*>(data), size);
    try {
        NKikimr::NSQS::TAwsRequestSignV4 sign(input);
        (void)sign.GetCanonicalRequest();
        (void)sign.GetStringToSign();
        (void)sign.GetParsedSignature();
        (void)sign.GetAccessKeyId();
        (void)sign.GetRegion();
        (void)sign.GetSigningTimestamp();
        (void)sign.CalcSignature("secret");
        (void)sign.Empty();
    } catch (...) {}

    FuzzedDataProvider fdp(data, size);
    try {
        (void)BuildStructuredRequest(fdp);
    } catch (...) {
    }

    return 0;
}
