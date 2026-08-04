#include <ydb/library/actors/http/http.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <array>

namespace {

TString SanitizeComponent(TStringBuf input, TStringBuf fallback) {
    static constexpr TStringBuf alphabet = "abcdefghijklmnopqrstuvwxyz0123456789-_.~";

    TString result;
    result.reserve(input.size());
    for (unsigned char ch : input) {
        result.push_back(alphabet[ch % alphabet.size()]);
    }

    if (result.empty()) {
        result = fallback;
    }
    return result;
}

TString MakeUrl(TStringBuf pathPart, TStringBuf queryPart, bool secure) {
    TString url = secure ? "https://example.com/" : "http://example.com/";
    url += SanitizeComponent(pathPart, "path");

    const TString query = SanitizeComponent(queryPart, "k=v");
    if (!query.empty()) {
        url += '?';
        url += query;
    }

    return url;
}

void FuzzStructuredRoundTrip(FuzzedDataProvider& fdp) {
    static constexpr std::array<TStringBuf, 7> methods = {
        "GET",
        "POST",
        "PUT",
        "PATCH",
        "DELETE",
        "HEAD",
        "OPTIONS",
    };
    static constexpr std::array<TStringBuf, 4> contentTypes = {
        "",
        "text/plain",
        "application/json",
        "application/octet-stream",
    };
    static constexpr std::array<TStringBuf, 3> acceptEncodings = {
        "",
        "gzip, deflate",
        "deflate",
    };

    const TString methodBytes = fdp.ConsumeRandomLengthString(32);
    const TString pathBytes = fdp.ConsumeRandomLengthString(128);
    const TString queryBytes = fdp.ConsumeRandomLengthString(128);
    const TString headerValue = fdp.ConsumeRandomLengthString(128);
    const TString body = fdp.ConsumeRandomLengthString(1024);
    const TString rawRequest = fdp.ConsumeRemainingBytesAsString();

    const TStringBuf method = methods[methodBytes.empty() ? 0 : static_cast<unsigned char>(methodBytes[0]) % methods.size()];
    const TStringBuf contentType = contentTypes[pathBytes.empty() ? 0 : static_cast<unsigned char>(pathBytes[0]) % contentTypes.size()];
    const TStringBuf acceptEncoding = acceptEncodings[queryBytes.empty() ? 0 : static_cast<unsigned char>(queryBytes[0]) % acceptEncodings.size()];
    const TString url = MakeUrl(pathBytes, queryBytes, fdp.ConsumeBool());

    NHttp::THttpOutgoingRequestPtr request = new NHttp::THttpOutgoingRequest(method, url, "HTTP", "1.1");
    request->Set<&NHttp::THttpRequest::Accept>("*/*");
    if (acceptEncoding) {
        request->Set<&NHttp::THttpRequest::AcceptEncoding>(acceptEncoding);
    }
    request->Set("X-Fuzz", headerValue);
    if (fdp.ConsumeBool()) {
        request->Set("Connection", fdp.ConsumeBool() ? "close" : "keep-alive");
    }
    if (!contentType.empty()) {
        request->Set<&NHttp::THttpRequest::ContentType>(contentType);
    }
    if (fdp.ConsumeBool()) {
        request->Set<&NHttp::THttpRequest::Body>(body);
    } else {
        request->Finish();
    }

    (void)request->GetDestination();
    (void)request->IsConnectionClose();
    (void)request->AsReadableString();
    (void)request->GetObfuscatedData();

    NHttp::THeadersBuilder requestHeaders;
    requestHeaders.Set("X-Request-Dup", "1");
    auto requestDuplicate = request->Duplicate(requestHeaders);
    (void)requestDuplicate->GetDestination();

    auto incomingRequest = request->Reverse();
    (void)incomingRequest->GetConnection();
    (void)incomingRequest->GetURL();
    (void)incomingRequest->GetParameters();

    NHttp::THeadersBuilder incomingHeaders;
    incomingHeaders.Set("X-Incoming-Dup", "1");
    auto incomingDuplicate = incomingRequest->Duplicate(incomingHeaders);
    (void)incomingDuplicate->GetConnection();

    auto forwarded = incomingRequest->Forward("https://mirror.example/base");
    (void)forwarded->GetDestination();

    NHttp::THeadersBuilder responseHeaders;
    responseHeaders.Set("Content-Type", contentType.empty() ? TStringBuf("text/plain") : contentType);
    responseHeaders.Set("X-Response", "1");
    if (fdp.ConsumeBool()) {
        responseHeaders.Set("Transfer-Encoding", "chunked");
    }

    auto response = incomingRequest->CreateIncompleteResponse(
        fdp.ConsumeBool() ? TStringBuf("200") : TStringBuf("503"),
        fdp.ConsumeBool() ? TStringBuf("OK") : TStringBuf("Service Unavailable"),
        responseHeaders);
    if (fdp.ConsumeBool()) {
        (void)response->EnableCompression();
    }
    if (fdp.ConsumeBool()) {
        response->SetBody(body);
    } else {
        response->Finish();
    }

    (void)response->IsConnectionClose();
    (void)response->IsNeedBody();
    (void)response->AsReadableString();
    (void)response->GetObfuscatedData();
    auto chunk = response->CreateDataChunk(body);
    (void)chunk->IsEndOfData();
    auto finalChunk = response->CreateDataChunk();
    (void)finalChunk->IsEndOfData();

    NHttp::THeadersBuilder responseExtra;
    responseExtra.Set("X-Response-Dup", "1");
    auto duplicatedResponse = response->Duplicate(incomingRequest, responseExtra);
    (void)duplicatedResponse->IsConnectionClose();

    auto reversedResponse = response->Reverse(request);
    (void)reversedResponse->IsConnectionClose();
    (void)reversedResponse->GetRequest();

    auto parsedRaw = NHttp::THttpOutgoingRequest::CreateRequestString(rawRequest);
    (void)parsedRaw->AsReadableString();
    (void)parsedRaw->GetDestination();
    auto parsedRawDuplicate = parsedRaw->Duplicate(requestHeaders);
    (void)parsedRawDuplicate->GetDestination();
    auto parsedRawIncoming = parsedRaw->Reverse();
    (void)parsedRawIncoming->GetConnection();
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    try {
        FuzzedDataProvider fdp(data, size);
        FuzzStructuredRoundTrip(fdp);
    } catch (...) {
    }

    return 0;
}
