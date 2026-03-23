#pragma once

#include <ydb/library/actors/http/http.h>

#include <util/generic/guid.h>
#include <util/string/ascii.h>

namespace NMVP {

constexpr TStringBuf REQUEST_ID_HEADER = "x-request-id";

struct TMvpLogContext {
    TString RequestId;
};

class IMvpLogContextProvider {
public:
    virtual ~IMvpLogContextProvider() = default;
    virtual const TMvpLogContext* GetLogContext() const = 0;
};

inline TString GetRequestId(const NHttp::THttpIncomingRequestPtr& request) {
    if (!request) {
        return {};
    }
    NHttp::THeaders headers(request->Headers);
    for (const auto& header : headers.Headers) {
        if (AsciiEqualsIgnoreCase(header.first, REQUEST_ID_HEADER)) {
            return TString(header.second);
        }
    }
    return {};
}

inline TString EnsureRequestIdHeader(NHttp::THttpIncomingRequestPtr& request) {
    TString requestId = GetRequestId(request);
    if (!requestId.empty()) {
        return requestId;
    }

    requestId = CreateGuidAsString();
    if (request) {
        NHttp::THeadersBuilder extraHeaders;
        extraHeaders.Set(REQUEST_ID_HEADER, requestId);
        request = request->Duplicate(extraHeaders);
    }
    return requestId;
}

inline TMvpLogContext CreateLogContext(const NHttp::THttpIncomingRequestPtr& request) {
    return {.RequestId = GetRequestId(request)};
}

inline void SetRequestIdHeader(const NHttp::THttpIncomingRequestPtr& request, NHttp::THeadersBuilder* headers) {
    if (!headers) {
        return;
    }
    TString requestId = GetRequestId(request);
    if (!requestId.empty()) {
        headers->Set(REQUEST_ID_HEADER, requestId);
    }
}

inline TString GetLogPrefix(const TMvpLogContext* context) {
    if (!context || context->RequestId.empty()) {
        return {};
    }
    return TStringBuilder() << "request id: " << context->RequestId << ", ";
}

inline TString GetLogPrefix(const NHttp::THttpIncomingRequestPtr& request) {
    TMvpLogContext context = CreateLogContext(request);
    return GetLogPrefix(&context);
}

} // namespace NMVP
