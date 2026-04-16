#pragma once

#include <ydb/library/actors/http/http.h>

#include <util/generic/guid.h>

namespace NMVP {

constexpr TStringBuf REQUEST_ID_HEADER = "X-Request-Id";
constexpr size_t MAX_REQUEST_ID_LENGTH = 256;

struct TMvpLogContext {
    TString RequestId;
};

TString GetLogPrefix(const TMvpLogContext* context);
TMvpLogContext CreateMvpLogContext(const NHttp::THttpIncomingRequestPtr& request);

class TMvpLogContextProvider {
protected:
    TMvpLogContext LogContext;

public:
    TMvpLogContextProvider() = default;
    explicit TMvpLogContextProvider(const TMvpLogContext& logContext)
        : LogContext(logContext)
    {}

    virtual ~TMvpLogContextProvider() = default;
    virtual const TMvpLogContext* GetLogContext() const {
        return &LogContext;
    }

    TString GetLogPrefix() const {
        return NMVP::GetLogPrefix(GetLogContext());
    }

    void SetLogContext(const TMvpLogContext& logContext) {
        LogContext = logContext;
    }
};

inline bool IsValidRequestId(TStringBuf requestId) {
    return !requestId.empty()
        && requestId.size() <= MAX_REQUEST_ID_LENGTH
        && NHttp::IsValidHeaderData(requestId);
}

inline TString GetRequestId(const NHttp::THttpIncomingRequestPtr& request) {
    if (!request) {
        return {};
    }
    NHttp::THeaders headers(request->Headers);
    if (TStringBuf requestId = headers.Get(REQUEST_ID_HEADER); IsValidRequestId(requestId)) {
        return TString(requestId);
    }
    return {};
}

inline TMvpLogContext CreateMvpLogContext(const NHttp::THttpIncomingRequestPtr& request) {
    TString requestId = GetRequestId(request);
    if (requestId.empty()) {
        requestId = CreateGuidAsString();
    }
    return {.RequestId = std::move(requestId)};
}

inline TString GetLogPrefix(const TMvpLogContext* context) {
    if (!context || context->RequestId.empty()) {
        return {};
    }
    return TStringBuilder() << "X-Request-Id: " << context->RequestId << ", ";
}

inline void SetRequestIdHeader(NHttp::THeadersBuilder* headers, const TMvpLogContext* context) {
    if (!headers || !context || !IsValidRequestId(context->RequestId)) {
        return;
    }
    headers->Set(REQUEST_ID_HEADER, context->RequestId);
}

} // namespace NMVP
