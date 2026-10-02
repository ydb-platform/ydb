#pragma once

#include <ydb/library/actors/http/http.h>

#include <util/generic/guid.h>

namespace NMVP {

constexpr TStringBuf REQUEST_ID_HEADER = "X-Request-Id";
constexpr size_t MAX_REQUEST_ID_LENGTH = 64;

inline bool IsValidRequestId(TStringBuf requestId) {
    return !requestId.empty()
        && requestId.size() <= MAX_REQUEST_ID_LENGTH
        && NHttp::IsValidHeaderData(requestId);
}

inline TString NormalizeRequestId(TStringBuf requestId) {
    if (IsValidRequestId(requestId)) {
        return TString(requestId);
    }
    return CreateGuidAsString();
}

class TMvpLogContext {
private:
    TString RequestId;

public:
    TMvpLogContext()
        : RequestId(NormalizeRequestId({}))
    {}

    explicit TMvpLogContext(TStringBuf requestId)
        : RequestId(NormalizeRequestId(requestId))
    {}

    TStringBuf GetRequestId() const {
        return RequestId;
    }
};

TString GetLogPrefix(const TMvpLogContext* context);
TStringBuf GetRequestId(const TMvpLogContext* context);
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

    TStringBuf GetRequestId() const {
        return NMVP::GetRequestId(GetLogContext());
    }

    void SetLogContext(const TMvpLogContext* logContext) {
        LogContext = logContext ? *logContext : TMvpLogContext();
    }
};

inline TStringBuf GetRequestId(const TMvpLogContext* context) {
    if (!context) {
        return {};
    }
    return context->GetRequestId();
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
    return TMvpLogContext(GetRequestId(request));
}

inline TString GetLogPrefix(const TMvpLogContext* context) {
    if (!context) {
        return {};
    }
    return TStringBuilder() << "X-Request-Id: " << context->GetRequestId() << ", ";
}

inline void SetRequestIdHeader(NHttp::THeadersBuilder& headers, TStringBuf requestId) {
    headers.Set(REQUEST_ID_HEADER, requestId);
}

} // namespace NMVP
