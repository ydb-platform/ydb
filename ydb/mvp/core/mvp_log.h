#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/http/http.h>

#include <util/generic/guid.h>

namespace NMVP {

enum EService : NActors::NLog::EComponent {
    MIN = 257,
    Logger,
    MVP,
    GRPC,
    QUERY,
    MAX
};

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
    return TString(NHttp::THeaders(request->Headers).Get(REQUEST_ID_HEADER));
}

inline TString EnsureRequestId(NHttp::THttpIncomingRequestPtr& request) {
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

inline TMvpLogContext CreateLogContext(NHttp::THttpIncomingRequestPtr& request) {
    return {.RequestId = EnsureRequestId(request)};
}

inline TString GetLogPrefix(const TMvpLogContext* context) {
    if (!context || context->RequestId.empty()) {
        return {};
    }
    return TStringBuilder() << "request id: " << context->RequestId << ", ";
}

inline const TMvpLogContext* GetLogContextPtr(const TMvpLogContext& context) {
    return &context;
}

inline const TMvpLogContext* GetLogContextPtr(const TMvpLogContext* context) {
    return context;
}

#define BLOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, EService::MVP, stream)
#define BLOG_I(stream) LOG_INFO_S(*NActors::TlsActivationContext, EService::MVP, stream)
#define BLOG_W(stream) LOG_WARN_S(*NActors::TlsActivationContext, EService::MVP, stream)
#define BLOG_NOTICE(stream) LOG_NOTICE_S(*NActors::TlsActivationContext, EService::MVP, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*NActors::TlsActivationContext, EService::MVP, stream)
#define BLOG_GRPC_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, EService::GRPC, stream)
#define BLOG_GRPC_DC(context, stream) LOG_DEBUG_S(context, EService::GRPC, stream)
#define BLOG_QUERY_I(stream) LOG_INFO_S(*NActors::TlsActivationContext, EService::QUERY, stream)

#define BLOG_D_CTX(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, EService::MVP, NMVP::GetLogPrefix(GetLogContext()) << stream)
#define BLOG_I_CTX(stream) LOG_INFO_S(*NActors::TlsActivationContext, EService::MVP, NMVP::GetLogPrefix(GetLogContext()) << stream)
#define BLOG_W_CTX(stream) LOG_WARN_S(*NActors::TlsActivationContext, EService::MVP, NMVP::GetLogPrefix(GetLogContext()) << stream)
#define BLOG_NOTICE_CTX(stream) LOG_NOTICE_S(*NActors::TlsActivationContext, EService::MVP, NMVP::GetLogPrefix(GetLogContext()) << stream)
#define BLOG_ERROR_CTX(stream) LOG_ERROR_S(*NActors::TlsActivationContext, EService::MVP, NMVP::GetLogPrefix(GetLogContext()) << stream)
#define BLOG_GRPC_D_CTX(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, EService::GRPC, NMVP::GetLogPrefix(GetLogContext()) << stream)
#define BLOG_QUERY_I_CTX(stream) LOG_INFO_S(*NActors::TlsActivationContext, EService::QUERY, NMVP::GetLogPrefix(GetLogContext()) << stream)

}
