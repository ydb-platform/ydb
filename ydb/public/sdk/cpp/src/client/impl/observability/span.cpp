#include "span.h"

#include <ydb/public/sdk/cpp/src/client/impl/observability/constants.h>
#include <ydb/public/sdk/cpp/src/client/impl/observability/error_category/error_category.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/common/log_lazy.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/db_driver_state/state.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <util/string/cast.h>

#include <exception>

namespace NYdb::inline Dev::NObservability {

namespace {

constexpr int DefaultGrpcPort = 2135;

std::string_view YdbClientApiAttributeValue(const std::string& clientType) noexcept {
    return clientType.empty() ? SpanValue::kClientApiUnspecified : std::string_view(clientType);
}

void ParseEndpoint(const std::string& endpoint, std::string& host, int& port) {
    port = DefaultGrpcPort;

    if (endpoint.empty()) {
        host = endpoint;
        return;
    }

    if (endpoint.front() == '[') {
        auto bracketEnd = endpoint.find(']');
        if (bracketEnd != std::string::npos) {
            host = endpoint.substr(1, bracketEnd - 1);
            if (bracketEnd + 2 < endpoint.size() && endpoint[bracketEnd + 1] == ':') {
                try {
                    port = std::stoi(endpoint.substr(bracketEnd + 2));
                } catch (...) {}
            }
            return;
        }
    }

    auto pos = endpoint.rfind(':');
    if (pos != std::string::npos) {
        host = endpoint.substr(0, pos);
        try {
            port = std::stoi(endpoint.substr(pos + 1));
        } catch (...) {}
    } else {
        host = endpoint;
    }
}

void EmitExceptionEvent(NTrace::ISpan& span,
    std::string_view type,
    std::string_view message,
    std::string_view stacktrace
) {
    if (stacktrace.empty()) {
        span.AddEvent(SpanEvent::kException, {
            {SpanAttr::kExceptionType, type},
            {SpanAttr::kExceptionMessage, message},
        });
    } else {
        span.AddEvent(SpanEvent::kException, {
            {SpanAttr::kExceptionType, type},
            {SpanAttr::kExceptionMessage, message},
            {SpanAttr::kExceptionStacktrace, stacktrace},
        });
    }
}

void SafeLogRequestSpanError(TLog& log, const char* message, std::exception_ptr exception) noexcept {
    try {
        if (!exception) {
            LOG_LAZY(log, TLOG_ERR, std::string("TRequestSpan: ") + message + ": (no active exception)");
            return;
        }
        try {
            std::rethrow_exception(exception);
        } catch (const std::exception& e) {
            LOG_LAZY(log, TLOG_ERR, std::string("TRequestSpan: ") + message + ": " + e.what());
            return;
        } catch (...) {
        }
        LOG_LAZY(log, TLOG_ERR, std::string("TRequestSpan: ") + message + ": (unknown)");
    } catch (...) {
    }
}

} // namespace

std::shared_ptr<TRequestSpan> TRequestSpan::Create(const std::string& ydbClientType
    , std::shared_ptr<NTrace::ITracer> tracer
    , const std::string& requestName
    , const std::string& discoveryEndpoint
    , const std::string& database
    , const TLog& log
    , NTrace::ESpanKind kind
    , const std::shared_ptr<TRequestSpan>& parent
) {
    NTrace::ISpan* parentRaw = parent ? parent->Span_.get() : nullptr;
    return std::shared_ptr<TRequestSpan>(new TRequestSpan(
        ydbClientType,
        std::move(tracer),
        requestName,
        discoveryEndpoint,
        database,
        log,
        kind,
        parentRaw
    ));
}

std::shared_ptr<TRequestSpan> TRequestSpan::CreateForClientRetry(const std::string& ydbClientType
    , std::shared_ptr<NTrace::ITracer> tracer
    , const std::shared_ptr<TDbDriverState>& dbDriverState
) {
    return Create(
        ydbClientType,
        std::move(tracer),
        std::string(SpanName::kRetryRoot),
        dbDriverState->DiscoveryEndpoint,
        dbDriverState->Database,
        dbDriverState->Log,
        NTrace::ESpanKind::INTERNAL
    );
}

std::shared_ptr<TRequestSpan> TRequestSpan::CreateForRetryAttempt(const std::string& ydbClientType
    , std::shared_ptr<NTrace::ITracer> tracer
    , const std::shared_ptr<TDbDriverState>& dbDriverState
    , std::uint32_t attempt
    , std::int64_t backoffMs
    , const std::shared_ptr<TRequestSpan>& parent
) {
    auto span = Create(
        ydbClientType,
        std::move(tracer),
        std::string(SpanName::kRetryAttempt),
        dbDriverState->DiscoveryEndpoint,
        dbDriverState->Database,
        dbDriverState->Log,
        NTrace::ESpanKind::INTERNAL,
        parent
    );
    if (span) {
        span->SetRetryAttributes(attempt, backoffMs);
    }
    return span;
}

TRequestSpan::TRequestSpan(const std::string& ydbClientType
    , std::shared_ptr<NTrace::ITracer> tracer
    , const std::string& requestName
    , const std::string& discoveryEndpoint
    , const std::string& database
    , const TLog& log
    , NTrace::ESpanKind kind
    , NTrace::ISpan* parent
) : Log_(log) {
    if (!tracer) {
        return;
    }

    std::string host;
    int port;
    ParseEndpoint(discoveryEndpoint, host, port);

    try {
        Span_ = tracer->StartSpan(requestName, kind, parent);
        if (!Span_) {
            return;
        }
        Span_->SetAttribute(SpanAttr::kDbSystemName, SpanValue::kDbSystemYdb);
        Span_->SetAttribute(SpanAttr::kDbNamespace, database);
        Span_->SetAttribute(SpanAttr::kDbOperationName, requestName);
        Span_->SetAttribute(SpanAttr::kYdbClientApi, YdbClientApiAttributeValue(ydbClientType));
        Span_->SetAttribute(SpanAttr::kServerAddress, host);
        Span_->SetAttribute(SpanAttr::kServerPort, static_cast<int64_t>(port));
    } catch (...) {
        SafeLogRequestSpanError(Log_, "failed to initialize span", std::current_exception());
        Span_.reset();
    }
}

TRequestSpan::~TRequestSpan() noexcept {
    End(EStatus::CLIENT_INTERNAL_ERROR);
}

void TRequestSpan::SetPeerEndpoint(const std::string& endpoint) noexcept {
    SetPeerEndpoint(endpoint, /*nodeId=*/0, /*location=*/"");
}

void TRequestSpan::SetPeerEndpoint(const std::string& endpoint, std::uint64_t nodeId, const std::string& location) noexcept {
    if (!Span_) {
        return;
    }
    try {
        if (!endpoint.empty()) {
            std::string host;
            int port;
            ParseEndpoint(endpoint, host, port);
            Span_->SetAttribute(SpanAttr::kNetworkPeerAddress, host);
            Span_->SetAttribute(SpanAttr::kNetworkPeerPort, static_cast<int64_t>(port));
        }
        if (nodeId != 0) {
            Span_->SetAttribute(SpanAttr::kYdbNodeId, static_cast<int64_t>(nodeId));
        }
        if (!location.empty()) {
            Span_->SetAttribute(SpanAttr::kYdbNodeDc, location);
        }
    } catch (...) {
        SafeLogRequestSpanError(Log_, "failed to set peer endpoint", std::current_exception());
    }
}

void TRequestSpan::AddEvent(std::string_view name, NTrace::TAttributes attributes) noexcept {
    if (!Span_) {
        return;
    }
    try {
        Span_->AddEvent(name, attributes);
    } catch (...) {
        SafeLogRequestSpanError(Log_, "failed to add event", std::current_exception());
    }
}

std::unique_ptr<NTrace::IScope> TRequestSpan::Activate() noexcept {
    if (!Span_) {
        return nullptr;
    }
    try {
        return Span_->Activate();
    } catch (...) {
        SafeLogRequestSpanError(Log_, "failed to activate span", std::current_exception());
        return nullptr;
    }
}

void TRequestSpan::End(EStatus status) noexcept {
    if (Span_) {
        try {
            if (status != EStatus::SUCCESS) {
                const auto statusName = ToString(status);
                const auto errorType = CategorizeErrorType(status);
                if (errorType == kErrorTypeYdb) {
                    Span_->SetAttribute(SpanAttr::kDbResponseStatusCode, statusName);
                }
                Span_->SetAttribute(SpanAttr::kErrorType, errorType);
                EmitExceptionEvent(*Span_, errorType, statusName, /*stacktrace=*/"");
                Span_->SetStatus(NTrace::ESpanStatus::Error, statusName);
            }
            Span_->End();
        } catch (...) {
            SafeLogRequestSpanError(Log_, "failed to finalize span", std::current_exception());
        }
        Span_.reset();
    }
}

void TRequestSpan::EndWithException(const std::string& exceptionType, const std::string& message) noexcept {
    if (Span_) {
        try {
            Span_->SetAttribute(SpanAttr::kErrorType, exceptionType);
            EmitExceptionEvent(*Span_, exceptionType, message, /*stacktrace=*/"");
            Span_->SetStatus(NTrace::ESpanStatus::Error, message);
            Span_->End();
        } catch (...) {
            SafeLogRequestSpanError(Log_, "failed to finalize span (exception)", std::current_exception());
        }
        Span_.reset();
    }
}

void TRequestSpan::SetRetryCount(std::uint32_t count) noexcept {
    if (!Span_ || count == 0) {
        return;
    }
    try {
        Span_->SetAttribute(SpanAttr::kYdbRetryCount, static_cast<int64_t>(count));
    } catch (...) {
        SafeLogRequestSpanError(Log_, "failed to set retry count", std::current_exception());
    }
}

void TRequestSpan::SetRetryAttributes(std::uint32_t attempt, std::int64_t backoffMs) noexcept {
    if (!Span_ || attempt == 0) {
        return;
    }
    try {
        Span_->SetAttribute(SpanAttr::kYdbRetryAttempt, static_cast<int64_t>(attempt));
        Span_->SetAttribute(SpanAttr::kYdbRetryBackoffMs, backoffMs);
    } catch (...) {
        SafeLogRequestSpanError(Log_, "failed to set retry attributes", std::current_exception());
    }
}

} // namespace NYdb::NObservability
