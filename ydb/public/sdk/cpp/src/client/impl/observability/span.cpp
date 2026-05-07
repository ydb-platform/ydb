#include "span.h"

#include "operation_name.h"

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
constexpr const char* kRetryRootSpanName = "ydb.RunWithRetry";
constexpr const char* kRetryAttemptSpanName = "ydb.Try";

std::string YdbClientApiAttributeValue(const std::string& clientType) noexcept {
    return clientType.empty() ? std::string("Unspecified") : clientType;
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
    const std::string& type,
    const std::string& message,
    const std::string& stacktrace)
{
    std::map<std::string, std::string> attrs{
        {"exception.type", type},
        {"exception.message", message},
    };
    if (!stacktrace.empty()) {
        attrs.emplace("exception.stacktrace", stacktrace);
    }
    span.AddEvent("exception", attrs);
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
        kRetryRootSpanName,
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
        kRetryAttemptSpanName,
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
        const auto operationName = NormalizeOperationName(requestName);
        Span_ = tracer->StartSpan(operationName, kind, parent);
        if (!Span_) {
            return;
        }
        Span_->SetAttribute("db.system.name", "ydb");
        Span_->SetAttribute("db.namespace", database);
        Span_->SetAttribute("db.operation.name", operationName);
        Span_->SetAttribute("ydb.client.api", YdbClientApiAttributeValue(ydbClientType));
        Span_->SetAttribute("server.address", host);
        Span_->SetAttribute("server.port", static_cast<int64_t>(port));
    } catch (...) {
        SafeLogRequestSpanError(Log_, "failed to initialize span", std::current_exception());
        Span_.reset();
    }
}

TRequestSpan::~TRequestSpan() noexcept {
    End(EStatus::CLIENT_INTERNAL_ERROR);
}

void TRequestSpan::SetPeerEndpoint(const std::string& endpoint) noexcept {
    if (!Span_ || endpoint.empty()) {
        return;
    }
    try {
        std::string host;
        int port;
        ParseEndpoint(endpoint, host, port);
        Span_->SetAttribute("network.peer.address", host);
        Span_->SetAttribute("network.peer.port", static_cast<int64_t>(port));
    } catch (...) {
        SafeLogRequestSpanError(Log_, "failed to set peer endpoint", std::current_exception());
    }
}

void TRequestSpan::AddEvent(const std::string& name, const std::map<std::string, std::string>& attributes) noexcept {
    if (!Span_) {
        return;
    }
    try {
        Span_->AddEvent(name, attributes);
    } catch (...) {
        SafeLogRequestSpanError(Log_, "failed to add event", std::current_exception());
    }
}

void TRequestSpan::RecordException(const std::string& type, const std::string& message, const std::string& stacktrace) noexcept {
    if (!Span_) {
        return;
    }
    try {
        EmitExceptionEvent(*Span_, type, message, stacktrace);
    } catch (...) {
        SafeLogRequestSpanError(Log_, "failed to record exception", std::current_exception());
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
            const auto statusName = ToString(status);
            Span_->SetAttribute("db.response.status_code", statusName);
            if (status != EStatus::SUCCESS) {
                const auto errorType = CategorizeErrorType(status);
                Span_->SetAttribute("error.type", std::string(errorType));
                EmitExceptionEvent(*Span_, std::string(errorType), statusName, /*stacktrace=*/"");
                Span_->SetStatus(NTrace::ESpanStatus::Error, statusName);
            }
            Span_->End();
        } catch (...) {
            SafeLogRequestSpanError(Log_, "failed to finalize span", std::current_exception());
        }
        Span_.reset();
    }
}

void TRequestSpan::SetRetryCount(std::uint32_t count) noexcept {
    if (!Span_ || count == 0) {
        return;
    }
    try {
        Span_->SetAttribute("ydb.retry.count", static_cast<int64_t>(count));
    } catch (...) {
        SafeLogRequestSpanError(Log_, "failed to set retry count", std::current_exception());
    }
}

void TRequestSpan::SetRetryAttributes(std::uint32_t attempt, std::int64_t backoffMs) noexcept {
    if (!Span_ || attempt == 0) {
        return;
    }
    try {
        Span_->SetAttribute("ydb.retry.attempt", static_cast<int64_t>(attempt));
        Span_->SetAttribute("ydb.retry.backoff_ms", backoffMs);
    } catch (...) {
        SafeLogRequestSpanError(Log_, "failed to set retry attributes", std::current_exception());
    }
}

} // namespace NYdb::NObservability
