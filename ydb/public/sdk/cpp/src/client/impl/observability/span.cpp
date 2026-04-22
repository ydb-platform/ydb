#include "span.h"

#include <ydb/public/sdk/cpp/src/client/impl/internal/common/log_lazy.h>

#include <util/string/cast.h>

#include <exception>

namespace NYdb::inline Dev::NObservability {

namespace {

constexpr int DefaultGrpcPort = 2135;

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

TRequestSpan::TRequestSpan(std::shared_ptr<NTrace::ITracer> tracer
    , const std::string& requestName
    , const std::string& endpoint
    , const std::string& database
    , const TLog& log
    , const std::string& ydbClientType
) : Log_(log) {
    if (!tracer) {
        return;
    }

    std::string host;
    int port;
    ParseEndpoint(endpoint, host, port);

    try {
        Span_ = tracer->StartSpan(requestName, NTrace::ESpanKind::CLIENT);
        if (!Span_) {
            return;
        }
        Span_->SetAttribute("db.system.name", "ydb");
        Span_->SetAttribute("db.namespace", database);
        Span_->SetAttribute("db.operation.name", requestName);
        Span_->SetAttribute("ydb.client.api", YdbClientApiAttributeValue(ydbClientType));
        Span_->SetAttribute("server.address", host);
        Span_->SetAttribute("server.port", static_cast<int64_t>(port));
    } catch (...) {
        SafeLogRequestSpanError(Log_, "failed to initialize span", std::current_exception());
        Span_.reset();
    }
}

TRequestSpan::~TRequestSpan() noexcept {
    if (Span_) {
        try {
            Span_->End();
        } catch (...) {
            SafeLogRequestSpanError(Log_, "failed to end span", std::current_exception());
        }
    }
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

void TRequestSpan::End(EStatus status) noexcept {
    if (Span_) {
        try {
            Span_->SetAttribute("db.response.status_code", ToString(status));
            if (status != EStatus::SUCCESS) {
                Span_->SetAttribute("error.type", ToString(status));
            }
            Span_->End();
        } catch (...) {
            SafeLogRequestSpanError(Log_, "failed to finalize span", std::current_exception());
        }
        Span_.reset();
    }
}

} // namespace NYdb::NObservability
