#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/trace/trace.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <library/cpp/logger/log.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>

namespace NYdb::inline Dev {

class TDbDriverState;

} // namespace NYdb::inline Dev

namespace NYdb::inline Dev::NObservability {

class TRequestSpan {
public:
    static std::shared_ptr<TRequestSpan> Create(
        const std::string& ydbClientType
        , std::shared_ptr<NTrace::ITracer> tracer
        , const std::string& requestName
        , const std::string& discoveryEndpoint
        , const std::string& database
        , const TLog& log
        , NTrace::ESpanKind kind = NTrace::ESpanKind::CLIENT
        , const std::shared_ptr<TRequestSpan>& parent = nullptr
    );

    static std::shared_ptr<TRequestSpan> CreateForClientRetry(
        const std::string& ydbClientType
        , std::shared_ptr<NTrace::ITracer> tracer
        , const std::shared_ptr<TDbDriverState>& dbDriverState
    );

    static std::shared_ptr<TRequestSpan> CreateForRetryAttempt(
        const std::string& ydbClientType
        , std::shared_ptr<NTrace::ITracer> tracer
        , const std::shared_ptr<TDbDriverState>& dbDriverState
        , std::uint32_t attempt
        , std::int64_t backoffMs
        , const std::shared_ptr<TRequestSpan>& parent = nullptr
    );

    ~TRequestSpan() noexcept;

    TRequestSpan(const TRequestSpan&) = delete;
    TRequestSpan& operator=(const TRequestSpan&) = delete;

    void SetPeerEndpoint(const std::string& endpoint) noexcept;
    void AddEvent(const std::string& name, const std::map<std::string, std::string>& attributes = {}) noexcept;
    void RecordException(const std::string& type, const std::string& message, const std::string& stacktrace = {}) noexcept;
    std::unique_ptr<NTrace::IScope> Activate() noexcept;
    void SetRetryCount(std::uint32_t count) noexcept;

    void End(EStatus status) noexcept;

private:
    TRequestSpan(const std::string& ydbClientType
        , std::shared_ptr<NTrace::ITracer> tracer
        , const std::string& requestName
        , const std::string& discoveryEndpoint
        , const std::string& database
        , const TLog& log
        , NTrace::ESpanKind kind
        , NTrace::ISpan* parent
    );

    void SetRetryAttributes(std::uint32_t attempt, std::int64_t backoffMs) noexcept;

    TLog Log_;
    std::shared_ptr<NTrace::ISpan> Span_;
};

} // namespace NYdb::NObservability
