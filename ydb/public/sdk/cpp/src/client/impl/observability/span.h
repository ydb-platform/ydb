#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/trace/trace.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <library/cpp/logger/log.h>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace NYdb::inline Dev {

class TDbDriverState;

} // namespace NYdb::inline Dev

namespace NYdb::inline Dev::NObservability {

class TRequestSpan {
public:
    static std::shared_ptr<TRequestSpan> Create(
        std::string_view ydbClientType
        , std::shared_ptr<NTrace::ITracer> tracer
        , std::string_view requestName
        , std::string_view discoveryEndpoint
        , std::string_view database
        , const TLog& log
        , NTrace::ESpanKind kind = NTrace::ESpanKind::CLIENT
        , const std::shared_ptr<TRequestSpan>& parent = nullptr
    );

    static std::shared_ptr<TRequestSpan> CreateForClientRetry(
        std::string_view ydbClientType
        , std::shared_ptr<NTrace::ITracer> tracer
        , const std::shared_ptr<TDbDriverState>& dbDriverState
    );

    static std::shared_ptr<TRequestSpan> CreateForRetryAttempt(
        std::string_view ydbClientType
        , std::shared_ptr<NTrace::ITracer> tracer
        , const std::shared_ptr<TDbDriverState>& dbDriverState
        , std::uint32_t attempt
        , std::int64_t backoffMs
        , const std::shared_ptr<TRequestSpan>& parent = nullptr
    );

    ~TRequestSpan() noexcept;

    TRequestSpan(const TRequestSpan&) = delete;
    TRequestSpan& operator=(const TRequestSpan&) = delete;

    void SetPeerEndpoint(std::string_view endpoint) noexcept;
    void SetPeerEndpoint(std::string_view endpoint, std::uint64_t nodeId, std::string_view location) noexcept;
    void AddEvent(std::string_view name, NTrace::TAttributes attributes = {}) noexcept;
    std::unique_ptr<NTrace::IScope> Activate() noexcept;
    void SetRetryCount(std::uint32_t count) noexcept;
    void SetRetryAttributes(std::uint32_t attempt, std::int64_t backoffMs) noexcept;

    void End(EStatus status) noexcept;
    void EndWithException(std::string_view exceptionType, std::string_view message) noexcept;

private:
    TRequestSpan(std::string_view ydbClientType
        , std::shared_ptr<NTrace::ITracer> tracer
        , std::string_view requestName
        , std::string_view discoveryEndpoint
        , std::string_view database
        , const TLog& log
        , NTrace::ESpanKind kind
        , NTrace::ISpan* parent
    );

    TLog Log_;
    std::shared_ptr<NTrace::ISpan> Span_;
};

} // namespace NYdb::NObservability
