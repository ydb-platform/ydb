#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/trace/trace.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>

#include <library/cpp/logger/log.h>

#include <map>
#include <memory>
#include <string>

namespace NYdb::inline Dev::NObservability {

class TRequestSpan {
public:
    TRequestSpan(std::shared_ptr<NTrace::ITracer> tracer
        , const std::string& requestName
        , const std::string& endpoint
        , const std::string& database
        , const TLog& log
        , const std::string& ydbClientType = {}
    );
    ~TRequestSpan() noexcept;

    void SetPeerEndpoint(const std::string& endpoint) noexcept;
    void AddEvent(const std::string& name, const std::map<std::string, std::string>& attributes = {}) noexcept;

    void End(EStatus status) noexcept;

private:
    TLog Log_;
    std::shared_ptr<NTrace::ISpan> Span_;
};

} // namespace NYdb::NObservability
