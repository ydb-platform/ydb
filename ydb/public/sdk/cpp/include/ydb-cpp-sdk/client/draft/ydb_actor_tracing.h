#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_settings.h>

#include <memory>
#include <string>

namespace NYdb::inline Dev::NActorTracing {

struct TTraceStartSettings : public TOperationRequestSettings<TTraceStartSettings> {};
struct TTraceStopSettings : public TOperationRequestSettings<TTraceStopSettings> {};
struct TTraceFetchSettings : public TOperationRequestSettings<TTraceFetchSettings> {};

class TTraceFetchResult : public TStatus {
public:
    TTraceFetchResult(TStatus&& status, std::string&& traceData)
        : TStatus(std::move(status))
        , TraceData_(std::move(traceData))
    {}

    const std::string& GetTraceData() const { return TraceData_; }

private:
    std::string TraceData_;
};

using TAsyncTraceFetchResult = NThreading::TFuture<TTraceFetchResult>;

class TActorTracingClient {
public:
    explicit TActorTracingClient(const TDriver& driver, const TCommonClientSettings& settings = {});
    ~TActorTracingClient();

    TAsyncStatus TraceStart(const TTraceStartSettings& settings = {});
    TAsyncStatus TraceStop(const TTraceStopSettings& settings = {});
    TAsyncTraceFetchResult TraceFetch(const TTraceFetchSettings& settings = {});

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

} // namespace NYdb::NActorTracing
