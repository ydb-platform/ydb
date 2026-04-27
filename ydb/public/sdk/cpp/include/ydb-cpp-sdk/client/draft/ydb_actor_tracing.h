#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fluent_settings_helpers.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace NYdb::inline Dev::NActorTracing {

struct TTraceStartSettings : public TOperationRequestSettings<TTraceStartSettings> {
    FLUENT_SETTING_VECTOR(uint32_t, NodeIds);
};

struct TTraceStopSettings : public TOperationRequestSettings<TTraceStopSettings> {
    FLUENT_SETTING_VECTOR(uint32_t, NodeIds);
};

struct TTraceFetchSettings : public TOperationRequestSettings<TTraceFetchSettings> {
    FLUENT_SETTING_VECTOR(uint32_t, NodeIds);
};

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
