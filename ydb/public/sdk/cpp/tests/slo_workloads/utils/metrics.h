#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <util/datetime/base.h>


struct TRequestData {
    TDuration Delay;
    NYdb::EStatus Status;
    std::uint64_t RetryAttempts;
};

class IMetricsPusher {
public:
    virtual ~IMetricsPusher() = default;

    virtual void PushRequestData(const TRequestData& requestData) = 0;
};

std::unique_ptr<IMetricsPusher> CreateOtelMetricsPusher(const std::string& metricsPushUrl, const std::string& operationType);
std::unique_ptr<IMetricsPusher> CreateNoopMetricsPusher();
