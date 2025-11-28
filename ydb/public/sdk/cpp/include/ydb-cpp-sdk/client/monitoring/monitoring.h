#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

namespace Ydb {
namespace Monitoring {
    class SelfCheckResult;
    class ClusterStateResult;
}
}

namespace NYdb::inline Dev {

class TProtoAccessor;

namespace NMonitoring {

////////////////////////////////////////////////////////////////////////////////

enum class EStatusFlag {
    UNSPECIFIED = 0,
    GREY = 1,
    GREEN = 2,
    BLUE = 3,
    YELLOW = 4,
    ORANGE = 5,
    RED = 6,
};

struct TSelfCheckSettings : public TOperationRequestSettings<TSelfCheckSettings>{
    FLUENT_SETTING_OPTIONAL(bool, ReturnVerboseStatus);
    FLUENT_SETTING_OPTIONAL(bool, NoMerge);
    FLUENT_SETTING_OPTIONAL(bool, NoCache);
    FLUENT_SETTING_OPTIONAL(EStatusFlag, MinimumStatus);
    FLUENT_SETTING_OPTIONAL(uint32_t, MaximumLevel);
};

struct TClusterStateSettings : public TOperationRequestSettings<TClusterStateSettings> {
    FLUENT_SETTING_OPTIONAL(uint32_t, DurationSeconds);
    FLUENT_SETTING_OPTIONAL(uint32_t, PeriodSeconds);
};


class TSelfCheckResult : public TStatus {
    friend class NYdb::TProtoAccessor;
public:
    TSelfCheckResult(TStatus&& status, Ydb::Monitoring::SelfCheckResult&& result);
private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

using TAsyncSelfCheckResult = NThreading::TFuture<TSelfCheckResult>;

class TClusterStateResult : public TStatus {
    friend class NYdb::TProtoAccessor;
public:
    TClusterStateResult(TStatus&& status, Ydb::Monitoring::ClusterStateResult&& result);
private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

using TAsyncClusterStateResult = NThreading::TFuture<TClusterStateResult>;

class TMonitoringClient {
    class TImpl;

public:
    TMonitoringClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    TAsyncSelfCheckResult SelfCheck(const TSelfCheckSettings& settings = TSelfCheckSettings());

    TAsyncClusterStateResult ClusterState(const TClusterStateSettings& settings = TClusterStateSettings());
private:
    std::shared_ptr<TImpl> Impl_;
};

}
}
