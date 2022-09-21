#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

namespace Ydb {
namespace Monitoring {
    class SelfCheckResult;
}
}

namespace NYdb {

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
    FLUENT_SETTING_OPTIONAL(EStatusFlag, MinimumStatus);
    FLUENT_SETTING_OPTIONAL(ui32, MaximumLevel);
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

class TMonitoringClient {
    class TImpl;

public:
    TMonitoringClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    TAsyncSelfCheckResult SelfCheck(const TSelfCheckSettings& settings = TSelfCheckSettings());
private:
    std::shared_ptr<TImpl> Impl_;
};

}
}
