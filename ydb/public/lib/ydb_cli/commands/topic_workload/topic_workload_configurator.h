#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/logger/log.h>
#include <util/generic/string.h>

#include <atomic>
#include <memory>

#include <cstddef>

namespace NYdb::NConsoleClient {

struct TTopicWorkloadConfiguratorParams {
    size_t TotalSec;
    size_t WarmupSec;
    const NYdb::TDriver& Driver;
    std::shared_ptr<TLog> Log;
    std::shared_ptr<std::atomic<bool>> ErrorFlag;
    TString Database;
    TString TopicName;
    size_t ConsumerCount = 0;
};

class TTopicWorkloadConfiguratorWorker {
public:
    explicit TTopicWorkloadConfiguratorWorker(const TTopicWorkloadConfiguratorParams& params);

    void Process(TInstant endTime);

private:
    void AddConsumers(NYdb::NTopic::TTopicClient& client) const;
    void DropConsumers(NYdb::NTopic::TTopicClient& client) const;

    void AlterTopic(NYdb::NTopic::TTopicClient& client,
                    const NYdb::NTopic::TAlterTopicSettings& settings) const;

    static TString GetConsumerName(size_t index);

    TTopicWorkloadConfiguratorParams Params;
};

}
