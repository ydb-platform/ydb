#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/logger/log.h>
#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/system/types.h>

#include <atomic>
#include <memory>

#include <cstddef>

namespace NYdb::NConsoleClient {

class TCommandWorkloadTopicDescribe {
public:
    static TString GenerateConsumerName(const TString& consumerPrefix, ui32 consumerIdx);
    static NTopic::TConsumerDescription DescribeConsumer(const TString& database, const TString& topicName, const TString& consumerName, const NYdb::TDriver& driver);
    static TString GenerateFullTopicName(const TString& database, const TString& topicName);
    static NTopic::TTopicDescription DescribeTopic(const TString& database, const TString& topicName, const NYdb::TDriver& driver);
};

struct TTopicWorkloadDescriberParams {
    size_t TotalSec;
    size_t WarmupSec;
    const NYdb::TDriver& Driver;
    std::shared_ptr<TLog> Log;
    std::shared_ptr<std::atomic<bool>> ErrorFlag;
    TString Database;
    TString TopicName;
    TString ConsumerName;
    bool NeedDescribeTopic = false;
    bool NeedDescribeConsumer = false;
};

class TTopicWorkloadDescriberWorker {
public:
    explicit TTopicWorkloadDescriberWorker(const TTopicWorkloadDescriberParams& params);

    void Process(TInstant endTime);

private:
    TTopicWorkloadDescriberParams Params;
};

}
