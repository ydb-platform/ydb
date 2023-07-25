#pragma once

#include <util/system/types.h>
#include <util/string/type.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

namespace NYdb {
    namespace NConsoleClient {
        class TCommandWorkloadTopicDescribe {
        public:
            static TString GenerateConsumerName(const TString& consumerPrefix, ui32 consumerIdx);
            static TString GenerateFullTopicName(const TString& database, const TString& topicName);
            static NTopic::TTopicDescription DescribeTopic(const TString& database, const TString& topicName, const NYdb::TDriver& driver);
        };
    }
}