#pragma once

#include <util/system/types.h>
#include <util/string/type.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

namespace NYdb {
    namespace NConsoleClient {
        class TCommandWorkloadTopicDescribe {
        public:
            static TString GenerateConsumerName(ui32 consumerIdx);
            static NTopic::TTopicDescription DescribeTopic(TString database, const NYdb::TDriver& driver);
        };
    }
}