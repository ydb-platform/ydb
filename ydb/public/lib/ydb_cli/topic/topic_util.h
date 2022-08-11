#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

namespace NYdb::NConsoleClient {
    namespace {
        constexpr TDuration DefaultCheckTopicExistenceTimeout = TDuration::Seconds(1);
    }

    class TTopicInitializationChecker {
    public:
        TTopicInitializationChecker(NTopic::TTopicClient& topicClient)
            : TopicClient_(topicClient) {
        }

        void CheckTopicExistence(TString& topicPath, TString consumer = "", TDuration timeout = DefaultCheckTopicExistenceTimeout) {
            NTopic::TAsyncDescribeTopicResult descriptionFuture = TopicClient_.DescribeTopic(topicPath);
            descriptionFuture.Wait(timeout);
            NTopic::TDescribeTopicResult description = descriptionFuture.GetValueSync();
            ThrowOnError(description);

            if (consumer == "") {
                return;
            }

            bool hasConsumer = false;
            for (const auto& rr : description.GetTopicDescription().GetConsumers()) {
                if (rr.GetConsumerName() == consumer) {
                    hasConsumer = true;
                    break;
                }
            }

            if (!hasConsumer) {
                throw yexception() << "No consumer \"" << consumer << "\" found";
            }
        }

    private:
        NTopic::TTopicClient TopicClient_;
    };
} // namespace NYdb::NConsoleClient