#include <aws/sqs/SQSClient.h>
#include <util/datetime/base.h>
#include <util/system/types.h>
#include <ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_workload_stats_collector.h>

#include "message_groups_locker.h"

namespace NYdb::NConsoleClient {

    class TSQSClientWrapper: public Aws::SQS::SQSClient {
        public:
            explicit TSQSClientWrapper(
                const Aws::Auth::AWSCredentials& credentials,
                const Aws::Client::ClientConfiguration& clientConfiguration,
                std::shared_ptr<Aws::SQS::SQSClient> client,
                std::shared_ptr<TSqsWorkloadStatsCollector> statsCollector,
                bool fifoValidationEnabled);

            ~TSQSClientWrapper() = default;

            Aws::SQS::Model::SendMessageBatchOutcome SendMessageBatch(
                const Aws::SQS::Model::SendMessageBatchRequest& request) const override;
            Aws::SQS::Model::ReceiveMessageOutcome ReceiveMessage(
                const Aws::SQS::Model::ReceiveMessageRequest& request) const override;
            Aws::SQS::Model::DeleteMessageBatchOutcome DeleteMessageBatch(
                const Aws::SQS::Model::DeleteMessageBatchRequest& request)
                const override;
            Aws::SQS::Model::GetQueueUrlOutcome GetQueueUrl(
                const Aws::SQS::Model::GetQueueUrlRequest& request) const override;
        private:
            std::shared_ptr<Aws::SQS::SQSClient> Client;
            std::shared_ptr<TSqsWorkloadStatsCollector> StatsCollector;

            mutable TMessageGroupsLocker SenderMessageGroupsLocker;
            mutable TMessageGroupsLocker ReceiverMessageGroupsLocker;

            mutable std::mutex MessageGroupsMutex;
            mutable std::unordered_map<Aws::String, ui64> LastReceivedMessageInGroup;

            bool fifoValidationEnabled;
    };

} // namespace NYdb::NConsoleClient