#include "sqs_client_wrapper.h"
#include "consts.h"
#include "utils.h"

#include <aws/sqs/model/SendMessageBatchRequest.h>
#include <util/datetime/base.h>

#include <fmt/format.h>
#include <unordered_set>

namespace NYdb::NConsoleClient {

    TSQSClientWrapper::TSQSClientWrapper(
        const Aws::Auth::AWSCredentials& credentials,
        const Aws::Client::ClientConfiguration& clientConfiguration,
        std::shared_ptr<Aws::SQS::SQSClient> client,
        std::shared_ptr<TSqsWorkloadStatsCollector> statsCollector,
        bool fifoValidationEnabled)
        : SQSClient(credentials, clientConfiguration), Client(client), StatsCollector(statsCollector), fifoValidationEnabled(fifoValidationEnabled)
    {}

    Aws::SQS::Model::SendMessageBatchOutcome TSQSClientWrapper::SendMessageBatch(
        const Aws::SQS::Model::SendMessageBatchRequest& request) const {
        std::unordered_set<Aws::String> messageGroups;
        for (auto& entry : request.GetEntries()) {
            if (entry.MessageGroupIdHasBeenSet()) {
                messageGroups.insert(entry.GetMessageGroupId());
            }
        }
        if (fifoValidationEnabled) {
            SenderMessageGroupsLocker.Lock(messageGroups);
        }

        auto now = TInstant::Now().MilliSeconds();
        auto messagePrefix = fmt::format("{}{}", now, SQS_MESSAGE_START_TIME_SEPARATOR);

        for (auto& entry : request.GetEntries()) {
            auto& nonConstEntry = const_cast<Aws::SQS::Model::SendMessageBatchRequestEntry&>(entry);
            auto& messageBody = const_cast<Aws::String&>(entry.GetMessageBody());

            for (size_t i = 0; i < messagePrefix.size(); ++i) {
                if (i == messageBody.size()) {
                    messageBody.push_back(messagePrefix[i]);
                    continue;
                }
                messageBody[i] = messagePrefix[i];
            }

            nonConstEntry.WithMessageBody(messageBody);
            if (entry.MessageGroupIdHasBeenSet()) {
                messageGroups.insert(entry.GetMessageGroupId());
            }
        }

        try {
            auto response = Client->SendMessageBatch(request);
            if (fifoValidationEnabled) {
                SenderMessageGroupsLocker.Unlock(messageGroups);
            }

            return response;
        } catch (const std::exception& e) {
            if (fifoValidationEnabled) {
                SenderMessageGroupsLocker.Unlock(messageGroups);
            }

            throw;
        }
    }

    Aws::SQS::Model::ReceiveMessageOutcome TSQSClientWrapper::ReceiveMessage(const Aws::SQS::Model::ReceiveMessageRequest& request) const {
        auto response = Client->ReceiveMessage(request);
        auto now = TInstant::Now().MilliSeconds();

        std::unordered_set<Aws::String> messageGroups;
        for (auto& message : response.GetResult().GetMessages()) {
            const auto& attributes = message.GetAttributes();
            auto messageGroupId = attributes.find(Aws::SQS::Model::MessageSystemAttributeName::MessageGroupId);
            if (messageGroupId != attributes.end()) {
                messageGroups.insert(messageGroupId->second);
            }
        }

        if (fifoValidationEnabled) {
            ReceiverMessageGroupsLocker.Lock(messageGroups);
        }

        std::unique_lock lock(MessageGroupsMutex);
        for (auto& message : response.GetResult().GetMessages()) {
            const auto& body = message.GetBody();
            auto sendTimestamp = ExtractSendTimestamp(body);

            TSqsWorkloadStats::GotMessageEvent event{
                body.size(),
                now - sendTimestamp,
                1
            };
            StatsCollector->AddGotMessageEvent(event);

            if (!fifoValidationEnabled) {
                continue;
            }

            const auto& attributes = message.GetAttributes();
            auto messageGroupId = attributes.find(Aws::SQS::Model::MessageSystemAttributeName::MessageGroupId);
            if (messageGroupId != attributes.end()) {
                auto lastReceivedMessageInGroup = LastReceivedMessageInGroup.find(messageGroupId->second);
                if (lastReceivedMessageInGroup != LastReceivedMessageInGroup.end() && lastReceivedMessageInGroup->second > sendTimestamp) {
                    ReceiverMessageGroupsLocker.Unlock(messageGroups);
                    throw std::runtime_error("Message received out of order");
                }
                LastReceivedMessageInGroup[messageGroupId->second] = sendTimestamp;
            }
        }

        if (fifoValidationEnabled) {
            ReceiverMessageGroupsLocker.Unlock(messageGroups);
        }

        return response;
    }

    Aws::SQS::Model::DeleteMessageBatchOutcome TSQSClientWrapper::DeleteMessageBatch(
        const Aws::SQS::Model::DeleteMessageBatchRequest& request) const {
        return Client->DeleteMessageBatch(request);
    }

    Aws::SQS::Model::GetQueueUrlOutcome TSQSClientWrapper::GetQueueUrl(const Aws::SQS::Model::GetQueueUrlRequest& request) const {
        return Client->GetQueueUrl(request);
    }

} // namespace NYdb::NConsoleClient
