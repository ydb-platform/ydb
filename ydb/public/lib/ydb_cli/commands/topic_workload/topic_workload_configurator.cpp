#include "topic_workload_configurator.h"
#include "topic_workload_defines.h"

using namespace NYdb::NConsoleClient;

TTopicWorkloadConfiguratorWorker::TTopicWorkloadConfiguratorWorker(const TTopicWorkloadConfiguratorParams& params)
    : Params(params)
{
}

void TTopicWorkloadConfiguratorWorker::Process(TInstant endTime)
{
    Sleep(TDuration::Seconds(Params.WarmupSec));

    NYdb::NTopic::TTopicClient client(Params.Driver);

    while (!*Params.ErrorFlag) {
        auto now = Now();
        if (now > endTime) {
            break;
        }

        AddConsumers(client);
        DropConsumers(client);

        Sleep(TDuration::Seconds(3));
    }
}

TString TTopicWorkloadConfiguratorWorker::GetConsumerName(size_t index)
{
    return TStringBuilder() << "test-consumer-" << index;
}

void TTopicWorkloadConfiguratorWorker::AddConsumers(NYdb::NTopic::TTopicClient& client) const
{
    NYdb::NTopic::TAlterTopicSettings settings;

    for (size_t j = 0; j < Params.ConsumerCount; ++j) {
        settings.BeginAddConsumer(GetConsumerName(j));
    }

    AlterTopic(client, settings);

    WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
              << Params.ConsumerCount << " consumers have been added to the topic " << Params.TopicName);
}

void TTopicWorkloadConfiguratorWorker::DropConsumers(NYdb::NTopic::TTopicClient& client) const
{
    NYdb::NTopic::TAlterTopicSettings settings;

    for (size_t j = 0; j < Params.ConsumerCount; ++j) {
        settings.AppendDropConsumers(GetConsumerName(j));
    }

    AlterTopic(client, settings);

    WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
              << Params.ConsumerCount << " consumers were deleted from the topic " << Params.TopicName);
}

void TTopicWorkloadConfiguratorWorker::AlterTopic(NYdb::NTopic::TTopicClient& client,
                                                  const NYdb::NTopic::TAlterTopicSettings& settings) const
{
    auto result = client.AlterTopic(Params.TopicName, settings).GetValueSync();
    if (!result.IsSuccess()) {
        WRITE_LOG(Params.Log, ELogPriority::TLOG_ERR, TStringBuilder()
                  << "Failed to alter topic " << Params.TopicName
                  << ": " << result.GetIssues().ToOneLineString());
    }
}
