#include "topic_workload_describe.h"

#include "topic_workload_defines.h"


using namespace NYdb::NConsoleClient;

TString TCommandWorkloadTopicDescribe::GenerateConsumerName(const TString& consumerPrefix, ui32 consumerIdx)
{
    TString consumerName = TStringBuilder() << consumerPrefix << '-' << consumerIdx;
    return consumerName;
}

TString TCommandWorkloadTopicDescribe::GenerateFullTopicName(const TString& database, const TString& topicName)
{
    TString fullTopicName = TStringBuilder() << database << "/" << topicName;
    return fullTopicName;
}

NYdb::NTopic::TTopicDescription TCommandWorkloadTopicDescribe::DescribeTopic(const TString& database, const TString& topicName, const NYdb::TDriver& driver)
{
    NYdb::NTopic::TTopicClient topicClient(driver);

    TString fullTopicName = GenerateFullTopicName(database, topicName);
    auto result = topicClient.DescribeTopic(fullTopicName, {}).GetValueSync();

    if (!result.IsSuccess() || result.GetIssues()) {
        throw yexception() << "Error describe topic " << fullTopicName;
    }

    NYdb::NTopic::TTopicDescription description = result.GetTopicDescription();
    if (description.GetTotalPartitionsCount() == 0) {
        throw yexception() << "Topic " << fullTopicName << " does not have partitions.";
    }

    return description;
}