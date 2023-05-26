#include "topic_workload_describe.h"

#include "topic_workload_defines.h"


using namespace NYdb::NConsoleClient;

TString TCommandWorkloadTopicDescribe::GenerateConsumerName(ui32 consumerIdx)
{
    TString consumerName = TStringBuilder() << CONSUMER_PREFIX << '-' << consumerIdx;
    return consumerName;
}

NYdb::NTopic::TTopicDescription TCommandWorkloadTopicDescribe::DescribeTopic(TString database, const NYdb::TDriver& driver)
{
    NYdb::NTopic::TTopicClient topicClient(driver);

    auto topicName = database + "/" + TOPIC;
    auto result = topicClient.DescribeTopic(topicName, {}).GetValueSync();

    if (!result.IsSuccess() || result.GetIssues()) {
        Cerr << "Error describe topic " << topicName << ": " << (NYdb::TStatus)result << "\n";
        exit(EXIT_FAILURE);
    }

    NYdb::NTopic::TTopicDescription description = result.GetTopicDescription();
    if (description.GetTotalPartitionsCount() == 0) {
        Cerr << "Topic " << topicName << " does not exists.\n";
        exit(EXIT_FAILURE);
    }

    return description;
}