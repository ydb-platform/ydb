#pragma once

#include "setup.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>


namespace NYdb::inline Dev::NTopic::NTests {

void DescribeTopic(ITopicTestSetup& setup, TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation);

void DescribeConsumer(ITopicTestSetup& setup, TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation);

void DescribePartition(ITopicTestSetup& setup, TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation);

}
