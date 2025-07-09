#pragma once

#include "setup.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>


namespace NYdb::inline Dev::NTopic::NTests {

void DescribeTopicTest(ITopicTestSetup& setup, TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation);

void DescribeConsumerTest(ITopicTestSetup& setup, TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation);

void DescribePartitionTest(ITopicTestSetup& setup, TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation);

}
