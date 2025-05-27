#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

Y_DECLARE_OUT_SPEC(, NYdb::NTopic::TDescribeTopicResult, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(, NYdb::NTopic::TDescribeConsumerResult, o, x) {
    return x.Out(o);
}
