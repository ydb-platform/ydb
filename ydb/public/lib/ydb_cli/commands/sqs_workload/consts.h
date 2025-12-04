namespace NYdb::NConsoleClient {

constexpr const char* kSQSWorkloadActionHeader = "X-SQS-Workload-Action";
constexpr const char* kSQSWorkloadActionReceive = "Receive";
constexpr const char* kSQSWorkloadActionSend = "Send";
constexpr const char* kSQSWorkloadActionDelete = "Delete";

constexpr const char* kSQSMessageCountHeader = "X-SQS-Message-Count";
constexpr const char* kSQSMessageTotalSizeHeader = "X-SQS-Message-Total-Size";
constexpr const char* kSQSMessageUUIDHeader = "X-SQS-Request-UUID";

constexpr const char kSQSMessageStartTimeSeparator = '_';

} // namespace NYdb::NConsoleClient