#include "topic_workload_params.h"

#include "topic_workload_defines.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

using namespace NYdb::NConsoleClient;

ui32 TCommandWorkloadTopicParams::StrToCodec(const TString& str) {
    THashMap<TString, NYdb::NTopic::ECodec> codecs{
        {"raw", NYdb::NTopic::ECodec::RAW},
        {"gzip", NYdb::NTopic::ECodec::GZIP},
        {"zstd", NYdb::NTopic::ECodec::ZSTD}};
    TString loweredStr(str);
    loweredStr.to_lower();
    codecs.contains(loweredStr) ?: throw yexception() << "Unsupported codec: " << str;
    return (ui32)codecs[loweredStr];
}

TString TCommandWorkloadTopicParams::GenerateConsumerName(ui32 consumerIdx)
{
    TString consumerName = TStringBuilder() << CONSUMER_PREFIX << '-' << consumerIdx;
    return consumerName;
}