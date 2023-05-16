#include "topic_workload_params.h"

#include "topic_workload_defines.h"

#include <library/cpp/getopt/small/last_getopt_support.h>
#include <library/cpp/regex/pcre/regexp.h>

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

ui64 TCommandWorkloadTopicParams::StrToBytes(const TString& str)
{
    TString loweredStr(str);
    loweredStr.to_lower();
    auto len = loweredStr.size();
    if (len == 0)
        ythrow NLastGetopt::TUsageException() << "Empty string can't be parsed as a valid number";

    TRegExMatch regex("\\d+[kmg]?");
    if (!regex.Match(loweredStr.c_str()))
        ythrow NLastGetopt::TUsageException() << "'" << str << "' can't be parsed as a valid number";

    THashMap<char, ui64> suffixes{
        {'k', 1024},
        {'m', 1024 * 1024},
        {'g', 1024 * 1024 * 1024}};

    char lastChar = loweredStr[len - 1];

    auto suffix = suffixes.find(lastChar);
    if (suffix)
    {
        ui64 ret = std::stoul(loweredStr.substr(0, len - 1));
        ret *= suffix->second;
        return ret;
    } else {
        ui64 ret = std::stoul(loweredStr);
        return ret;
    }
}
