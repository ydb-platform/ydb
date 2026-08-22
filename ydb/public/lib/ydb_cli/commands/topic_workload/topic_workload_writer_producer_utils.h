#pragma once

#include "topic_workload_writer.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/string.h>
#include <util/stream/str.h>
#include <util/system/yassert.h>

#include <string_view>

namespace NYdb::NConsoleClient::NTopicWorkloadWriterInternal {

inline TString GetGeneratedMessage(const TTopicWorkloadWriterParams& params, ui64 messageId) {
    // GeneratedMessages is expected to be pre-filled (see GenerateMessages()).
    Y_ABORT_UNLESS(!params.GeneratedMessages.empty());
    return params.GeneratedMessages[messageId % params.GeneratedMessages.size()];
}

inline TString GenerateMetaKeyValue(ui64 messageId, const TTopicWorkloadWriterParams& params) {
    TString keyValue;
    if (params.KeyPrefix.Defined()) {
        TStringOutput so(keyValue);
        so << *params.KeyPrefix;
        if (params.KeyCount > 0) {
            so << '.' << ((messageId + params.KeySeed) % params.KeyCount);
        }
    }
    return keyValue;
}

inline NYdb::NTopic::TWriteMessage::TMessageMeta MakeKeyMeta(std::string_view key) {
    NYdb::NTopic::TWriteMessage::TMessageMeta meta;
    meta.emplace_back("__key", TString(key));
    return meta;
}

inline NYdb::NTopic::TWriteMessage::TMessageMeta GenerateOptionalKeyMeta(
    ui64 messageId,
    const TTopicWorkloadWriterParams& params
) {
    NYdb::NTopic::TWriteMessage::TMessageMeta meta;
    if (params.KeyPrefix.Defined()) {
        meta.emplace_back("__key", GenerateMetaKeyValue(messageId, params));
    }
    return meta;
}

} // namespace NYdb::NConsoleClient::NTopicWorkloadWriterInternal

