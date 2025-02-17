#include "helpers.h"

#include <library/cpp/protobuf/util/repeated_field_utils.h>

namespace NKikimr::NGRpcProxy::V1 {

bool HasMessages(const PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch& data) {
    for (const auto& partData : data.partition_data()) {
        for (const auto& batch : partData.batches()) {
            if (batch.message_data_size() > 0) {
                return true;
            }
        }
    }
    return false;
}

// TODO: remove after refactor
bool HasMessages(const Topic::StreamReadMessage::ReadResponse& data) {
    for (const auto& partData : data.partition_data()) {
        for (const auto& batch : partData.batches()) {
            if (batch.message_data_size() > 0) {
                return true;
            }
        }
    }
    return false;
}


TString CleanupCounterValueString(const TString& value) {
    // Internal Monitoring system requires metrics values to be no longer than 200 characters
    // and prohibits some ASCII characters.

    TString clean;
    constexpr auto valueLenghtLimit = 200;

    for (auto c : value) {
        switch (c) {
        case '|':
        case '*':
        case '?':
        case '"':
        case '\'':
        case '`':
        case '\\':
            continue;
        default:
            clean.push_back(c);
            if (clean.size() == valueLenghtLimit) {
                break;
            }
        }
    }
    return clean;
}


TString DropUserAgentSuffix(const TString& userAgent) {
    auto ua = TStringBuf(userAgent);
    TStringBuf beforeParen, afterParen;
    ua.Split('(', beforeParen, afterParen);
    while (beforeParen.ends_with(' ')) {
        beforeParen.Chop(1);
    }
    return TString(beforeParen);
}

}
