#pragma once

#include <ydb/core/testlib/test_client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_session.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/datetime/base.h>

#include <tuple>

namespace NKikimr::NPQ::NTest {

void EnableTopicBatching(Tests::TServerSettings& settings);

// Writes groups of messages as kafka batches: one tuple is {first seqno, message count, payload fill char}.
void WriteKafkaBatchMessages(
    NYdb::NTopic::TTopicClient& client,
    const TString& topicPath,
    const TString& producerId,
    size_t dataSize,
    ui64 maxBatchMessageCount,
    const TVector<std::tuple<ui64, ui32, char>>& writes,
    bool directWriteToPartition = true,
    TInstant baseCreateTimestamp = TInstant::MilliSeconds(1000));

} // namespace NKikimr::NPQ::NTest
