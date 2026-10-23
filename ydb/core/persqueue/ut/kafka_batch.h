#pragma once

#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NPQ {

TString MakeKafkaBatchPayload(
    const TVector<TString>& values,
    ui64 baseSequence = 1,
    NKafka::ECompressionType compression = NKafka::ECompressionType::NONE,
    const TVector<TString>& keys = {});

// MakeKafkaBatchPayload in a serialized TDataChunk with the KAFKA_BATCH codec: the CmdWrite data of a Kafka batch
TString MakeKafkaBatchData(
    const TVector<TString>& values,
    ui64 baseSequence = 1,
    NKafka::ECompressionType compression = NKafka::ECompressionType::NONE,
    const TVector<TString>& keys = {});

void CmdWriteKafkaBatch(
    const ui32 partition,
    const TString& sourceId,
    ui64 seqNo,
    const TVector<TString>& values,
    TTestContext& tc,
    i64 offset = -1,
    NKafka::ECompressionType batchCompression = NKafka::ECompressionType::NONE,
    const TVector<TString>& keys = {});

} // namespace NKikimr::NPQ
