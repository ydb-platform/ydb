#pragma once

#include <ydb/public/sdk/cpp/src/library/kafka/kafka_records.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKafka::NTest {

inline void AssertKafkaBatchPayload(TStringBuf payload, size_t expectedRecordsCount, char expectedFill, size_t expectedDataSize) {
    const auto batch = ReadKafkaRecordBatch(payload);
    UNIT_ASSERT_VALUES_EQUAL(batch.Records.size(), expectedRecordsCount);

    for (const auto& record : batch.Records) {
        UNIT_ASSERT_C(record.Value.has_value(), "Kafka batch record has no value");
        UNIT_ASSERT_VALUES_EQUAL(record.Value->size(), expectedDataSize);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(record.Value->data(), record.Value->size()), TString(expectedDataSize, expectedFill));
    }
}

} // namespace NKafka::NTest
