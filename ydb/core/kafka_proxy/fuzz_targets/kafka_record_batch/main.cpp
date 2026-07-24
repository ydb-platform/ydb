#include <ydb/core/kafka_proxy/fuzz_targets/kafka_fuzz_common.h>

namespace {

using namespace NKafka;
using NKafka::NFuzz::TFuzzedDataProvider;

TKafkaRecordBatch BuildRecordBatch(TFuzzedDataProvider& fdp, NFuzz::TBytesStorage& bytesStorage) {
    TKafkaRecordBatch batch;
    batch.Magic = 2;
    batch.Attributes = 0;
    batch.BaseOffset = fdp.ConsumeIntegral<TKafkaInt64>();
    batch.LastOffsetDelta = fdp.ConsumeIntegral<TKafkaInt32>();
    batch.BaseTimestamp = fdp.ConsumeIntegral<TKafkaInt64>();
    batch.MaxTimestamp = fdp.ConsumeIntegral<TKafkaInt64>();
    batch.ProducerId = fdp.ConsumeIntegral<TKafkaInt64>();
    batch.ProducerEpoch = fdp.ConsumeIntegral<TKafkaInt16>();
    batch.BaseSequence = fdp.ConsumeIntegral<TKafkaInt32>();

    batch.Records.resize(NFuzz::ConsumeCount(fdp, 2));
    for (auto& record : batch.Records) {
        record.Length = fdp.ConsumeIntegral<TKafkaInt32>();
        record.Attributes = fdp.ConsumeIntegral<TKafkaInt8>();
        record.TimestampDelta = fdp.ConsumeIntegral<TKafkaInt64>();
        record.OffsetDelta = fdp.ConsumeIntegral<TKafkaInt32>();
        if (fdp.ConsumeBool()) {
            record.Key = bytesStorage.Hold(NFuzz::ConsumeString(fdp));
        }
        if (fdp.ConsumeBool()) {
            record.Value = bytesStorage.Hold(NFuzz::ConsumeString(fdp, 64));
        }
        record.Headers.resize(NFuzz::ConsumeCount(fdp, 2));
        for (auto& header : record.Headers) {
            header.Key = bytesStorage.Hold(NFuzz::ConsumeString(fdp));
            header.Value = bytesStorage.Hold(
                fdp.ConsumeBool() ? NFuzz::ConsumeString(fdp, 64) : TString());
        }
    }

    return batch;
}

TKafkaRecordBatchV0 BuildLegacyRecordBatch(TFuzzedDataProvider& fdp, NFuzz::TBytesStorage& bytesStorage, TKafkaVersion version) {
    TKafkaRecordBatchV0 batch;
    batch.Offset = fdp.ConsumeIntegral<TKafkaInt64>();
    batch.Record.MessageSize = fdp.ConsumeIntegral<TKafkaInt32>();
    batch.Record.Crc = fdp.ConsumeIntegral<TKafkaInt32>();
    batch.Record.Magic = version;
    batch.Record.Attributes = 0;
    if (version >= 1) {
        batch.Record.Timestamp = fdp.ConsumeIntegral<TKafkaInt64>();
    }
    if (fdp.ConsumeBool()) {
        batch.Record.Key = bytesStorage.Hold(NFuzz::ConsumeString(fdp));
    }
    batch.Record.Value = bytesStorage.Hold(NFuzz::ConsumeString(fdp, 64));
    return batch;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    TFuzzedDataProvider fdp(data, size);
    NFuzz::TBytesStorage bytesStorage;

    if (fdp.ConsumeBool()) {
        const TKafkaRecordBatch batch = BuildRecordBatch(fdp, bytesStorage);
        TString serialized;
        try {
            serialized = NFuzz::SerializeMessage(batch, 2);
        } catch (...) {
            return 0;
        }
        const TString truncated = NFuzz::MaybeTruncate(fdp, serialized);
        try {
            NFuzz::ParseMessage<TKafkaRecordBatch>(truncated, 2);
        } catch (...) {
        }
    } else {
        const TKafkaVersion version = fdp.ConsumeBool() ? 0 : 1;
        const TKafkaRecordBatchV0 batch = BuildLegacyRecordBatch(fdp, bytesStorage, version);
        TString serialized;
        try {
            serialized = NFuzz::SerializeMessage(batch, version);
        } catch (...) {
            return 0;
        }
        const TString truncated = NFuzz::MaybeTruncate(fdp, serialized);
        try {
            NFuzz::ParseMessage<TKafkaRecordBatchV0>(truncated, version);
        } catch (...) {
        }
    }

    return 0;
}
