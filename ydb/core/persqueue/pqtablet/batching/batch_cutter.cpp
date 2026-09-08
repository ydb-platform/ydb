#include "batch_cutter.h"

#include <ydb/core/persqueue/public/codecs/kafka.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_records.h>
#include <ydb/public/api/protos/draft/persqueue_common.pb.h>

#include <library/cpp/streams/zstd/zstd.h>

#include <exception>

#include <util/stream/output.h>
#include <util/stream/zlib.h>
#include <util/string/builder.h>

namespace NKikimr::NPQ::NBatching {
namespace {

NPersQueueCommon::ECodec ToDataChunkCodec(NKafka::ECompressionType compressionType) {
    switch (compressionType) {
        case NKafka::ECompressionType::GZIP:
            return NPersQueueCommon::GZIP;
        case NKafka::ECompressionType::ZSTD:
            return NPersQueueCommon::ZSTD;
        default:
            return NPersQueueCommon::RAW;
    }
}

TString CompressPayload(TStringBuf data, NPersQueueCommon::ECodec codec) {
    TString result;
    TStringOutput output(result);
    switch (codec) {
        case NPersQueueCommon::GZIP: {
            TZLibCompress gzip(&output, ZLib::GZip);
            gzip.Write(data.data(), data.size());
            gzip.Finish();
            output.Finish();
            return result;
        }
        case NPersQueueCommon::ZSTD: {
            TZstdCompress zstd(&output);
            zstd.Write(data.data(), data.size());
            zstd.Finish();
            output.Finish();
            return result;
        }
        default:
            return TString(data);
    }
}

TString UnexpectedCodecError(const TBatchCutterData& data) {
    const auto& dataChunk = data.DataChunk;
    if (dataChunk.HasCodec() && dataChunk.GetCodec() == KafkaBatchCodec()) {
        return {};
    }
    return TStringBuilder() << "unexpected data chunk codec for kafka batch cutter"
        << " has_codec=" << dataChunk.HasCodec()
        << " codec=" << (dataChunk.HasCodec() ? static_cast<int>(dataChunk.GetCodec()) : -1)
        << " expected_codec=" << static_cast<int>(KafkaBatchCodec())
        << " offset=" << data.ReadResult.GetOffset();
}

std::expected<ui64, TString> TryRecordOffset(ui64 baseOffset, i64 offsetDelta, ui64 parentOffset) {
    // Kafka encodes offsetDelta as a signed varint, but a valid RecordBatch uses
    // non-negative deltas (0, 1, ...). A negative or overflowing value is corrupt client data.
    if (offsetDelta < 0) {
        return std::unexpected(TStringBuilder() << "negative kafka record offset delta"
            << " offset_delta=" << offsetDelta
            << " base_offset=" << baseOffset
            << " offset=" << parentOffset);
    }
    const ui64 offset = baseOffset + static_cast<ui64>(offsetDelta);
    if (offset < baseOffset) {
        return std::unexpected(TStringBuilder() << "kafka record offset overflow"
            << " offset_delta=" << offsetDelta
            << " base_offset=" << baseOffset
            << " offset=" << parentOffset);
    }
    return offset;
}

} // namespace

std::expected<TVector<TReadResult>, TString> TKafkaBatchCutter::Cut(const TBatchCutterData& data, const ui64 readStartOffset) const {
    const auto& dataChunk = data.DataChunk;
    if (dataChunk.GetChunkType() != NKikimrPQClient::TDataChunk::REGULAR) {
        return TVector<TReadResult>{data.ReadResult};
    }

    if (TString error = UnexpectedCodecError(data); !error.empty()) {
        return std::unexpected(std::move(error));
    }

    try {
        const auto batch = NKafka::ReadKafkaRecordBatch(dataChunk.GetData());
        if (batch.Records.empty()) {
            return TVector<TReadResult>{data.ReadResult};
        }

        const auto codec = ToDataChunkCodec(batch.CompressionType());

        TVector<TReadResult> result;
        result.reserve(batch.Records.size());

        TReadResult itemTemplate(data.ReadResult);
        itemTemplate.ClearData();
        itemTemplate.SetLogicalMessageCount(1);
        itemTemplate.SetIsBatch(false);
        itemTemplate.ClearUncompressedSize();

        NKikimrPQClient::TDataChunk itemChunk(dataChunk);
        itemChunk.ClearData();
        itemChunk.SetCodec(codec);

        const ui64 baseOffset = data.ReadResult.GetOffset();
        for (size_t i = 0; i < batch.Records.size(); ++i) {
            auto offset = TryRecordOffset(baseOffset, batch.Records[i].OffsetDelta, data.ReadResult.GetOffset());
            if (!offset) {
                return std::unexpected(std::move(offset).error());
            }
            if (*offset < readStartOffset) {
                continue;
            }

            const auto& record = batch.Records[i];
            const ui64 seqNo = NKafka::GetRecordSeqNo(batch, i, record);

            TReadResult item(itemTemplate);
            item.SetOffset(*offset);
            item.SetSeqNo(seqNo);

            itemChunk.SetSeqNo(seqNo);
            if (record.Value) {
                itemChunk.SetData(CompressPayload(TStringBuf(record.Value->data(), record.Value->size()), codec));
            } else {
                itemChunk.ClearData();
            }
            TString serializedChunk;
            Y_PROTOBUF_SUPPRESS_NODISCARD itemChunk.SerializeToString(&serializedChunk);
            item.SetData(std::move(serializedChunk));

            if (record.Key) {
                item.SetPartitionKey(TString(record.Key->data(), record.Key->size()));
            }
            const i64 timestamp = batch.BaseTimestamp + record.TimestampDelta;
            if (timestamp > 0) {
                item.SetCreateTimestampMS(timestamp);
            }
            result.push_back(std::move(item));
        }

        return result;
    } catch (const std::exception& e) {
        return std::unexpected(TString(e.what()));
    }
}

std::expected<THashMap<TString, ui64>, TString> TKafkaBatchCutter::GetKeys(const TBatchCutterData& data, const ui64 readStartOffset) const {
    const auto& dataChunk = data.DataChunk;
    if (dataChunk.GetChunkType() != NKikimrPQClient::TDataChunk::REGULAR) {
        return THashMap<TString, ui64>{};
    }

    if (TString error = UnexpectedCodecError(data); !error.empty()) {
        return std::unexpected(std::move(error));
    }

    try {
        const auto batch = NKafka::ReadKafkaRecordBatch(dataChunk.GetData());
        const ui64 baseOffset = data.ReadResult.GetOffset();
        THashMap<TString, ui64> result;
        for (const auto& record : batch.Records) {
            auto offset = TryRecordOffset(baseOffset, record.OffsetDelta, data.ReadResult.GetOffset());
            if (!offset) {
                return std::unexpected(std::move(offset).error());
            }
            if (*offset < readStartOffset) {
                continue;
            }

            if (!record.Key) {
                continue;
            }

            TString key;
            key.assign(record.Key->data(), record.Key->size());
            result[key] = *offset;
        }

        return result;
    } catch (const std::exception& e) {
        return std::unexpected(TString(e.what()));
    }
}

} // namespace NKikimr::NPQ::NBatching
