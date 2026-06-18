#include <ydb/library/actors/core/actor.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/base/ticket_parser.h>
#include "ydb/core/kafka_proxy/kafka_metrics.h"
#include "ydb/core/kafka_proxy/kafka_constants.h"
#include <ydb/core/persqueue/public/fetcher/fetch_request_actor.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/write_meta/write_meta.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <ydb/public/api/protos/draft/persqueue_common.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>

#include <ydb/library/kafka/kafka.h>
#include <ydb/library/kafka/kafka_records.h>

#include "actors.h"
#include "kafka_fetch_actor.h"


namespace NKafka {

static constexpr size_t SizeOfZeroVarint = 1;
static constexpr size_t BatchFirstTwoFieldsSize = 12;
static constexpr size_t KafkaMagic = 2;

NPersQueueCommon::ECodec KafkaBatchCodec() {
    return static_cast<NPersQueueCommon::ECodec>(static_cast<int>(Ydb::Topic::CODEC_KAFKA_BATCH) - 1);
}

void FillRecordFromDataChunk(TKafkaRecord& record, const NKikimrPQClient::TDataChunk& dataChunk) {
    for (const auto& metadata : dataChunk.GetMessageMeta()) {
        if (metadata.key() == "__key") {
            record.SetKey(metadata.value());
        } else {
            record.AddHeader(metadata.key(), metadata.value());
        }
    }

    if (dataChunk.HasData()) {
        record.SetValue(dataChunk.GetData());
    }
}

NActors::IActor* CreateKafkaFetchActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TFetchRequestData>& message) {
    return new TKafkaFetchActor(context, correlationId, message);
}

void TKafkaFetchActor::Bootstrap(const NActors::TActorContext& ctx) {
    SendFetchRequests(ctx);
    Become(&TKafkaFetchActor::StateWork);
}

void TKafkaFetchActor::SendFetchRequests(const TActorContext& ctx) {
    Response->Responses.resize(FetchRequestData->Topics.size());
    KAFKA_LOG_D(TStringBuilder() << "Fetch actor: New request. DatabasePath: " << Context->DatabasePath
        << " MaxWaitMs: " << FetchRequestData->MaxWaitMs << " MaxBytes: " << FetchRequestData->MaxBytes);
    for (size_t topicIndex = 0; topicIndex <  Response->Responses.size(); topicIndex++) {
        auto partPQRequests = PrepareFetchRequestData(topicIndex);
        auto ruPerRequest = topicIndex == 0 && Context->Config.GetMeteringV2Enabled();
        auto consumer = Context->GroupId.empty() ? NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER : Context->GroupId;
        NKikimr::NPQ::TFetchRequestSettings request {
            .Database = Context->DatabasePath,
            .Consumer = consumer,
            .Partitions = partPQRequests,
            .MaxWaitTimeMs = FetchRequestData->MaxWaitMs <= 0 ? 0u : FetchRequestData->MaxWaitMs,
            .TotalMaxBytes = FetchRequestData->MaxBytes <= 0 ? 8_MB : FetchRequestData->MaxBytes,
            .RuPerRequest = ruPerRequest,
            .CanReadBatches = true,
            .RequestId = 0,
            .RlCtx = Context->RlContext,
            .UserToken = Context->UserToken
        };
        auto fetchActor = NKikimr::NPQ::CreatePQFetchRequestActor(request, NKikimr::MakeSchemeCacheID(), ctx.SelfID);
        auto actorId = ctx.Register(fetchActor);
        PendingResponses++;

        TopicIndexes[actorId] = topicIndex;
    }
}

TVector<NKikimr::NPQ::TPartitionFetchRequest> TKafkaFetchActor::PrepareFetchRequestData(const size_t topicIndex) {
    auto& topicKafkaRequest = FetchRequestData->Topics[topicIndex];
    TFetchResponseData::TFetchableTopicResponse& topicKafkaResponse = Response->Responses[topicIndex];

    topicKafkaResponse.Topic = topicKafkaRequest.Topic;

    TVector<NKikimr::NPQ::TPartitionFetchRequest> partPQRequests;
    partPQRequests.resize(topicKafkaRequest.Partitions.size());
    topicKafkaResponse.Partitions.resize(topicKafkaRequest.Partitions.size());
    for (size_t partIndex = 0; partIndex < topicKafkaRequest.Partitions.size(); partIndex++) {
        auto& partKafkaRequest = topicKafkaRequest.Partitions[partIndex];
        KAFKA_LOG_D(TStringBuilder() << "Fetch actor: New request. Topic: " << topicKafkaRequest.Topic.value()
            << " Partition: " << partKafkaRequest.Partition
            << " FetchOffset: " << partKafkaRequest.FetchOffset
            << " PartitionMaxBytes: " << partKafkaRequest.PartitionMaxBytes);
        auto& partPQRequest = partPQRequests[partIndex];
        partPQRequest.Topic = NormalizePath(Context->DatabasePath, topicKafkaRequest.Topic.value()); // FIXME(savnik): handle empty topic
        partPQRequest.Partition = partKafkaRequest.Partition;
        partPQRequest.Offset = partKafkaRequest.FetchOffset;
        partPQRequest.MaxBytes = partKafkaRequest.PartitionMaxBytes;
    }
    return partPQRequests;
}

void TKafkaFetchActor::Handle(NKikimr::TEvPQ::TEvFetchResponse::TPtr& ev, const TActorContext& ctx) {
    --PendingResponses;

    size_t topicIndex = CheckTopicIndex(ev);
    if (topicIndex == std::numeric_limits<size_t>::max()) {
        return RespondIfRequired(ctx);
    }

    auto& topicResponse = Response->Responses[topicIndex];

    if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
        HandleErrorResponse(ev, topicResponse);
    } else {
        HandleSuccessResponse(ev, topicResponse, ctx);
    }
    RespondIfRequired(ctx);
}

size_t TKafkaFetchActor::CheckTopicIndex(const NKikimr::TEvPQ::TEvFetchResponse::TPtr& ev) {
    auto topicIt = TopicIndexes.find(ev->Sender);
    Y_DEBUG_ABORT_UNLESS(topicIt != TopicIndexes.end());

    if (topicIt == TopicIndexes.end()) {
        KAFKA_LOG_ERROR("Fetch actor: Received unexpected TEvFetchResponse. Ignoring. Expect malformed/incompled fetch reply.");
        return std::numeric_limits<size_t>::max();
    }

    return topicIt->second;
}

void TKafkaFetchActor::HandleErrorResponse(const NKikimr::TEvPQ::TEvFetchResponse::TPtr& ev, TFetchResponseData::TFetchableTopicResponse& topicResponse) {
    const auto code = ConvertErrorCode(ev->Get()->Status);

    for (auto& partitionResponse : topicResponse.Partitions) {
        partitionResponse.ErrorCode = code;
    }
    KAFKA_LOG_ERROR("Fetch actor: Failed to get responses for topic: " << topicResponse.Topic << ". Code: "  << ev->Get()->Status << ". Reason: " << ev->Get()->Message);
}

void TKafkaFetchActor::HandleSuccessResponse(const NKikimr::TEvPQ::TEvFetchResponse::TPtr& ev, TFetchResponseData::TFetchableTopicResponse& topicResponse, const TActorContext& ctx) {
    for (i32 partitionIndex = 0; partitionIndex < ev->Get()->Response.GetPartResult().size(); partitionIndex++) {
        auto partPQResponse = ev->Get()->Response.GetPartResult()[partitionIndex];
        auto& partKafkaResponse = topicResponse.Partitions[partitionIndex];
        std::optional<TString> timestampType;
        if (ev->Get()->Response.HasTimestampType()) {
            timestampType = ev->Get()->Response.GetTimestampType();
        }

        partKafkaResponse.PartitionIndex = partPQResponse.GetPartition();
        partKafkaResponse.ErrorCode = ConvertErrorCode(partPQResponse.GetReadResult().GetErrorCode());

        if (partPQResponse.GetReadResult().GetErrorCode() != NPersQueue::NErrorCode::EErrorCode::OK) {
            KAFKA_LOG_D("Fetch actor: Failed to get responses for topic: " << topicResponse.Topic <<
                ", partition: " << partPQResponse.GetPartition() <<
                ". Code: " << static_cast<size_t>(partPQResponse.GetReadResult().GetErrorCode()) <<
                ". Reason: " + partPQResponse.GetReadResult().GetErrorReason());
        }

        partKafkaResponse.HighWatermark = partPQResponse.GetReadResult().GetMaxOffset();
        partKafkaResponse.LastStableOffset = partPQResponse.GetReadResult().GetMaxOffset();
        Response->ThrottleTimeMs = std::max(Response->ThrottleTimeMs, static_cast<i32>(partPQResponse.GetReadResult().GetWaitQuotaTimeMs()));
        if (partPQResponse.GetReadResult().GetResult().size() == 0) {
            continue;
        }

        FillRecordsBatch(partPQResponse, partKafkaResponse.Records, timestampType, ctx);
    }
}

void TKafkaFetchActor::FillRecordsBatch(const NKikimrClient::TPersQueueFetchResponse_TPartResult& partPQResponse,
                                        TKafkaBytesHolder& records,
                                        const std::optional<TString> timestampType,
                                        const TActorContext& ctx) {
    ui64 messagesCount = 0;
    bool allResultsAreKafkaBatches = partPQResponse.GetReadResult().GetResult().size() > 0;

    for (const auto& result : partPQResponse.GetReadResult().GetResult()) {
        const auto dataChunk = NKikimr::GetDeserializedData(result.GetData());
        const bool isKafkaBatch = dataChunk.GetChunkType() == NKikimrPQClient::TDataChunk::REGULAR &&
            dataChunk.GetCodec() == KafkaBatchCodec();

        KAFKA_LOG_D("Fetch actor: Kafka record candidate. ResultsCount=" << partPQResponse.GetReadResult().GetResult().size()
            << ", Offset=" << result.GetOffset()
            << ", IsBatch=" << result.GetIsBatch()
            << ", MessageCount=" << result.GetMessageCount()
            << ", ChunkType=" << static_cast<int>(dataChunk.GetChunkType())
            << ", Codec=" << (dataChunk.HasCodec() ? static_cast<int>(dataChunk.GetCodec()) : -1)
            << ", KafkaBatchCodec=" << static_cast<int>(KafkaBatchCodec())
            << ", DataSize=" << dataChunk.GetData().size()
            << ", IsKafkaBatch=" << isKafkaBatch);

        if (!isKafkaBatch) {
            allResultsAreKafkaBatches = false;
        }
    }

    if (allResultsAreKafkaBatches) {
        TString kafkaBatchRecords;
        for (const auto& result : partPQResponse.GetReadResult().GetResult()) {
            const auto dataChunk = NKikimr::GetDeserializedData(result.GetData());
            kafkaBatchRecords += dataChunk.GetData();
            messagesCount += result.GetMessageCount();
        }
        auto topicWithoutDb = GetTopicNameWithoutDb(Context->DatabasePath, partPQResponse.GetTopic());
        ctx.Send(MakeKafkaMetricsServiceID(), new TEvKafka::TEvUpdateCounter(messagesCount, BuildLabels(Context, "", topicWithoutDb, "api.kafka.fetch.messages", "")));
        records = std::move(kafkaBatchRecords);
        return;
    }

    TKafkaRecordBatch recordsBatch;

    ui64 baseOffset = 0;
    ui64 baseTimestamp = 0;
    ui64 baseSequense = 0;
    ui64 lastOffset = 0;
    ui64 maxTimestamp = 0;
    bool first = true;
    messagesCount = 0;

    auto addRecord = [&](TKafkaRecord record, ui64 offset, ui64 timestamp, ui64 seqNo) {
        if (first) {
            baseOffset = offset;
            baseTimestamp = timestamp;
            baseSequense = seqNo;
            first = false;
        }

        record.OffsetDelta = offset - baseOffset;
        record.TimestampDelta = timestamp - baseTimestamp;
        record.Length = record.Size(TKafkaRecord::MessageMeta::PresentVersions.Max) - SizeOfZeroVarint;

        lastOffset = offset;
        maxTimestamp = std::max(maxTimestamp, timestamp);
        recordsBatch.Records.push_back(std::move(record));
        ++messagesCount;
    };

    for (const auto& result : partPQResponse.GetReadResult().GetResult()) {
        const auto dataChunk = NKikimr::GetDeserializedData(result.GetData());
        if (dataChunk.GetChunkType() != NKikimrPQClient::TDataChunk::REGULAR) {
            continue;
        }

        const bool isKafkaBatch = dataChunk.HasCodec() && dataChunk.GetCodec() == KafkaBatchCodec();
        if (isKafkaBatch) {
            auto batch = ReadRecordBatch(dataChunk.GetData());
            for (ui64 recordIndex = 0; recordIndex < batch.Records.size(); ++recordIndex) {
                auto& record = batch.Records[recordIndex];
                const ui64 offset = result.GetOffset() + record.OffsetDelta;
                const ui64 timestamp = timestampType == MESSAGE_TIMESTAMP_LOG_APPEND
                    ? result.GetWriteTimestampMS()
                    : batch.BaseTimestamp + record.TimestampDelta;
                addRecord(std::move(record), offset, timestamp, result.GetSeqNo() + recordIndex);
            }
            continue;
        }

        auto fillTimestamp = [&timestampType, &result]() {
            if (timestampType == MESSAGE_TIMESTAMP_LOG_APPEND) {
                return result.GetWriteTimestampMS();
            }
            return result.GetCreateTimestampMS();
        };

        const ui64 baseOffset = result.GetOffset();
        const ui64 baseTimestamp = fillTimestamp();
        const ui64 baseSequense = result.GetSeqNo();

        TKafkaRecord record;

        FillRecordFromDataChunk(record, dataChunk);

        NYdb::NTopic::ECodec codec = static_cast<NYdb::NTopic::ECodec>(dataChunk.GetCodec() + 1);
        TString codecValue;
        switch (codec) {
            case NYdb::NTopic::ECodec::RAW:
                codecValue = "RAW";
                break;
            case NYdb::NTopic::ECodec::GZIP:
                codecValue = "GZIP";
                break;
            case NYdb::NTopic::ECodec::LZOP:
                codecValue = "LZOP";
                break;
            case NYdb::NTopic::ECodec::ZSTD:
                codecValue = "ZSTD";
                break;
            default:
                codecValue = std::to_string(static_cast<uint32_t>(codec));
        }

        record.AddHeader("__codec", std::move(codecValue));

        addRecord(std::move(record), baseOffset, baseTimestamp, baseSequense);
    }

    recordsBatch.Magic = KafkaMagic;
    recordsBatch.BaseOffset = baseOffset;
    recordsBatch.LastOffsetDelta = lastOffset - baseOffset;
    recordsBatch.BaseTimestamp = baseTimestamp;
    recordsBatch.MaxTimestamp = maxTimestamp;
    recordsBatch.BaseSequence = baseSequense;
    //recordsBatch.Attributes https://kafka.apache.org/documentation/#recordbatch

    recordsBatch.BatchLength = recordsBatch.Size(TKafkaRecordBatch::MessageMeta::PresentVersions.Max) - BatchFirstTwoFieldsSize;
    KAFKA_LOG_D("Fetch actor: RecordBatch info. BaseOffset: " << recordsBatch.BaseOffset << ", LastOffsetDelta: " << recordsBatch.LastOffsetDelta <<
        ", BaseTimestamp: " << recordsBatch.BaseTimestamp << ", MaxTimestamp: " << recordsBatch.MaxTimestamp <<
        ", BaseSequence: " << recordsBatch.BaseSequence << ", BatchLength: " << recordsBatch.BatchLength);
    auto topicWithoutDb = GetTopicNameWithoutDb(Context->DatabasePath, partPQResponse.GetTopic());
    ctx.Send(MakeKafkaMetricsServiceID(), new TEvKafka::TEvUpdateCounter(messagesCount, BuildLabels(Context, "", topicWithoutDb, "api.kafka.fetch.messages", "")));
    records = WriteKafkaRecordBatch(recordsBatch);
}

void TKafkaFetchActor::RespondIfRequired(const TActorContext& ctx) {
    if (PendingResponses == 0) {
        Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, Response, ErrorCode));
        Die(ctx);
    }
}

void TKafkaFetchActor::Handle(TEvKafka::TEvFetchActorStateRequest::TPtr& ev) {
    Send(ev->Sender, new TEvKafka::TEvFetchActorStateResponse(TopicIndexes));
}

} // namespace NKafka
