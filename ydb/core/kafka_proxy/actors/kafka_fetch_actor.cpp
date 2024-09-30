#include <ydb/library/actors/core/actor.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/base/ticket_parser.h>
#include "ydb/core/kafka_proxy/kafka_metrics.h"
#include <ydb/core/persqueue/fetch_request_actor.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/user_info.h>
#include <ydb/core/persqueue/write_meta.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>

#include "actors.h"
#include "kafka_fetch_actor.h"


namespace NKafka {

static constexpr size_t SizeOfZeroVarint = 1;
static constexpr size_t BatchFirstTwoFieldsSize = 12;
static constexpr size_t KafkaMagic = 2;

NActors::IActor* CreateKafkaFetchActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TFetchRequestData>& message) {
    return new TKafkaFetchActor(context, correlationId, message);
}

void TKafkaFetchActor::Bootstrap(const NActors::TActorContext& ctx) {
    SendFetchRequests(ctx);
    Become(&TKafkaFetchActor::StateWork);
}

void TKafkaFetchActor::SendFetchRequests(const TActorContext& ctx) {
    Response->Responses.resize(FetchRequestData->Topics.size());
    KAFKA_LOG_D(TStringBuilder() << "Fetch actor: New request. DatabasePath: " << Context->DatabasePath << " MaxWaitMs: " << FetchRequestData->MaxWaitMs << " MaxBytes: " << FetchRequestData->MaxBytes);
    for (size_t topicIndex = 0; topicIndex <  Response->Responses.size(); topicIndex++) {
        TVector<NKikimr::NPQ::TPartitionFetchRequest> partPQRequests;
        PrepareFetchRequestData(topicIndex, partPQRequests);
        auto ruPerRequest = topicIndex == 0 && Context->Config.GetMeteringV2Enabled();
        NKikimr::NPQ::TFetchRequestSettings request(Context->DatabasePath, partPQRequests, FetchRequestData->MaxWaitMs, FetchRequestData->MaxBytes, Context->RlContext, *Context->UserToken, 0, ruPerRequest);
        auto fetchActor = NKikimr::NPQ::CreatePQFetchRequestActor(request, NKikimr::MakeSchemeCacheID(), ctx.SelfID);
        auto actorId = ctx.Register(fetchActor);
        PendingResponses++;

        TopicIndexes[actorId] = topicIndex;
    }
}

void TKafkaFetchActor::PrepareFetchRequestData(const size_t topicIndex, TVector<NKikimr::NPQ::TPartitionFetchRequest>& partPQRequests) {
    auto& topicKafkaRequest = FetchRequestData->Topics[topicIndex];
    TFetchResponseData::TFetchableTopicResponse& topicKafkaResponse = Response->Responses[topicIndex];
    topicKafkaResponse.Topic = topicKafkaRequest.Topic;

    partPQRequests.resize(topicKafkaRequest.Partitions.size());
    topicKafkaResponse.Partitions.resize(topicKafkaRequest.Partitions.size());
    for (size_t partIndex = 0; partIndex < topicKafkaRequest.Partitions.size(); partIndex++) {
        auto& partKafkaRequest = topicKafkaRequest.Partitions[partIndex];
        KAFKA_LOG_D(TStringBuilder() << "Fetch actor: New request. Topic: " << topicKafkaRequest.Topic.value() << " Partition: " << partKafkaRequest.Partition << " FetchOffset: " << partKafkaRequest.FetchOffset << " PartitionMaxBytes: " << partKafkaRequest.PartitionMaxBytes);
        auto& partPQRequest = partPQRequests[partIndex];
        partPQRequest.Topic = NormalizePath(Context->DatabasePath, topicKafkaRequest.Topic.value()); // FIXME(savnik): handle empty topic
        partPQRequest.Partition = partKafkaRequest.Partition;
        partPQRequest.Offset = partKafkaRequest.FetchOffset;
        partPQRequest.MaxBytes = partKafkaRequest.PartitionMaxBytes;
        partPQRequest.ClientId = Context->GroupId.Empty() ? NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER : Context->GroupId;
    }
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

        partKafkaResponse.PartitionIndex = partPQResponse.GetPartition();
        partKafkaResponse.ErrorCode = ConvertErrorCode(partPQResponse.GetReadResult().GetErrorCode());

        if (partPQResponse.GetReadResult().GetErrorCode() != NPersQueue::NErrorCode::EErrorCode::OK) {
            KAFKA_LOG_ERROR("Fetch actor: Failed to get responses for topic: " << topicResponse.Topic <<
                ", partition: " << partPQResponse.GetPartition() <<
                ". Code: " << static_cast<size_t>(partPQResponse.GetReadResult().GetErrorCode()) <<
                ". Reason: " + partPQResponse.GetReadResult().GetErrorReason());
        }

        partKafkaResponse.HighWatermark = partPQResponse.GetReadResult().GetMaxOffset();
        Response->ThrottleTimeMs = std::max(Response->ThrottleTimeMs, static_cast<i32>(partPQResponse.GetReadResult().GetWaitQuotaTimeMs()));
        if (partPQResponse.GetReadResult().GetResult().size() == 0) {
            continue;
        }

        auto& recordsBatch = partKafkaResponse.Records.emplace();
        FillRecordsBatch(partPQResponse, recordsBatch, ctx);
    }
}

void TKafkaFetchActor::FillRecordsBatch(const NKikimrClient::TPersQueueFetchResponse_TPartResult& partPQResponse, TKafkaRecordBatch& recordsBatch, const TActorContext& ctx) {
    recordsBatch.Records.resize(partPQResponse.GetReadResult().GetResult().size());

    ui64 baseOffset = 0;
    ui64 baseTimestamp = 0;
    ui64 baseSequense = 0;
    ui64 lastOffset = 0;
    ui64 lastTimestamp = 0;
    bool first = true;

    for (i32 recordIndex = 0; recordIndex < partPQResponse.GetReadResult().GetResult().size(); recordIndex++) {
        auto& result = partPQResponse.GetReadResult().GetResult()[recordIndex];
        if (first) {
            baseOffset = result.GetOffset();
            baseTimestamp = result.GetWriteTimestampMS();
            baseSequense = result.GetSeqNo();
            first = false;
        }

        lastOffset = result.GetOffset();
        lastTimestamp = result.GetWriteTimestampMS();
        auto& record = recordsBatch.Records[recordIndex];

        record.DataChunk = NKikimr::GetDeserializedData(result.GetData());
        if (record.DataChunk.GetChunkType() != NKikimrPQClient::TDataChunk::REGULAR) {
            continue;
        }

        for (auto& metadata : record.DataChunk.GetMessageMeta()) {
            if (metadata.key() == "__key") {
                record.Key = metadata.value();
            } else {
                TKafkaHeader header;
                header.Key = metadata.key();
                header.Value = metadata.value();
                record.Headers.push_back(header);
            }
        }

        record.Value = record.DataChunk.GetData();
        record.OffsetDelta = lastOffset - baseOffset;
        record.TimestampDelta = lastTimestamp - baseTimestamp;

        record.Length = record.Size(TKafkaRecord::MessageMeta::PresentVersions.Max) - SizeOfZeroVarint;
        KAFKA_LOG_D("Fetch actor: Record info. OffsetDelta: " << record.OffsetDelta <<
            ", TimestampDelta: " << record.TimestampDelta << ", Length: " << record.Length);
    }

    recordsBatch.Magic = KafkaMagic;
    recordsBatch.BaseOffset = baseOffset;
    recordsBatch.LastOffsetDelta = lastOffset - baseOffset;
    recordsBatch.BaseTimestamp = baseTimestamp;
    recordsBatch.MaxTimestamp = lastTimestamp;
    recordsBatch.BaseSequence = baseSequense;
    //recordsBatch.Attributes https://kafka.apache.org/documentation/#recordbatch

    recordsBatch.BatchLength = recordsBatch.Size(TKafkaRecordBatch::MessageMeta::PresentVersions.Max) - BatchFirstTwoFieldsSize;
    KAFKA_LOG_D("Fetch actor: RecordBatch info. BaseOffset: " << recordsBatch.BaseOffset << ", LastOffsetDelta: " << recordsBatch.LastOffsetDelta <<
        ", BaseTimestamp: " << recordsBatch.BaseTimestamp << ", MaxTimestamp: " << recordsBatch.MaxTimestamp <<
        ", BaseSequence: " << recordsBatch.BaseSequence << ", BatchLength: " << recordsBatch.BatchLength);
    auto topicWithoutDb = GetTopicNameWithoutDb(Context->DatabasePath, partPQResponse.GetTopic());
    ctx.Send(MakeKafkaMetricsServiceID(), new TEvKafka::TEvUpdateCounter(recordsBatch.Records.size(), BuildLabels(Context, "", topicWithoutDb, "api.kafka.fetch.messages", "")));
}

void TKafkaFetchActor::RespondIfRequired(const TActorContext& ctx) {
    if (PendingResponses == 0) {
        Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, Response, ErrorCode));
        Die(ctx);
    }
}


} // namespace NKafka
