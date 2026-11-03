#include "kafka_batch.h"

#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_messages_int.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_records.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

TString MakeKafkaBatchPayload(
    const TVector<TString>& values,
    ui64 baseSequence,
    NKafka::ECompressionType compression,
    const TVector<TString>& keys)
{
    UNIT_ASSERT(keys.empty() || keys.size() == values.size());

    NKafka::TKafkaRecordBatch batch;
    batch.BaseOffset = 0;
    batch.Magic = 2;
    batch.Attributes = static_cast<NKafka::TKafkaRecordBatch::AttributesMeta::Type>(compression);
    batch.LastOffsetDelta = values.size() - 1;
    batch.BaseTimestamp = 1000;
    batch.MaxTimestamp = 1000 + values.size() - 1;
    batch.ProducerId = 42;
    batch.ProducerEpoch = 0;
    batch.BaseSequence = baseSequence;

    batch.Records.reserve(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        NKafka::TKafkaRecord record;
        record.TimestampDelta = i;
        record.OffsetDelta = i;
        if (!keys.empty()) {
            record.SetKey(TString{keys[i]});
        }
        record.SetValue(TString{values[i]});
        record.Length = record.Size(2)
            - NKafka::NPrivate::SizeOfVarint<NKafka::TKafkaRecord::LengthMeta::Type>(0);
        batch.Records.push_back(std::move(record));
    }
    batch.BatchLength = batch.Size(2) - sizeof(NKafka::TKafkaRecordBatch::BaseOffsetMeta::Type) - sizeof(NKafka::TKafkaRecordBatch::BatchLengthMeta::Type);
    return NKafka::WriteKafkaRecordBatch(batch);
}

TString MakeKafkaBatchData(
    const TVector<TString>& values,
    ui64 baseSequence,
    NKafka::ECompressionType compression,
    const TVector<TString>& keys)
{
    NKikimrPQClient::TDataChunk dataChunk;
    dataChunk.SetChunkType(NKikimrPQClient::TDataChunk::REGULAR);
    dataChunk.SetCodec(static_cast<NPersQueueCommon::ECodec>(static_cast<int>(Ydb::Topic::CODEC_KAFKA_BATCH) - 1));
    dataChunk.SetData(MakeKafkaBatchPayload(values, baseSequence, compression, keys));
    TString serializedDataChunk;
    Y_ENSURE(dataChunk.SerializeToString(&serializedDataChunk));
    return serializedDataChunk;
}

void CmdWriteKafkaBatch(
    const ui32 partition,
    const TString& sourceId,
    ui64 seqNo,
    const TVector<TString>& values,
    TTestContext& tc,
    i64 offset,
    NKafka::ECompressionType batchCompression,
    const TVector<TString>& keys)
{
    TAutoPtr<IEventHandle> handle;
    TEvPersQueue::TEvResponse* result = nullptr;
    ui32& msgSeqNo = tc.MsgSeqNoMap[partition];
    TString& cookie = tc.OwnerCookieMap[partition];

    for (i32 retriesLeft = 2; retriesLeft > 0; --retriesLeft) {
        try {
            THolder<TEvPersQueue::TEvRequest> request(new TEvPersQueue::TEvRequest);
            tc.Runtime->ResetScheduledCount();
            auto* req = request->Record.MutablePartitionRequest();
            req->SetPartition(partition);
            req->SetOwnerCookie(cookie);
            req->SetMessageNo(msgSeqNo);
            if (offset >= 0) {
                req->SetCmdWriteOffset(offset);
            }

            auto* write = req->AddCmdWrite();
            write->SetSourceId(sourceId);
            write->SetSeqNo(seqNo);
            write->SetData(MakeKafkaBatchData(values, seqNo, batchCompression, keys));
            write->SetLogicalMessageCount(values.size());
            write->SetIsBatch(true);
            write->SetMaxSeqNo(seqNo + values.size() - 1);

            tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
            result = tc.Runtime->GrabEdgeEventIf<TEvPersQueue::TEvResponse>(handle,
                [](const TEvPersQueue::TEvResponse& ev) {
                    return ev.Record.HasPartitionResponse()
                        && ev.Record.GetPartitionResponse().CmdWriteResultSize() > 0
                        || ev.Record.GetErrorCode() != NPersQueue::NErrorCode::OK;
                });

            UNIT_ASSERT(result);
            if (result->Record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                tc.Runtime->DispatchEvents();
                retriesLeft = 3;
                continue;
            }
            if (result->Record.GetErrorCode() == NPersQueue::NErrorCode::WRONG_COOKIE) {
                cookie = CmdSetOwner(tc.Runtime.Get(), tc.TabletId, tc.Edge, partition).first;
                msgSeqNo = 0;
                retriesLeft = 3;
                continue;
            }

            UNIT_ASSERT_VALUES_EQUAL_C(
                static_cast<ui32>(result->Record.GetErrorCode()),
                static_cast<ui32>(NPersQueue::NErrorCode::OK),
                result->Record.DebugString());
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetPartitionResponse().CmdWriteResultSize(), 1u);
            retriesLeft = 0;
        } catch (NActors::TSchedulingLimitReachedException) {
            UNIT_ASSERT_VALUES_EQUAL(retriesLeft, 2);
            retriesLeft = 3;
        }
    }
    ++msgSeqNo;
}

} // namespace NKikimr::NPQ
