#pragma once

#include "fill_batched_data_offset.h"
#include "helpers.h"
#include "partition_id.h"
#include "persqueue_utils.h"

#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/public/codecs/pqv1.h>
#include <ydb/core/persqueue/public/write_meta/write_meta.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/protos/msgbus_pq.pb.h>

#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>

#include <google/protobuf/util/time_util.h>
#include <util/charset/utf8.h>

#ifndef YDB_LOG_THIS_FILE_COMPONENT
#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PQ_READ_PROXY
#endif

namespace NKikimr::NGRpcProxy::V1 {

using namespace PersQueue::V1;
using namespace Topic;

inline i64 GetBatchWriteTimestampMS(PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::Batch* batch) {
    return static_cast<i64>(batch->write_timestamp_ms());
}
inline i64 GetBatchWriteTimestampMS(Topic::StreamReadMessage::ReadResponse::Batch* batch) {
    return ::google::protobuf::util::TimeUtil::TimestampToMilliseconds(batch->written_at());
}

inline void SetBatchWriteTimestampMS(PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::Batch* batch, i64 value) {
    batch->set_write_timestamp_ms(value);
}
inline void SetBatchWriteTimestampMS(Topic::StreamReadMessage::ReadResponse::Batch* batch, i64 value) {
    *batch->mutable_written_at() = ::google::protobuf::util::TimeUtil::MillisecondsToTimestamp(value);
}

inline TString GetBatchSourceId(PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::Batch* batch) {
    AFL_ENSURE(batch);
    return batch->source_id();
}

inline TString GetBatchSourceId(Topic::StreamReadMessage::ReadResponse::Batch* batch) {
    AFL_ENSURE(batch);
    return batch->producer_id();
}

inline void SetBatchExtraField(PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::Batch* batch, TString key, TString value) {
    AFL_ENSURE(batch)("key", key);
    auto* item = batch->add_extra_fields();
    item->set_key(std::move(key));
    item->set_value(std::move(value));
}

inline void SetBatchExtraField(Topic::StreamReadMessage::ReadResponse::Batch* batch, TString key, TString value) {
    AFL_ENSURE(batch)("key", key);
    (*batch->mutable_write_session_meta())[key] = std::move(value);
}

inline i32 GetDataChunkCodec(const NKikimrPQClient::TDataChunk& proto) {
    if (proto.HasCodec()) {
        return proto.GetCodec() + 1;
    }
    return 0;
}

template<typename TReadResponse>
bool FillBatchedData(
        TReadResponse* data, const NKikimrClient::TCmdReadResult& res,
        const TPartitionId& Partition, ui64 ReadIdToResponse, ui64& ReadOffset, ui64& WTime, ui64 EndOffset,
        const NPersQueue::TTopicConverterPtr& topic) {
    constexpr EProtocol Protocol = std::is_same_v<TReadResponse, PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch> ? EProtocol::PQv1 : EProtocol::Topic;
    auto* partitionData = data->add_partition_data();

    if constexpr (Protocol == EProtocol::PQv1) {
        partitionData->mutable_topic()->set_path(topic->GetFederationPath());
        partitionData->set_cluster(topic->GetCluster());
        partitionData->set_partition(Partition.Partition);
        partitionData->set_deprecated_topic(topic->GetClientsideName());
        partitionData->mutable_cookie()->set_assign_id(Partition.AssignId);
        partitionData->mutable_cookie()->set_partition_cookie(ReadIdToResponse);

    } else {
        partitionData->set_partition_session_id(Partition.AssignId);
    }

    bool hasOffset = false;
    bool hasData = false;

    i32 batchCodec = 0; // UNSPECIFIED

    typename TReadResponse::Batch* currentBatch = nullptr;
    for (ui32 i = 0; i < res.ResultSize(); ++i) {
        const auto& r = res.GetResult(i);
        WTime = r.GetWriteTimestampMS();
        const ui64 messageCount = BatchedResultMessageCount(r.GetLogicalMessageCount());
        // When reading from the middle of a batch, tablet returns the whole blob
        // with base offset below ReadOffset; SDK skips already-committed records.
        AFL_ENSURE(BatchedResultCoversReadOffset(r.GetOffset(), r.GetLogicalMessageCount(), ReadOffset))
            ("partition", Partition)
            ("topic", topic->GetPrimaryPath())
            ("offset", r.GetOffset())
            ("message_count", messageCount)
            ("logical_message_count", r.GetLogicalMessageCount())
            ("read_offset", ReadOffset)
            ("result_index", i)
            ("end_offset", EndOffset);
        AdvanceReadOffsetFromBatchedResult(r.GetOffset(), r.GetLogicalMessageCount(), ReadOffset);
        hasOffset = true;

        auto proto(GetDeserializedData(r.GetData()));

        if (!proto.has_codec()) {
            proto.set_codec(NPersQueueCommon::RAW);
        }

        if (proto.GetChunkType() != NKikimrPQClient::TDataChunk::REGULAR) {
            continue; //TODO - no such chunks must be on prod
        }

        TString sourceId;
        if (!r.GetSourceId().empty()) {
            if (!NPQ::NSourceIdEncoding::IsValidEncoded(r.GetSourceId())) {
                YDB_LOG_ERROR("Read bad sourceId from offset seqNo sourceId",
                    {"partition", Partition},
                    {"offset", r.GetOffset()},
                    {"seqNo", r.GetSeqNo()},
                    {"sourceId", r.GetSourceId()});
            }
            sourceId = NPQ::NSourceIdEncoding::Decode(r.GetSourceId());
        }

        if (!currentBatch || GetBatchWriteTimestampMS(currentBatch) != static_cast<i64>(r.GetWriteTimestampMS()) ||
            GetBatchSourceId(currentBatch) != sourceId ||
            (Protocol == EProtocol::Topic && GetDataChunkCodec(proto) != batchCodec)) {
            // If write time and source id are the same, the rest fields will be the same too.
            currentBatch = partitionData->add_batches();
            i64 write_ts = static_cast<i64>(r.GetWriteTimestampMS());
            AFL_ENSURE(write_ts >= 0)
                ("partition", Partition)
                ("write_ts", write_ts)
                ("offset", r.GetOffset());
            SetBatchWriteTimestampMS(currentBatch, write_ts);
            SetBatchSourceId(currentBatch, std::move(sourceId));
            batchCodec = GetDataChunkCodec(proto);
            if constexpr (Protocol == EProtocol::Topic) {
                currentBatch->set_codec(batchCodec);
            }

            if (proto.HasMeta()) {
                const auto& header = proto.GetMeta();
                if (header.HasServer()) {
                    SetBatchExtraField(currentBatch, "server", header.GetServer());
                }
                if (header.HasFile()) {
                    SetBatchExtraField(currentBatch, "file", header.GetFile());
                }
                if (header.HasIdent()) {
                    SetBatchExtraField(currentBatch, "ident", header.GetIdent());
                }
                if (header.HasLogType()) {
                    SetBatchExtraField(currentBatch, "logtype", header.GetLogType());
                }
            }
            if (proto.HasExtraFields()) {
                const auto& map = proto.GetExtraFields();
                for (const auto& kv : map.GetItems()) {
                    SetBatchExtraField(currentBatch, kv.GetKey(), kv.GetValue());
                }
            }

            if (proto.HasIp() && IsUtf(proto.GetIp())) {
                if constexpr (Protocol == EProtocol::PQv1) {
                    currentBatch->set_ip(proto.GetIp());
                } else {
                    SetBatchExtraField(currentBatch, "_ip", proto.GetIp());
                }
            }
        }

        auto* message = currentBatch->add_message_data();

        message->set_seq_no(r.GetSeqNo());
        message->set_offset(r.GetOffset());
        message->set_data(proto.GetData());
        message->set_uncompressed_size(r.GetUncompressedSize());
        if constexpr (Protocol == EProtocol::PQv1) {
            message->set_create_timestamp_ms(r.GetCreateTimestampMS());

            message->set_explicit_hash(r.GetExplicitHash());
            message->set_partition_key(r.GetPartitionKey());

            if (proto.HasCodec()) {
                message->set_codec(NPQ::ToV1Codec((NPersQueueCommon::ECodec)proto.GetCodec()));
            }
        } else {
            *message->mutable_created_at() =
                ::google::protobuf::util::TimeUtil::MillisecondsToTimestamp(r.GetCreateTimestampMS());

            message->set_message_group_id(GetBatchSourceId(currentBatch));
            auto* msgMeta = message->mutable_metadata_items();
            *msgMeta = (proto.GetMessageMeta());
        }
        hasData = true;
    }

    const ui64 realReadOffset = res.HasRealReadOffset() ? res.GetRealReadOffset() : 0;

    if (!hasOffset) { //no data could be read from partition at offset ReadOffset - no data in partition at all???
        ReadOffset = Min(Max(ReadOffset + 1, realReadOffset + 1), EndOffset);
    }
    return hasData;
}

} // namespace NKikimr::NGRpcProxy::V1
