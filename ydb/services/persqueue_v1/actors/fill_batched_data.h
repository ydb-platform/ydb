#pragma once

#include "partition_id.h"

#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>

namespace NKikimr::NGRpcProxy::V1 {

template <typename TReadResponse>
bool FillBatchedData(
        TReadResponse* data, const NKikimrClient::TCmdReadResult& res,
        const TPartitionId& Partition, ui64 ReadIdToResponse, ui64& ReadOffset, ui64& WTime, ui64 EndOffset,
        const NPersQueue::TTopicConverterPtr& topic);

} // namespace NKikimr::NGRpcProxy::V1
