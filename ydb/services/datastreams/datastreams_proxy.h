#pragma once

#include "events.h"

#include <ydb/public/api/grpc/draft/ydb_datastreams_v1.pb.h>

#include <ydb/core/client/server/grpc_base.h>
#include <ydb/core/grpc_services/rpc_calls.h>

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>


namespace NKikimr {
namespace NGRpcService {

using TEvDataStreamsCreateStreamRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::CreateStreamRequest, Ydb::DataStreams::V1::CreateStreamResponse>;
using TEvDataStreamsDeleteStreamRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::DeleteStreamRequest, Ydb::DataStreams::V1::DeleteStreamResponse>;
using TEvDataStreamsDescribeStreamRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::DescribeStreamRequest, Ydb::DataStreams::V1::DescribeStreamResponse>;
using TEvDataStreamsRegisterStreamConsumerRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::RegisterStreamConsumerRequest, Ydb::DataStreams::V1::RegisterStreamConsumerResponse>;
using TEvDataStreamsDeregisterStreamConsumerRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::DeregisterStreamConsumerRequest, Ydb::DataStreams::V1::DeregisterStreamConsumerResponse>;
using TEvDataStreamsDescribeStreamConsumerRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::DescribeStreamConsumerRequest, Ydb::DataStreams::V1::DescribeStreamConsumerResponse>;
using TEvDataStreamsPutRecordRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::PutRecordRequest, Ydb::DataStreams::V1::PutRecordResponse>;
using TEvDataStreamsListStreamsRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::ListStreamsRequest, Ydb::DataStreams::V1::ListStreamsResponse>;
using TEvDataStreamsListShardsRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::ListShardsRequest, Ydb::DataStreams::V1::ListShardsResponse>;
using TEvDataStreamsPutRecordsRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::PutRecordsRequest, Ydb::DataStreams::V1::PutRecordsResponse>;
using TEvDataStreamsGetRecordsRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::GetRecordsRequest, Ydb::DataStreams::V1::GetRecordsResponse>;
using TEvDataStreamsGetShardIteratorRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::GetShardIteratorRequest, Ydb::DataStreams::V1::GetShardIteratorResponse>;
using TEvDataStreamsSubscribeToShardRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::SubscribeToShardRequest, Ydb::DataStreams::V1::SubscribeToShardResponse>;
using TEvDataStreamsDescribeLimitsRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::DescribeLimitsRequest, Ydb::DataStreams::V1::DescribeLimitsResponse>;
using TEvDataStreamsDescribeStreamSummaryRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::DescribeStreamSummaryRequest, Ydb::DataStreams::V1::DescribeStreamSummaryResponse>;
using TEvDataStreamsDecreaseStreamRetentionPeriodRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::DecreaseStreamRetentionPeriodRequest, Ydb::DataStreams::V1::DecreaseStreamRetentionPeriodResponse>;
using TEvDataStreamsIncreaseStreamRetentionPeriodRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::IncreaseStreamRetentionPeriodRequest, Ydb::DataStreams::V1::IncreaseStreamRetentionPeriodResponse>;
using TEvDataStreamsUpdateShardCountRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::UpdateShardCountRequest, Ydb::DataStreams::V1::UpdateShardCountResponse>;
using TEvDataStreamsUpdateStreamModeRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::UpdateStreamModeRequest, Ydb::DataStreams::V1::UpdateStreamModeResponse>;
using TEvDataStreamsUpdateStreamRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::UpdateStreamRequest, Ydb::DataStreams::V1::UpdateStreamResponse>;
using TEvDataStreamsSetWriteQuotaRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::SetWriteQuotaRequest, Ydb::DataStreams::V1::SetWriteQuotaResponse>;
using TEvDataStreamsListStreamConsumersRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::ListStreamConsumersRequest, Ydb::DataStreams::V1::ListStreamConsumersResponse>;
using TEvDataStreamsAddTagsToStreamRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::AddTagsToStreamRequest, Ydb::DataStreams::V1::AddTagsToStreamResponse>;
using TEvDataStreamsDisableEnhancedMonitoringRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::DisableEnhancedMonitoringRequest, Ydb::DataStreams::V1::DisableEnhancedMonitoringResponse>;
using TEvDataStreamsEnableEnhancedMonitoringRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::EnableEnhancedMonitoringRequest, Ydb::DataStreams::V1::EnableEnhancedMonitoringResponse>;
using TEvDataStreamsListTagsForStreamRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::ListTagsForStreamRequest, Ydb::DataStreams::V1::ListTagsForStreamResponse>;
using TEvDataStreamsMergeShardsRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::MergeShardsRequest, Ydb::DataStreams::V1::MergeShardsResponse>;
using TEvDataStreamsRemoveTagsFromStreamRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::RemoveTagsFromStreamRequest, Ydb::DataStreams::V1::RemoveTagsFromStreamResponse>;
using TEvDataStreamsSplitShardRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::SplitShardRequest, Ydb::DataStreams::V1::SplitShardResponse>;
using TEvDataStreamsStartStreamEncryptionRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::StartStreamEncryptionRequest, Ydb::DataStreams::V1::StartStreamEncryptionResponse>;
using TEvDataStreamsStopStreamEncryptionRequest = TGrpcRequestOperationCall<Ydb::DataStreams::V1::StopStreamEncryptionRequest, Ydb::DataStreams::V1::StopStreamEncryptionResponse>;

}
}
